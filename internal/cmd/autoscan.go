package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"syscall"

	"github.com/databricks/databricks-sdk-go"
	"github.com/hiddenlayer-engineering/hl-databricks/internal/dbx"
	"github.com/hiddenlayer-engineering/hl-databricks/internal/dbxapi"
	"github.com/hiddenlayer-engineering/hl-databricks/internal/hl"
	"github.com/hiddenlayer-engineering/hl-databricks/internal/utils"
	"github.com/reugn/go-quartz/quartz"
	"github.com/spf13/cobra"
	"golang.org/x/term"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var autoscanCmd = &cobra.Command{
	Use:   "autoscan",
	Short: "Sets up automated model scanning in Databricks",
	Long:  "Sets up automated model scanning in DataBricks, using the HiddenLayer Model Scanner.",
	Run: func(cmd *cobra.Command, args []string) {
		config := readConfig() // Read the configuration file, if it exists
		// Get Databricks credentials from the user, if needed (not already in the config)
		dbxClient := configDbxCreds(config)
		configDbxResources(config, dbxClient) // Get Databricks resources from the user, if needed
		configHlCreds(config)                 // Get HiddenLayer credentials from the user, if needed
		dbx.Autoscan(context.Background(), config)
	},
}

func init() {
	rootCmd.AddCommand(autoscanCmd)
}

func GetOAuthToken(dbxhost string) string {
	usersHomeDir, err := os.UserHomeDir()
	if err != nil {
		fmt.Println("Error getting user home directory")
		usersHomeDir = ""
	}
	usersDatabrickTokenCache := usersHomeDir + "/.databricks/token-cache.json"
	tokenCachePath := inputStringValue("Please enter the full path to your Databricks token cache (default: ~/.databricks/token-cache.json)", false, true, usersDatabrickTokenCache)
	token := GetOAuthTokenFromFile(tokenCachePath, dbxhost)
	return token
}

func GetOAuthTokenFromFile(path string, dbxHost string) string {
	tokenCache, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("Error reading token-cache.json")
		return ""
	}

	var tokenCacheMap map[string]interface{}
	err = json.Unmarshal(tokenCache, &tokenCacheMap)
	if err != nil {
		fmt.Println("Error parsing token-cache.json")
		return ""
	}

	//get the token at [tokens][dbxhost][access_token]
	if tokenCacheMap["tokens"] != nil {
		tokens := tokenCacheMap["tokens"].(map[string]interface{})
		if tokens[dbxHost] != nil {
			token := tokens[dbxHost].(map[string]interface{})
			if token["access_token"] != nil {
				return token["access_token"].(string)
			}
		}
	}
	return ""
}

// configDbxCreds checks if the Databricks credentials were read from the configuration file.
// If not, then get them from the user and write them into the in-memory config.
func configDbxCreds(config *utils.Config) *databricks.WorkspaceClient {
	var dbxClient *databricks.WorkspaceClient

	// If the Databricks host and token are not in the configuration file, get them from the user.
	// Check that we can authenticate successfully. If not, get new credentials from the user.
	// Keep going until authentication works.
	for {
		if config.DbxHost == "" || config.DbxToken == "" {
			config.DbxHost = inputDbxHost()
			if config.DbxHost != "" {
				config.DbxToken = GetOAuthToken(config.DbxHost)

				if config.DbxToken == "" {
					fmt.Println("No OAuth Token found falling back to PAT")
					config.DbxToken = inputStringValue("Please enter Databricks personal token or sign in with Databrick's CLI and try again", true, false)
				} else {
					fmt.Println("Using OAuth Token from file")
				}
			}
		}
		// check if token passed in is a file
		if stats, err := os.Stat(config.DbxToken); err == nil && !stats.IsDir() {
			token := GetOAuthTokenFromFile(config.DbxToken, config.DbxHost)
			if token != "" {
				fmt.Println("Using OAuth Token from file")
				config.DbxToken = token
			} else {
				fmt.Println("No OAuth Token found falling back to PAT")
				config.DbxToken = inputStringValue("Please enter Databricks personal token or sign in with Databrick's CLI and try again", true, false)
			}
		}
		if config.DbxHost == "" || config.DbxToken == "" {
			// indicate host and token are required
			fmt.Println("Databricks host and token are required. Please try again.")
			config.DbxHost = ""
			config.DbxToken = ""
			continue
		}
		var err error
		dbxClient, err = dbx.Auth(config.DbxHost, config.DbxToken)
		if err == nil {
			fmt.Println("Successfully authenticated to Databricks at " + config.DbxHost)
			break
		} else {
			fmt.Printf("Error authenticating to Databricks: %v. Please try again.\n", err)
			config.DbxHost = ""
			config.DbxToken = ""
		}
	}

	return dbxClient
}

func retrieveSchemaFromCommandLine(dbxClient *databricks.WorkspaceClient) utils.CatalogSchemaConfig {
	for {
		var config utils.CatalogSchemaConfig
		config.Catalog = inputStringValue("Catalog in Databricks Unity Catalog", false, false)
		if config.Catalog == "" {
			// intentional user exit
			return utils.CatalogSchemaConfig{}
		}
		config.Schema = inputStringValue("Schema with models to scan, within the catalog", false, false)

		configOk := confirmSchema(config, dbxClient)
		if configOk {
			return config
		} else {
			continue
		}
	}
}

func retrieveClusterFromCommandLine(dbxClient *databricks.WorkspaceClient) string {
	for {
		clusterId := inputStringValue("Databricks cluster ID", false, false)
		if clusterId == "" {
			// intentional user exit
			return ""
		}

		clusterOk := confirmCluster(clusterId, dbxClient)
		if clusterOk {
			return clusterId
		} else {
			continue
		}
	}
}

func confirmSchema(config utils.CatalogSchemaConfig, dbxClient *databricks.WorkspaceClient) bool {
	if schemaExists := dbx.SchemaExists(dbxClient, config.Catalog, config.Schema); schemaExists {
		fmt.Printf("Confirming schema '%s' in catalog '%s' found in Unity Catalog\n", config.Schema, config.Catalog)
		return true
	} else {
		fmt.Printf("Schema %s in catalog %s not found in Unity Catalog. Please try again.\n", config.Schema, config.Catalog)
		return false
	}
}

func confirmCluster(clusterId string, dbxClient *databricks.WorkspaceClient) bool {
	if clusterExists := dbx.ClusterExists(dbxClient, clusterId); clusterExists {
		fmt.Printf("Confirming cluster with ID=%s found in Databricks\n", clusterId)
		return true
	} else {
		fmt.Printf("Cluster %s not found in Databricks. Please try again.\n", clusterId)
		return false
	}
}

func validateCronExpression(expression string) error {
	// Try to parse the expression
	_, err := quartz.NewCronTrigger(expression)
	if err != nil {
		return fmt.Errorf("invalid quartz cron expression: %v", err)
	}

	return nil
}

func configDbxResources(config *utils.Config, dbxClient *databricks.WorkspaceClient) {
	for {
		if config.DbxClusterId == "" {
			clusterId := retrieveClusterFromCommandLine(dbxClient)
			if clusterId == "" {
				// intentional user exit
				log.Fatal("No cluster to run monitoring job, exiting")
			}
			config.DbxClusterId = clusterId
		} else {
			if !confirmCluster(config.DbxClusterId, dbxClient) {
				fmt.Println("Cluster not found in Databricks, please provide a valid cluster ID")
				config.DbxClusterId = ""
				continue
			}
		}
		// cluster will have been validated

		// Get the Databricks service principal to run the job as.
		// This is optional, so only prompt if it's not already in the configuration.
		if config.DbxRunAs == "" {
			config.DbxRunAs = inputStringValue("Service principal application ID to run the job as (optional)", false, true)
			// Check that the service principal exists in Databricks. If not, keep asking until it does or a blank value is entered.
			for config.DbxRunAs != "" {
				fmt.Println("Checking service principal in Databricks..." + config.DbxRunAs)
				if servicePrincipalExists := dbxapi.ServicePrincipalExists(config.DbxRunAs, config.DbxHost, config.DbxToken); servicePrincipalExists {
					fmt.Printf("Confirming service principal '%s' found in Databricks\n", config.DbxRunAs)
					break
				} else {
					fmt.Printf("Service principal %s not found in Databricks. Please try again.\n", config.DbxRunAs)
					config.DbxRunAs = inputStringValue("Service principal to run the job as (optional)", false, true)
				}
			}
		} else {
			if !dbxapi.ServicePrincipalExists(config.DbxRunAs, config.DbxHost, config.DbxToken) {
				fmt.Printf("Service principal %s not found in Databricks. Please try again.\n", config.DbxRunAs)
				config.DbxRunAs = ""
				continue
			} else {
				fmt.Printf("Confirming service principal '%s' found in Databricks\n", config.DbxRunAs)
			}
		}

		for config.DbxMaxActiveScanJobs == "" {
			config.DbxMaxActiveScanJobs = inputStringValue("Please enter the Max Number of concurrent scan jobs (default: 10)", false, true, "10")
		}

		for config.DbxPollingQuartzCron == "" {
			fmt.Println("Quartz Expression format: https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html")
			config.DbxPollingQuartzCron = inputStringValue("desired polling interval for the scan job in quartz cron format (default: 0 0 */12 * * ? which is 12hrs)", false, true, "0 0 */12 * * ?")
			err := validateCronExpression(config.DbxPollingQuartzCron)
			if err != nil {
				fmt.Printf("Error validating cron expression, please try again: %v\n", err)
				config.DbxPollingQuartzCron = ""
			}
		}

		if len(config.DbxSchemas) == 0 {
			for {
				fmt.Println("Add a new schema to monitor, or press Enter to finish")
				schema := retrieveSchemaFromCommandLine(dbxClient)
				if schema == (utils.CatalogSchemaConfig{}) {
					if len(config.DbxSchemas) == 0 {
						log.Fatal("No schemas to monitor, exiting")
					}
					// intentional user exit
					break
				}
				// schema will have been validated
				config.DbxSchemas = append(config.DbxSchemas, schema)
			}
			return
		}

		var validSchemas []utils.CatalogSchemaConfig
		for _, schema := range config.DbxSchemas {
			if !confirmSchema(schema, dbxClient) {
				// Message indicating what the issue will have been printed already, just ask for updated config
				replacementConfig := retrieveSchemaFromCommandLine(dbxClient)
				if replacementConfig == (utils.CatalogSchemaConfig{}) {
					// user wants to skip this schema, remove it
					continue
				} else {
					// replace existing (bad) schema config with new (validated) one
					validSchemas = append(validSchemas, replacementConfig)
				}
			} else {
				validSchemas = append(validSchemas, schema)
			}
		}
		if len(validSchemas) == 0 {
			log.Fatal("No schemas to monitor, exiting")
		}
		config.DbxSchemas = validSchemas
		return
	}
}

var regions = []string{"US", "EU", "CUSTOM"}
var databricksSecretNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,128}$`)

func retrieveHLApiUrl() (string, string, string, error) {
	region := ""
	for {
		region = inputStringValue("Region of HiddenLayer API US/EU/CUSTOM (default: US)", false, false, "US")
		region = cases.Upper(language.English).String(region)
		if slices.Contains(regions, region) {
			break
		} else {
			fmt.Println("Invalid region. Please try again.")
		}
	}
	switch region {
	case "US":
		return "https://api.us.hiddenlayer.ai", "https://auth.hiddenlayer.ai", "https://console.us.hiddenlayer.ai", nil
	case "EU":
		return "https://api.eu.hiddenlayer.ai", "https://auth.eu.hiddenlayer.ai", "https://console.eu.hiddenlayer.ai", nil
	case "CUSTOM":
		apiUrl := inputStringValue("HiddenLayer API URL (default: https://api.us.hiddenlayer.ai)", false, false, "https://api.us.hiddenlayer.ai")
		authUrl := inputStringValue("HiddenLayer Auth URL (default: https://auth.hiddenlayer.ai)", false, false, "https://auth.hiddenlayer.ai")
		consoleUrl := inputStringValue("HiddenLayer Console URL (default: https://console.us.hiddenlayer.ai)", false, false, "https://console.us.hiddenlayer.ai")
		return apiUrl, authUrl, consoleUrl, nil
	default:
		return "", "", "", fmt.Errorf("invalid region: %s", region)
	}
}

func configHlCreds(config *utils.Config) {
	if config.HlApiUrl == "" {
		apiUrl, authUrl, consoleUrl, err := retrieveHLApiUrl()
		if err != nil {
			log.Fatalf("Error retrieving HiddenLayer API URL: %v", err)
		}
		config.HlApiUrl = apiUrl
		config.HlAuthUrl = authUrl
		config.HlConsoleUrl = consoleUrl
	}
	hlApi, err := url.Parse(config.HlApiUrl)
	if err != nil {
		log.Fatalf("Error parsing HiddenLayer API URL: %v", err)
	}
	// determine if user is configuring for an enterprise scanner i.e. not a hiddenlayer.ai API url
	enterpriseScanner := !strings.HasSuffix(hlApi.Hostname(), ".hiddenlayer.ai")

	// Only need HL Api keys if using a Saas product
	if (config.HlApiKeyName == "" || config.HlClientID == "" || config.HlClientSecret == "") && !enterpriseScanner {
		config.HlClientID = inputStringValue("HiddenLayer client ID", false, false)
		config.HlClientSecret = inputStringValue("HiddenLayer client secret", true, false)
		for {
			config.HlApiKeyName = inputStringValue("Name of Databricks Secret to create (to store HiddenLayer API Credentials)", false, false)
			if config.HlApiKeyName == "" {
				fmt.Println("No Secret key name provided. Please try again.")
				continue
			}
			if len(config.HlApiKeyName) > 128 {
				fmt.Println("Secret key name must be less than 128 characters. Please try again.")
				continue
			}
			if !databricksSecretNameRegex.MatchString(config.HlApiKeyName) {
				fmt.Println("Secret key name must contain only letters, numbers, underscores, and periods. Please try again.")
				continue
			}
			break
		}
	}

	// console url only needed if using a Saas product
	if config.HlConsoleUrl == "" && !enterpriseScanner {
		config.HlConsoleUrl = inputStringValue("HiddenLayer Console URL (default: https://console.us.hiddenlayer.ai", false, false, "https://console.us.hiddenlayer.ai")
	}

	// Validate the HiddenLayer credentials by authenticating to the HiddenLayer API (if Saas)
	if !enterpriseScanner {
		_, err := hl.Auth(config.HlAuthUrl, config.HlClientID, config.HlClientSecret)
		if err == nil {
			fmt.Println("Successfully authenticated to HiddenLayer")
		} else {
			log.Fatalf("Error authenticating to HiddenLayer: %v", err)
		}
	}
}

// inputStringValue prompts the user to enter a string value for a given name.
// If hideIt is true, the input will not be echoed to the terminal.
func inputStringValue(name string, hideIt bool, allowEmpty bool, defaultValue ...string) string {
	var value string
	for {
		var prompt string
		if hideIt {
			prompt = fmt.Sprintf("Enter %s [will be hidden for security]: ", name)
		} else {
			prompt = fmt.Sprintf("Enter %s: ", name)
		}
		fmt.Print(prompt)
		var err error
		if hideIt {
			value, err = readPassword()
		} else {
			value, err = bufio.NewReader(os.Stdin).ReadString('\n')
		}
		if err != nil {
			fmt.Printf("Error reading %s: %v. Please try again.\n", name, err)
			continue
		}
		value = strings.TrimSpace(value) // Remove leading/trailing whitespace
		if value != "" {
			break
		} else {
			if len(defaultValue) > 0 {
				return defaultValue[0]
			}
			if allowEmpty {
				fmt.Println("No input provided for optional parameter. Continuing...")
			}
			return ""
		}
	}
	return value
}

func inputDbxHost() string {
	var dbxHost string
	for {
		fmt.Print("Enter Databricks workspace URL [e.g., https://adb-1234567890123456.7.azuredatabricks.net]: ")
		_, err := fmt.Scanln(&dbxHost)
		if err != nil {
			fmt.Printf("Error reading Databricks workspace URL: %v. Please try again.\n", err)
			continue
		}
		if !strings.HasPrefix(dbxHost, "https://") {
			fmt.Println("Databricks workspace URL must start with 'https://'. Please try again.")
			continue
		}
		dbxHost = strings.TrimSuffix(dbxHost, "/") // Remove trailing slash if present
		if !strings.HasSuffix(dbxHost, "azuredatabricks.net") && !strings.HasSuffix(dbxHost, "databricks.com") {
			fmt.Println("Databricks workspace URL must end with 'azuredatabricks.net' or 'databricks.com'. Please try again.")
			continue
		}
		dbxHost = strings.TrimSpace(dbxHost)
		if dbxHost != "" {
			break
		}

	}
	return dbxHost
}

// readConfig reads the configuration file and returns a Config object.
// If the configuration file is not found, that's OK, return an empty Config.
// If the configuration file is found but invalid, print an error and exit.
func readConfig() *utils.Config {
	config, err := utils.InitConfig()
	if err != nil {
		var configNotFound *utils.ConfigNotFound
		// The config file is optional so OK if it's missing
		if errors.As(err, &configNotFound) {
			config = &utils.Config{} // Return an empty Config
		} else {
			fmt.Printf("Error reading the configuration file: %v\n", err)
			os.Exit(1)
		}
	}

	return config
}

// readPassword reads a password from stdin without echoing it to the terminal.
// It returns the password as a string.
func readPassword() (string, error) {
	// Get the file descriptor for stdin
	fd := int(syscall.Stdin)

	// Read password without echo
	password, err := term.ReadPassword(fd)
	if err != nil {
		return "", err
	}

	// Print a newline since ReadPassword doesn't do it
	fmt.Println()

	return string(password), nil
}
