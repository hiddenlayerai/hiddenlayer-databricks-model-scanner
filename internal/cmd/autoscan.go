package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"syscall"

	"github.com/databricks/databricks-sdk-go"
	"github.com/hiddenlayer-engineering/hl-databricks/internal/dbx"
	"github.com/hiddenlayer-engineering/hl-databricks/internal/dbxapi"
	"github.com/hiddenlayer-engineering/hl-databricks/internal/hl"
	"github.com/hiddenlayer-engineering/hl-databricks/internal/utils"
	"github.com/spf13/cobra"
	"golang.org/x/term"
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
			config.DbxToken = inputStringValue("Databricks token", true, false)
		}
		var err error
		dbxClient, err = dbx.Auth(config.DbxHost, config.DbxToken)
		if err == nil {
			fmt.Println("Successfully authenticated to Databricks")
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
		config.Catalog = inputStringValue("Catalog in Databricks Unity Catalog", false)
		config.Schema = inputStringValue("Schema with models to scan, within the catalog", false)

		if config.Catalog == "" || config.Schema == "" {
			// intenional user exist
			return utils.CatalogSchemaConfig{}
		}

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
		clusterId := inputStringValue("Databricks cluster ID", false)
		if clusterId == "" {
			// intenional user exist
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

func configDbxResources(config *utils.Config, dbxClient *databricks.WorkspaceClient) {
	clusterId := retrieveClusterFromCommandLine(dbxClient)
	if clusterId == "" {
		// intentional user exit
		fmt.Println("No cluster to run monitoring job, exiting")
		return
	}
	// cluster will have been validated
	config.DbxClusterId = clusterId
	if len(config.DbxSchemas) == 0 {
		for {
			fmt.Println("Add a new schema to monitor, or press Enter to finish")
			schema := retrieveSchemaFromCommandLine(dbxClient)
			if schema == (utils.CatalogSchemaConfig{}) {
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
	config.DbxSchemas = validSchemas
}

func configHlCreds(config *utils.Config) {
	if config.HlApiUrl == "" {
		config.HlApiUrl = inputStringValue("HiddenLayer API URL (default: https://api.us.hiddenlayer.ai)", false, false, "https://api.us.hiddenlayer.ai")
	}
	hlApi, err := url.Parse(config.HlApiUrl)
	if err != nil {
		log.Fatalf("Error parsing HiddenLayer API URL: %v", err)
	}
	// determine if user is configuring for an enterprise scanner i.e. not a hiddenlayer.ai API url
	enterpriseScanner := !strings.HasSuffix(hlApi.Hostname(), ".hiddenlayer.ai")

	// Only need HL Api keys if using a Saas product
	if (config.HlApiKeyName == "" || config.HlClientID == "" || config.HlClientSecret == "") && !enterpriseScanner {
		config.HlApiKeyName = inputStringValue("HiddenLayer API key name", false, false)
		config.HlClientID = inputStringValue("HiddenLayer client ID", false, false)
		config.HlClientSecret = inputStringValue("HiddenLayer client secret", true, false)
	}

	// console url only needed if using a Saas product
	if config.HlConsoleUrl == "" && !enterpriseScanner {
		config.HlConsoleUrl = inputStringValue("HiddenLayer Console URL (default: https://console.us.hiddenlayer.ai", false, false, "https://console.us.hiddenlayer.ai")
	}

	// Validate the HiddenLayer credentials by authenticating to the HiddenLayer API (if Saas)
	if !enterpriseScanner {
		_, err := hl.Auth(config.HlClientID, config.HlClientSecret)
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
			_, err = fmt.Scanln(&value)
		}
		if err != nil {
			if err.Error() == "unexpected newline" && len(defaultValue) > 0 {
				return defaultValue[0]
			}
			if strings.TrimSpace(value) == "" && allowEmpty == true {
				fmt.Println("No input provided for optional parameter. Continuing...")
				return ""
			}
			fmt.Printf("Error reading %s: %v. Please try again.\n", name, err)
			continue
		}
		value = strings.TrimSpace(value) // Remove leading/trailing whitespace
		if value != "" {
			break
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
