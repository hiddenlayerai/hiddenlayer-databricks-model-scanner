package dbx

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/jobs"
	"github.com/databricks/databricks-sdk-go/service/workspace"
	"github.com/google/uuid"
	"github.com/hiddenlayer-engineering/hl-databricks/internal/utils"
)

// Constants
const modelMonitorNotebookName = "hl_monitor_models"

// Source files to upload to the Databricks workspace from this project
//
//go:embed notebooks/*.py
var sourceFiles embed.FS

func init() {
	// Set log output to stdout and include the date, time, and file name in log messages
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// Autoscan sets up automatic model scanning in Databricks, using the HiddenLayer Model Scanner.
func Autoscan(ctx context.Context, config *utils.Config) {
	// Sanity-check the configuration
	if config.DbxHost == "" || config.DbxToken == "" {
		log.Fatalf("Databricks host and token must be provided")
	}

	// Authenticate to Databricks
	dbx_client, err := Auth(config.DbxHost, config.DbxToken)
	if err != nil {
		log.Fatalf("Unable to authenticate to Databricks, got this error: %s", err.Error())
	}

	if !config.UsesEnterpriseModelScanner() {
		// Store the HiddenLayer credentials in the Databricks secret store for use by the Python notebooks
		// Only needed when using Saas
		storeHLCreds(ctx, dbx_client, config)
	}

	// Upload auto-scan Python files to the Databricks workspace
	uploadPythonFiles(dbx_client)

	// Run the monitor notebook periodically to detect and scan new model versions
	scheduleMonitorJob(ctx, dbx_client, config)

	fmt.Println("Finished setting up automated HiddenLayer model scanning")
}

// secretsScopeName returns the name of the Databricks secrets scope for HiddenLayer credentials.
// The name must be unique across Unity Catalog schemas within the workspace.
// This convention must match between the Go and Python code.
func secretsScopeName(catalog string, schema string) string {
	return fmt.Sprintf("hl_scan.%s.%s", catalog, schema)
}

// StoreHLCreds stores the HiddenLayer API key name, client ID, and client secret in the Databricks secret store.
// Use a secrets scope named "hl_<catalog_name>_<schema_name>" for uniqueness across Unity Catalog schemas.
func storeHLCreds(ctx context.Context, client *databricks.WorkspaceClient, config *utils.Config) {
	// Sanity-check the configuration
	if len(config.DbxSchemas) == 0 {
		log.Fatalf("Databricks catalogs and schemas must be provided")
	}
	// if using the Saas model scanner, ensure HL credentials are provided
	if !config.UsesEnterpriseModelScanner() && (config.HlClientID == "" || config.HlClientSecret == "") {
		log.Fatalf("HiddenLayer client ID and secret must be provided")
	}

	for _, schemaToMonitor := range config.DbxSchemas {
		// this is a redundant check, calling code should have confirmed this already. Never hurts to be sure
		if !config.UsesEnterpriseModelScanner() {
			// Create the scope if it doesn't already exist
			scopeName := secretsScopeName(schemaToMonitor.Catalog, schemaToMonitor.Schema)
			err := client.Secrets.CreateScope(ctx, workspace.CreateScope{Scope: scopeName})
			if err != nil {
				if !strings.Contains(err.Error(), "already exists") {
					log.Fatalf("Error creating secret scope %s: %s", scopeName, err.Error())
				}
			}
			// Create the secret. The key is the HL API key name, and the value is "<client ID>:<client secret>".
			// This convention must match between the Go and Python code.
			err = client.Secrets.PutSecret(ctx, workspace.PutSecret{
				Scope:       scopeName,
				Key:         config.HlApiKeyName,
				StringValue: fmt.Sprintf("%s:%s", config.HlClientID, config.HlClientSecret),
			})
			if err != nil {
				if !strings.Contains(err.Error(), "already exists") {
					log.Fatalf("Error creating secret %s in scope %s: %s", config.HlApiKeyName, scopeName, err.Error())
				}
			}

			// Double-check that the secret was created successfully
			secret, err := client.Secrets.GetSecret(ctx, workspace.GetSecretRequest{Key: config.HlApiKeyName, Scope: scopeName})
			if err != nil {
				log.Fatalf("Error fetching secret %s from scope %s: %s", config.HlApiKeyName, scopeName, err.Error())
			}
			decodedBytes, err := base64.StdEncoding.DecodeString(secret.Value)
			if err != nil {
				log.Fatalf("failed to decode secret: %s", err.Error())
			}
			decodedSecret := string(decodedBytes)
			if decodedSecret != fmt.Sprintf("%s:%s", config.HlClientID, config.HlClientSecret) {
				// For security, don't echo the secret in the error message
				log.Fatalf("Secret %s in scope %s has the wrong value", config.HlApiKeyName, scopeName)
			}
		}
	}
}

// getHLWorkspaceDirectory returns the path to the HiddenLayer workspace directory in the Databricks workspace.
func getHLWorkspaceDirectory() string {
	return fmt.Sprintf("/Shared/HiddenLayer/%s", utils.Version)
}

// Upload auto-scan Python files to the Databricks workspace
func uploadPythonFiles(client *databricks.WorkspaceClient) {
	entries, err := sourceFiles.ReadDir("notebooks")
	if err != nil {
		log.Fatal(err)
	}
	workspaceDir := getHLWorkspaceDirectory()

	// Create the workspace directory if it doesn't exist
	err = client.Workspace.Mkdirs(context.Background(), workspace.Mkdirs{
		Path: workspaceDir,
	})
	if err != nil {
		log.Fatalf("Error creating workspace directory %s: %v", workspaceDir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fmt.Printf("Uploading %s\n", entry.Name())
		source := fmt.Sprintf("notebooks/%s", entry.Name())
		// Upload the Python file.
		// When computing the destination path, do it Unix-style because this is a Databricks path, not a local path.
		uploadPythonFile(client, source, fmt.Sprintf("%s/%s", workspaceDir, entry.Name()))
	}
}

// uploadPythonFile uploads a Python file to the Databricks workspace
// Import files as notebooks, except for the common code, which is imported automatically as a script.
func uploadPythonFile(client *databricks.WorkspaceClient, source string, dest string) {
	// Read the Python file from the embedded filesystem
	content, err := sourceFiles.ReadFile(source)
	if err != nil {
		log.Fatalf("Error reading Python file: %v", err.Error())
	}

	// Import the file into the workspace.
	// Use ImportFormatAuto so that notebooks are imported as notebooks and scripts are imported as scripts.
	// ImportFormatSource causes all the files to be imported as notebooks.
	encodedContent := base64.StdEncoding.EncodeToString(content)
	importRequest := workspace.Import{
		Content:  encodedContent,
		Format:   workspace.ImportFormatAuto,
		Language: workspace.LanguagePython,
		Path:     dest,
	}
	err = client.Workspace.Import(context.Background(), importRequest)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			// If the file already exists, we can ignore the error
			fmt.Printf("File %s already exists in workspace, skipping upload\n", dest)
			return
		}
		log.Fatalf("Error importing Python file %s to workspace file %s: %v", source, dest, err)
	}
}

// Schedule the monitor job to run periodically. The monitor job finds new model versions and scans them.
func scheduleMonitorJob(ctx context.Context, client *databricks.WorkspaceClient, config *utils.Config) {
	// Get location of the monitor notebook
	workspaceDir := getHLWorkspaceDirectory()
	// This is a Unix-style path because it's a Databricks path, not a local path, so don't use filepath.Join
	notebookPath := fmt.Sprintf("%s/%s", workspaceDir, modelMonitorNotebookName)

	// Create a schedule for running the notebook.
	// If you change the schedule, update the job_name accordingly.
	schedule := jobs.CronSchedule{
		QuartzCronExpression: config.DbxPollingQuartzCron,
		//QuartzCronExpression: "0 * * * * ?", // Run every minute (useful for testing)
		TimezoneId: "UTC",
	}
	const job_name = "hl_find_new_model_versions"

	// Build the parameter list for the notebook job
	catalogAndSchemasParam, err := json.Marshal(config.DbxSchemas)
	if err != nil {
		log.Fatalf("Error marshalling catalog and schemas: %v", err)
	}
	params := []jobs.JobParameterDefinition{
		{Name: "schemas", Default: string(catalogAndSchemasParam)},
		{Name: "hl_api_key_name", Default: config.HlApiKeyName},
		{Name: "hl_api_url", Default: config.HlApiUrl},
		{Name: "hl_auth_url", Default: config.HlAuthUrl},
		{Name: "hl_console_url", Default: config.HlConsoleUrl},
	}

	// Create and schedule the notebook job
	notebookTask := jobs.NotebookTask{
		NotebookPath: notebookPath,
		BaseParameters: map[string]string{
			"MAX_ACTIVE_SCAN_JOBS": config.DbxMaxActiveScanJobs},
	}
	createJob := jobs.CreateJob{Name: job_name,
		Tasks: []jobs.Task{{
			Description:       "Poll for new model versions and scan them using HiddenLayer",
			ExistingClusterId: config.DbxClusterId,
			TaskKey:           uuid.New().String(),
			TimeoutSeconds:    0,
			NotebookTask:      &notebookTask,
		}},
		Parameters: params,
		Schedule:   &schedule,
	}
	if config.DbxRunAs != "" {
		createJob.RunAs = &jobs.JobRunAs{ServicePrincipalName: config.DbxRunAs}
	} else {
		fmt.Println("No run_as user provided, setting runner to the user who created the job")
	}

	job, err := client.Jobs.Create(ctx, createJob)
	if err != nil {
		log.Fatalf("Error scheduling model monitoring job: %v", err)
	}
	fmt.Printf("Scheduled monitoring job with ID: %d\n", job.JobId)
}
