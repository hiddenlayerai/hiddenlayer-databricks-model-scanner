package dbx

import (
	"context"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/compute"
)

func AuthFromFile(configFile, profile string) (*databricks.WorkspaceClient, error) {
	config := &databricks.Config{
		Profile:    profile,
		ConfigFile: configFile,
	}

	dbxClient, err := databricks.NewWorkspaceClient(config)
	if err != nil {
		return nil, err
	}

	err = checkAuth(dbxClient)
	if err != nil {
		return nil, err
	}

	return dbxClient, nil
}

// Auth returns a new WorkspaceClient using the provided host and token.
// Check that the client is authenticated by listing clusters in the workspace.
func Auth(dbxHost string, dbxToken string) (*databricks.WorkspaceClient, error) {
	config := &databricks.Config{
		Host:  dbxHost,
		Token: dbxToken,
	}
	dbxClient, err := databricks.NewWorkspaceClient(config)
	if err != nil {
		return nil, err
	}

	err = checkAuth(dbxClient)
	if err != nil {
		return nil, err
	}

	return dbxClient, nil
}

func checkAuth(dbxClient *databricks.WorkspaceClient) error {
	// Check that authentication worked, by listing clusters in the workspace
	_, err := dbxClient.Clusters.ListAll(context.Background(), compute.ListClustersRequest{})
	return err
}

// DefaultAuth returns a new WorkspaceClient using the default host and token read from ~/.databrickscfg.
func DefaultAuth() (*databricks.WorkspaceClient, error) {
	return AuthFromFile("", "")
}
