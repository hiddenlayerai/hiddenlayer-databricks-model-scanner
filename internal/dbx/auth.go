package dbx

import (
	"context"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/compute"
)

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

	// Check that authentication worked, by listing clusters in the workspace
	_, err = dbxClient.Clusters.ListAll(context.Background(), compute.ListClustersRequest{})
	if err != nil {
		return nil, err
	}

	return dbxClient, nil
}

// DefaultAuth returns a new WorkspaceClient using the default host and token read from ~/.databrickscfg.
func DefaultAuth() (*databricks.WorkspaceClient, error) {
	dbxClient, err := databricks.NewWorkspaceClient()
	if err != nil {
		return nil, err
	}
	return dbxClient, nil
}
