package dbx

import (
	"context"
	"fmt"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/compute"
	"log"
	"strings"
)

// SchemaExists checks if the specified schema exists in the specified catalog in the Databricks Unity Catalog.
// Log a fatal error and exit if the Databricks call fails in an unexpected way.
func SchemaExists(dbxClient *databricks.WorkspaceClient, catalogName string, schemaName string) bool {
	schemaFullName := fmt.Sprintf("%s.%s", catalogName, schemaName)
	_, err := dbxClient.Schemas.GetByFullName(context.Background(), schemaFullName)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return false
		} else {
			log.Fatalf("Error fetching schema: %v", err)
		}
	}
	return true
}

// ClusterExists checks if the specified cluster exists in the Databricks workspace.
// Log a fatal error and exit if the Databricks call fails in an unexpected way.
func ClusterExists(dbxClient *databricks.WorkspaceClient, clusterID string) bool {
	_, err := dbxClient.Clusters.Get(context.Background(), compute.GetClusterRequest{ClusterId: clusterID})
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return false
		} else {
			log.Fatalf("Error fetching cluster: %v", err)
		}
	}
	return true
}
