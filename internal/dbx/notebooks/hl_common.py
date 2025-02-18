# This file has code that is shared across HiddenLayer notebooks.

from databricks.sdk.runtime import dbutils
import json
from mlflow import MlflowClient
from mlflow.entities.model_registry import ModelVersion
from mlflow.exceptions import RestException
from typing import List, Tuple

# Constants

# Scan status values (superset of HL Model Scanner values)
STATUS_UNSCANNED = "unscanned"
STATUS_PENDING = "pending"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_CANCELED = "canceled"
STATUS_SKIPPED = "skipped"

# MLflow model version status. We only care about "READY".
# See https://mlflow.org/docs/2.9.1/java_api/org/mlflow/api/proto/ModelRegistry.ModelVersionStatus.html
MODEL_VERSION_STATUS_READY = "READY"

# Tag names
HL_SCAN_STATUS="hl_scan_status"    # combines client-side and server-side status
HL_SCAN_THREAT_LEVEL="hl_scan_threat_level"
HL_SCAN_UPDATED_AT="hl_scan_updated_at"
HL_SCAN_SCANNER_VERSION="hl_scan_scanner_version"
HL_SCAN_URL="hl_scan_url"           # console URL for the scan
HL_SCAN_MESSAGE="hl_scan_message"   # use this tag to record an error message
HL_SCAN_RUN_ID="hl_scan_run_id"     # temporary tag to track the DBx scan job

# Custom exception classes

class ModelVersionError(Exception):
    """Base class for errors related to model versions in Unity Catalog."""
    def __init__(self, model_version: ModelVersion, message: str):
        self.model_version = model_version
        super().__init__(message)

class ModelVersionNotFound(ModelVersionError):
    """Exception for when a model version cannot be found in Unity Catalog."""
    def __init__(self, model_version: ModelVersion):
        message = f"Could not find model version '{model_version.version}' for model '{model_version.name}'"
        super().__init__(model_version, message)


# Good for performance to create the MlflowClient just once.
# Avoid using a global variable, which makes testing harder.

_mlflow_client = None   # private cache, for use only by this function
def mlflow_client() -> MlflowClient:
  """Get the MlflowClient singleton. Create it if necessary."""
  global _mlflow_client
  if not _mlflow_client:
    _mlflow_client = MlflowClient()
  return _mlflow_client

def is_job_run() -> bool:
    """Return true if this notebook is being run as a job, false otherwise."""
    try:
        # Fetch notebook context metadata
        context = dbutils.entry_point.getDbutils().notebook().getContext().toJson()
        context_json = json.loads(context)
        # Check for the job ID in the context. If it's there, then this is a job run.
        tags = context_json['tags']
        is_job_run = bool(tags and tags.get('jobId'))
        return is_job_run
    except Exception as e:
        # If context fetching fails, assume this is an interactive run
        print(f"Exception in is_job_run: {str(e)}")
        return False

def set_model_version_tag(model_version: ModelVersion, key: str, value: str) -> None:
    client = mlflow_client()
    client.set_model_version_tag(
        name=model_version.name,
        version=model_version.version,
        key=key,
        value=value)

def clear_tags(model_version: ModelVersion, keep_tags: List[str] = []) -> None:
    """Clear all tags on the model version, except for any tags in the optional keep_tags list."""
    client = mlflow_client()
    # Refresh the ModelVersion to ensure we have fresh data, otherwise this won't work
    mv = get_model_version(full_model_name=model_version.name, mv_num=model_version.version)
    tags = mv.tags.keys()
    
    # Delete each tag
    for tag_key in tags:
        if not tag_key in keep_tags:
            client.delete_model_version_tag(
                name=mv.name,
                version=mv.version,
                key=tag_key
            )

def get_model_version(
    full_model_name: str,
    mv_num: int
) -> ModelVersion:
    """
    Get the specified model version from the MLflow model registry.
    
    Args:
        full_model_name (str): Full name of the model, in the format <catalog>.<schema>.<model_name>
        mv_num (int): Version of the model to find

    Returns:
        ModelVersion: MLflow ModelVersion object
        
    Raises:
        ModelVersionNotFound: If the specified model version cannot be found
        ModelVersionError: If some other error happened
    """
    client: MlflowClient = mlflow_client()
    mv_num_str = str(mv_num)
    try:
        # Get and return the specific model version
        return client.get_model_version(
            name=full_model_name,
            version=mv_num_str
        )
        
    # If the specified model version doesn't exist, we should get an mlflow.exceptions.RestException
    # with a message like RESOURCE_DOES_NOT_EXIST: Routine or Model '<full_model_name>' does not exist.
    # Raise a specific ModelVersionNotFound exception if so, otherwise a generic ModelVersionError.
    except RestException as e:
        mv = ModelVersion(full_model_name, mv_num_str, 0, 0)
        if "RESOURCE_DOES_NOT_EXIST" in str(e):
            raise ModelVersionNotFound(mv) from e
        else:
            raise ModelVersionError(mv, f"Failed to get model version {str(mv)}: {str(e)}") from e
