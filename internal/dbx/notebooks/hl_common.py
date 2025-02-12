# This file has code that is shared across HiddenLayer notebooks.

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

# Tests
# Simple sanity checks can execute on every notebook run, but we don't want to run lots of tests every time.
# Instead, we'll run registered tests only on demand.
tests = {}

def register_test(test_name, test_func: callable) -> None:
    tests[test_name] = test_func

def run_tests() -> None:
    print(f"Running tests:")
    try:
      for test_name, test in tests.items():
          print(test_name)
          test()
    except Exception as e:
      print(f"Test failure: {e}")
      raise
    print("All tests passed.")


# Good for performance to create the MlflowClient just once.
# Avoid using a global variable, which makes testing harder.

from mlflow import MlflowClient

_mlflow_client = None   # private cache, for use only by this function
def mlflow_client() -> MlflowClient:
  """Get the MlflowClient singleton. Create it if necessary."""
  global _mlflow_client
  if not _mlflow_client:
    _mlflow_client = MlflowClient()
  return _mlflow_client

if __name__ == "__main__":
    # Unit test
    assert isinstance(mlflow_client(), MlflowClient)
    assert mlflow_client() is mlflow_client()   # should return the cached client


# Utility methods for model versions

from mlflow import MlflowClient
from mlflow.entities.model_registry import ModelVersion
from mlflow.exceptions import RestException
from typing import List

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

# Integration test
# For testing: function to get the first model version from the MLflow registry, 
# just so we have a ModelVersion to test with.

import mlflow
from mlflow.entities.model_registry import ModelVersion

def get_any_model_version() -> ModelVersion:
    """Get the first model version from the MLflow registry."""
    # Create an MLflow client
    client = mlflow_client()
    
    # Fetch all registered models
    registered_models = client.search_registered_models()
    if not registered_models:
        raise Exception("No models found in the MLflow registry.")
    
    # Iterate through registered models to find the first model version
    for model in registered_models:
        model_name = model.name
        model_versions = client.search_model_versions(f"name='{model_name}'", max_results=1)
        if model_versions:
            return model_versions[0]
    
    raise Exception("No model versions found in the MLflow registry.")

# Example usage
# model_version = get_any_model_version()
# print(f"Model Version: {model_version.version} from model '{model_version.name}'")

def test_get_model_version() -> None:
    client = mlflow_client()
    #Find a model version, any version should do. If there are none, that's legit but we can't test.
    try:
        mv_known = get_any_model_version()
        mv_test = get_model_version(mv_known.name, int(mv_known.version))
        assert mv_test is not None, "Retrieved model version was empty."
        assert mv_test.name == mv_known.name, "The name of the retrieved model version should match the known model version name."
        assert mv_test.version == mv_known.version, "The version of the retrieved model version should match the known model version."
    except ModelVersionError as e:
        print("Noting that no model versions were found in the Model Registry")
        pass

def test_get_bad_model_version() -> None:
    try:
        mv = get_model_version("fake_model", 1)
        raise Exception("Test test_get_bad_model_version failed, expected ModelVersionNotFound exception")
    except ModelVersionNotFound as e:
        pass

register_test("test_get_model_version", test_get_model_version)
register_test("test_get_bad_model_version", test_get_bad_model_version)

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

# Manual test - uncomment and run the code below. Tricky to automate because it has side effects on the registry.
# Could use mocking but that's verbose and not a good test.
# def get_test_mv():
#     return get_model_version("integrations_sandbox.default.demo_wine_quality", 1)
# clear_tags(get_test_mv())
# assert not get_test_mv().tags 
# set_model_version_tag(get_test_mv(), "k1", "v1")
# set_model_version_tag(get_test_mv(), "k2", "v2")
# assert get_test_mv().tags == {"k1": "v1", "k2": "v2"}
# clear_tags(get_test_mv(), ["k2"])
# assert get_test_mv().tags == {"k2": "v2"}
# clear_tags(get_test_mv())
# assert not get_test_mv().tags
