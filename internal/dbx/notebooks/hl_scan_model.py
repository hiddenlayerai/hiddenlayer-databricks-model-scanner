# Databricks notebook source
# This notebook scans a model version for risks, using the HiddenLayer (HL) SaaS Model Scanner.
# Record the outcome as tags in the MLflow Model Registry that is integrated into Unity Catalog.
# Python version: 3.11+
# Status of this notebook: alpha

# Job parameters:
# * full_model_name (string) - fully qualified name of the model to be scanned: <catalog>.<schema>.<model_name>
# * model_version_num (int) - MLflow version to be scanned
# * hl_api_key_name (string) - name of the HL API key, used to get credentials from the Databricks secrets store
# * hl_api_url (string) - Optional parameter to enable the scanner to use an Enterprise self-hosted model scanner

# Steps:
# Retrieve the job parameters
# Assert that the specified model version exists (assume that the caller validated the catalog, schema, and model names)
# Retrieve HL API key info from the Databricks secrets store.
# Authenticate to HL modscan.
# Add scan tags: hl_scan_status = pending, hl_scan_updated_at = <datetime>
# Send the model to be scanned as an aggregate (modscan v3 feature), synchronously.
#   Use the existing Python SDK until the updated one is ready.
#   Omit model version for now until we can specify it, or hack the SDK to pass it through
# On completion:
#   Update tags: hl_scan_status, hl_scan_id, hl_scan_updated_at, ...

# Define all the functions and constants up front, then do the real work after that.

# COMMAND ----------

# Install the HiddenLayer SDK if it's not there already.  Pin the version to avoid surprises.

import importlib
from IPython.display import display, Javascript

if not importlib.util.find_spec("hiddenlayer"):
    # same as "%pip install" but we can't do that within an if statement
    get_ipython().run_line_magic('pip', 'install hiddenlayer-sdk==1.2.2')
    # same as "%restart_python" but we can't do that within an if statement
    display(Javascript('Jupyter.notebook.kernel.restart()'))

# COMMAND ----------

# Import HL code that is shared across notebooks

from hl_common import *

# COMMAND ----------

class Configuration:
    """Configuration for this job"""
    full_model_name: str
    model_version_num: str
    hl_api_key_name: str
    hl_api_url: str
    hl_console_url: str
    def __init__(self, full_model_name, model_version_num, hl_api_key_name, hl_api_url, hl_console_url):
        self.full_model_name = full_model_name
        self.model_version_num = model_version_num
        self.hl_api_key_name = hl_api_key_name
        self.hl_api_url = hl_api_url
        self.hl_console_url = hl_console_url

# In production, parameters are passed in.
# For interactive debugging, set parameters here to whatever you need.
dev_full_model_name = "integrations_sandbox.default.sk-learn-random-forest"
dev_model_version_num = "1"
dev_hl_api_key_name = "hiddenlayer-key"

def get_job_params() -> Configuration:
    """Return full model name, version number (int), and HL API key name"""
    if not is_job_run():
        print(f"Running in interactive mode. Using dev parameters: " +
              f"{dev_full_model_name}, {dev_model_version_num}, {dev_hl_api_key_name} .")
        full_model_name = dev_full_model_name
        model_version_num = dev_model_version_num
        hl_api_key_name = dev_hl_api_key_name
    else:
        full_model_name = dbutils.widgets.get("full_model_name")
        assert full_model_name is not None, "full_model_name is a required job parameter"

        model_version_num = dbutils.widgets.get("model_version_num")
        assert model_version_num is not None, "model_version_num is a required job parameter"

        hl_api_key_name = dbutils.widgets.get("hl_api_key_name")
        assert hl_api_key_name is not None, "hl_api_key_name is a required job parameter"

        hl_api_url = dbutils.widgets.get("hl_api_url")
        assert hl_api_url is not None, "hl_api_url is a required job parameter"

        hl_console_url = dbutils.widgets.get("hl_console_url")
        assert hl_console_url is not None, "hl_console_url is a required job parameter"

    try:
        model_version_num = int(model_version_num)
    except ValueError:
        raise ValueError(f"model_version_num job parameter must be an integer, got '{model_version_num}'")

    return Configuration(full_model_name, model_version_num, hl_api_key_name, hl_api_url, hl_console_url)


# COMMAND ----------

from typing import Tuple

def parse_full_model_name(full_model_name: str) -> Tuple[str, str, str]:
  """Parse full model name into catalog, schema, and model name. Return those parts."""
  parts = full_model_name.split(".")
  assert len(parts) == 3, f"Invalid full model name {full_model_name}"  # if this happens, it's a programming error
  catalog_name = parts[0]
  schema_name = parts[1]
  model_name = parts[2]
  return catalog_name, schema_name, model_name

# Unit test
assert parse_full_model_name("catalog.schema.model") == ("catalog", "schema", "model")
try:
    parse_full_model_name("invalid_model_name")
except AssertionError as e:
    assert str(e) == "Invalid full model name invalid_model_name"
else:
    assert False, "Expected AssertionError not raised"

# COMMAND ----------

# We're about to submit the model version to the HL Model Scanner. Tag the model version accordingly.

from datetime import datetime
from mlflow.entities.model_registry import ModelVersion

def tag_for_scanning(model_version: ModelVersion) -> None:
  """Set the status and update time tags on the model version"""
  set_model_version_tag(model_version, HL_SCAN_STATUS, STATUS_PENDING)
  set_model_version_tag(model_version, HL_SCAN_UPDATED_AT, datetime.now().isoformat())

# Manual test - uncomment and run the code below.
# def get_test_mv():
#     return get_model_version("integrations_sandbox.default.sk-learn-random-forest", 1)
# clear_tags(get_test_mv())
# tag_for_scanning(get_test_mv())
# print(get_test_mv().tags)
# clear_tags(get_test_mv())

# COMMAND ----------

import sys
from mlflow.entities.model_registry import ModelVersion

def fail_and_exit_with_message(model_version: ModelVersion, message: str) -> None:
    # Erase all previous tags, except keep the run_id for debugging
    clear_tags(model_version, [HL_SCAN_RUN_ID])

    set_model_version_tag(model_version, HL_SCAN_STATUS, STATUS_FAILED)
    set_model_version_tag(model_version, HL_SCAN_MESSAGE, message)
    set_model_version_tag(model_version, HL_SCAN_UPDATED_AT, datetime.now().isoformat())

    # Raise an exception, rather than calling dbutils.notebook.exit(), so that the job will show as failed.
    raise Exception(f"Scanning model {model_version.name}, version {model_version.version} failed: {message}")

# COMMAND ----------

# Fetch and cache HiddenLayer API credentials

# Prerequisite: HiddenLayer credentials must be stored in the Databricks secrets store.
# The installer should take care of that.
# The secrets scope is "hl_scan.<catalog>.<schema>", allowing each schema to have its own credentials.

# For testing purposes, you can run these commands in a Linux shell to set up credentials for testing:
# databricks auth login --host <https URL for your Databricks cluster>
# databricks secrets create-scope yourscope
# databricks secrets put-secret yourscope <key_name> --string-value "<client_id>:<client_secret>"

from collections import defaultdict
from dataclasses import dataclass

def secrets_scope(catalog: str, schema: str) -> str:
    """Given the Unity Catalog catalog and schema, use that to create and return a secrets scope name
    that is unique within the workspace."""
    return f"hl_scan.{catalog}.{schema}"

@dataclass
class HLCredentials:
    client_id: str
    client_secret: str
    def __repr__(self):
        """Return a string representation of the credentials.
        Include only part of the client secret to avoid leaking it."""
        return f"HLCredentials(client_id={self.client_id}, client_secret={self.client_secret[0:4]}...)"

class BadHLCredentials(Exception):
    """Custom exception for bad HiddenLayer credentials."""
    def __init__(self, message):
        super().__init__(message)

_hl_api_creds = defaultdict(dict)       # Each entry is a scope dict
def get_hl_api_creds(catalog: str, schema: str, hl_api_key_name: str):
    """Return the credentials for the given catalog and schema. Cache them."""
    scope = secrets_scope(catalog, schema)
    global _hl_api_creds
    scope_dict = _hl_api_creds[scope]   # will be non-empty because of defaultdict
    creds: HLCredentials = scope_dict.get(hl_api_key_name)
    if not creds:
        secret = dbutils.secrets.get(scope, hl_api_key_name)
        if not secret:
            raise BadHLCredentials(f"No secret found for {hl_api_key_name} in scope {scope}")
        if not ":" in secret:
            raise BadHLCredentials(f"Invalid secret for {hl_api_key_name} in scope {scope}: must be a colon-separated client_id:client_secret string")
        client_id, client_secret = secret.split(":")
        creds = HLCredentials(client_id=client_id, client_secret=client_secret)
        scope_dict[hl_api_key_name] = creds
    return creds

# Manual test
# creds = get_hl_api_creds("integrations_sandbox", "default", "hiddenlayer-key")
# print(creds)  # only a few chars of the client secret will be printed out, so this is OK

# COMMAND ----------

# Scan the model folder using the HiddenLayer API

from hiddenlayer import HiddenlayerServiceClient
from hiddenlayer.sdk.models import ScanResults

def hl_auth(hl_creds: HLCredentials, hl_api_url: str) -> HiddenlayerServiceClient:
    """Return a HiddenlayerServiceClient authenticated with the given credentials."""
    hl_client = HiddenlayerServiceClient(
        host=hl_api_url,
        api_id=hl_creds.client_id,
        api_key=hl_creds.client_secret)
    return hl_client

def _reverse_full_model_name(full_model_name: str) -> str:
    """Reverse the order of the full model name, so that the model name goes first, ahead of schema and catalog.
    The model name is the most important info and we want that visible in the HL console UI."""
    parts = full_model_name.split(".")
    return f"{parts[2]}.{parts[1]}.{parts[0]}"

def hl_scan_folder(hl_client: HiddenlayerServiceClient,
                   full_model_name: str, model_version_num: int, local_dir: str) -> ScanResults:
    """Scan model artifacts in the local directory using the credentials. Return the scan results."""
    hl_model_name = _reverse_full_model_name(full_model_name)
    return hl_client.model_scanner.scan_folder(
        model_name=hl_model_name, model_version=model_version_num, path=local_dir)

# Manual test
# import tempfile
# full_model_name, model_version_num, hl_api_key_name = get_job_params()
# hl_creds = get_hl_api_creds("integrations_sandbox", "default", hl_api_key_name)
# hl_client = hl_auth(hl_creds)
# mv = get_model_version(full_model_name, model_version_num)
# run_id = mv.run_id
# temp_dir = tempfile.mkdtemp(prefix="hl_scan_test_", dir="/tmp")
# local_path = mlflow_client().download_artifacts(run_id, "", temp_dir)
# print(local_path)
# scan_results = hl_scan_folder(hl_creds, hl_client, full_model_name, model_version_num + 1, local_path)
# print(scan_results)

# COMMAND ----------

# After scanning, set model version tags in the registry

def tag_model_version_with_scan_results(model_version: ModelVersion, scan_results: ScanResults, hl_console_url: str):
    """Tag the model version in the MLflow model registry with the scan results."""
    clear_tags(model_version)   # erase any stale tags
    status = scan_results.status
    set_model_version_tag(model_version, HL_SCAN_STATUS, status)
    if status == "done":
        set_model_version_tag(model_version, HL_SCAN_THREAT_LEVEL, scan_results.severity)
        set_model_version_tag(model_version, HL_SCAN_UPDATED_AT, scan_results.end_time)
        set_model_version_tag(model_version, HL_SCAN_SCANNER_VERSION, scan_results.version)
        hl_scan_url = f"{hl_console_url}/model-details/{scan_results.model_id}/scans/{scan_results.scan_id}"
        set_model_version_tag(model_version, HL_SCAN_URL, hl_scan_url)

# COMMAND ----------

# *** MAIN CELL THAT DRIVES EVERYTHING ***
# Scan the model version and save results as model registry tags.

import tempfile
import mlflow

# Get job parameters - the model to scan (actually "model version", we'll be sloppy for the sake of brevity)
config = get_job_params()
print(f"Processing model: {config.full_model_name}, version {config.model_version_num}")

# Look up the model and get the info we need for scanning
mv = get_model_version(config.full_model_name, config.model_version_num)
run_id = mv.run_id
source = mv.source

# Now that we have the model, we can record status going forward by using tags on the model
# Errors above will cause the notebook to blow out and fail the job.
# That shouldn't happen. (However, noting that it's possible if unlikely that the model gets deleted before this job runs.)
if not run_id and not source:
    fail_and_exit_with_message(mv, "Model version has no run_id or source, so we can't scan it")

try:
    # Download model artifacts to a temporary location for scanning. Prefix the directory name to identify it as holding
    # scan data. Suffix the directory name for uniqueness and to link it to the model version.
    with tempfile.TemporaryDirectory(suffix=config.full_model_name, prefix="hl_scan_", dir="/tmp") as temp_dir:
        client = mlflow_client()
        if run_id:
            # See https://mlflow.org/docs/latest/python_api/mlflow.client.html?highlight=download_artifacts#mlflow.client.MlflowClient.download_artifacts
            local_path = client.download_artifacts(run_id=run_id, path="", dst_path=temp_dir)
        else:
            # See https://mlflow.org/docs/latest/python_api/mlflow.artifacts.html?highlight=download_artifacts#mlflow.artifacts.download_artifacts
            local_path = mlflow.artifacts.download_artifacts(artifact_uri=source, dst_path=temp_dir)
        #local_path="/tmp/hl_debug"     # for debugging
        catalog, schema, _ = parse_full_model_name(config.full_model_name)
        hl_creds = get_hl_api_creds(catalog, schema, config.hl_api_key_name)
        hl_client = hl_auth(hl_creds, config.hl_api_url)
        print(f"Scanning model artifacts in {local_path}")
        # For testing, bump the version number to simulate a new version: or delete the model card in the console UI
        #model_version_num += 2
        tag_for_scanning(mv)
        scan_results = hl_scan_folder(hl_client, config.full_model_name, config.model_version_num, local_path)
        tag_model_version_with_scan_results(mv, scan_results, config.hl_console_url)
except Exception as e:
    message = f"Unexpected error scanning model: {e}"
    if hasattr(e, 'status') and e.status == 400:
        # Bad HTTP request
        if e.body == '{"detail":"sensor with name/ version already exists"}':   # string matching here is brittle
            message = "A given model version can only be scanned once by HiddenLayer."
    fail_and_exit_with_message(mv, message)


# COMMAND ----------

# Run the registered tests manually, when desired.
#run_tests()
