# Databricks notebook source
# This HiddenLayer (HL) notebook polls for new models (that is, versions). On finding a new model, it triggers an HL scan job.
# Record the outcome as tags in the MLflow Model Registry that is integrated into Databricks (DBx) Unity Catalog (UC).
# Support only the models stored in that integrated model registry, not the deprecated "Workspace Model Registry".
# Run as a recurring scheduled job.
# Terminology: for brevity, we'll say "model" in the comments sometimes when we really mean "model version".
# Python version: 3.11+
# Status of this notebook: WIP

# Job parameters:
# * catalog (string) - name of catalog to monitor, within UC
# * schema (string) - name of schema to monitor, within the UC catalog
# * hl_api_key_name (string) - name of the HL API key, used to get credentials from the Databricks (DBx) secrets store

# Steps:
#
# One-Time Initialization:
#   To prepare for auto-scanning, mark all models in the schema with the tag hl_scan_status: unscanned.
#   (Rather than writing out some kind of init marker, we can just look for any HL tags. If we find any, then HL init
#   has been done. In the corner case of an empty schema, we'll just keep trying to init, that's OK.)
#
# Poll for New Model Versions and Trigger Scans
# On each periodic poll:
#   List all the latest models that are not marked as unscanned.
#   For each such model:
#     If we have reached the cap on # of active scan jobs, then stop. Otherwise:
#     Trigger an HL scan job, which calls modscan and updates tags (see below)
#     Set a job timeout (to ?)
#     Send args: full model name, model version num (int), HL API key name
#     Update the model’s hl_scan_status tag to pending
#
# Check the status of all jobs with hl_scan_status pending.
# If the job has failed, then retry? (TODO: specify retry logic, when to give up, exponential backoff, etc.)

# Developer Notes:
# As mentioned above, the MLflow model registry is embedded within Unity Catalog (UC).
# MLflow doesn't know anything about UC catalogs and schemas, nor does it support prefix search for models, so use
# the DBx SDK when we need to list models. On the flip side, the DBx SDK doesn't give us tags, we have to go to 
# MLflow to get that info.
# The MLflow documentation here says that you can use like/ilike operators with search_model_versions:
# https://mlflow.org/docs/latest/python_api/mlflow.client.html#mlflow.client.MlflowClient.search_model_versions
# but the DBx implementation doesn't support that.

# COMMAND ----------

# Import HL code that is shared across notebooks

from hl_common import *


# COMMAND ----------

# Constants

# STATUS_NONE indicates that the model version doesn't have an hl_scan_status tag.
# We could use None, but that's not a string, so we'll use an empty string.
# Give this string value a name to make it less confusing, or at least easier to track
STATUS_NONE = ""

# Name of the file that we create to mark that one-time initialization has been done.
INIT_MARKER_FILENAME = "hl_init_marker.txt"

# Name of the notebook to run to trigger HL scans.
HL_SCAN_NOTEBOOK="hl_scan_model"

# Timeout for HL scan jobs, including queuing. Make it very generous in case of system load.
# Also, model files are often big, so uploads can take a while.
HL_SCAN_NOTEBOOK_TIMEOUT_MINS=4800

# Maximum number of scan jobs that we'll allow to run at once.
# HL modscan has a queueing system so can handle receiving lots of jobs, but active jobs burn disk space
# and network bandwith.
MAX_ACTIVE_SCAN_JOBS = 10

# COMMAND ----------

class Configuration:
    """Configuration for this job"""
    catalog: str
    schema: str
    hl_api_key_name: str
    hl_api_url: str
    hl_console_url: str
    def __init__(self, catalog, schema, hl_api_key_name, hl_api_url, hl_console_url):
        self.catalog = catalog
        self.schema = schema
        self.hl_api_key_name = hl_api_key_name
        self.hl_api_url = hl_api_url
        self.hl_console_url = hl_console_url

def get_job_params() -> Configuration:
    """Return catalog, schema, and HL API key name"""
    if not is_job_run():
        print(f"Running in interactive mode. Using dev parameters: " +
              f"{dev_catalog}, {dev_schema}, {dev_hl_api_key_name} .")
        catalog = dev_catalog
        schema = dev_schema
        hl_api_key_name = dev_hl_api_key_name
    else:
        catalog = dbutils.widgets.get("catalog")
        assert catalog is not None, "catalog is a required job parameter"

        schema = dbutils.widgets.get("schema")
        assert schema is not None, "schema is a required job parameter"

        hl_api_key_name = dbutils.widgets.get("hl_api_key_name")
        assert hl_api_key_name is not None, "hl_api_key_name is a required job parameter"

        hl_api_url = dbutils.widgets.get("hl_api_url")
        assert hl_api_url is not None, "hl_api_url is a required job parameter"

        hl_console_url = dbutils.widgets.get("hl_console_url")
        assert hl_console_url is not None, "hl_console_url is a required job parameter"

    return Configuration(catalog, schema, hl_api_key_name, hl_api_url, hl_console_url)


# COMMAND ----------

from databricks.sdk import WorkspaceClient

_workspace_client = None   # private cache, for use only by this function
def workspace_client() -> WorkspaceClient:
  """Get the WorkspaceClient singleton. Create it if necessary.
  Depends on having Databricks credentials in ~.databrickscfg .
  See https://docs.databricks.com/en/dev-tools/cli/profiles.html .
  We'll be supporting oauth as another option in the future."""
  global _workspace_client
  if not _workspace_client:
    _workspace_client = WorkspaceClient()
  return _workspace_client

# COMMAND ----------

import os
from databricks.sdk.runtime import dbutils

def getcwd() -> str:
    """Get the current directory (location of this notebook) and return it."""
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get())
    cwd = os.path.dirname(notebook_path)    # parent directory
    return cwd


# COMMAND ----------

from collections import defaultdict
from typing import Dict, Iterator
from databricks.sdk.service.catalog import RegisteredModelInfo
from mlflow.entities.model_registry import ModelVersion

def get_model_versions_by_status(catalog: str, schema: str, statuses: List[str]) -> Dict[str, List[ModelVersion]]:
    """Return a dict of the latest model versions in the UC schema with the given HL statuses.
    If no statuses are given, then ignore the status value.
    Keys are statuses, values are lists of model versions with that status.
    The returned dict is a defaultdict(list) so you can always look up all statuses in the dict."""
    dikt: Dict[str, List[ModelVersion]] = defaultdict(list)
    models: Iterator[RegisteredModelInfo] = workspace_client().registered_models.list(catalog_name=catalog, schema_name=schema)
    client = mlflow_client()
    for model in models:
        # Get the latest version of each model from MLflow.
        # Note that ModelVersion includes a tags field, but search_model_versions doesn't fill it in, at least not with Unity Catalog.
        # Ideally we would get the most recent version in one step using the args order_by=["version DESC"], max_dikts=1.
        # But that's not supported in Unity Catalog, so we have to crawl through *all* of the versions.
        latest_version = None
        max_version = -1
        for version in client.search_model_versions(filter_string=f"name='{model.full_name}'"):
            if int(version.version) > max_version:
                max_version = int(version.version)
                latest_version = version
        if latest_version:
            mv = client.get_model_version(latest_version.name, latest_version.version)   # get the tags
            tags = mv.tags
            status = tags.get(HL_SCAN_STATUS, STATUS_NONE)
            if status in statuses or not statuses:
                dikt[status].append(mv)
    return dikt

# Manual testing
# print(get_model_versions_by_status("integrations_sandbox", "default", []))

# COMMAND ----------

# Utilities for managing HL initialization status

from databricks.sdk.errors.platform import ResourceDoesNotExist
from databricks.sdk.service.workspace import ImportFormat
import io
from pathlib import Path

def get_init_marker_path() -> Path:
    """Return the path to the init marker file in the HL workspace folder."""
    return Path(getcwd()) / INIT_MARKER_FILENAME

def mark_init_done() -> None:
    """Drop a file in the HL workspace folder as a persistent marker that we have done one-time initialization."""
    init_marker_path = str(get_init_marker_path())
    workspace_client().workspace.upload(
        init_marker_path,
        io.BytesIO(b'The existence of this file indicates that HiddenLayer has been initialized'),
        format=ImportFormat.AUTO)

def clear_init_done() -> None:
    """Remove the init marker file in the HL workspace folder. If it doesn't exist, that's OK."""
    try:
        workspace_client().workspace.delete(str(get_init_marker_path()))
    except ResourceDoesNotExist:
        pass

def is_init_done() -> bool:
    """Return true if HiddenLayer initialization has been done, false otherwise."""
    try:
        status = workspace_client().workspace.get_status(get_init_marker_path())
        return True     # if the call didn't blow up, then the file exists
    except ResourceDoesNotExist:
        return False
    except Exception as e:
        dbutils.notebook.exit(f"Unknown error occurred while checking HiddenLayer initialization status: {e}")

# Manual test
# mark_init_done()
# print(is_init_done())

# COMMAND ----------

def init(catalog: str, schema: str) -> None:
    """Do one-time state initialization by marking all untagged models in the UC catalog/schema as unscanned."""
    mv_dict: Dict[str, List[ModelVersion]] = get_model_versions_by_status(catalog, schema, [])
    versions = mv_dict[STATUS_NONE]
    for mv in versions:
        set_model_version_tag(mv, HL_SCAN_STATUS, STATUS_UNSCANNED)
        set_model_version_tag(mv, HL_SCAN_UPDATED_AT, datetime.now().isoformat())
    mark_init_done()

# Manual test

# Start clean
# clear_init_done()
# for status, versions in get_model_versions_by_status("integrations_sandbox", "default", []).items():
#     for mv in versions:
#         clear_tags(mv)  # wipe all tags in all model versions

# init("integrations_sandbox", "default")
# print(len(get_model_versions_by_status("integrations_sandbox", "default", [STATUS_UNSCANNED])[STATUS_UNSCANNED]))
# print(is_init_done())

# COMMAND ----------

def get_cluster_id() -> str:
    """Return the cluster ID for the current cluster. This is useful for running compute jobs."""
    return spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# Manual test
print(get_cluster_id())

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, RunNowResponse, Task,\
    JobSettings, RunLifeCycleState, RunResultState
import time
from typing import Dict
import uuid

def run_notebook(job_name: str, notebook_path: str, cluster_id: str,
                 parameters: Dict[str, str]=None, timeout_minutes: int=60) -> int:
    """
    Run a Databricks notebook. Don't wait for it to finish.
    
    Args:
        job_name (str): Name of the job running the notebook
        notebook_path (str): Path to the notebook in Databricks workspace
        cluster_id (str): Existing cluster ID to run the notebook
        parameters (Dict[str, str]): Notebook parameters
        timeout_minutes (int): Maximum time to wait for completion in minutes
        
    Returns:
        int: Run ID
    """
    work: WorkspaceClient = workspace_client()
    try:
        # Create the job, with a single task
        notebook_task = NotebookTask(notebook_path=notebook_path, base_parameters=parameters)
        task = Task(description=job_name,
                    existing_cluster_id=cluster_id,
                    notebook_task=notebook_task,
                    task_key=str(uuid.uuid4()),                 # task key must be unique
                    timeout_seconds=timeout_minutes * 60)
        job = work.jobs.create(name=job_name, tasks=[task])
        job_id = job.job_id
        
        # Run the job
        run: RunNowResponse = work.jobs.run_now(job_id=job_id)
        run_id = run.run_id
        return run_id

    except Exception as e:
        print(f"Error running notebook {job_name}: {str(e)}")
        raise

# Manual test
# run_id = run_notebook(
#     job_name="hl_scan_model",
#     notebook_path="/Workspace/Shared/HiddenLayer/0.0.0/hl_scan_model",
#     cluster_id=get_cluster_id(),
#     parameters={"full_model_name": "integrations_sandbox.default.sk-learn-random-forest",
#                 "model_version_num": "1",
#                 "hl_api_key_name": "hiddenlayer-key"},
#     timeout_minutes=30
# )

# COMMAND ----------

from datetime import datetime
from mlflow.entities.model_registry import ModelVersion
from pathlib import Path

def scan_model(mv: ModelVersion, hl_api_key_name: str, hl_api_url: str, hl_console_url: str, timeout_minutes: int) -> int:
    """Run a scan job on a model version. Don't wait for it to finish. Return the run_id."""
    job_name = f"hl_scan_{mv.name}.{mv.version}"
    notebook_path = Path(getcwd()) / HL_SCAN_NOTEBOOK
    cluster_id = get_cluster_id()
    # For a ModelVersion in Unity Catalog, the name is the full name, including catalog and schema
    parameters={"full_model_name": mv.name,
                "model_version_num": str(mv.version),
                "hl_api_key_name": hl_api_key_name,
                "hl_api_url": hl_api_url,
                "hl_console_url": hl_console_url}
    run_id = run_notebook(job_name, str(notebook_path), cluster_id, parameters, timeout_minutes=timeout_minutes)
    # For debugging purposes, save the run_id as a temporary tag
    set_model_version_tag(mv, HL_SCAN_RUN_ID, run_id)
    return run_id

# Manual test
# run_id = scan_model(ModelVersion.from_dictionary(
#                         {"name": "integrations_sandbox.default.sk-learn-random-forest",
#                          "version": 1,
#                          "creation_timestamp": int(datetime.now().timestamp() * 1000)}),
#                     "hiddenlayer-key",
#                     10)
# print(run_id)

# COMMAND ----------

from datetime import datetime

def handle_job_timeouts(pending_model_versions: List[ModelVersion], timeout_minutes: int) -> None:
    """For model versions in the pending state (scan job unfinished), mark them as failed if the jobs have expired.
    Model versions in the input list must have the tags field populated."""
    for mv in pending_model_versions:
        if mv.tags is None:
            print(f"Error: tags are missing for model version {mv.name} version {mv.version}. Skipping stale job management.")
            continue
        status = mv.tags.get(HL_SCAN_STATUS)
        if status is None or status != STATUS_PENDING:
            print(f"Error: model version {mv.name} version {mv.version} is not in pending state. Skipping stale job management.")
            continue
        updated_at_tag = mv.tags.get(HL_SCAN_UPDATED_AT)
        if updated_at_tag is None:
            print(f"Error: model version {mv.name} version {mv.version} is missing the {HL_SCAN_UPDATED_AT} tag. Skipping stale job management.")
            continue
        
        updated_at_dt = datetime.fromisoformat(updated_at_tag)
        updated_at = updated_at_dt.timestamp()      # Unix timestamp (seconds since January 1, 1970)
        now = datetime.now().timestamp()
        if now - updated_at > (timeout_minutes * 60):
            # Job timed out, mark it as failed.
            # Erase all previous tags, except keep the run_id for debugging
            clear_tags(mv, [HL_SCAN_RUN_ID])
            set_model_version_tag(mv, HL_SCAN_STATUS, STATUS_FAILED)
            set_model_version_tag(mv, HL_SCAN_MESSAGE, "Scan job timed out")
            set_model_version_tag(mv, HL_SCAN_UPDATED_AT, datetime.now().isoformat())

# Manual test
# def get_my_model_version() -> ModelVersion:
#     return mlflow_client().get_model_version("integrations_sandbox.default.sk-learn-random-forest", '1')
# mv = get_my_model_version()
# assert mv
# set_model_version_tag(mv, HL_SCAN_STATUS, STATUS_PENDING)
# set_model_version_tag(mv, HL_SCAN_UPDATED_AT, datetime.now().isoformat())
# mv = get_my_model_version()     # have to refresh the model version to get the updated tags

# # Job should not time out
# handle_job_timeouts([mv], 1)
# assert mlflow_client().get_model_version(mv.name, mv.version).tags[HL_SCAN_STATUS] == STATUS_PENDING  # should not have timed out

# # Set the timestamp so as to trigger a timeout
# timeout = 1     # minute
# old_timetamp: str = datetime.fromtimestamp(datetime.now().timestamp() - (timeout * 60)).isoformat()
# set_model_version_tag(mv, HL_SCAN_UPDATED_AT, old_timetamp)
# mv = get_my_model_version()
# handle_job_timeouts([mv], timeout)
# assert mlflow_client().get_model_version(mv.name, mv.version).tags[HL_SCAN_STATUS] == STATUS_FAILED  # should have timed out

# COMMAND ----------

# *** MAIN CELL THAT DRIVES EVERYTHING ***
# Poll for new model versions and scan as needed

config = get_job_params()
mv_dict: Dict[str, List[ModelVersion]] = get_model_versions_by_status(config.catalog, config.schema, [STATUS_NONE, STATUS_PENDING])

# Do one-time init if needed
if not is_init_done():
    init(config.catalog, config.schema)

# If there are no new model versions (untagged so STATUS_NONE), then we're done.
num_new_models = len(mv_dict[STATUS_NONE])
if num_new_models == 0:
    dbutils.notebook.exit("There are no new model versions to scan.")

# Light up scan jobs, up to the limit.
# Note: our client-side scan status goes directly from pending to done. There is an intermediate "running" state
# on the server side, but that's not exposed through the Python SDK, which we call synchronously. 
num_active_jobs = len(mv_dict[STATUS_PENDING])
max_new_jobs = max(MAX_ACTIVE_SCAN_JOBS - num_active_jobs, 0)
num_new_jobs = min(max_new_jobs, num_new_models)
for i in range(num_new_jobs):
    mv = mv_dict[STATUS_NONE][i]
    run_id = scan_model(mv, config.hl_api_key_name, config.hl_api_url, config.hl_console_url, HL_SCAN_NOTEBOOK_TIMEOUT_MINS)
    print(f"Scanning model {mv.name} version {mv.version}, job run_id is {run_id}")

# Mark timed-out jobs as failed.
handle_job_timeouts(mv_dict[STATUS_PENDING], HL_SCAN_NOTEBOOK_TIMEOUT_MINS)
