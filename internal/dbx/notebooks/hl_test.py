# Tests
# Simple sanity checks can execute on every notebook run, but we don't want to run lots of tests every time.
# Instead, we'll run registered tests only on demand.

from hl_common import *

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


if __name__ == "__main__":
    # Unit test
    assert isinstance(mlflow_client(), MlflowClient)
    assert mlflow_client() is mlflow_client()   # should return the cached client

# Integration test
# For testing: function to get the first model version from the MLflow registry, 
# just so we have a ModelVersion to test with.

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

# Tests

def check_model_version_is_ready_for_scanning_happy_path() -> None:
  # Happy path test
  test_mv = ModelVersion(
      name="dummy model version",
      version="1",
      creation_timestamp=0,
      last_updated_timestamp=1)
  try:
      check_model_version_is_ready_for_scanning(test_mv)
  except Exception as e:
      raise Exception(f"Test of {check_model_version_is_ready_for_scanning} failed: {str(e)}")

def check_model_version_is_ready_for_scanning_bad_registry_status() -> None:
  # Test bad registry status
  test_mv = ModelVersion(
      name="dummy_model_version",
      version="1",
      creation_timestamp=0,
      last_updated_timestamp=1,
      status="model not ready")  # not a real status, but good enough for testing
  try:
      check_model_version_is_ready_for_scanning(test_mv)
      raise Exception("Test of check_model_version_is_ready_for_scanning failed, expected ModelVersionNotRegistered exception")
  except ModelVersionNotRegistered as e:
      pass

def check_model_version_is_ready_for_scanning_bad_scan_status() -> None:
# Test bad scan status
  from dataclasses import dataclass
  from typing import Dict

  @dataclass
  class DummyModelVersion:
      name: str
      version: str
      tags: Dict[str, str]

  test_mv = DummyModelVersion("dummy_model_version", "1", tags={HL_SCAN_STATUS: STATUS_DONE})
  try:
    check_model_version_is_ready_for_scanning(test_mv)
    raise Exception("Test of check_model_version_is_ready_for_scanning failed, expected ModelVersionAlreadyScanned exception")
  except ModelVersionAlreadyScanned as e:
      pass

register_test("check_model_version_is_ready_for_scanning_happy_path", check_model_version_is_ready_for_scanning_happy_path)
register_test("check_model_version_is_ready_for_scanning_bad_registry_status", check_model_version_is_ready_for_scanning_bad_registry_status)
register_test("check_model_version_is_ready_for_scanning_bad_scan_status", check_model_version_is_ready_for_scanning_bad_scan_status)

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
