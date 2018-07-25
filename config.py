import os

# Path configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TESTS_DIR = os.path.join(BASE_DIR, "tests")
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
DATA_DIR = os.path.join(BASE_DIR, "data")

BUNDLE_UUIDS_PATH = os.path.join(TESTS_DIR, "bundle_uuids.json")
GET_LAMBDA_DURATION_SCRIPT_PATH = os.path.join(SCRIPTS_DIR, "get_Lambda_durations.sh")