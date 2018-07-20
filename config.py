import os

# Path configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TESTS_DIR = os.path.join(BASE_DIR, 'tests')
BUNDLE_UUIDS_PATH = os.path.join(TESTS_DIR, "bundle_uuids.json")