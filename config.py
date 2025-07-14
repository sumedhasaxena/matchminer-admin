import os

MATCHMINER_SERVER = ""
TOKEN = ""

# File watcher interval in minutes for checking new files
WATCHER_INTERVAL_MINUTES = 120

# Set PATIENT_DATA_BASE_DIR to the directory path where patient's (clinical and genomic) JSON files will be present.
# Ideally this would be the root directory of MATCHMINER_PATIENT repository.
PATIENT_DATA_BASE_DIR = r"path\to\matchminer-patient"
PATIENT_DIR = os.path.join(PATIENT_DATA_BASE_DIR, "patient_data_reviewed")
PATIENT_CLINICAL_JSON_DIR = "clinical"
PATIENT_GENOMIC_JSON_DIR = "genomic"

# Set TRIAL_DATA_BASE_DIR to the directory path where trial JSON files will be present.
# Ideally this would be the root directory of NCT2CTML repository.
TRIAL_DATA_BASE_DIR = r"path\to\nct2ctml"
TRIAL_DIR = os.path.join(TRIAL_DATA_BASE_DIR, "trial_data_reviewed")

TRIAL_ENV_CONFIG_PATH = "matchminer_trial_data_env_config.json"

TRIAL_JSON_PROCESSED_DIR = "trial_data_processed"
PATIENT_JSON_PROCESSED_DIR = "patient_data_processed"

TRIAL_ENDPOINT = "/api/trial"
CLINICAL_ENDPOINT = "/api/clinical"
GENOMIC_ENDPOINT = "/api/genomic"