import os

MATCHMINER_SERVER = ""
TOKEN = ""

# Set BASE_DIRCTORY to the folder path where patient and trial data JSON files will be present
# This should be the absolute path to the folder containing the PATIENT_FOLDER and TRIAL_JSON_FOLDER folders.
BASE_DIR = ""

TRIAL_ENV_CONFIG_PATH = "matchminer_trial_data_env_config.json"
TRIAL_JSON_FOLDER = os.path.join(BASE_DIR, "trial_data")
TRIAL_JSON_PROCESSED_FOLDER = os.path.join(BASE_DIR, "trial_data_processed")
PATIENT_FOLDER = os.path.join(BASE_DIR, "patient_data")
PATIENT_CLINICAL_JSON_FOLDER = "clinical"
PATIENT_GENOMIC_JSON_FOLDER = "genomic"
TRIAL_ENDPOINT = "/api/trial"
CLINICAL_ENDPOINT = "/api/clinical"
GENOMIC_ENDPOINT = "/api/genomic"