import os
import json
from dotenv import load_dotenv

# Load environment-specific config
env = os.getenv('ENVIRONMENT', 'NA')
if env == 'development':
    load_dotenv('.env.dev')
    print("Using development environment")
elif env == 'production':
    load_dotenv('.env')
    print("Using production environment")
else:
    raise ValueError("Invalid environment. Please set the ENVIRONMENT environment variable to 'development' or 'production'.")

# Get sensitive configuration from environment variables
MATCHMINER_SERVER = os.getenv("MATCHMINER_SERVER", "http://localhost:1952")
TOKEN = os.getenv("TOKEN", "")

# Validate required environment variables
if not TOKEN:
    raise ValueError("TOKEN environment variable is required. Please set it in your .env file.")

PATIENT_DATA_BASE_DIR = os.getenv("PATIENT_DATA_BASE_DIR")
PATIENT_DIR = os.path.join(PATIENT_DATA_BASE_DIR, "patient_data")
PATIENT_CLINICAL_JSON_DIR = "clinical"
PATIENT_GENOMIC_JSON_DIR = "genomic"

TRIAL_DATA_BASE_DIR = os.getenv("TRIAL_DATA_BASE_DIR")
TRIAL_NCT_DATA_DIR = os.path.join(TRIAL_DATA_BASE_DIR, "cache", "nct")
TRIAL_DIR = os.path.join(TRIAL_DATA_BASE_DIR, "ctml", "json")
TRIAL_STATUS_CSV_PATH = os.path.join(TRIAL_NCT_DATA_DIR, "trial_status.csv")
TRIAL_ENV_CONFIG_PATH = "matchminer_trial_data_env_config.json"

TRIAL_JSON_PROCESSED_DIR = "trial_data_processed"
PATIENT_JSON_PROCESSED_DIR = "patient_data_processed"

TRIAL_ENDPOINT = "/api/trial"
CLINICAL_ENDPOINT = "/api/clinical"
GENOMIC_ENDPOINT = "/api/genomic"