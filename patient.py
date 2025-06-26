"""
This script inserts patient clinical and genomic documents into the Matchminer system.
It processes all files in the patient_data/clinical folder, inserts clinical data,
retrieves the generated clinical_id, and looks for the corresponding genomic document
in the patient_data/genomic folder. Once found, it inserts clinical_id into all genomic
data and proceeds to insert genomic data into the Matchminer system.
"""

import os
import json
import requests
import urllib.parse
from loguru import logger
import config

def insert_all_patient_documents():
    clinical_data_path = os.path.join(config.PATIENT_FOLDER, config.PATIENT_CLINICAL_JSON_FOLDER)
    files = [f for f in os.listdir(clinical_data_path) if f.endswith(".json")]

    for file in files:
        full_path = os.path.join(clinical_data_path, file)
        if not os.path.isfile(full_path):
            continue
        
        clinical_data = load_json(full_path)
        if clinical_data is None:
            continue

        clinical_id = post_clinical_data(clinical_data, file)
        if not clinical_id:
            logger.error(f"Failed to get clinical ID for {file}. Skipping genomic data.")
            continue

        genomic_document_full_path = os.path.join(config.PATIENT_FOLDER, config.PATIENT_GENOMIC_JSON_FOLDER, file)
        if os.path.isfile(genomic_document_full_path):
            genomic_data = load_json(genomic_document_full_path)
            if genomic_data is not None:
                for item in genomic_data:
                    item["CLINICAL_ID"] = clinical_id
                    item["SAMPLE_ID"] = clinical_data["SAMPLE_ID"]
                logger.debug(f"Genomic document: {item}")
                post_genomic_data(genomic_data, file)

def load_json(file_path):
    """Load JSON data from a file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as json_file:
            return json.load(json_file)
    except Exception as e:
        logger.error(f"Failed to load JSON from {file_path}: {e}")
        return None

def post_clinical_data(clinical_data: dict, file_name: str) -> str:
    """Send an API request to insert clinical data into the Matchminer system."""
    endpoint_url = urllib.parse.urljoin(config.MATCHMINER_SERVER, config.CLINICAL_ENDPOINT)
    logger.info(f"Posting clinical data to {endpoint_url}")

    return make_post_request(endpoint_url, clinical_data, file_name)

def post_genomic_data(genomic_data: dict, file_name: str) -> None:
    """Send an API request to insert genomic data into the Matchminer system."""
    endpoint_url = urllib.parse.urljoin(config.MATCHMINER_SERVER, config.GENOMIC_ENDPOINT)
    logger.info(f"Posting genomic data to {endpoint_url}")

    make_post_request(endpoint_url, genomic_data, file_name)

def make_post_request(endpoint_url: str, data: dict, file_name: str) -> str:
    """General method to make a POST request and handle responses."""
    try:
        response = requests.post(endpoint_url, json=data, headers={'Authorization': f"Basic {config.TOKEN}", 'Content-Type': 'application/json'}, verify=False)
        response.raise_for_status()

        logger.info(f"Successfully inserted data for {file_name}")
        return response.json().get('_id')
    except requests.exceptions.HTTPError as err:
        logger.error(f"HTTP error occurred for {file_name}: {err}, Response: {err.response.content}")
    except Exception as err:
        logger.error(f"Other error occurred for {file_name}: {err}")

def main():
    insert_all_patient_documents()

if __name__ == "__main__":
    main()