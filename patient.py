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
import system 

def insert_all_patient_documents():
    clinical_data_path = os.path.join(config.PATIENT_FOLDER, config.PATIENT_CLINICAL_JSON_FOLDER)
    genomic_data_path = os.path.join(config.PATIENT_FOLDER, config.PATIENT_GENOMIC_JSON_FOLDER)
    
    # Check if clinical folder exists
    if not os.path.exists(clinical_data_path):
        logger.error(f"Clinical data folder does not exist: {clinical_data_path}")
        return False
    
    # Create processed folders if they don't exist
    processed_clinical_folder = os.path.join(config.PATIENT_JSON_PROCESSED_FOLDER, config.PATIENT_CLINICAL_JSON_FOLDER)
    processed_genomic_folder = os.path.join(config.PATIENT_JSON_PROCESSED_FOLDER, config.PATIENT_GENOMIC_JSON_FOLDER)
    
    if not os.path.exists(processed_clinical_folder):
        os.makedirs(processed_clinical_folder, exist_ok=True)
    if not os.path.exists(processed_genomic_folder):
        os.makedirs(processed_genomic_folder, exist_ok=True)
    
    files = [f for f in os.listdir(clinical_data_path) if f.endswith(".json")]

    any_success = False
    for file in files:
        clinical_full_path = os.path.join(clinical_data_path, file)
        if not os.path.isfile(clinical_full_path):
            continue
        
        clinical_data = load_json(clinical_full_path)
        if clinical_data is None:
            continue

        clinical_id = post_clinical_data(clinical_data, file)
        if not clinical_id:
            logger.error(f"Failed to get clinical ID for {file}. Skipping genomic data.")
            continue

        genomic_document_full_path = os.path.join(genomic_data_path, file)
        genomic_success = False
        
        if os.path.isfile(genomic_document_full_path):
            genomic_data = load_json(genomic_document_full_path)
            if genomic_data is not None:
                for item in genomic_data:
                    item["CLINICAL_ID"] = clinical_id
                    item["SAMPLE_ID"] = clinical_data["SAMPLE_ID"]
                logger.debug(f"Genomic document: {item}")
                genomic_success = post_genomic_data(genomic_data, file)
        
        # If clinical data was successfully inserted, consider it a success
        if clinical_id:
            try:
                # Move clinical file
                clinical_dest_path = os.path.join(processed_clinical_folder, file)
                _move_file_with_retry(clinical_full_path, clinical_dest_path)
                
                # Move genomic file if it exists
                if os.path.isfile(genomic_document_full_path):
                    genomic_dest_path = os.path.join(processed_genomic_folder, file)
                    _move_file_with_retry(genomic_document_full_path, genomic_dest_path)
                
                any_success = True
                logger.debug(f"Successfully processed and moved files for {file}")
            except Exception as e:
                logger.error(f"Error moving files for {file}: {e}")
                # Still consider it a success if data was inserted, even if file moving failed
                any_success = True
        else:
            logger.debug(f"Clinical data insertion failed for {file}")
    
    # Call run_matchengine once after all files processed, if any were successful
    if any_success:
        system.run_matchengine()
    
    logger.debug(f"insert_all_patient_documents returning: {any_success}")
    return any_success

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

def post_genomic_data(genomic_data: dict, file_name: str) -> bool:
    """Send an API request to insert genomic data into the Matchminer system."""
    endpoint_url = urllib.parse.urljoin(config.MATCHMINER_SERVER, config.GENOMIC_ENDPOINT)
    logger.info(f"Posting genomic data to {endpoint_url}")

    return make_post_request(endpoint_url, genomic_data, file_name) is not None

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

def _move_file_with_retry(source_path, dest_path, max_retries=3, delay=1):
    """
    Move a file with retry logic to handle Windows file locking issues.
    
    Parameters:
    source_path (str): Source file path
    dest_path (str): Destination file path
    max_retries (int): Maximum number of retry attempts
    delay (float): Delay between retries in seconds
    """
    import time
    import shutil
    
    for attempt in range(max_retries):
        try:
            # Try rename first (more efficient)
            os.rename(source_path, dest_path)
            logger.info(f"Successfully moved {source_path} to {dest_path}")
            return
        except PermissionError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Permission error moving file (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(delay)
                continue
            else:
                # Last attempt failed, try copy and delete
                try:
                    logger.info(f"Attempting copy and delete for {source_path}")
                    shutil.copy2(source_path, dest_path)
                    os.remove(source_path)
                    logger.info(f"Successfully copied and deleted {source_path}")
                    return
                except Exception as copy_error:
                    logger.error(f"Failed to copy and delete {source_path}: {copy_error}")
                    raise e
        except Exception as e:
            logger.error(f"Unexpected error moving file {source_path}: {e}")
            raise e

def main():
    insert_all_patient_documents()

if __name__ == "__main__":
    main()