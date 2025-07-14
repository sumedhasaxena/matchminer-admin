"""
This project is supposed to only interact with matchminer system.
This script contains logic to insert trial documents to matchminer system or
to get the max/highest protocol_id and protocol_number for existing trials in the system.
Steps for auto-incrementing protocol_id/number for inserting trials:
1. Read trial JSON files from a directory
2. For each trial
    2.1 Read the trial env variables from file 
    2.2 Increment env variables for protocol_id and protocol_no
    2.3 Update the trial with new protocol_id and protocol_no
    2.4 Post the trial to matchminer server
    2.5 if successful, save protocol_id and protocol_no back in trial env config
"""

# Insert all trials from JSON files:
# python trial.py trial insert

# Get a trial by protocol number:
# python trial.py trial get --protocol_no 2024060101

# Get max protocol_id and protocol_no from all trials:
# python trial.py system get_max_pid_pno

# Get all NCT IDs from all trials:
# python trial.py system get_all_nct_ids


import os
import json
from datetime import datetime
import requests
import urllib.parse
from loguru import logger
import config
import argparse
import system

def load_environment_variables():
    """
    Reads the trial-related variables from an env config file

    Parameters:
    None

    Returns:
    JSON data for trial env variables
    """
    if os.path.exists(config.TRIAL_ENV_CONFIG_PATH):
        with open(config.TRIAL_ENV_CONFIG_PATH, 'r') as env_config:
            return json.load(env_config)
    else:
        raise Exception(f"Env config path {config.TRIAL_ENV_CONFIG_PATH} not found")

def post_trial(trial_data):
    """
    Sends an API request to insert trial data into matchminer system.
    
    Parameters:
    trial_data (dict) : Trial Data
    """
    try:
        endpoint_url = f'{urllib.parse.urljoin(f"{config.MATCHMINER_SERVER}", config.TRIAL_ENDPOINT)}'
        print(f"Posting request to {endpoint_url}")

        headers = {}
        headers['Authorization'] = f"Basic {config.TOKEN}"
        headers['Content-Type'] = f"application/json"

        response = requests.post(endpoint_url, json=trial_data, headers=headers, verify=False)
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}, {err.response.content}")  # Handle HTTP errors
    except Exception as err:
        print(f"Other error occurred: {err}")  # Handle other exceptions

def get_max_protocol_id_and_number():
    """
    Sends an API request to get the max/highest protocol_id and protocol_number from the trials in matchminer system.
    
    Parameters:
    None

    Returns:
    Tuple : max_protocol_id, max_protocol_no
    """
    try:
        projection = {"protocol_id": 1, "protocol_no": 1}
        params = {"projection": json.dumps(projection)}
        endpoint_url = f'{urllib.parse.urljoin(f"{config.MATCHMINER_SERVER}", config.TRIAL_ENDPOINT)}'
        
        headers = {}
        headers['Authorization'] = f"Basic {config.TOKEN}"
        headers['Content-Type'] = f"application/json"
        
        response = requests.get(endpoint_url, headers=headers, params = params, verify=False)
        #print(response.json())
        response.raise_for_status()
        data = json.loads(response.content)
        items = data["_items"]
        sorted_items_by_id = sorted(items, key=lambda x: x["protocol_id"])

        max_item = sorted_items_by_id[-1]

        max_protocol_id = max_item["protocol_id"]
        max_protocol_no = max_item["protocol_no"]

        return max_protocol_id, max_protocol_no
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}, {err.response.content}")  # Handle HTTP errors
    except Exception as err:
        print(f"Other error occurred: {err}")  # Handle other exceptions

def get_all_nct_ids():
    """
    Sends an API request to get a list of NCTIds for all NCT trials in matchminer system.
    
    Parameters:
    None

    Returns:
    List : NCT trial numbers
    """
    try:
        nct_ids = []
        projection = {"nct_id": 1}
        params = {"projection": json.dumps(projection)}
        endpoint_url = f'{urllib.parse.urljoin(f"{config.MATCHMINER_SERVER}", config.TRIAL_ENDPOINT)}'
        
        headers = {}
        headers['Authorization'] = f"Basic {config.TOKEN}"
        headers['Content-Type'] = f"application/json"
        
        response = requests.get(endpoint_url, headers=headers, params=params, verify=False)
        response.raise_for_status()
        data = json.loads(response.content)
        items = data["_items"]

        for trial in items:
            nct_id = trial["nct_id"] 
            if nct_id.startswith('NCT'): # means its a trial from clinicaltrials.gov
                nct_ids.append(nct_id)
        return nct_ids
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}, {err.response.content}")  # Handle HTTP errors
    except Exception as err:
        print(f"Other error occurred: {err}")  # Handle other exceptions

def pre_process_trial_data(data, env_variables):
    """
    Attaches the protocol_id and protocol_number to the trial data dictionary

    Parameters:
    data (dict): Trial data
    env_variables (dict): Env variables for trial data

    Returns:
    data (dict): Updated trial data with protocol_id and protocol_no set from the env variables
    """

    if 'protocol_id' in data:
        data['protocol_id'] = env_variables.get('protocol_id_counter', data['protocol_id'])

    if 'protocol_no' in data:
        data['protocol_no'] = env_variables.get('protocol_no', data['protocol_no'])

    print(f"Protocol ID: {data.get('protocol_id', 'N/A')}, Protocol Number: {data.get('protocol_no', 'N/A')}")
    return data

def update_env_variables(env_variables):
    """
    Updates/Increments the locally stored trial env variables.
    Increments protocol_id by 1
    Sets protocol_number to yyyymmdd+counter format. The counter keeps incrementing for trials inserted on same date and resets for a new date.

    Parameters:
    env_variables (dict): Env variables for trial data

    Returns:
    env_variables (dict): Env variables for trial data
    """
    today = datetime.now()
    today_formatted_date = today.strftime("%Y%m%d")

    protocol_no_counter = f"{(int(env_variables['protocol_no_counter']) + 1):02d}"# Zero-padded to 2 digits

    current_date = env_variables['current_date']
    if current_date is None or current_date != today_formatted_date: #date changed: reset the counter for protocol_no
        protocol_no_counter = '00'
        current_date = today_formatted_date

    # Save the new incremented value and protocol_no in the environment variables
    env_variables['protocol_no_counter'] = protocol_no_counter
    env_variables['current_date'] = current_date
    env_variables['protocol_no'] = f"{current_date}{protocol_no_counter}"

    protocol_id_counter = env_variables['protocol_id_counter']
    env_variables['protocol_id_counter'] = protocol_id_counter + 1

    return env_variables

def save_environment_variables(env_vars):
    """
    Save trial env variables back in config
    """
    with open(config.TRIAL_ENV_CONFIG_PATH, 'w') as file:
        json.dump(env_vars, file, indent=4)

def main():
    parser = argparse.ArgumentParser(description="Trial operations for Matchminer.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Insert trials
    insert_parser = subparsers.add_parser("insert", help="Insert trials from JSON files")
    
    # Get trial by protocol number
    get_parser = subparsers.add_parser("get", help="Get trial by protocol number")
    get_parser.add_argument("--protocol_no", type=str, required=True, help="Protocol number to fetch")
    
    # Update trial by protocol number
    update_parser = subparsers.add_parser("update", help="Update trial by protocol number")
    update_parser.add_argument("--protocol_no", type=str, required=True, help="Protocol number to update")
    update_parser.add_argument("--updated_trial_file", type=str, required=True, help="File name with updated trial JSON")

    # Get max protocol ID and number
    get_max_parser = subparsers.add_parser("get_max_pid_pno", help="Get max protocol_id and protocol_no from all trials")
    
    # Get all NCT IDs
    get_nct_parser = subparsers.add_parser("get_all_nct_ids", help="Get all NCT IDs from all trials")

    args = parser.parse_args()

    if args.command == "insert":
        insert_trials()
    elif args.command == "get":
        trial = get_trial_by_protocol_no(args.protocol_no)
        print(json.dumps(trial, indent=2) if trial else "No trial found.")
    elif args.command == "update":
        result = update_trial_by_protocol_no(args.protocol_no, args.updated_trial_file)
        if result:
            print("Update successful.")
        else:
            print("Update failed.")
    elif args.command == "get_max_pid_pno":
        max_protocol_id, max_protocol_no = get_max_protocol_id_and_number()
        print(max_protocol_id, max_protocol_no)
    elif args.command == "get_all_nct_ids":
        all_nct_ids = get_all_nct_ids()
        save_to_file(all_nct_ids, "all_nct_ids", 'json')
        print("NCT Ids for all trials saved at all_nct_ids.json")

def insert_trials():
    """
    Code to insert trials into matchminer system.
    It looks for matchminer-compliant trial JSON files at a specified path, and for each trial:
     - calls a method to increment trial env variables
     - calls a method to attach the updated env variables into the trial
     - calls a method to insert trial into matchminer system
     - if successful, calls a method to save incremented env variables
     - moves processed file to trial_data_processed folder
    """
    # Check if trial folder exists
    if not os.path.exists(config.TRIAL_DIR):
        logger.error(f"Trial folder does not exist: {config.TRIAL_DIR}")
        return False
    
    processed_folder = config.TRIAL_JSON_PROCESSED_DIR
    if not os.path.exists(processed_folder):
        os.makedirs(processed_folder, exist_ok=True)

    files = os.listdir(config.TRIAL_DIR)
    any_success = False
    for file in files:
        full_path = os.path.join(config.TRIAL_DIR, file)
        if os.path.isfile(full_path) and file.endswith(".json"):
            try:
                with open(full_path, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)

                env_variables = load_environment_variables()
                
                updated_env_variables = update_env_variables(env_variables)

                updated_data = pre_process_trial_data(data, updated_env_variables)
                response = post_trial(updated_data)
                if response and response.status_code >= 200 and response.status_code < 300:
                    logger.info(f"Successfully inserted {file}")
                    save_environment_variables(updated_env_variables)
                    # Move processed file to processed_folder with retry logic
                    dest_path = os.path.join(processed_folder, file)
                    _move_file_with_retry(full_path, dest_path)
                    any_success = True
                else:
                    logger.error(f"Error while posting trial {json.dumps(updated_data)}, response: {response}")
            except Exception as e:
                logger.error(f"Error processing file {file}: {e}")
                continue
    # Call run_matchengine once after all files processed, if any were successful
    if any_success:
        system.run_matchengine()
    
    return any_success

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

def save_to_file(data: dict, file_name :str, format:str):        
    if format == "json":
        path_to_save_at = os.path.join(config.TRIAL_DIR, f'{file_name}.json')
        with open(path_to_save_at, "w") as json_file: 
            json.dump(data, json_file)

def get_trial_by_protocol_no(protocol_no: str):
    """
    Fetch a trial from matchminer system by protocol_no via GET request.

    Parameters:
    protocol_no (str): Protocol number to search for

    Returns:
    dict or None: Trial data if found, else None
    """
    try:
        endpoint_url = f'{urllib.parse.urljoin(f"{config.MATCHMINER_SERVER}", config.TRIAL_ENDPOINT)}'
        headers = {
            'Authorization': f"Basic {config.TOKEN}",
            'Content-Type': "application/json"
        }
        # Build filter for protocol_no
        params = {
            'where': json.dumps({"protocol_no": protocol_no})
        }
        response = requests.get(endpoint_url, headers=headers, params=params, verify=False)
        response.raise_for_status()
        data = response.json()
        items = data.get("_items", [])
        if items:
            if len(items) > 1:
                logger.warning(f"Found {len(items)} trials with protocol_no: {protocol_no}. Returning the first one.")
            return items[0]  # Return the first matching trial
        else:
            print(f"No trial found with protocol_no: {protocol_no}")
            return None
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}, {err.response.content}")
    except Exception as err:
        print(f"Other error occurred: {err}")
    return None

def update_trial_by_protocol_no(protocol_no: str, updated_json_file_name :str):
    """
    Update a trial in matchminer system by protocol_no via PUT request.

    Parameters:
    protocol_no (str): Protocol number to search for
    updated_json_file_name (str): JSON file containing the updated trial data

    Returns:
    bool: True if update was successful, False otherwise
    """

    if not updated_json_file_name:
        raise ValueError("File name with updated trial JSON must be provided")
    
    # Get trial by protocol_no
    existing_trial = get_trial_by_protocol_no(protocol_no)
    if not existing_trial:
        logger.error(f"No trial found with protocol_no: {protocol_no}")
        return False
    
    id = existing_trial.get('_id')
    etag = existing_trial.get('_etag')

    try:
        with open(updated_json_file_name, 'r', encoding='utf-8') as f:
            updated_data = json.load(f)
    except FileNotFoundError:
        logger.error(f"File not found: {updated_json_file_name}")
        return False
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in file {updated_json_file_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error reading {updated_json_file_name}: {e}")
        return False

    updated_data.pop('_id', None)   
    updated_data.pop('_etag', None)
    updated_data.pop('_summary', None)    
    updated_data.pop('_updated', None)
    updated_data.pop('_created', None)
    updated_data.pop('_links', None)

    try:
        endpoint_url = f'{urllib.parse.urljoin(f"{config.MATCHMINER_SERVER}", config.TRIAL_ENDPOINT)}/{id}'
        print(f"Updating trial at {endpoint_url}")
        headers = {
            'Authorization': f"Basic {config.TOKEN}",
            'Content-Type': "application/json",
            'If-Match': etag  # Use the ETag for optimistic concurrency control
        }
        # Build filter for protocol_no
        params = {
            'where': json.dumps({"protocol_no": protocol_no})
        }
        response = requests.put(endpoint_url, headers=headers, params=params, json=updated_data, verify=False)
        response.raise_for_status()
        # Only run matchengine for single update, not in batch
        system.run_matchengine()
        return True
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}, {err.response.content}")
    except Exception as err:
        print(f"Other error occurred: {err}")
    return False

if __name__ == "__main__":
    logger.add('update_matchminer.log', rotation = '1 MB', encoding="utf-8", format="{time} {level} - Line: {line} - {message}")
    main()
