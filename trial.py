import os
import json
from datetime import datetime
import requests
import urllib.parse
from loguru import logger
import config
import argparse
import system
import csv

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

def put_trial(matchminer_id,trial_data,etag):
    """
    Sends an API request to update trial data in matchminer system.
    
    Parameters:
    trial_data (dict) : Trial Data
    """
    try:        
        endpoint_url = f'{urllib.parse.urljoin(f"{config.MATCHMINER_SERVER}", config.TRIAL_ENDPOINT)}/{matchminer_id}'
        print(f"Posting request to {endpoint_url}")

        headers = {
            'Authorization': f"Basic {config.TOKEN}",
            'Content-Type': "application/json",
            'If-Match': etag 
        }

        response = requests.put(endpoint_url, json=trial_data, headers=headers, verify=False)
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

def save_last_run_environment(last_run_date_per_trial: dict):
    """
    Update the last run date for each trial that was processed
    """   
    
    with open('last_run_config.json', 'w') as f:
        json.dump(last_run_date_per_trial, f, indent=2)
    
    logger.info(f"Updated and saved last_run_config.json")

def main():
    parser = argparse.ArgumentParser(description="Trial operations for Matchminer.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Insert/update trials based on trial_status.csv present in nct2ctml repo
    upsert_parser = subparsers.add_parser("upsert", help="Insert or update trials from JSON files present in nct2ctml/cache/ctml directory and trial_status.csv")
    
    # Get trial by protocol number
    get_parser = subparsers.add_parser("get", help="Get trial by protocol number")
    get_parser.add_argument("--protocol_no", type=str, required=True, help="Protocol number to fetch")
    
    # Update trial by protocol number
    update_parser = subparsers.add_parser("update", help="Update trial by protocol number. To be used when updated trial JSON is available.")
    update_parser.add_argument("--protocol_no", type=str, required=True, help="Protocol number to update")
    update_parser.add_argument("--updated_trial_file", type=str, required=True, help="File name with updated trial JSON")

    # Get max protocol ID and number
    get_max_parser = subparsers.add_parser("get_max_pid_pno", help="Get max protocol_id and protocol_no from all trials")
    
    args = parser.parse_args()

    if args.command == "upsert":
        process_trials()
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

def process_trials():
    """
    Code to process trials. 
    It looks for matchminer-compliant trial JSON files at a specified path, and for each trial:
     - calls a method to insert trial into matchminer system
     - calls a method to update trial in matchminer system
     - calls a method to close trial in matchminer system
     - calls a method to run matchengine
    """
    # Check if trial folder exists
    if not os.path.exists(config.TRIAL_DIR):
        logger.error(f"Trial folder does not exist: {config.TRIAL_DIR}")
        return False
    
    # Check if trial_status.csv exists
    if not os.path.exists(config.TRIAL_STATUS_CSV_PATH):
        logger.error(f"Trial status csv not found: {config.TRIAL_STATUS_CSV_PATH}")
        return False
    
    processed_folder = config.TRIAL_JSON_PROCESSED_DIR
    if not os.path.exists(processed_folder):
        os.makedirs(processed_folder, exist_ok=True)
    
    try:
        with open("last_run_config.json", "r") as f:
            last_run_date_per_trial = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        last_run_date_per_trial = {}

    # read trial_status.csv and filter trials to process based on last_run
    trials_to_process = []
    logger.info(f"Preparing a list of trials to process which were updated after last matcminer_admin's run")
    with open(config.TRIAL_STATUS_CSV_PATH, 'r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row['nct_id'] == "NA":
                trial_id = row['local_protocol_ids'].split('|')[0]
            else:
                trial_id = row['nct_id']
            if row['entry_last_updated_date'] > last_run_date_per_trial.get(trial_id, "1900-01-01"): # if the trial is not found in last_run_date_per_trial, use a very old date, so that its processed
                trials_to_process.append(row)

    trials_to_update = [] # stores filename, matchminer id, protocol_id, protocol_no of trials to update
    trials_to_insert = [] # stores filename which would contain trial data to be inserted
    trials_to_close = [] # stores matchminer id, trial data of trials to close

    for trial_to_process in trials_to_process:
        nct_id = trial_to_process['nct_id']
        file_name = _get_trial_file_name(trial_to_process)
        if nct_id and nct_id != 'NA':
            trial_in_mm = get_trial_by_nct_id(nct_id)
            if trial_in_mm:
                if trial_to_process['status'] == 'closed':
                    if trial_in_mm['status'] != 'closed':
                        trials_to_close.append((trial_in_mm['_id'], trial_in_mm['nct_id'], trial_in_mm))
                    else:
                        logger.info(f"Trial {nct_id} is already closed in matchminer")
                else:
                    trials_to_update.append((file_name, trial_in_mm['_id'], trial_in_mm['protocol_id'], trial_in_mm['protocol_no'], trial_in_mm['_etag']))
            else:                
                trials_to_insert.append(file_name)
        else:
            local_protocol_ids_string = trial_to_process['local_protocol_ids']
            if local_protocol_ids_string and local_protocol_ids_string != 'NA':
                local_protocol_ids = local_protocol_ids_string.split('|')
                trial_in_mm = get_trial_by_local_protocol_ids(local_protocol_ids)
                if trial_in_mm:
                    trials_to_update.append((file_name,trial_in_mm['_id'], trial_in_mm['protocol_id'], trial_in_mm['protocol_no'], trial_in_mm['_etag']))
                else:
                    trials_to_insert.append(file_name)

    logger.info(f"Trials to insert: {trials_to_insert}")
    logger.info(f"Trials to update: {[trial[0] for trial in trials_to_update]}")
    logger.info(f"Trials to close: {[trial[1] for trial in trials_to_close]}")

    any_success = False

    # process new trials
    any_success = _process_trials_to_insert(trials_to_insert, last_run_date_per_trial, any_success)

    # process trials to update
    any_success = _process_trials_to_update(trials_to_update, last_run_date_per_trial, any_success)

    # process trials to close
    any_success = _process_trials_to_close(trials_to_close, last_run_date_per_trial, any_success)
           
    # Call run_matchengine to refresh patient-trial matches once after all files processed, if any were successful
    if any_success:
        system.run_matchengine()
    
    # Update LAST_RUN environment variable to current date
    save_last_run_environment(last_run_date_per_trial)
    
    return any_success


def _process_trials_to_insert(trials_to_insert:list, last_run_date_per_trial: dict, any_success:bool):
    """
    Process trials that need to be inserted.
    
    Parameters:
    trials_to_insert (list): List of file names to insert
    any_success (bool): Current success status
    
    Returns:
    bool: Updated success status
    """
    for trial_to_insert in trials_to_insert:
        file_name = trial_to_insert
        full_path = os.path.join(config.TRIAL_DIR, file_name)
        if os.path.isfile(full_path):            
            try:
                with open(full_path, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)

                env_variables = load_environment_variables()                    
                updated_env_variables = update_env_variables(env_variables)
                updated_data = pre_process_trial_data(data, updated_env_variables)
                response = post_trial(updated_data)
                if response and response.status_code >= 200 and response.status_code < 300:
                    logger.info(f"Successfully inserted {file_name}")
                    save_environment_variables(updated_env_variables)
                    last_run_date_per_trial[file_name.split('.')[0]] = datetime.now().strftime("%Y-%m-%d")
                    any_success = True
                else:
                    logger.error(f"Error while posting trial {file_name}, response: {response}")
            except Exception as e:
                logger.error(f"Error processing file {file_name}: {e}")
                continue
        else:
            logger.error(f"File not found: {full_path}")
            continue
    
    return any_success

def _process_trials_to_update(trials_to_update:list, last_run_date_per_trial: dict, any_success:bool):
    """
    Process trials that need to be updated.
    
    Parameters:
    trials_to_update (list): List of tuples containing (file_name, matchminer_id, protocol_id, protocol_no, etag)
    any_success (bool): Current success status
    
    Returns:
    bool: Updated success status
    """
    for trial_to_update in trials_to_update: 
        file_name = trial_to_update[0]
        matchminer_id = trial_to_update[1]
        protocol_id = trial_to_update[2]
        protocol_no = trial_to_update[3]
        etag = trial_to_update[4]

        full_path = os.path.join(config.TRIAL_DIR, file_name)
        if os.path.isfile(full_path):
            try:
                with open(full_path, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)
                    data['protocol_id'] = protocol_id
                    data['protocol_no'] = protocol_no

                    response = put_trial(matchminer_id, data, etag)
                    if response and response.status_code >= 200 and response.status_code < 300:
                        logger.info(f"Successfully updated {file_name}")                        
                        last_run_date_per_trial[file_name.split('.')[0]] = datetime.now().strftime("%Y-%m-%d")
                        any_success = True
                    else:
                        logger.error(f"Error while updating trial {json.dumps(data)}, response: {response}")
            except Exception as e:
                logger.error(f"Error processing file {file_name}: {e}")
                continue
        else:
            logger.error(f"File not found: {full_path}")
            continue
    
    return any_success

def _process_trials_to_close(trials_to_close:list, last_run_date_per_trial: dict, any_success:bool):
    """
    Process trials that need to be closed.
    
    Parameters:
    trials_to_close (list): List of tuples containing (matchminer_id, nct_id, trial_data_in_mm)
    any_success (bool): Current success status
    
    Returns:
    bool: Updated success status
    """
    for trial_to_close in trials_to_close:
        matchminer_id = trial_to_close[0]
        trial_data_in_mm = trial_to_close[2]
        response = close_trial(matchminer_id, trial_data_in_mm)
        if response:
            last_run_date_per_trial[trial_data_in_mm['nct_id']] = datetime.now().strftime("%Y-%m-%d")
            any_success = True
            logger.info(f"Successfully closed {matchminer_id}")
        else:
            logger.error(f"Error while closing trial {matchminer_id}")
    
    return any_success

def save_to_file(data: dict, file_name :str, format:str):        
    if format == "json":
        path_to_save_at = os.path.join(config.TRIAL_DIR, f'{file_name}.json')
        with open(path_to_save_at, "w") as json_file: 
            json.dump(data, json_file)

def _get_trial_file_name(trial_to_process: dict):
    if trial_to_process['nct_id'] != "NA": 
        file_name = trial_to_process['nct_id'] + '.json'
    else:
        file_name = trial_to_process['local_protocol_ids'].split('|')[0] + '.json' #assuming  that the file name is same as first local protocol id
    return file_name

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

def get_trial_by_mm_id(_id: str):
    """
    Fetch a trial from matchminer system by matchminer id via GET request.

    Parameters:
    _id (str): Matchminer id to search for

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
            'where': json.dumps({"_id": _id})
        }
        response = requests.get(endpoint_url, headers=headers, params=params, verify=False)
        response.raise_for_status()
        data = response.json()
        items = data.get("_items", [])
        if items:
            if len(items) > 1:
                logger.warning(f"Found {len(items)} trials with id: {_id}. Returning the first one.")
            return items[0]  # Return the first matching trial
        else:
            print(f"No trial found with id: {_id}")
            return None
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}, {err.response.content}")
    except Exception as err:
        print(f"Other error occurred: {err}")
    return None

def get_trial_by_nct_id(nct_id: str):
    """
    Fetch a trial from matchminer system by nct_id via GET request.

    Parameters:
    nct_id (str): NCT ID to search for

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
            'where': json.dumps({"nct_id": nct_id})
        }
        response = requests.get(endpoint_url, headers=headers, params=params, verify=False)
        response.raise_for_status()
        data = response.json()
        items = data.get("_items", [])
        if items:
            if len(items) > 1:
                logger.warning(f"Found {len(items)} trials with nct_id: {nct_id}. Returning the first one.")
            return items[0]  # Return the first matching trial
        else:            
            return None
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}, {err.response.content}")
    except Exception as err:
        print(f"Other error occurred: {err}")
    return None

def get_trial_by_local_protocol_ids(local_protocol_ids: list):
    """
    Fetch a trial from matchminer system by local protocol ids via GET request.

    Parameters:
    local_protocol_ids (list): list of local protocol ids

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
            'where': json.dumps({"protocol_ids":{"$in":[local_protocol_ids.join(',')]}})
        }
        response = requests.get(endpoint_url, headers=headers, params=params, verify=False)
        response.raise_for_status()
        data = response.json()
        items = data.get("_items", [])
        if items:
            if len(items) > 1:
                logger.warning(f"Found {len(items)} trials with nct_id: {local_protocol_ids}. Returning the first one.")
            return items[0]  # Return the first matching trial
        else:
            print(f"No trial found with local_protocol_ids: {local_protocol_ids}")
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

def close_trial(mm_id: str, existing_trial :dict, force_refresh_matchengine: bool = False):
    """
    Close a trial in matchminer system by matchminer id via PUT request.

    Parameters:
    mm_id (str): Matchminer id to search for
    existing_trial (dict): Existing trial data

    Returns:
    bool: True if update was successful, False otherwise
    """
    
    id = mm_id
    etag = existing_trial.get('_etag')
    existing_trial['status'] = 'closed'    

    existing_trial.pop('_id', None)   
    existing_trial.pop('_etag', None)
    existing_trial.pop('_summary', None)    
    existing_trial.pop('_updated', None)
    existing_trial.pop('_created', None)
    existing_trial.pop('_links', None)

    try:
        endpoint_url = f'{urllib.parse.urljoin(f"{config.MATCHMINER_SERVER}", config.TRIAL_ENDPOINT)}/{id}'
        print(f"Closing trial at {endpoint_url}")
        headers = {
            'Authorization': f"Basic {config.TOKEN}",
            'Content-Type': "application/json",
            'If-Match': etag  
        }
        response = requests.put(endpoint_url, headers=headers, json=existing_trial, verify=False)
        response.raise_for_status()

        if force_refresh_matchengine:
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
