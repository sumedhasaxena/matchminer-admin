"""
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

import os
import json
from datetime import datetime
import requests
import urllib.parse
from loguru import logger
import config
import argparse

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
        print(response.json())
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

    print("Updated Request Body:", data) 
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

def main(operation:str):
    if operation == 'insert':
        #insert_trials()
        return None
    elif operation == 'get_max_pid_pno':
        max_protocol_id, max_protocol_no = get_max_protocol_id_and_number()
        print(max_protocol_id, max_protocol_no)
    elif operation == "get_all_nct_ids":
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
    """
    files = os.listdir(config.TRIAL_JSON_FOLDER)
    for file in files:
        full_path = os.path.join(config.TRIAL_JSON_FOLDER, file)
        if os.path.isfile(full_path) and file.endswith(".json"):
            with open(full_path, 'r', encoding='utf-8') as json_file:
                data = json.load(json_file)

                env_variables = load_environment_variables()
                
                updated_env_variables = update_env_variables(env_variables)

                updated_data = pre_process_trial_data(data, updated_env_variables)
                response = post_trial(updated_data)
                if response and response.status_code >= 200 and response.status_code < 300:
                    logger.info(f"Successfully inserted {file}")
                    save_environment_variables(updated_env_variables)
                else:
                    logger.error(f"Error while posting trial {json.dumps(updated_data)}, response: {response}")

def save_to_file(data: dict, file_name :str, format:str):        
    if format == "json":
        path_to_save_at = os.path.join(config.TRIAL_JSON_FOLDER, f'{file_name}.json')
        with open(path_to_save_at, "w") as json_file: 
            json.dump(data, json_file)

if __name__ == "__main__":
    logger.add('update_matchminer.log', rotation = '1 MB', encoding="utf-8", format="{time} {level} - Line: {line} - {message}")
    
    parser = argparse.ArgumentParser(description="Operations: Insert trials or get max protocol_id/number from existing trials")
   
    parser.add_argument("operation", choices=['insert', 'get_max_pid_pno', 'get_all_nct_ids'], help='Specify the operation to perform.')
    args = parser.parse_args()
    main(args.operation)
