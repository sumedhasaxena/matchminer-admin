# NCT IDs to insert or update in Matchminer.
# to be used when we have a specific list of nct ids to process instead of letting the application read from trial_status.csv
# Run: python trial.py process_specific_nct_ids
# Each ID is expected to have a matching JSON file at
# {TRIAL_DATA_BASE_DIR}/ctml/json/{nct_id}.json
NCT_IDS_TO_PROCESS = [
    # "NCT01234567",
]
