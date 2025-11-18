import urllib.parse
from loguru import logger
import config
import requests
import json

def get_all_patient_clinical():    
    try:
        endpoint_url = f'{urllib.parse.urljoin(f"{config.MATCHMINER_SERVER}", "/api/clinical")}'
        headers = {
            'Authorization': f"Basic {config.TOKEN}",
            'Content-Type': "application/json"
        }
       
        response = requests.get(endpoint_url, headers=headers, verify=False)
        response.raise_for_status()
        data = response.json()
        items = data.get("_items", [])
        return items
    except requests.exceptions.HTTPError as err:
        logger.error(f"HTTP error occurred: {err}, {err.response.content}")
    except Exception as err:
        logger.error(f"Other error occurred: {err}")
    return None

def get_all_patient_genomic():
    
    try:
        endpoint_url = f'{urllib.parse.urljoin(f"{config.MATCHMINER_SERVER}", "/api/genomic")}'
        headers = {
            'Authorization': f"Basic {config.TOKEN}",
            'Content-Type': "application/json"
        }
       
        response = requests.get(endpoint_url, headers=headers, verify=False)
        response.raise_for_status()
        data = response.json()
        items = data.get("_items", [])
        return items
    except requests.exceptions.HTTPError as err:
        logger.error(f"HTTP error occurred: {err}, {err.response.content}")
    except Exception as err:
        logger.error(f"Other error occurred: {err}")
    return None

def get_patient_genomic_by_clinical_id(clinical_id:str):
    
    try:
        endpoint_url = f'{urllib.parse.urljoin(f"{config.MATCHMINER_SERVER}", "/api/genomic")}'
        headers = {
            'Authorization': f"Basic {config.TOKEN}",
            'Content-Type': "application/json"
        }

        params = {
            'where': json.dumps({"CLINICAL_ID":clinical_id})}
       
        response = requests.get(endpoint_url, headers=headers, params = params, verify=False)
        response.raise_for_status()
        data = response.json()
        items = data.get("_items", [])
        return items
    except requests.exceptions.HTTPError as err:
        logger.error(f"HTTP error occurred: {err}, {err.response.content}")
    except Exception as err:
        logger.error(f"Other error occurred: {err}")
    return None

def get_trial_matches():
    
    try:
        test_clinical_ids = ["6913104e09d08e8e768aa1d5", "6913101309d08e8e768a8f97"]
        endpoint_url = f'{urllib.parse.urljoin(f"{config.MATCHMINER_SERVER}", "/api/trial_match")}'
        headers = {
            'Authorization': f"Basic {config.TOKEN}",
            'Content-Type': "application/json"
        }
        # Build filter for protocol_no
        params = {
            #'where': json.dumps({"clinical_id":{ "$in": test_clinical_ids },"show_in_ui":True,"is_disabled": False}),
            'where': json.dumps({"show_in_ui":True,"is_disabled": False}),
            'projection': json.dumps({
            "sample_id": 1,
            "oncotree_primary_diagnosis_name": 1,
            "match_type": 1,
            "sort_order": 1,
            "protocol_no": 1,
            "clinical_id": 1,
    })
        }
        response = requests.get(endpoint_url, headers=headers, params=params, verify=False)
        response.raise_for_status()
        data = response.json()
        items = data.get("_items", [])
        return items
    except requests.exceptions.HTTPError as err:
        logger.error(f"HTTP error occurred: {err}, {err.response.content}")
    except Exception as err:
        logger.error(f"Other error occurred: {err}")
    return None

def organize_matches_by_patient_protocol_and_type(matches, patients_with_genomic_data: set[str]):
    """
    Build {clinical_id -> {protocol_no -> {gene_type_match_count, generic_clinical_match_count}}}.
    """
    matches_per_patient = {}
    skipped = set()

    for match in matches or []:
        clinical_id = match.get("clinical_id")
        protocol_no = match.get("protocol_no")
        match_type = match.get("match_type")

        if not clinical_id or not protocol_no or not match_type:
            continue
        if clinical_id not in patients_with_genomic_data:
            skipped.add(clinical_id)
            continue

        protocol_counts = matches_per_patient.setdefault(clinical_id, {}).setdefault(
            protocol_no,
            {"gene_type_match_count": 0, "generic_clinical_match_count": 0},
        )

        if match_type == "gene":
            protocol_counts["gene_type_match_count"] += 1
        elif match_type == "generic_clinical":
            protocol_counts["generic_clinical_match_count"] += 1

    logger.info(f"Number of patients with trial matches: {len(matches_per_patient)}")
    logger.info(f"Skipped patients IDs count: {len(skipped)}")
    return matches_per_patient

def compute_trial_match_stats(matches_by_patient):
    """
    Compute summary statistics for the organized matches structure.
    """
    stats_per_patient = {}
    for clinical_id, protocols in matches_by_patient.items():
        gene_trials = sum(1 for counts in protocols.values() if counts["gene_type_match_count"] > 0)
        gene_matches = sum(counts["gene_type_match_count"] for counts in protocols.values())
        total_trials = len(protocols)

        stats_per_patient[clinical_id] = {
            "total_trials_matched": total_trials,
            "trials_matched_by_gene_type": gene_trials,
            "total_gene_type_matches_across_all_trials": gene_matches,
        }
    return stats_per_patient

def main():
    all_patients = 0
    all_clinical = get_all_patient_clinical()
    if all_clinical:
        all_patients = len(all_clinical)
        logger.info(f"Total patients: {all_patients}")
    else:
        logger.error("No clinical data found")
        return
    
    patients_with_genomic_data = set()   # this will store the unique clinical IDs of patients with genomic data

    all_genomic = get_all_patient_genomic()
    if all_genomic:
        for rec in all_genomic:
            clinical_id = rec.get("CLINICAL_ID")
            if clinical_id:
                patients_with_genomic_data.add(clinical_id)
    
    patients_with_genomic_data_count = len(patients_with_genomic_data)
    logger.info(f"Total patients with genomic records: {patients_with_genomic_data_count}")
    
    # Get all trial matches and organize by clinical_id, protocol_no, and match_type counts
    matches = get_trial_matches()
    if matches:
        matches_per_patient = organize_matches_by_patient_protocol_and_type(matches, patients_with_genomic_data)
        stats_per_patient = compute_trial_match_stats(matches_per_patient)

        patient_count = len(stats_per_patient)
        total_trials = sum(s["total_trials_matched"] for s in stats_per_patient.values())
        total_gene_trials = sum(s["trials_matched_by_gene_type"] for s in stats_per_patient.values())

        avg_trials = (total_trials / patient_count) if patient_count else 0
        avg_gene_trials = (total_gene_trials / patient_count) if patient_count else 0

        logger.info(
            "Average trials matched per patient (all matches, across all patients with genomic alterations): "
            f"{avg_trials:.2f}"
        )
        logger.info(
            "Average trials matched per patient (gene level, across all patients with genomic alterations): "
            f"{avg_gene_trials:.2f}"
        )
        logger.info("------------------------------------------------------------")
    else:
        logger.warning("No trial matches found")
        matches_per_patient = {}
    
if __name__ == "__main__":
    logger.add('match_stats.log', rotation = '1 MB', encoding="utf-8", format="{time} {level} - Line: {line} - {message}", level="INFO")
    main()