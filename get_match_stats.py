import urllib.parse
import csv
from pathlib import Path
from loguru import logger
import config
import requests
import json

FOUNDATION_MED_SUMMARY_PATH = Path(__file__).with_name("foundation_med_summary_grouped.csv")

def get_all_case_report_clinical():    
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

def get_all_case_report_genomic():
    
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

def get_case_report_genomic_by_clinical_id(clinical_id:str):
    
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
            # 'where': json.dumps({"clinical_id":{ "$in": test_clinical_ids },"show_in_ui":True,"is_disabled": False}),
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

def load_arbitrary_sample_mapping(csv_path: Path = FOUNDATION_MED_SUMMARY_PATH) -> dict[str, list[str]]:
    """
    Return {arbitrary_id -> [sample_id, ...]} based on the aggregated Foundation Medicine CSV.
    """
    mapping: dict[str, list[str]] = {}
    try:
        with csv_path.open(encoding="utf-8", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                arbitrary_id = (row or {}).get("arbitrary_id")
                sample_ids_raw = (row or {}).get("report_ids")
                if not arbitrary_id or not sample_ids_raw:
                    continue
                sample_ids = [sample.strip() for sample in sample_ids_raw.split(",") if sample.strip()]
                if sample_ids:
                    mapping[arbitrary_id] = sample_ids
    except FileNotFoundError:
        logger.warning(f"Foundation Medicine summary file not found at {csv_path}")
    except Exception as err:
        logger.error(f"Failed to load Foundation Medicine summary file: {err}")
    return mapping

def organize_matches_by_protocol_and_type(matches, case_reports_with_genomic_data: set[str]):
    """
    Build {case_report_sample_id -> {protocol_no -> {gene_type_match_count, generic_clinical_match_count}}}.
    """
    matches_per_case_report = {}
    skipped = set()

    for match in matches or []:
        clinical_id = match.get("clinical_id")
        sample_id = match.get("sample_id")
        protocol_no = match.get("protocol_no")
        match_type = match.get("match_type")

        if not clinical_id or not sample_id or not protocol_no or not match_type:
            continue
        if clinical_id not in case_reports_with_genomic_data:
            skipped.add(clinical_id)
            continue

        matched_protocols = matches_per_case_report.setdefault(sample_id, {})
        match_type_counts = matched_protocols.setdefault(
            protocol_no,
            {"gene_type_match_count": 0, "generic_clinical_match_count": 0},
        )

        if match_type == "gene":
            match_type_counts["gene_type_match_count"] += 1
        elif match_type == "generic_clinical":
            match_type_counts["generic_clinical_match_count"] += 1

    logger.info(f"Number of case_reports with trial matches: {len(matches_per_case_report)}")
    logger.info(f"Skipped case_reports IDs count: {len(skipped)}")
    return matches_per_case_report

def compute_trial_match_stats(matches_by_case_report):
    """
    Compute summary statistics for the organized matches structure and aggregate them per arbitrary_id.
    """

    #get stats per case report (per sample_id in MM DB)
    stats_per_case_report = {}
    for sample_id, protocols in matches_by_case_report.items(): #matches_by_case_report contains only the case reports with genomic data
        gene_trials = sum(1 for counts in protocols.values() if counts["gene_type_match_count"] > 0)
        gene_matches = sum(counts["gene_type_match_count"] for counts in protocols.values())
        total_trials = len(protocols)

        stats_per_case_report[sample_id] = { #stats_per_case_report contains stats only for the case reports with genomic data
            "total_trials_matched": total_trials,
            "trials_matched_by_gene_type": gene_trials,
            "total_gene_type_matches_across_all_trials": gene_matches,
        }
    mapping = load_arbitrary_sample_mapping() #this will load foundation medicine patients that may or may not have genomic data
    stats_per_arbitrary_id = {}

    #get stats per arbitrary_id (per patient)
    aggregated_samples = set()
    for arbitrary_id, sample_ids in mapping.items(): #looping through foundation med patients with or without genomic data
        aggregated = {
            "total_trials_matched": 0,
            "trials_matched_by_gene_type": 0,
            "total_gene_type_matches_across_all_trials": 0,
        }
        for sample_id in sample_ids: #since one arbitrary_id may have multiple sample_ids, we sum up the numbers for all sample_ids
            sample_stats = stats_per_case_report.get(sample_id)
            if not sample_stats: # filtering out foundation medicine patient without genomic data
                continue
            aggregated_samples.add(sample_id)
            aggregated["total_trials_matched"] += sample_stats["total_trials_matched"]
            aggregated["trials_matched_by_gene_type"] += sample_stats["trials_matched_by_gene_type"]
            aggregated["total_gene_type_matches_across_all_trials"] += sample_stats["total_gene_type_matches_across_all_trials"]

        if any(aggregated.values()):
            stats_per_arbitrary_id[arbitrary_id] = aggregated

    for sample_id, sample_stats in stats_per_case_report.items():
        if sample_id in aggregated_samples:
            continue
        stats_per_arbitrary_id[sample_id] = {
            "total_trials_matched": sample_stats["total_trials_matched"],
            "trials_matched_by_gene_type": sample_stats["trials_matched_by_gene_type"],
            "total_gene_type_matches_across_all_trials": sample_stats["total_gene_type_matches_across_all_trials"],
        }

    if not stats_per_arbitrary_id and stats_per_case_report:
        logger.warning("No arbitrary ID aggregates created; check CSV mapping inputs.")
    return stats_per_arbitrary_id

def main():
    all_case_reports = 0
    all_clinical = get_all_case_report_clinical()
    if all_clinical:
        all_case_reports = len(all_clinical)
        logger.info(f"Total case_reports: {all_case_reports}")
    else:
        logger.error("No clinical data found")
        return
    
    case_reports_with_genomic_data = set()   # this will store the unique clinical IDs of case_reports with genomic data

    all_genomic = get_all_case_report_genomic()
    if all_genomic:
        for rec in all_genomic:
            clinical_id = rec.get("CLINICAL_ID")
            if clinical_id:
                case_reports_with_genomic_data.add(clinical_id)
    
    case_reports_with_genomic_data_count = len(case_reports_with_genomic_data)
    logger.info(f"Total case_reports with genomic records: {case_reports_with_genomic_data_count}")
    
    # Get all trial matches and organize by clinical_id, protocol_no, and match_type counts
    matches = get_trial_matches()
    if matches:
        matches_per_case_report = organize_matches_by_protocol_and_type(matches, case_reports_with_genomic_data)        
        logger.debug(f'Trial matches per case_report {matches_per_case_report}')

        stats_per_arbitrary_id = compute_trial_match_stats(matches_per_case_report)
        logger.debug(f'Stats per arbitrary_id: {stats_per_arbitrary_id}')

        arbitrary_id_count = len(stats_per_arbitrary_id) #unique patients with genomic data and atleast 1 trial match

        total_trials = sum(s["total_trials_matched"] for s in stats_per_arbitrary_id.values())
        total_gene_trials = sum(s["trials_matched_by_gene_type"] for s in stats_per_arbitrary_id.values())

        avg_trials = (total_trials / arbitrary_id_count) if arbitrary_id_count else 0
        avg_gene_trials = (total_gene_trials / arbitrary_id_count) if arbitrary_id_count else 0

        # log stats
        logger.info(f'Total patients with genomic data: {arbitrary_id_count}')

        logger.info(
            "Average trials matched per arbitrary_id (all matches, across all mapped arbitrary_id): "
            f"{avg_trials:.2f}"
        )
        logger.info(
            "Average trials matched per arbitrary_id (gene level, across all mapped arbitrary_id): "
            f"{avg_gene_trials:.2f}"
        )
        logger.info("------------------------------------------------------------")
    else:
        logger.warning("No trial matches found")
        matches_per_case_report = {}
    
if __name__ == "__main__":
    logger.add('match_stats.log', rotation = '1 MB', encoding="utf-8", format="{time} {level} - Line: {line} - {message}", level="INFO")
    main()