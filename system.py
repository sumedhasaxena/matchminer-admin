import requests
import config
import urllib.parse
from loguru import logger

def run_matchengine():
    """
    Sends a POST request to /api/run_matchengine with an empty body.
    """
    endpoint_url = urllib.parse.urljoin(config.MATCHMINER_SERVER, "/api/run_matchengine")
    headers = {
        'Authorization': f"Basic {config.TOKEN}",
        'Content-Type': "application/json"
    }
    try:
        response = requests.post(endpoint_url, headers=headers, json={}, verify=False)
        response.raise_for_status()
        print("Matchengine run request sent successfully.")
        logger.info("run_matchengine was executed.")
        return response
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}, {err.response.content}")
        logger.error(f"run_matchengine HTTP error: {err}, {err.response.content}")
    except Exception as err:
        print(f"Other error occurred: {err}")
        logger.error(f"run_matchengine other error: {err}")
    return None
