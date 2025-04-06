import re
import requests
from typing import List, Dict
from prefect import task
import shlex
import json
import time


@task
def query_mardi_kg(arxiv_id: str, paper: Dict, max_retries: int = 5, retry_delay: float = 2.0) -> List[Dict]:
    """Query the MaRDI MediaWiki API for pages mentioning a specific arXiv ID.

    This function queries the MaRDI knowledge graph via its MediaWiki API
    for a custom string pattern based on the arXiv ID (in the "Publication" namespace (4206)).
    Found results are enriched with information from the given dictionary.

    Args:
        arxiv_id (str): The arXiv identifier (e.g., "2104.06175").
        paper (Dict): Dictionary containing metadata for the paper,
                      including optional fields like `repo_url`, `mentioned_in_paper`, etc.
      max_retries (int, optional): Maximum number of retries before raising an error.
      retry_delay(float, optional): Delay between retries in seconds.

    Returns:
        List[Dict]: A list of matching result entries with extracted and enriched metadata,
                    including arXiv ID, title, QID, and snippet context.
    """
    base_url = "https://portal.mardi4nfdi.de/w/api.php"
    search_string = f"arXiv{arxiv_id}MaRDI"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": search_string,
        "srnamespace": "4206",
        "format": "json"
    }

    last_exception = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(base_url, data=params)
            response.raise_for_status()
            data = response.json()
            break
        except requests.RequestException as e:
            last_exception = e
            if attempt < max_retries:
                time.sleep(retry_delay)
    else:
        # All retries failed
        print("All retries failed. Curl for debugging:")
        print(generate_curl_command(base_url, params))
        raise last_exception

    results = []
    for r in data.get("query", {}).get("search", []):
        snippet = r.get("snippet", "")
        clean_snippet = snippet.replace("<span class=\"searchmatch\">", "").replace("</span>", "")
        qid_match = re.search(r"QID(Q\d+)", clean_snippet)
        qid = qid_match.group(1) if qid_match else None

        results.append({
            "qid": qid,
            "arxiv_id": arxiv_id,
            "title": r.get("title", "(no title)"),
            "repo_url": paper.get("repo_url"),
            "is_official": paper.get("is_official"),
            "mentioned_in_paper": paper.get("mentioned_in_paper"),
            "mentioned_in_github": paper.get("mentioned_in_github"),
            "pwc_page": paper.get("paper_url"),
            "snippet": clean_snippet
        })

    if not results:
        results.append({
            "qid": None,
            "arxiv_id": arxiv_id,
            "title": None,
            "repo_url": paper.get("repo_url"),
            "is_official": paper.get("is_official"),
            "mentioned_in_paper": paper.get("mentioned_in_paper"),
            "mentioned_in_github": paper.get("mentioned_in_github"),
            "pwc_page": paper.get("paper_url"),
            "snippet": ""
        })

    return results


def generate_curl_command(url, params=None, json_data=False):
    """
    Generate a curl command equivalent to requests.post().

    Args:
        url (str): The target URL.
        params (dict): Dictionary of data to send.
        json_data (bool): If True, send as JSON; otherwise, as form data.

    Returns:
        str: A curl command string.
    """
    if params is None:
        params = {}

    if json_data:
        data_str = json.dumps(params)
        escaped_data = shlex.quote(data_str)
        curl_cmd = f"curl -X POST {shlex.quote(url)} -H 'Content-Type: application/json' -d {escaped_data}"
    else:
        data_str = '&'.join(f'{k}={v}' for k, v in params.items())
        escaped_data = shlex.quote(data_str)
        curl_cmd = f"curl -X POST {shlex.quote(url)} -d {escaped_data}"

    return curl_cmd
