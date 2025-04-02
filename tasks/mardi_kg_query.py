import re
import requests
from typing import List, Dict
from prefect import task


@task
def query_mardi_kg(arxiv_id: str, paper: Dict) -> List[Dict]:
    """Query the MaRDI MediaWiki API for pages mentioning a specific arXiv ID.

    This function queries the MaRDI knowledge graph via its MediaWiki API
    for a custom string pattern based on the arXiv ID (in the "Publication" namespace (4206)).
    Found results are enriched with information from the given dictionary.

    Args:
        arxiv_id (str): The arXiv identifier (e.g., "2104.06175").
        paper (Dict): Dictionary containing metadata for the paper,
                      including optional fields like `repo_url`, `mentioned_in_paper`, etc.

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

    response = requests.post(base_url, data=params)
    response.raise_for_status()
    data = response.json()

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

    # If no results, return a single entry with qid and title = None
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
