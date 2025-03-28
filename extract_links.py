import requests
import ijson
import json
import os
import re
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

JSON_INPUT = "./data/links_pwc.json"
JSON_OUTPUT = "./data/results.json"

def search_arxiv(arxiv_id: str, paper: dict):
    """Search the MaRDI MediaWiki API for a specific arXiv ID.

    Args:
        arxiv_id (str): The arXiv identifier (e.g., '2104.06175').
        paper (dict): The full paper record from the input JSON.

    Returns:
        list[dict]: A list of matching results, enriched with arXiv ID, repo info, and QID.
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

        # Extract QID
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

    return results

def load_existing_results(path):
    """Load existing results and metadata from JSON file.

    Args:
        path (str): Path to the results JSON file.

    Returns:
        dict: A dictionary with 'total_hits', 'last_processed_arxiv_id', and 'hits'.
    """
    if not os.path.exists(path):
        return {
            "total_hits": 0,
            "last_processed_arxiv_id": None,
            "hits": []
        }

    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            print("⚠️ Warning: Could not decode existing result file. Starting fresh.")
            return {
                "total_hits": 0,
                "last_processed_arxiv_id": None,
                "hits": []
            }

def save_results(data, path):
    """Save the complete results and metadata to a JSON file.

    Args:
        data (dict): Dictionary with metadata and results.
        path (str): Path to the output file.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    """Main processing loop (parallelized with max 5 threads)."""
    data = load_existing_results(JSON_OUTPUT)
    results = data.get("hits", [])
    last_id = data.get("last_processed_arxiv_id")
    hit_counter = data.get("total_hits", len(results))

    # Step 1: Collect papers to process (skip until last ID)
    papers_to_process = []
    found_last = last_id is None

    with open(JSON_INPUT, 'r', encoding='utf-8') as f:
        all_papers = ijson.items(f, 'item')
        for paper in all_papers:
            arxiv_id = paper.get("paper_arxiv_id")
            if not arxiv_id:
                continue
            if not found_last:
                if arxiv_id == last_id:
                    found_last = True
                continue
            papers_to_process.append((arxiv_id, paper))

    total = len(papers_to_process)
    tqdm_bar = tqdm(total=total, desc="Processing papers", leave=True)

    # Step 2: Parallel execution
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_arxiv = {
            executor.submit(search_arxiv, arxiv_id, paper): arxiv_id
            for arxiv_id, paper in papers_to_process
        }

        for future in as_completed(future_to_arxiv):
            tqdm_bar.update(1)
            arxiv_id = future_to_arxiv[future]

            try:
                hits = future.result()
                if hits:
                    for hit in hits:
                        hit_counter += 1
                        tqdm.write(f"\n🔢 [{hit_counter}] 📄 {hit['title']}\n   {hit['snippet']}")
                        results.append(hit)
            except Exception as e:
                tqdm.write(f"❌ Error processing {arxiv_id}: {e}")

            # Always update metadata
            data["hits"] = results
            data["total_hits"] = hit_counter
            data["last_processed_arxiv_id"] = arxiv_id

            if hit_counter % 10 == 0:
                save_results(data, JSON_OUTPUT)

    tqdm_bar.close()
    save_results(data, JSON_OUTPUT)
    print(f"\n✅ Done. Total hits: {hit_counter}")


if __name__ == "__main__":
    main()
