from pathlib import Path
from typing import Set
from collections import Counter
from prefect import task, get_run_logger
from tasks.fetch import query_mardi_kg
from tasks.storage import insert_hits
import ijson
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


@task
def process_pwc_dump(db_path: str, json_input: str, batch_size: int, max_workers: int) -> None:
    """Main flow to process arXiv papers and store search results in SQLite.

    This function loads paper metadata from a JSON file, searches a MediaWiki
    API for references to arXiv papers, stores matching hits in a SQLite database,
    and updates metadata such as total hits and last processed paper.

    Args:
        json_input (str): Path to the JSON input file.
        db_path (str): Path to the SQLite database.
        batch_size (int): Number of items per batch.
        max_workers (int): Number of threads for parallel requests.
    """

    # Set logging
    logger = get_run_logger()

    # Load already-processed arXiv IDs from DB
    existing_ids = _load_existing_arxiv_ids(path=db_path)
    logger.info("Found %d arXiv IDs already processed in DB.", len(existing_ids))

    skip_count = 0
    papers_to_process = []

    logger.info("Loading papers from %s …", json_input)

    # Check if the downloaded file actually exists
    if not Path(json_input).exists():
        logger.error("Required file does not exist")
        raise FileNotFoundError(f"Input file not found: {json_input}")

    # Load paperswithcode dump file and skip the already processed ones
    with open(json_input, 'r', encoding='utf-8') as f:
        all_papers = ijson.items(f, 'item')

        for paper in all_papers:
            arxiv_id = paper.get("paper_arxiv_id")
            if not arxiv_id:
                continue

            if arxiv_id in existing_ids:
                skip_count += 1
                continue  # skip until we reach the last processed one

            papers_to_process.append((arxiv_id, paper))

    logger.info("Collected %d papers to process (skipped %d - might be higher, because of duplicate arXiv IDs)", len(papers_to_process), skip_count)
    logger.info("Processing %d papers in batches of %d …", len(papers_to_process), batch_size)

    # For each entry in paperswithcode dump file:
    #   - query MaRDI KG whether paper is available
    #   - if yes: add to "hits" database
    batch_count = 0
    start_time = time.perf_counter()
    total_papers = len(papers_to_process)

    for batch in _batchify(papers_to_process, batch_size):
        batch_count += 1
        batch_hits = []
        start = time.perf_counter()

        logger.info("Processing batch #%d - total items so far: %d", batch_count, (batch_count*batch_size))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_arxiv = {
                executor.submit(query_mardi_kg.fn, arxiv_id, paper): arxiv_id
                for arxiv_id, paper in batch
            }

            for future in as_completed(future_to_arxiv):
                arxiv_id = future_to_arxiv[future]

                try:
                    hits = future.result()
                    if hits:
                        batch_hits.extend(hits)
                except Exception as e:
                    logger.error("Error while processing %s: %s", arxiv_id, e)
                    # Raise again to fail the whole flow
                    raise

        if batch_hits:
            insert_hits.submit(batch_hits, path_and_file=db_path).wait()

        end = time.perf_counter()
        duration = end - start

        papers_processed = batch_count * batch_size
        elapsed = end - start_time
        est_total = (elapsed / papers_processed) * total_papers if papers_processed else 0
        est_remaining = est_total - elapsed

        logger.info(
            "Batch # %d completed in %.3f sec — estimated time left: %.1f min",
            batch_count, duration, est_remaining / 60
        )

    logger.info("Done.")


def _batchify(items, size):
    """Yield successive batches from a list of items.

    Args:
        items (list): The list of items to batch.
        size (int): The maximum number of items per batch.

    Yields:
        list: A sublist of items of length `size` (except possibly the last batch).
    """
    for i in range(0, len(items), size):
        yield items[i:i + size]



def _load_existing_arxiv_ids(path: str = "data/results.db") -> Set[str]:
    """Fetch all arXiv IDs already in the hits table.

    Args:
        path (str): Path to the SQLite database.

    Returns:
        Set[str]: A set of arXiv IDs already processed.
    """
    import sqlite3
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("SELECT arxiv_id FROM hits")
    rows = cur.fetchall()
    conn.close()
    return {row[0] for row in rows}
