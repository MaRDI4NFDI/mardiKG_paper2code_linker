import logging
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
from prefect import task, get_run_logger, flow
from mardiclient import MardiClient, MardiItem
from wikibaseintegrator import datatypes
from wikibaseintegrator.models import References, Reference
from wikibaseintegrator.wbi_enums import ActionIfExists
from prefect.blocks.system import Secret


@task
def link_repos_to_mardi_kg(db_path: str = "./data/results.db", secrets_path: str = "secrets.conf") -> None:
    """Update MaRDI KG with repository links from stored search results in the SQLite DB.

    This task reads credentials, loads unprocessed records from the SQLite 'hits' table,
    and for each valid record, adds a 'has companion code repository' (P1687) statement
    to the corresponding MaRDI KG item. Successfully processed entries are marked in the DB.

    Args:
        db_path (str): Path to the results database.
        secrets_path (str): Path to the secrets file.
    """
    logger = get_run_logger()

    # Read username/password from file
    creds = _read_credentials(secrets_path)
    if not creds:
        logger.error("No valid credentials found. Please check '%s'", secrets_path)
        return

    # Initialize MaRDI KG client
    mc = MardiClient(user=creds["user"], password=creds["password"], login_with_bot=True)

    # Get items to be updated from the database
    hits = _load_hits(db_path)
    logger.info("Loaded %d items pending MaRDI update from %s", len(hits), db_path)

    # Process items: for each item, add repo information to KG item
    # (running tasks concurrently using Prefect-native mapping)

    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_hit = {
            executor.submit(_process_hit.fn, hit, db_path, mc): hit
            for hit in hits
        }

        for future in as_completed(future_to_hit):
            hit = future_to_hit[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error while processing hit {hit.get('arxiv_id', '?')}: {e}")
                raise  # optionally re-raise to fail the flow

    duration = time.perf_counter() - start
    logger.info("Finished updating %d items in %.2f seconds", len(hits), duration)


def _process_hit(hit: Dict, db_path: str, mc: MardiClient) -> None:
    """Process a single hit and update the corresponding MaRDI KG item.

    This function checks the presence of necessary fields, constructs a reference,
    adds it to the KG item, and marks the item as updated in the local database.

    Args:
        hit (Dict): A dictionary containing hit information (qid, repo_url, etc.).
        db_path (str): Path to the SQLite database for marking the entry as updated.
        mc (MardiClient): An authenticated MaRDI KG client instance.
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    qid = hit.get("qid")
    repo_url = hit.get("repo_url")
    pwc_url = hit.get("pwc_page")
    mentioned_in_paper = hit.get("mentioned_in_paper", False)
    mentioned_in_github = hit.get("mentioned_in_github", False)

    harvested_from = (
        "publication" if mentioned_in_paper else
        "repository README" if mentioned_in_github else
        "unknown"
    )

    # Update actual KG item and update item in local DB
    if qid and repo_url and pwc_url:
        logger.info(f"Linking {qid} with {repo_url}")
        _update_kg_item_with_repo(mc, qid, repo_url, pwc_url, harvested_from)
        _mark_updated(db_path, hit["arxiv_id"])
    else:
        logger.warning(f"Skipping due to missing fields: {hit}")



def _read_credentials(path: str) -> Optional[Dict[str, str]]:
    """Read user credentials either from prefect server (mardi-kg-user / mardi-kg-password)
    or from a secrets file.

    Args:
        path (str): Path to the secrets file.

    Returns:
        Optional[Dict[str, str]]: Dictionary with 'user' and 'password' or None if invalid/missing.
    """
    # Try first the built-in mechanism
    secrets = _read_credentials_from_prefect()
    if secrets is not None:
        return secrets

    # Try to get from file
    try:
        with open(path) as f:
            creds = dict(
                line.strip().split("=", 1)
                for line in f if "=" in line
            )
        if "user" not in creds or "password" not in creds:
            return None
        return creds
    except Exception:
        return None


def _read_credentials_from_prefect() -> Optional[Dict[str, str]]:
    try:
        user = Secret.load("mardi-kg-user").get()
        password = Secret.load("mardi-kg-password").get()
        return {"user": user, "password": password}
    except Exception as e:
        return None


def _load_hits(db_path: str) -> List[Dict]:
    """Load unprocessed hits from the SQLite DB.

    Args:
        db_path (str): Path to the SQLite database.

    Returns:
        List[Dict]: List of hit entries that have not yet been marked as updated.
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM hits WHERE updated_in_mardi_kg = 0 AND qid IS NOT NULL")
        rows = cur.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    except Exception as e:
        get_run_logger().error(f"Error reading hits from DB: {e}")
        return []


def _mark_updated(db_path: str, arxiv_id: str) -> None:
    """Mark a specific arXiv ID entry as updated in the SQLite database.

    Args:
        db_path (str): Path to the SQLite database.
        arxiv_id (str): The arXiv ID of the hit to mark as updated.
    """
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("UPDATE hits SET updated_in_mardi_kg = 1 WHERE arxiv_id = ?", (arxiv_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        get_run_logger().error(f"Error updating flag for {arxiv_id}: {e}")


def _update_kg_item_with_repo(mc: MardiClient, QID: str, repo_url: str,
                              repo_reference_url: str, harvested_from_label: str) -> None:
    """Add or update a repository link (P1687) on a MaRDI KG item.

    Args:
        mc (MardiClient): Authenticated MardiClient instance.
        QID (str): QID of the MaRDI KG item.
        repo_url (str): URL of the companion code repository.
        repo_reference_url (str): PapersWithCode reference page URL.
        harvested_from_label (str): Description of the source ('publication', 'README', etc.).
    """
    item: MardiItem = mc.item.get(entity_id=QID)

    # Prepare reference:
    #   - Reference: P1688 (PapersWithCode reference URL) = repo_reference_url
    #   - Reference: P1689 (extracted from) = harvested_from_label
    # See also: https://github.com/LeMyst/WikibaseIntegrator?tab=readme-ov-file#manipulate-claim-add-references
    new_references = References()
    new_reference = Reference()

    new_reference.add(datatypes.String(prop_nr='P1688', value=repo_reference_url))
    new_reference.add(datatypes.String(prop_nr='P1689', value=harvested_from_label))

    new_references.add(new_reference)

    # Prepare statement:
    #   - Property: P1687 (has companion code repository)
    #   - Value: url_to_repo
    new_claim = datatypes.String(
        prop_nr='P1687',
        value=repo_url,
        references=new_references
    )

    # Add the claim
    item.claims.add(new_claim, action_if_exists=ActionIfExists.REPLACE_ALL)

    # Write the new data
    item.write()
