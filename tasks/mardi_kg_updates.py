import sqlite3
from typing import List, Dict, Optional
from prefect import task, get_run_logger
from mardiclient import MardiClient, MardiItem
from wikibaseintegrator import datatypes
from wikibaseintegrator.models import References, Reference
from wikibaseintegrator.wbi_enums import ActionIfExists
from prefect.blocks.system import Secret
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

DB_LOCK = threading.Lock()


@task
def link_repos_to_mardi_kg(
    db_path: str = "./data/results.db",
    secrets_path: str = "secrets.conf",
    max_workers: int = 5
) -> None:
    """Update MaRDI KG with repository links from stored search results in the SQLite DB.

    This task reads credentials, loads unprocessed records from the SQLite 'hits' table,
    and for each valid record, adds a 'has companion code repository' (P1687) statement
    to the corresponding MaRDI KG item. Successfully processed entries are marked in the DB.

    Args:
        db_path (str): Path to the results database.
        secrets_path (str): Path to the secrets file.
        max_workers (int): Number of threads to use for parallel processing.
    """
    logger = get_run_logger()

    creds = _read_credentials(secrets_path)
    if not creds:
        logger.error("No valid credentials found. Please check '%s'", secrets_path)
        return

    mc = MardiClient(user=creds["user"], password=creds["password"], login_with_bot=True)
    hits = _load_hits(db_path)

    logger.info("Loaded %d items pending MaRDI update from %s", len(hits), db_path)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_process_single_kg_update, hit, mc, db_path, idx + 1): hit["arxiv_id"]
            for idx, hit in enumerate(hits)
        }

        for future in as_completed(futures):
            arxiv_id = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error("❌ Failed to process %s: %s", arxiv_id, e)


def _process_single_kg_update(hit: Dict, mc: MardiClient, db_path: str, count: int):
    logger = get_run_logger()

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

    if qid and repo_url and pwc_url:
        logger.info(f"[{count}] Linking {qid} with {repo_url}")
        _update_kg_item_with_repo(mc, qid, repo_url, pwc_url, harvested_from)
        with DB_LOCK:
            _mark_updated(db_path, hit["arxiv_id"])
    else:
        logger.warning(f"[{count}] Skipping due to missing fields: {hit}")


def _read_credentials(path: str) -> Optional[Dict[str, str]]:
    secrets = _read_credentials_from_prefect()
    if secrets is not None:
        return secrets

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
    except Exception:
        return None


def _load_hits(db_path: str) -> List[Dict]:
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
    item: MardiItem = mc.item.get(entity_id=QID)

    new_references = References()
    new_reference = Reference()
    new_reference.add(datatypes.String(prop_nr='P1688', value=repo_reference_url))
    new_reference.add(datatypes.String(prop_nr='P1689', value=harvested_from_label))
    new_references.add(new_reference)

    new_claim = datatypes.String(
        prop_nr='P1687',
        value=repo_url,
        references=new_references
    )

    item.claims.add(new_claim, action_if_exists=ActionIfExists.REPLACE_ALL)
    item.write()
