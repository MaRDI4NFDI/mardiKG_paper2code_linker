from typing import Optional, Dict

from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret

from tasks.process_pwc_dump import process_pwc_dump
from tasks.storage import init_db
from tasks.download import download_and_unzip_links_file
from tasks.mardi_kg_updates import link_repos_to_mardi_kg
from pathlib import Path
import logging
from prefect.variables import Variable
import json

from utils.LakeClient import LakeClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# BEFORE THIS WORKFLOW CAN BE DEPLOYED TO A PREFECT SERVER:
#  - Create block secrets
#      - mardi-kg-password
#      - mardi-kg-user
#      - lakefs-user
#      - lakefs-password

@flow
def process_papers(
    data_path: str,
    db_file: str,
    links_file_url: str,
    batch_size: int,
    max_workers: int,
    lakefs_url: str,
    lakefs_repo: str,
    lakefs_path_and_file: str
):
    logger = get_run_logger()

    # Set config
    db_path_and_file = str(Path(data_path) / db_file)

    # Check whether data path exists
    Path(data_path).mkdir(parents=True, exist_ok=True)
    logger.info("Ensured data directory exists at: %s", data_path)

    # Check whether database file exists
    if not (Path(data_path) / db_file).exists():
        logger.warning(f"Database file not found at {db_path_and_file}, trying to download...")
        download_db.submit(
            db_path_and_file=str(db_path_and_file),
            lakefs_url=lakefs_url,
            lakefs_repo=lakefs_repo,
            lakefs_path_and_file=lakefs_path_and_file).wait()
    else:
        logger.info(f"Using existing DB file at {db_path_and_file}")

    # Init database
    init_db.submit(path_and_file=db_path_and_file).wait()

    # Download JSON from paperswithcode
    json_input = download_and_unzip_links_file(url=links_file_url)

    logger.info("Using JSON file: %s", json_input)

    # Go through the pwc dump file
    process_pwc_dump.submit(
        db_path=db_path_and_file,
        json_input=json_input,
        batch_size=batch_size,
        max_workers=max_workers
    ).wait()

    # Final step: Link results to MaRDI KG
    logger.info("Starting KG update...")
    link_repos_to_mardi_kg.submit(
        db_path=db_path_and_file,
        max_workers=max_workers
    ).wait()


@task
def download_db( db_path_and_file, lakefs_url: str, lakefs_repo: str, lakefs_path_and_file:str, secrets_path: str = "secrets.conf" ) -> None:

    logger = get_run_logger()

    creds = _read_lakefs_credentials(secrets_path)
    if not creds:
        logger.error("No valid credentials found. Please check '%s'", secrets_path)
        return

    # Initialize LakeFS client
    lakefs_user = creds["user"]
    lakefs_pwd = creds["password"]
    client = LakeClient(lakefs_url, lakefs_user, lakefs_pwd)

    if client.file_exists(lakefs_repo, "main", lakefs_path_and_file):
        logger.info("Found DB file at lakeFS. Downloading...")
        content = client.load_file(lakefs_repo, "main", lakefs_path_and_file)
        if not content:
            logger.error("Failed downloading DB file from lakeFS.")
            raise Exception("Failed downloading DB file from lakeFS.")

    # Save content to local file
    db_path = Path(db_path_and_file)
    with open(db_path, "wb") as f:
        f.write(content)

    logger.info("Successfully saved DB file to '%s'", db_path)


def _read_lakefs_credentials(path: str = "secrets.conf") -> Optional[Dict[str, str]]:
    """Read user credentials either from prefect server (lakefs-user / lakefs-password)
    or from a secrets file.

    Args:
        path (str): Path to the secrets file.

    Returns:
        Optional[Dict[str, str]]: Dictionary with 'user' and 'password' or None if invalid/missing.
    """
    # Try first the built-in mechanism
    secrets = _read_lakefs_credentials_from_prefect()
    if secrets is not None:
        return secrets

    # Try to get from file
    try:
        with open(path, encoding="utf-8") as f:
            creds = {}
            for line in f:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    creds[key.strip()] = value.strip()

        if "lakefs-user" not in creds or "lakefs-password" not in creds:
            return None

        return creds
    except Exception:
        return None


def _read_lakefs_credentials_from_prefect() -> Optional[Dict[str, str]]:
    """Read credentials using Prefect's block system.

    Returns:
        Optional[Dict[str, str]]: Dictionary with 'user' and 'password', or None if secrets could not be loaded.
    """
    try:
        user = Secret.load("lakefs-user").get()
        password = Secret.load("lakefs-password").get()
        return {"user": user, "password": password}
    except Exception as e:
        return None


if __name__ == "__main__":
    process_papers(
        data_path="./data",
        db_file="results.db",
        links_file_url="https://production-media.paperswithcode.com/about/links-between-papers-and-code.json.gz",
        batch_size=1000,
        max_workers=50,
        lakefs_url="https://lake-bioinfmed.zib.de",
        lakefs_repo="mardi-workflows-files",
        lakefs_path_and_file="mardiKG_paper2code_linker/results.db"
    )
