from prefect import flow, get_run_logger

from tasks.process_pwc_dump import process_pwc_dump
from tasks.storage import init_db
from tasks.download import download_and_unzip_links_file, download_db
from tasks.mardi_kg_updates import link_repos_to_mardi_kg
from tasks.upload_db import upload_db_to_lakefs
from pathlib import Path
import logging

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
    """
    Orchestrates the workflow for updating publication items at the MaRDI Knoledge Graph with
    information of existing code repositories - which is taken from the paperswithcode database.

    This flow:
      - Initializes the local database that stores information from previous runs
        (If the local database doesn't exist, it will be downloaded from lakeFS)
      - Downloads the latest links dump from PapersWithCode
      - Goes through each entry in the dump to check if it exists in the MaRDI Knowledge Graph
      - Updates the found items in the MaRDI Knowledge Graph

    Args:
        data_path (str): Path where intermediate data and the database file should be stored locally.
        db_file (str): Name of the SQLite database file used to store intermediate results.
        links_file_url (str): URL to the compressed PapersWithCode links JSON file.
        batch_size (int): Number of records to process per batch during dump parsing.
        max_workers (int): Maximum number of parallel workers used during processing.
        lakefs_url (str): URL of the lakeFS instance used to fetch the initial database file.
        lakefs_repo (str): lakeFS repository name where the database file is stored.
        lakefs_path_and_file (str): Path to the database file in the lakeFS repository.
    """

    logger = get_run_logger()

    # Set config
    db_path_and_file = str(Path(data_path) / db_file)

    # Check whether data path exists
    Path(data_path).mkdir(parents=True, exist_ok=True)
    logger.info("Ensured data directory exists at: %s", data_path)

    # Download database file if it does not exist
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

    # Link results to MaRDI KG
    logger.info("Starting KG update...")
    link_repos_to_mardi_kg.submit(
        db_path=db_path_and_file,
        max_workers=max_workers
    ).wait()

    # Upload new db file to lakeFS
    logger.info("Upload new DB file to lakeFS...")
    upload_db_to_lakefs.submit(
        db_path_and_file=str(db_path_and_file),
        lakefs_url=lakefs_url,
        lakefs_repo=lakefs_repo,
        lakefs_path_and_file=lakefs_path_and_file).wait()




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
