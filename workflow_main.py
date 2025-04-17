# If you are developing locally, make sure to be on the most current version of the tools lib:
# -> pip install -e ../mardiportal-workflowtools

import getpass
import socket

from prefect import flow, get_run_logger

from tasks.process_pwc_dump import process_pwc_dump
from tasks.storage import init_db
from tasks.download import download_and_unzip_links_file, download_db_lakefs
from tasks.mardi_kg_updates import link_repos_to_mardi_kg
from tasks.upload import upload_to_lakefs
from pathlib import Path
import logging

from utils.logger_helper import configure_prefect_logging_to_file

# Set paths
DATA_PATH="./data" # Path where intermediate data and the database file should be stored locally.
DB_FILE="results.db" # Name of the SQLite database file used to store intermediate results.

# Set basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

@flow
def process_papers(
    links_file_url: str,
    batch_size: int,
    max_workers: int,
    lakefs_url: str,
    lakefs_repo: str,
    lakefs_path_and_file: str
):
    """
    Orchestrates the workflow for updating publication items at the MaRDI Knowledge Graph with
    information of existing code repositories - which is taken from the paperswithcode database.

    This flow:
      - Initializes the local database that stores information from previous runs
        (If the local database doesn't exist, it will be downloaded from lakeFS)
      - Downloads the latest links dump from PapersWithCode
      - Goes through each entry in the dump to check if it exists in the MaRDI Knowledge Graph
      - Updates the found items in the MaRDI Knowledge Graph

    Args:
        links_file_url (str): URL to the compressed PapersWithCode links JSON file.
        batch_size (int): Number of records to process per batch during dump parsing.
        max_workers (int): Maximum number of parallel workers used during processing.
        lakefs_url (str): URL of the lakeFS instance used to fetch the initial database file.
        lakefs_repo (str): lakeFS repository name where the database file is stored.
        lakefs_path_and_file (str): Path to the database file in the lakeFS repository.
    """

    # Configure logging
    logfile_name = "workflow.log.txt"
    configure_prefect_logging_to_file( logfile_name )
    logger = get_run_logger()
    logger.info(f"Starting workflow on system: {socket.gethostname()} by user: {getpass.getuser()}")

    # Set config
    db_path_and_file = str(Path(DATA_PATH) / DB_FILE)

    # Check whether data path exists
    Path(DATA_PATH).mkdir(parents=True, exist_ok=True)
    logger.info("Ensured data directory exists at: %s", DATA_PATH)

    # Download database file if it does not exist
    if not (Path(DATA_PATH) / DB_FILE).exists():
        logger.warning(f"Database file not found at {db_path_and_file}, trying to download...")
        download_db_lakefs.submit(
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

    # Extract only the path part (without the file name) for the destination in lakeFS
    lakefs_path = Path(lakefs_path_and_file).parent.as_posix()
    if lakefs_path == ".":
        lakefs_path = ""  # upload to root of the repo

    upload_to_lakefs.submit(
        path_and_file=str(db_path_and_file),
        lakefs_url=lakefs_url,
        lakefs_repo=lakefs_repo,
        lakefs_path=lakefs_path,
        msg="Upload new DB version"
    ).wait()

    # Upload logfile to lakeFS
    logger.info("Upload logfile to lakeFS...")
    upload_to_lakefs.submit(
        path_and_file=logfile_name,
        lakefs_url=lakefs_url,
        lakefs_repo=lakefs_repo,
        lakefs_path=lakefs_path,
        msg="Upload logs"
    ).wait()

    logger.info("Workflow complete.")



if __name__ == "__main__":
    process_papers(
        links_file_url="https://production-media.paperswithcode.com/about/links-between-papers-and-code.json.gz",
        batch_size=10,
        max_workers=3,
        lakefs_url="https://lake-bioinfmed.zib.de",
        lakefs_repo="mardi-workflows-files",
        lakefs_path_and_file="mardiKG_paper2code_linker/results.db"
    )
