import gzip
import shutil
import os

import requests
from datetime import datetime
from prefect import task, get_run_logger
from pathlib import Path

from utils.LakeClient import LakeClient
from utils.secrets_helper import read_credentials

from utils.IPFSClient import IPFSClient



@task
def download_and_unzip_links_file(
    url: str = "https://production-media.paperswithcode.com/about/links-between-papers-and-code.json.gz",
    output_dir: str = "./data"
) -> str:
    """Download and unzip the PapersWithCode links JSON file with a timestamped filename.
    If the file already exists for today's date, skip downloading.

    Args:
        url (str): URL of the gzipped JSON file.
        output_dir (str): Directory to store the uncompressed JSON file.

    Returns:
        str: Path to the unzipped JSON file.
    """
    logger = get_run_logger()
    os.makedirs(output_dir, exist_ok=True)

    today_str = datetime.today().strftime("%Y%m%d")
    output_path = os.path.join(output_dir, f"links__{today_str}.json")
    tmp_gz_path = output_path + ".gz"

    if os.path.exists(output_path):
        logger.info("File already exists: %s — skipping download.", output_path)
        return output_path

    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(tmp_gz_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    with gzip.open(tmp_gz_path, "rb") as f_in, open(output_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    os.remove(tmp_gz_path)

    logger.info("Download complete: %s", output_path)
    return output_path



@task
def download_db_lakefs(db_path_and_file, lakefs_url: str, lakefs_repo: str, lakefs_path_and_file:str, secrets_path: str = "secrets.conf") -> None:
    """
    Downloads the database file from a previous run of the flow from a lakeFS repository and
    saves it locally.

    This function checks whether the specified file exists in the given lakeFS repository and,
    if found, downloads its content and writes it to the provided local path. Credentials are
    retrieved from Prefect secret blocks or a local secrets file.

    Args:
        db_path_and_file (str): The local file path (including filename) where the database should be saved.
        lakefs_url (str): The URL of the lakeFS server to connect to.
        lakefs_repo (str): The name of the lakeFS repository where the file is stored.
        lakefs_path_and_file (str): The path (in lakeFS) to the file to be downloaded.
        secrets_path (str, optional): Path to a fallback local secrets file if Prefect block secrets are unavailable. Defaults to "secrets.conf".

    Raises:
        Exception: If the file exists in lakeFS but content could not be retrieved.
        FileNotFoundError: If the file does not exist in the specified lakeFS repository.
    """
    logger = get_run_logger()

    creds = read_credentials("lakefs", secrets_path)
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


@task
def download_db_ipfs(
    db_path_and_file: str,
    ipfs_api_url: str,
    mfs_path: str,
    secrets_path: str = "secrets.conf"
) -> None:
    """
    Downloads a database file from an IPFS node using an MFS tag and saves it locally.

    This function looks up the tagged file (using MFS) and downloads it using the IPFS HTTP API.
    IPFS credentials are retrieved from a secrets file.

    Args:
        db_path_and_file (str): The local file path (including filename) where the DB should be saved.
        ipfs_api_url (str): The base IPFS API URL (e.g. "https://ipfs-admin.portal.mardi4nfdi.de").
        mfs_path (str): The full MFS path to the file (e.g. "/tags/mydb-latest.db").
        secrets_path (str, optional): Path to the IPFS secrets config file. Defaults to "secrets.conf".

    Raises:
        Exception: If download fails for any reason.
    """
    logger = get_run_logger()

    creds = read_credentials("ipfs", secrets_path)
    if not creds:
        logger.error("No valid credentials found. Please check '%s'", secrets_path)
        return

    client = IPFSClient(
        _host=ipfs_api_url,
        _user=creds["user"],
        _password=creds["password"]
    )

    success = client.download_by_tag(mfs_path, db_path_and_file)
    if not success:
        logger.error("Failed to download DB from IPFS path '%s'", mfs_path)
        raise Exception(f"Failed to download DB from IPFS path '{mfs_path}'")

    logger.info("Successfully saved DB file to '%s'", db_path_and_file)
