import gzip
import shutil
import os
from pathlib import Path
from typing import Optional, Dict

import requests
from datetime import datetime
from prefect import task, get_run_logger

from utils.LakeClient import LakeClient
from utils.PrefectHelper import read_lakefs_credentials_from_prefect


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
def download_db( db_path_and_file, lakefs_url: str, lakefs_repo: str, lakefs_path_and_file:str, secrets_path: str = "secrets.conf" ) -> None:
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
    secrets = read_lakefs_credentials_from_prefect()
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