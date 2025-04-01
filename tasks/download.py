import gzip
import shutil
import os
from pathlib import Path

import requests
from datetime import datetime
from prefect import task, get_run_logger

from utils.LakeClient import LakeClient
from utils.PrefectHelper import _read_lakefs_credentials


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

