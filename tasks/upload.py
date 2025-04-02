from utils.LakeClient import LakeClient
from prefect import task, get_run_logger

from utils.secrets_helper import read_credentials
from utils.IPFSClient import IPFSClient

@task
def upload_to_lakefs( path_and_file: str,
                         lakefs_url: str, lakefs_repo: str, lakefs_path:str,
                         secrets_path: str = "secrets.conf" ) -> None:
    """
    Uploads a local database file to a specified path in a lakeFS repository and commits the upload.

    This function reads lakeFS credentials from a secrets file, initializes a LakeClient,
    uploads the file to the given lakeFS path, and creates a commit in the 'main' branch.

    Args:
        path_and_file (str): The local file path (including filename) to upload.
        lakefs_url (str): The URL of the lakeFS instance.
        lakefs_repo (str): The name of the lakeFS repository to upload to.
        lakefs_path (str): The destination path in the lakeFS repository (no file name).
        secrets_path (str, optional): Path to the secrets configuration file containing lakeFS credentials.
            Defaults to "secrets.conf".

    Returns:
        None

    Raises:
        Logs an error and exits early if credentials cannot be read.
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

    # Upload
    logger.info(f"Uploading {path_and_file} to lakeFS ({lakefs_repo} -> main -> {lakefs_path})")
    files_to_upload = [path_and_file]
    client.upload_to_lakefs(files_to_upload, _repo=lakefs_repo, _branch="main", _lakefs_repo_subpath=lakefs_path)

    # Commit
    commit_id = client.commit_to_lakefs(repo=lakefs_repo, branch="main", msg="Upload new DB version", metadata={"source": "mardiKG_paper2code_linker.tasks.upload_db"})
    if commit_id:
        logger.info(f"Commited with ID: {commit_id}")
    else:
        logger.info(f"Not commited - no change detected in DB.")


@task
def upload_to_IPFS(
    path_and_file: str,
    ipfs_api_url: str,
    mfs_path: str,
    secrets_path: str = "secrets.conf"
) -> None:
    """
    Uploads a local file to the IPFS node and tags it under an MFS path.

    The function uploads the file via the IPFS HTTP API (with Basic Auth),
    and stores it at the specified MFS path, replacing any existing file at that path.

    Args:
        path_and_file (str): The local file path to upload.
        ipfs_api_url (str): The base IPFS API URL (e.g., "https://ipfs-admin.portal.mardi4nfdi.de").
        mfs_path (str): MFS path under which the file should be stored (e.g., "/tags/db-latest.db").
        secrets_path (str, optional): Path to the IPFS secrets config file. Defaults to "secrets.conf".

    Raises:
        Exception: If the upload or tagging fails.
    """
    logger = get_run_logger()

    creds = read_credentials("ipfs", secrets_path)
    if not creds:
        logger.error("No valid IPFS credentials found. Please check '%s'", secrets_path)
        return

    client = IPFSClient(
        _host=ipfs_api_url,
        _user=creds["user"],
        _password=creds["password"]
    )

    # Upload & tag
    logger.info(f"Uploading {path_and_file} to IPFS and tagging it as '{mfs_path}'")
    success = client.tag_file(path_and_file, mfs_path)

    if not success:
        logger.error(f"Upload failed or tagging failed for {path_and_file}")
        raise Exception(f"Upload or tagging failed for '{mfs_path}'")

    logger.info(f"Upload and tagging successful: {mfs_path}")