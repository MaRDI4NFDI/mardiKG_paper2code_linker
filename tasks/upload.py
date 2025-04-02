from utils.secrets_helper import read_lakefs_credentials
from utils.LakeClient import LakeClient
from prefect import task, get_run_logger

@task
def upload_to_lakefs( db_path_and_file: str,
                         lakefs_url: str, lakefs_repo: str, lakefs_path:str,
                         secrets_path: str = "secrets.conf" ) -> None:
    """
    Uploads a local database file to a specified path in a lakeFS repository and commits the upload.

    This function reads lakeFS credentials from a secrets file, initializes a LakeClient,
    uploads the file to the given lakeFS path, and creates a commit in the 'main' branch.

    Args:
        db_path_and_file (str): The local file path (including filename) to upload.
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

    creds = read_lakefs_credentials(secrets_path)
    if not creds:
        logger.error("No valid credentials found. Please check '%s'", secrets_path)
        return

    # Initialize LakeFS client
    lakefs_user = creds["user"]
    lakefs_pwd = creds["password"]
    client = LakeClient(lakefs_url, lakefs_user, lakefs_pwd)

    # Upload
    logger.info(f"Uploading {db_path_and_file} to lakeFS ({lakefs_repo} -> main -> {lakefs_path})")
    files_to_upload = [db_path_and_file]
    client.upload_to_lakefs(files_to_upload, _repo=lakefs_repo, _branch="main", _lakefs_repo_subpath=lakefs_path)

    # Commit
    commit_id = client.commit_to_lakefs(repo=lakefs_repo, branch="main", msg="Upload new DB version", metadata={"source": "mardiKG_paper2code_linker.tasks.upload_db"})
    logger.info(f"Commited with ID: {commit_id}")
