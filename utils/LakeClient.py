import os
from typing import Optional, List, Dict, Union

import boto3
import lakefs_sdk
from botocore.config import Config
from lakefs_sdk import Configuration, models, HealthCheckApi, ObjectsApi, AuthApi


class LakeClient:
    """Client for interacting with LakeFS using lakefs_sdk and boto3."""

    def __init__(self, _host: str, _user: str, _password: str) -> None:
        """
        Initialize the LakeFS client.

        Args:
            host (str): LakeFS API host URL.
            user (str): Username or access key.
            password (str): Password or secret key.
        """
        configuration = Configuration(host=_host + "/api/v1", username=_user, password=_password)
        self.lakefs_api_client = lakefs_sdk.ApiClient(configuration)

        # Init botos3 client
        self.s3_client = boto3.client(
            's3',
            endpoint_url=_host,
            aws_access_key_id=_user,
            aws_secret_access_key=_password,
            region_name='us-east-1',
            config=Config(s3={'addressing_style': 'path'})
        )


    def health_check(self) -> bool:
        """
        Perform a health check on the LakeFS server.

        Returns:
            bool: True if healthy, False otherwise.
        """
        try:
            HealthCheckApi(self.lakefs_api_client).health_check()
            print("LakeFS is healthy.")
            return True
        except Exception as e:
            print(f"[health_check] Error: {e}")
            return False

    def file_exists(self, repository: str, ref: str, path: str) -> bool:
        """
        Check if a file exists.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        try:
            ObjectsApi(self.lakefs_api_client).head_object(repository, ref, path)
            print("File exists.")
            return True
        except Exception as e:
            print(f"[file_exists] Error: {e}")
            return False

    def load_file(self, repository: str, ref: str, path: str) -> Optional[str]:
        """
        Load the content of a file.

        Returns:
            Optional[str]: UTF-8 content or None.
        """
        try:
            response = ObjectsApi(self.lakefs_api_client).get_object(repository, ref, path, presign=False)
            return response
        except Exception as e:
            print(f"[load_file] Error: {e}")
            return None

    def list_objects(self, repository: str, ref: str, amount: int = 100) -> Optional[List[Dict[str, Union[str, int]]]]:
        """
        List objects in a reference.

        Returns:
            Optional[List[Dict[str, Union[str, int]]]]: File info list or None.
        """
        try:
            response = ObjectsApi(self.lakefs_api_client).list_objects(
                repository,
                ref,
                presign=False,
                user_metadata=False,
                amount=amount
            )
            return [{"path": obj.path, "size_bytes": obj.size_bytes} for obj in response.results]
        except Exception as e:
            print(f"[list_objects] Error: {e}")
            return None

    def upload_to_lakefs(self, _file_paths: List[str], _repo: str, _branch: str, _lakefs_repo_subpath: str = "") -> List[int]:
        """
        Upload files using boto3.

        Returns:
            List[int]: HTTP status codes for uploads.
        """
        results = []
        for file_path in _file_paths:
            with open(file_path, 'rb') as f:
                filecontent = f.read()

                filename = os.path.basename(file_path)
                if _lakefs_repo_subpath:
                    key = f"{_branch}/{_lakefs_repo_subpath.strip('/')}/{filename}"
                else:
                    key = f"{_branch}/{filename}"

                print(f"Uploading key={key} to repo={repo}...")
                try:
                    response = self.s3_client.put_object(Body=filecontent, Bucket=_repo, Key=key)
                    code = response['ResponseMetadata']['HTTPStatusCode']
                    results.append(code)
                    print(f"Upload done: {code}")
                except Exception as e:
                    print(f"[upload_to_lakefs] Error: {e}")
                    results.append(-1)
        return results

    def commit_to_lakefs(self, repo: str, branch: str, msg: str, metadata: Optional[dict] = None) -> str:
        """
        Commit staged changes to a branch.

        Returns:
            str: Commit ID.
        """
        try:
            commit = models.CommitCreation(message=msg, metadata=metadata)
            response = self.commits_api.commit(repository=repo, branch=branch, commit_creation=commit)
            print(f"Commit successful: {response.id}")
            return response.id
        except Exception as e:
            print(f"[commit_to_lakefs] Error: {e}")
            raise

    def sync_repo_to_local(self, repo: str, branch: str, repo_subpath: str, local_dir: str, overwrite: bool = False) -> None:
        """
        Download files from LakeFS to a local path.

        Args:
            overwrite (bool): Overwrite existing local files. Defaults to False.
        """
        downloaded, skipped = 0, 0
        prefix = f"{branch}/{repo_subpath}".strip('/')

        print(f"Syncing from s3://{repo}/{prefix} to {local_dir}")

        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=repo, Prefix=prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    rel_path = os.path.relpath(key, prefix)
                    local_file = os.path.join(local_dir, rel_path)

                    if os.path.exists(local_file) and not overwrite:
                        skipped += 1
                        continue

                    os.makedirs(os.path.dirname(local_file), exist_ok=True)
                    self.s3_client.download_file(repo, key, local_file)
                    downloaded += 1
                    print(f"Downloaded: {local_file}")

            print(f"Sync complete: {downloaded} downloaded, {skipped} skipped.")
        except Exception as e:
            print(f"[sync_repo_to_local] Error: {e}")
            raise


if __name__ == "__main__":
    # import logging
    # logging.basicConfig(level=logging.DEBUG)

    # --- Setup ---
    host_url = "https://lake-bioinfmed.zib.de"
    username = "xxx"
    password = "xxx"

    client = LakeClient(host_url, username, password)
    repo = "sandbox"
    branch = "main"
    sub_dir =""

    # --- Health check ---
    # client.health_check()

    # --- Check and load file ---
    #path = "example1.txt"
    #if client.file_exists(repo, branch, path):
    #    content = client.load_file(repo, branch, path)
    #    print(content[:200] + "..." if content else "Failed to load file.")

    # --- List files ---
    #files = client.list_objects(repo, branch)
    #if files:
    #    print("First 5 files:")
    #    for f in files[:5]:
    #        print(f)

    # --- Upload example ---
    # files_to_upload = ["c:\\temp\\example1.txt", "c:\\temp\\example2.txt"]
    # client.upload_to_lakefs(files_to_upload, _repo=repo, _branch=branch, _lakefs_repo_subpath=sub_dir)

    # --- Commit example ---
    # commit_id = client.commit_to_lakefs(repo=repo, branch=branch, msg="Upload test files", metadata={"source": "script"})

    # --- Sync to local example ---
    # client.sync_repo_to_local(repo, branch, "uploads", local_dir="./downloads", overwrite=True)
