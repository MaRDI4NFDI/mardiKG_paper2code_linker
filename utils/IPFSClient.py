import json
import logging

import requests
from typing import Optional

from utils.secrets_helper import read_credentials

# Set basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # or INFO, WARNING, etc.


class IPFSClient:
    def __init__(self, _host: str, _user: str, _password: str) -> None:
        """
        Initializes the IPFS client with host and Basic Auth credentials.

        Args:
            _host (str): Base URL of the IPFS API (e.g., "https://ipfs.myportal.de").
            _user (str): Username for Basic Authentication.
            _password (str): Password for Basic Authentication.
        """
        self.api_base = _host.rstrip("/") + "/api/v0"
        self.auth = (_user, _password)

    def add_file(self, file_path: str, cid_version: int = 1, pin: bool = False) -> Optional[str]:
        """
        Uploads a file to the IPFS node.

        Args:
            file_path (str): Path to the file to upload.
            cid_version (int, optional): CID version to use (0 or 1). Defaults to 1.
            pin (bool, optional): Whether to pin the file after upload. Defaults to False.

        Returns:
            Optional[str]: The resulting CID (hash) of the uploaded file, or None if the upload fails.
        """
        import json

        params = {
            "cid-version": str(cid_version),
            "pin": str(pin).lower()
        }
        url = f"{self.api_base}/add"

        try:
            with open(file_path, "rb") as f:
                files = {"file": (file_path, f)}
                res = requests.post(url, params=params, files=files, auth=self.auth)
            res.raise_for_status()

            # Handle multi-line JSON output (common for binary files or directories)
            first_line = res.text.strip().splitlines()[0]
            cid = json.loads(first_line)["Hash"]

            logger.info(f"Uploaded: {file_path} → CID: {cid}")
            return cid
        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            return None


    def get_gateway_url(self, cid: str, gateway_host: Optional[str] = None) -> str:
        """
        Constructs the public gateway URL for accessing a file via its CID.

        Args:
            cid (str): The content identifier (CID) returned after uploading a file.
            gateway_host (Optional[str], optional): Custom gateway host (e.g., "https://my-gateway.com").
                Defaults to "https://ipfs.portal.mardi4nfdi.de".

        Returns:
            str: The complete URL to access the file via the public IPFS gateway.
        """
        base = gateway_host.rstrip("/") if gateway_host else "https://ipfs.portal.mardi4nfdi.de"
        return f"{base}/ipfs/{cid}"


    def download_file(self, cid: str, destination_path: str, gateway_host: Optional[str] = None) -> bool:
        """
        Downloads a file from IPFS via the public gateway and saves it locally.

        Args:
            cid (str): The content identifier (CID) of the file.
            destination_path (str): Local path to save the downloaded file.
            gateway_host (Optional[str], optional): Custom IPFS gateway host.
                Defaults to "https://ipfs.portal.mardi4nfdi.de".

        Returns:
            bool: True if the download succeeded, False otherwise.
        """
        url = self.get_gateway_url(cid, gateway_host)

        try:
            res = requests.get(url, stream=True)
            res.raise_for_status()

            with open(destination_path, "wb") as f:
                for chunk in res.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            logger.info(f"Downloaded CID {cid} to {destination_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading CID {cid}: {e}")
            return False

    def pin(self, cid: str) -> bool:
        """
        Pins a CID to your IPFS node to ensure it is retained.

        Args:
            cid (str): The content identifier (CID) to pin.

        Returns:
            bool: True if the pin was successful, False otherwise.
        """
        url = f"{self.api_base}/pin/add"
        params = {"arg": cid}

        try:
            res = requests.post(url, params=params, auth=self.auth)
            res.raise_for_status()
            logger.info(f"Pinned CID: {cid}")
            return True
        except Exception as e:
            logger.error(f"Error pinning CID {cid}: {e}")
            return False


    def unpin(self, cid: str) -> bool:
        """
        Unpins a CID from your IPFS node.

        Args:
            cid (str): The content identifier to unpin.

        Returns:
            bool: True if successful, False otherwise.
        """
        url = f"{self.api_base}/pin/rm"
        params = {"arg": cid}

        try:
            res = requests.post(url, params=params, auth=self.auth)
            res.raise_for_status()
            logger.info(f"Unpinned CID: {cid}")
            return True
        except Exception as e:
            logger.error(f"Error unpinning CID {cid}: {e}")
            return False

    def run_gc(self) -> bool:
        """
        Triggers garbage collection on the IPFS node.

        Returns:
            bool: True if successful, False otherwise.
        """
        url = f"{self.api_base}/repo/gc"

        try:
            res = requests.post(url, auth=self.auth)
            res.raise_for_status()
            logger.info("Garbage collection triggered.")
            return True
        except Exception as e:
            logger.error(f"Error running garbage collection: {e}")
            return False

    def list_pins(self, pin_type: str = "recursive") -> Optional[dict]:
        """
        Lists pinned CIDs on the IPFS node.

        Args:
            pin_type (str): Type of pins to list: "all", "recursive", "direct", "indirect". Defaults to "recursive".

        Returns:
            Optional[dict]: Dictionary of pinned CIDs and their types, or None on error.
        """
        url = f"{self.api_base}/pin/ls"
        params = {"type": pin_type}

        try:
            res = requests.post(url, params=params, auth=self.auth)  # POST instead of GET
            res.raise_for_status()
            pins = res.json().get("Keys", {})
            logger.info(f"Retrieved {len(pins)} pinned CIDs (type={pin_type})")
            return pins
        except Exception as e:
            logger.error(f"Error listing pins: {e}")
            return None



    def list_local_refs(self) -> Optional[list]:
        """
        Lists all local CIDs stored on the node (pinned and unpinned).

        Returns:
            Optional[list]: List of CIDs stored locally, or None on error.
        """
        url = f"{self.api_base}/refs/local"

        try:
            res = requests.post(url, auth=self.auth, stream=True)
            res.raise_for_status()

            # It's NDJSON (newline-delimited JSON), one CID per line
            cids = [json.loads(line)["Ref"] for line in res.iter_lines() if line]
            logger.info(f"Found {len(cids)} local CIDs")
            return cids
        except Exception as e:
            logger.error(f"Error listing local refs: {e}")
            return None


    def mkdir_mfs(self, path: str) -> bool:
        """
        Creates a directory in MFS if it doesn't already exist.

        Args:
            path (str): The MFS directory path to create (e.g., "/tags").

        Returns:
            bool: True if successful or already exists, False on error.
        """
        url = f"{self.api_base}/files/mkdir"
        params = {"arg": path, "parents": "true"}

        try:
            res = requests.post(url, params=params, auth=self.auth)
            res.raise_for_status()
            logger.info(f"Ensured MFS directory exists: {path}")
            return True
        except Exception as e:
            logger.error(f"Error creating MFS directory {path}: {e}")
            return False

    def remove_mfs_path(self, mfs_path: str) -> bool:
        """
        Removes a file or directory from MFS.

        Args:
            mfs_path (str): Path in MFS to remove.

        Returns:
            bool: True if successful or doesn't exist, False otherwise.
        """
        url = f"{self.api_base}/files/rm"
        params = {"arg": mfs_path, "force": "true"}

        try:
            res = requests.post(url, params=params, auth=self.auth)
            res.raise_for_status()
            logger.info(f"Removed existing MFS path: {mfs_path}")
            return True
        except requests.HTTPError as e:
            if e.response.status_code == 500 and "does not exist" in e.response.text:
                logger.debug(f"MFS path {mfs_path} did not exist.")
                return True
            logger.error(f"Error removing MFS path {mfs_path}: {e}")
            return False



    def tag_file(self, cid: str, mfs_path: str, overwrite: bool = False) -> bool:
        """
        Tags an existing uploaded CID in MFS by creating a virtual path (like a symbolic link).

        Args:
            cid (str): The IPFS CID to tag.
            mfs_path (str): Path in MFS to assign (e.g. "/tags/myfile.txt").
            overwrite (bool): If True, removes existing path before tagging. Defaults to False.

        Returns:
            bool: True if tagging succeeded, False otherwise.
        """
        url_cp = f"{self.api_base}/files/cp"
        params_cp = [("arg", f"/ipfs/{cid}"), ("arg", mfs_path)]

        try:
            # Create parent directories if needed
            parent_dir = "/" + "/".join(mfs_path.strip("/").split("/")[:-1])
            if parent_dir:
                self.mkdir_mfs(parent_dir)

            # Overwrite existing path if requested
            if overwrite:
                self.remove_mfs_path(mfs_path)

            # Attempt to create the MFS link
            res = requests.post(url_cp, params=params_cp, auth=self.auth)
            res.raise_for_status()

            logger.info(f"Tagged CID {cid} as MFS path {mfs_path}")
            return True

        except requests.HTTPError as e:
            if e.response.status_code == 500 and "file already exists" in e.response.text.lower():
                logger.warning(f"MFS path {mfs_path} already exists. Use overwrite=True to replace.")
                return False
            logger.error(f"Error tagging CID in MFS: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error tagging CID: {e}")
            return False



    def download_by_tag(self, mfs_path: str, destination_path: str) -> bool:
        """
        Downloads a file from MFS using its virtual path and saves it locally.

        Args:
            mfs_path (str): Path in MFS (e.g., "/tags/myfile.txt").
            destination_path (str): Local file path to save the content.

        Returns:
            bool: True if download succeeded, False otherwise.
        """
        url = f"{self.api_base}/files/read"
        params = {"arg": mfs_path}

        try:
            res = requests.post(url, params=params, auth=self.auth, stream=True)
            res.raise_for_status()

            with open(destination_path, "wb") as f:
                for chunk in res.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            logger.info(f"Downloaded {mfs_path} -> {destination_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading from MFS path {mfs_path}: {e}")
            return False

    def list_tags(self, mfs_dir: str = "/tags") -> Optional[dict]:
        """
        Lists all MFS tags under the given directory with their corresponding CIDs, sizes, and modification times.

        Args:
            mfs_dir (str): MFS directory to list (default is "/tags").

        Returns:
            Optional[dict]: Mapping of full MFS path (str) → metadata dict with keys:
                            - "cid" (str): CID the path points to
                            - "size" (int): File size in bytes
                            - "mtime" (int): Last modified time (Unix timestamp)
        """
        url_ls = f"{self.api_base}/files/ls"
        params_ls = {"arg": mfs_dir, "long": "true"}

        try:
            res = requests.post(url_ls, params=params_ls, auth=self.auth)
            res.raise_for_status()
            entries = res.json().get("Entries", [])

            tag_info = {}

            for entry in entries:
                name = entry["Name"]
                full_path = f"{mfs_dir.rstrip('/')}/{name}"

                url_stat = f"{self.api_base}/files/stat"
                params_stat = {"arg": full_path}

                res_stat = requests.post(url_stat, params=params_stat, auth=self.auth)
                res_stat.raise_for_status()
                stat = res_stat.json()

                tag_info[full_path] = {
                    "cid": stat["Hash"],
                    "size": stat.get("Size", 0),
                    "mtime": stat.get("Mtime", 0)
                }

            logger.info(f"Found {len(tag_info)} tags in {mfs_dir}")
            return tag_info

        except Exception as e:
            logger.error(f"Error listing tags in MFS directory {mfs_dir}: {e}")
            return None




if __name__ == "__main__":

    logger.info("Getting credentials...")

    creds = read_credentials("ipfs", "../secrets.conf", only_local=True)
    client = IPFSClient(
        _host="https://ipfs-admin.portal.mardi4nfdi.de",
        _user=creds["user"],
        _password=creds["password"]
    )

    pins = client.list_pins()
    if pins:
        for cid, info in pins.items():
            print(f"{cid} ({info['Type']})")

    tags = client.list_tags()
    if tags:
        for path, meta in tags.items():
            print(f"{path} -> CID: {meta['cid']}, Size: {meta['size']} bytes, Modified: {meta['mtime']}")



    cid = client.add_file("../README.md", cid_version=1, pin=True)

    if cid:
        logger.info(f"Public URL: {client.get_gateway_url(cid)}")

        # Tag it in MFS
        tag_path = "/tags/readme-latest.md"
        if client.tag_file(cid, tag_path, overwrite=True):
            logger.info(f"Tagged {cid} as {tag_path}")

        if client.tag_file(cid, tag_path, overwrite=True):
            logger.info(f"Tagged {cid} as {tag_path}")

        # Download the file by cid
        client.download_file(cid, "../downloaded-by-cid.md")

        # Download it by tag
        client.download_by_tag(tag_path, "../downloaded-by-tag.md")
