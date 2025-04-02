import logging
from typing import Optional, Dict
from prefect.blocks.system import Secret

# Set basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # or INFO, WARNING, etc.


def read_lakefs_credentials(path: str = "secrets.conf") -> Optional[Dict[str, str]]:
    """Read user lakefs credentials either from prefect server (lakefs-user / lakefs-password)
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

    logger.info("Could not read lakefs credentials from Prefect.")

    # Try to get from file
    try:
        with open(path, encoding="utf-8") as f:
            creds = {}
            for line in f:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    creds[key.strip()] = value.strip()

        if "lakefs-user" not in creds or "lakefs-password" not in creds:
            logger.info(f"Could not read lakefs credentials from {path}.")
            logger.warning("Could not get any credentials for lakefs.")
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


def read_mardikg_credentials(path: str = "secrets.conf") -> Optional[Dict[str, str]]:
    """Read user credentials either from prefect server (mardi-kg-user / mardi-kg-password)
    or from a secrets file.

    Args:
        path (str): Path to the secrets file.

    Returns:
        Optional[Dict[str, str]]: Dictionary with 'user' and 'password' or None if invalid/missing.
    """
    # Try first the built-in mechanism
    secrets = _read_mardikg_credentials_from_prefect()
    if secrets is not None:
        return secrets

    logger.info("Could not read mardiKG credentials from Prefect.")

    # Try to get from file
    try:
        with open(path) as f:
            creds = dict(
                line.strip().split("=", 1)
                for line in f if "=" in line
            )
        if "mardi-kg-user" not in creds or "mardi-kg-password" not in creds:
            logger.info(f"Could not read mardiKG credentials from {path}.")
            logger.warning("Could not get any credentials for mardiKG.")
            return None

        return {"user": creds["mardi-kg-user"], "password": creds["mardi-kg-password"]}

    except Exception:
        logger.warning("Could not get any credentials for mardiKG.")
        return None


def _read_mardikg_credentials_from_prefect() -> Optional[Dict[str, str]]:
    """Read user lakefs credentials either from prefect server (lakefs-user / lakefs-password)
    or from a secrets file.

    Args:
        path (str): Path to the secrets file.

    Returns:
        Optional[Dict[str, str]]: Dictionary with 'mardi-kg-user' and 'mardi-kg-password', or None if secrets could not be loaded.
    """
    try:
        user = Secret.load("mardi-kg-user").get()
        password = Secret.load("mardi-kg-password").get()
        return {"user": user, "password": password}
    except Exception as e:
        return None


def read_ipfs_credentials(path: str = "secrets.conf") -> Optional[Dict[str, str]]:
    """Read user credentials from a secrets file.

    Args:
        path (str): Path to the secrets file.

    Returns:
        Optional[Dict[str, str]]: Dictionary with 'user' and 'password' or None if invalid/missing.
    """
    # Try to get from file
    try:
        with open(path) as f:
            creds = dict(
                line.strip().split("=", 1)
                for line in f if "=" in line
            )
        if "ipfs-user" not in creds or "ipfs-password" not in creds:
            logger.info(f"Could not read ipfs credentials from {path}.")
            logger.warning("Could not get any credentials for ipfs.")
            return None

        return {"user": creds["ipfs-user"], "password": creds["ipfs-password"]}

    except Exception:
        return None