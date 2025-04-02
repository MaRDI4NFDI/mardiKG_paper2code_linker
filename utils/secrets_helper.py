from typing import Optional, Dict

from typing import Optional, Dict

from prefect.blocks.system import Secret



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


def read_mardikg_credentials(path: str) -> Optional[Dict[str, str]]:
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

    # Try to get from file
    try:
        with open(path) as f:
            creds = dict(
                line.strip().split("=", 1)
                for line in f if "=" in line
            )
        if "mardi-kg-user" not in creds or "mardi-kg-password" not in creds:
            return None
        return {"user": creds["mardi-kg-user"], "password": creds["mardi-kg-password"]}
    except Exception:
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
