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

def read_credentials(name: str, path: str = "secrets.conf", only_local: bool = False) -> Optional[Dict[str, str]]:
    """
    Read 'user' and 'password' credentials from Prefect secrets or a file.

    Args:
        name (str): The base name for the credentials, e.g., 'lakefs' or 'mardi-kg'.
        path (str): Path to the secrets file.

    Returns:
        Optional[Dict[str, str]]: Dictionary with 'user' and 'password', or None if not found.
    """
    # Try Prefect first - unless deactivated
    if not only_local:
        try:
            user = Secret.load(f"{name}-user").get()
            password = Secret.load(f"{name}-password").get()
            return {"user": user, "password": password}
        except Exception:
            logger.info(f"Could not read {name} credentials from Prefect.")

    # Try file
    try:
        with open(path, encoding="utf-8") as f:
            lines = dict(
                line.strip().split("=", 1)
                for line in f if "=" in line
            )

        user_key = f"{name}-user"
        password_key = f"{name}-password"

        if user_key not in lines or password_key not in lines:
            logger.warning(f"Missing {name} credentials in {path}.")
            return None

        return {"user": lines[user_key], "password": lines[password_key]}
    except Exception:
        logger.warning(f"Could not get {name} credentials from {path}.")
        return None
