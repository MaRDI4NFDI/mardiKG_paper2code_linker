import json
import logging
from mardiclient import MardiClient, MardiItem
from wikibaseintegrator import datatypes
from wikibaseintegrator.models import References, Reference
import sys

from wikibaseintegrator.wbi_enums import ActionIfExists

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_credentials( path: str="secrets.conf" ):
    """
    Reads credentials from a secrets file in key=value format.

    Args:
        path (str): Path to the secrets file. Defaults to 'secrets.conf'.

    Returns:
        dict | None: Dictionary with 'user' and 'password' if found, otherwise None.
    """
    try:
        credentials = {}

        # Read file
        with open(path) as f:
            for line in f:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    credentials[key.strip()] = value.strip()

        # Validate required keys
        if not credentials.get("user") or not credentials.get("password"):
            logger.error(f"Secrets file '{path}' is missing 'user' or 'password' entries.")
            return None

        return credentials
    except FileNotFoundError:
        print(f"Secrets file '{path}' not found. Exiting.")
        return None

def show_item( mc: MardiClient, QID: str ) -> None:
    """
    Fetches item and prints its data.

    Args:
        mc (MardiClient): Authenticated MardiClient instance.
        QID (str): QID of the item to display.
    """
    item: MardiItem = mc.item.get(entity_id=QID)
    print( item.labels.get() )
    print( item.descriptions.get() )
    print( item.get_value("P1687") )
    print( item.get_json() )


def add_repo_to_item( mc: MardiClient, QID: str, repo_url: str, repo_reference_url: str, harvested_from_label: str ) -> None:
    """
    Adds or replaces a 'has companion code repository' (P1687) statement on a KG publication item,
    including references ("PapersWithCode page" (P1688) and "extracted from" (P1689) ).

    Args:
        mc (MardiClient): Authenticated MardiClient instance.
        QID (str): QID of the target item.
        repo_url (str): The URL of the companion code repository.
        repo_reference_url (str): The URL of the reference source (e.g., PapersWithCode page).
        harvested_from_label (str): Description of where the information was extracted from (e.g., 'publication').
    """
    item: MardiItem = mc.item.get(entity_id=QID)

    # Prepare reference:
    #   - Reference: P1688 (PapersWithCode reference URL) = repo_reference_url
    #   - Reference: P1689 (extracted from) = harvested_from_label
    # See also: https://github.com/LeMyst/WikibaseIntegrator?tab=readme-ov-file#manipulate-claim-add-references
    new_references = References()
    new_reference = Reference()

    new_reference.add(datatypes.String(prop_nr='P1688', value=repo_reference_url))
    new_reference.add(datatypes.String(prop_nr='P1689', value=harvested_from_label))

    new_references.add(new_reference)

    # Prepare statement:
    #   - Property: P1687 (has companion code repository)
    #   - Value: url_to_repo
    new_claim = datatypes.String(
        prop_nr='P1687',
        value=repo_url,
        references=new_references
    )

    # Add the claim
    item.claims.add(new_claim, action_if_exists=ActionIfExists.REPLACE_ALL)

    # Write the new data
    item.write()

def load_link_info_from_json(path: str) -> list[dict]:
    """
    Loads linking metadata from a JSON file.

    Args:
        path (str): Path to the JSON file.

    Returns:
        list[dict]: List of item dictionaries containing metadata for linking.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("hits", [])  # <-- This is the key
    except Exception as e:
        logger.error(f"Failed to read JSON file '{path}': {e}")
        return []


def main():
    """
    Main processing loop:
    - Reads credentials
    - Initializes MardiClient
    - Loads linking data from JSON
    - Updates publication items in the MaRDI KG with linked repositories
    """

    # Get credentials
    creds = read_credentials()
    if creds is None:
        logger.error(f"No valid credentials found. Please make sure the 'secrets.conf' file exists and has a valid entry.")
        sys.exit(1)

    # Setup mardi client
    mc = MardiClient(user=creds["user"], password=creds["password"], login_with_bot=True)

    # Load linking data
    hits = load_link_info_from_json("./data/results.json")

    # Add link to each publication
    count = 0
    for hit in hits:
        count += 1

        qid = hit.get("qid")
        repo_url = hit.get("repo_url")
        pwc_url = hit.get("pwc_page")
        mentioned_in_paper = hit.get("mentioned_in_paper", False)
        mentioned_in_github = hit.get("mentioned_in_github", False)

        # Determine source of extraction
        if mentioned_in_paper:
            harvested_from_label = "publication"
        elif mentioned_in_github:
            harvested_from_label = "repository README"
        else:
            harvested_from_label = "unknown"

        # If all data is available: change item on MaRDI KG
        if qid and repo_url and pwc_url:
            logger.info(f"[{count}] Updating item {qid} with repo: {repo_url} and reference: {pwc_url}")
            add_repo_to_item(mc, QID=qid, repo_url=repo_url, repo_reference_url=pwc_url, harvested_from_label=harvested_from_label)
        else:
            logger.warning(f"Skipping entry due to missing values: {hit}")


if __name__ == "__main__":
    main()

# https://portal.mardi4nfdi.de/w/index.php?title=Publication:1111149&action=purge