from mardiclient import MardiClient, MardiItem
from wikibaseintegrator import datatypes
from wikibaseintegrator.wbi_enums import ActionIfExists

from utils.secrets_helper import read_credentials


def change_kg_item(mc: MardiClient, dataset_QID: str, publication_QID: str) -> None:
    """
    Updates the MaRDI Knowledge Graph:
       * Create a citation link (P223; cites work) from a publication to a dataset
       * Removes the (incorrect reverse P223) link from the dataset to the publication.

    Args:
        mc (MardiClient): Authenticated MardiClient instance.
        dataset_QID (str): QID of the dataset being cited.
        publication_QID (str): QID of the publication that cites the dataset.

    Returns:
        None
    """

    # Get QID of publication that is citing the dataset
    publication_item: MardiItem = mc.item.get(entity_id=publication_QID,retry_after=2)

    # Create the new P223 (cites work) claim to the publication
    new_claim = datatypes.Item(
        prop_nr='P223',
        value=dataset_QID
    )

    # Add the claim (append it without removing existing ones)
    publication_item.claims.add(new_claim, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

    # Write the new data
    publication_item.write(retry_after=2)

    # Remove P223 claims from the dataset item (i.e., dataset citing publication — wrong direction)
    dataset_item: MardiItem = mc.item.get(entity_id=dataset_QID, retry_after=2)

    for claim in dataset_item.claims.get('P223'):
        if claim.mainsnak.datavalue['value']['id'] == publication_QID:
            claim.remove()

    dataset_item.write(retry_after=2)



if __name__ == "__main__":

    secrets_path = ".."
    creds = read_credentials("mardi-kg", secrets_path)
    if not creds:
        raise Exception("No valid credentials found. Please check '%s'", secrets_path)

    # Initialize MaRDI client
    mc = MardiClient(user=creds["user"], password=creds["password"], login_with_bot=True)

    # List of (dataset_QID, citedWork_QID)
    qid_pairs = [
        ("Q6693580", "Q6767924"),
        ("Q6691664", "Q6767925"),
        ("Q6767928", "Q6767927"),
        ("Q6767932", "Q6767930"),
    ]

    for dataset_qid, publication_qid in qid_pairs:
        print(f"Updating: publication {publication_qid} cites dataset {dataset_qid}")
        change_kg_item(mc, dataset_qid, publication_qid)

    # change_kg_item(mc, "Q6767937", "Q6767927")
