from prefect import flow, get_run_logger
from tasks.process_pwc_dump import process_pwc_dump
from tasks.storage import init_db
from tasks.download import download_and_unzip_links_file
from tasks.mardi_kg_updates import link_repos_to_mardi_kg
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

DATA_PATH = "./data"
DB_PATH = DATA_PATH + "/results.db"
LINKS_FILE_URL = "https://production-media.paperswithcode.com/about/links-between-papers-and-code.json.gz"
BATCH_SIZE = 1000
MAX_WORKERS = 50


@flow
def process_papers(batch_size=BATCH_SIZE, max_workers=MAX_WORKERS):

    logger = get_run_logger()

    # Check whether data path exists
    Path(DATA_PATH).mkdir(parents=True, exist_ok=True)
    logger.info("Ensured data directory exists at: %s", DATA_PATH)

    # Init database and get meta data
    init_db.submit(path=DB_PATH).wait()

    # Download JSON from paperswithcode
    json_input = download_and_unzip_links_file(url=LINKS_FILE_URL)

    logger.info("Using JSON file: %s", json_input)

    # Go through the pwc dump file
    process_pwc_dump.submit(
        db_path=DB_PATH,
        json_input=json_input,
        batch_size=batch_size,
        max_workers=max_workers
    ).wait()

    # Final step: Link results to MaRDI KG
    logger.info("Starting KG update...")
    link_repos_to_mardi_kg(db_path=DB_PATH)


if __name__ == "__main__":
    process_papers()
