import sqlite3
from typing import Set
from typing import List, Dict
from prefect import task, get_run_logger


@task
def init_db(path_and_file: str = "data/results.db") -> None:
    """Initialize the SQLite database with the required tables.

    Args:
        path_and_file (str): Path to the SQLite database file.
    """
    conn = sqlite3.connect(path_and_file)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS hits (
            arxiv_id TEXT PRIMARY KEY,
            qid TEXT,
            title TEXT,
            repo_url TEXT,
            is_official BOOLEAN,
            mentioned_in_paper BOOLEAN,
            mentioned_in_github BOOLEAN,
            pwc_page TEXT,
            snippet TEXT,
            updated_in_mardi_kg BOOLEAN DEFAULT 0
        )
    """)

    conn.commit()
    conn.close()



@task
def insert_hits(hits: List[Dict], path_and_file: str = "data/results.db") -> None:
    """Insert or update a list of search result hits into the database.

    Args:
        hits (List[Dict]): A list of hit records to insert.
        path_and_file (str): Path to the SQLite database file.
    """
    conn = sqlite3.connect(path_and_file)
    cur = conn.cursor()

    for hit in hits:
        hit.setdefault("updated_in_mardi_kg", 0)
        cur.execute("""
            INSERT OR REPLACE INTO hits VALUES (
                :arxiv_id, :qid, :title, :repo_url, :is_official,
                :mentioned_in_paper, :mentioned_in_github, :pwc_page,
                :snippet, :updated_in_mardi_kg
            )
        """, hit)

    conn.commit()
    conn.close()

