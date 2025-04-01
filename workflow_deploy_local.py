# Run this for LOCAL execution:
#   prefect config unset PREFECT_API_URL
#   prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
#   prefect server start

from workflow_main import process_papers

if __name__ == "__main__":
    process_papers.serve(
        name="process_papers",
        parameters={
            "data_path": "./data",
            "db_file": "results.db",
            "links_file_url": "https://production-media.paperswithcode.com/about/links-between-papers-and-code.json.gz",
            "batch_size": 1000,
            "max_workers": 50,
            "lakefs_url": "https://lake-bioinfmed.zib.de",
            "lakefs_repo": "mardi-workflows-files",
            "lakefs_path_and_file": "mardiKG_paper2code_linker/results.db"
        }
    )
