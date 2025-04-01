# Run this for LOCAL execution:
#   prefect config unset PREFECT_API_URL
#   prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
#   prefect server start

from workflow_main import process_papers

if __name__ == "__main__":
    process_papers.serve(
        name="process_papers"
    )
