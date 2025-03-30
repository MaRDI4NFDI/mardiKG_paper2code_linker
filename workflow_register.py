# Run this:
#   $env:PREFECT_API_URL="http://127.0.0.1:4200/api"
#   prefect worker start --pool "default"

from workflow_main import process_papers  # your @flow-decorated function

if __name__ == "__main__":
    process_papers.serve(
        name="arxiv-pipeline",
        #work_pool_name="default",
        parameters={
            "batch_size": 1000,
            "max_workers": 30
        }
    )
