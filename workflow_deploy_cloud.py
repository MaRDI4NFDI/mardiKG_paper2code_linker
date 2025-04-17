# For execution on a Prefect server the secrets have to be set - see README.MD for details.

# Run this for CLOUD execution:
#   prefect cloud login

# To add a schedule:
#   * Go to "Deployments"
#   * Click on the workflow name
#   * Click on "+ Schedule" (top right corner)

from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/MaRDI4NFDI/mardiKG_paper2code_linker.git",
        entrypoint="workflow_main.py:process_papers",
    ).deploy(
        name="paper2code_linker",
        work_pool_name="CloudWorkerPool",
        parameters={
            "links_file_url": "https://production-media.paperswithcode.com/about/links-between-papers-and-code.json.gz",
            "batch_size": 1000,
            "max_workers": 50,
            "lakefs_url": "https://lake-bioinfmed.zib.de",
            "lakefs_repo": "mardi-workflows-files",
            "lakefs_path_and_file": "mardiKG_paper2code_linker/results.db"
        },
        job_variables={"pip_packages": [
            "boto3",
            "botocore",
            "ijson",
            "lakefs-sdk",
            "minio",
            "git+https://github.com/MaRDI4NFDI/mardiclient.git",
            "git+https://github.com/MaRDI4NFDI/mardiportal-workflowtools.git",
        ]},
    )



