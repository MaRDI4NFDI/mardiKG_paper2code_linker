# Run this for CLOUD execution:
#   prefect cloud login

from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/MaRDI4NFDI/mardiKG_paper2code_linker.git",
        entrypoint="workflow_main.py:process_papers",
    ).deploy(
        name="process_papers",
        work_pool_name="CloudWorkerPool",
        job_variables={"pip_packages": [
            "boto3",
            "botocore",
            "ijson",
            "lakefs-sdk",
            "minio",
            "git+https://github.com/MaRDI4NFDI/mardiclient.git"
        ]},
    )



