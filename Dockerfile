# Start from the official Prefect base image
FROM prefecthq/prefect:3-latest

# Install all your dependencies
RUN pip install --no-cache-dir \
    boto3 \
    botocore \
    ijson \
    lakefs-sdk \
    minio \
    git+https://github.com/MaRDI4NFDI/mardiclient.git \
    git+https://github.com/MaRDI4NFDI/mardiportal-workflowtools.git

# Set a consistent working directory and logging level (Best Practice)
WORKDIR /opt/prefect
ENV PREFECT_LOGGING_LEVEL=INFO