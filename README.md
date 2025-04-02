# MaRDI-KG Paper2Code Linker

This project automates the process of linking arXiv papers with their companion code repositories and updating 
the [MaRDI Knowledge Graph (KG)](https://portal.mardi4nfdi.de/wiki/Service:MaRDI_KG). It combines metadata 
from [PapersWithCode](https://paperswithcode.com/about) and updates MaRDI KG items using 
the [MaRDI Client](https://github.com/MaRDI4NFDI/mardiclient). 

It is implemented as a [Prefect](https://docs.prefect.io/v3/get-started/index) workflow and can be run standalone, on a local Prefect server 
or on the [Prefect Cloud](https://www.prefect.io/cloud).

### Main Workflow

1. Downloads the latest PapersWithCode JSON dump
2. Searches for the corresponding arXiv entries in the MaRDI KG
3. Updates mathcing MaRDI KG items with the companion code repository information

---

## Installation

- Clone the Git repository
- Create a virtual environment
- Install the dependencies: `pip install -r requirements.txt`
- Optional: [LakeFS](https://lakefs.io/) instance that stores the local database between runs

## Running Locally (Standalone)

- Create secrets file (see below)
- Run `python workflow_main.py`

## Running on a Local Prefect Server

#### Prepare Your local Prefect Environment (ONLY ONCE)
- Connect the server to your local environment: 
  `prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api`
- Start the server: `prefect server start`
- Create secrets at the Prefect server (ONLY ONCE) using [Block secrets](https://docs.prefect.io/v3/develop/blocks)

#### Deploy and Run 
- Run `python workflow_deploy_local.py`
- Go to the local web ui -> _Deployments_ -> Run the workflow

## Running on a Prefect Cloud Server

#### Prepare Your Prefect Cloud Environment (ONLY ONCE)

- Create an account at the Prefect Cloud
- Create a _WorkPool_ in the cloud web ui
- Create an API key in the cloud web ui
- Connect your local environment (within your virtual Python environment): 
   - `prefect cloud login -k APIKEY`
   - `prefect cloud login`
- Create secrets at the Prefect server using [Block secrets](https://docs.prefect.io/v3/develop/blocks)

#### Deploy and Run 
- Run `python workflow_deploy_cloud.py`
- Go to the web ui -> _Deployments_ -> Run the workflow


## Secrets
You need to have these key/value pairs, either in a local 
_secrets.config_ file (for local execution) or as Block secrets at the 
Prefect server (for server based execution):

- mardi-kg-user=xxx
- mardi-kg-password=xxx
- lakefs-user=xxx
- lakefs-password=xxx

Again, the lakeFS configuration is optional.
