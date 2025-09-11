from prefect import flow, task
import os, subprocess, sys
from pathlib import Path

@task
def extract_load_bronze():
    print("Ingestão é executada pelo serviço 'ingestor' do docker-compose.")

@task
def transform_silver():
    project_root = Path(__file__).resolve().parents[1]
    cmd = ["docker", "compose", "-f", str(project_root / "docker-compose.yml"), "run", "--rm", "transformer"]
    subprocess.check_call(cmd, cwd=str(project_root))

@flow
def etl_flow():
    extract_load_bronze()
    transform_silver()

if __name__ == "__main__":
    etl_flow()
