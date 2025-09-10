from prefect import flow, task
import os, subprocess, sys
from pathlib import Path

@task
def extract_load_bronze():
    print("Ingestão é executada pelo serviço 'ingestor' do docker-compose.")

@task
def transform_silver():
    project_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env.setdefault("CLICKHOUSE_HOST", "localhost")
    env.setdefault("SRC_TABLE", "default.data_ingestion")
    env.setdefault("DST_TABLE", "default.working_g2")

    cmd = [sys.executable, "-m", "transform.transform"]
    subprocess.check_call(cmd, cwd=str(project_root), env=env)

@flow
def etl_flow():
    extract_load_bronze()
    transform_silver()

if __name__ == "__main__":
    etl_flow()
