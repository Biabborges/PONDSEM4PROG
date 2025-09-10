
import os, time, io, json
from urllib.parse import urlparse
from minio import Minio
import pandas as pd
import clickhouse_connect
import requests

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minio12345")
S3_BUCKET    = os.getenv("S3_BUCKET", "ingestion")

CH_HOST   = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT   = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER   = os.getenv("CLICKHOUSE_USER", "chuser")
CH_PASS   = os.getenv("CLICKHOUSE_PASSWORD", "chpass")
CH_DB     = os.getenv("CLICKHOUSE_DATABASE", "default")
CH_TABLE  = os.getenv("CLICKHOUSE_TABLE", "data_ingestion")
CH_PROTO  = os.getenv("CLICKHOUSE_PROTOCOL", "http")

CH_PING_URL = f"{CH_PROTO}://{CH_HOST}:{CH_PORT}/ping"
CH_CONNECT_KW = dict(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS, database=CH_DB)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50000"))

def make_minio_client(endpoint: str) -> Minio:
    p = urlparse(endpoint)
    if p.scheme in ("http", "https"):
        netloc = p.netloc
        secure = (p.scheme == "https")
    else:
        netloc = endpoint
        secure = False
    return Minio(netloc, access_key=S3_ACCESS_KEY, secret_key=S3_SECRET_KEY, secure=secure)

def wait_clickhouse(max_wait_s: int = 180):
    start = time.time()
    while True:
        try:
            client = clickhouse_connect.get_client(**CH_CONNECT_KW)
            client.query("SELECT 1")
            return
        except Exception as e:
            if time.time() - start > max_wait_s:
                raise TimeoutError(f"ClickHouse não respondeu em {max_wait_s}s (SELECT 1): {e}")
            time.sleep(1)

def ch_client():
    return clickhouse_connect.get_client(**CH_CONNECT_KW)

def ensure_table(client):
    client.command(f'''
        CREATE TABLE IF NOT EXISTS {CH_DB}.{CH_TABLE} (
            timestamp_unix UInt64,
            data_value String,
            data_tag String
        ) ENGINE = MergeTree
        ORDER BY timestamp_unix
    ''')

def list_objects(mc: Minio, bucket: str):
    for obj in mc.list_objects(bucket, recursive=True):
        name = obj.object_name.lower()
        if name.endswith(".csv"):
            yield obj.object_name

def read_csv_from_minio(mc: Minio, bucket: str, object_name: str) -> pd.DataFrame:
    resp = mc.get_object(bucket, object_name)
    try:
        data = resp.read()
        return pd.read_csv(io.BytesIO(data))
    finally:
        resp.close()
        resp.release_conn()

def rows_to_payload(df: pd.DataFrame, object_name: str):
    now_unix = int(time.time())
    tag = f"{object_name}-{now_unix}"
    payload = []
    for rec in df.to_dict(orient="records"):
        j = json.dumps(rec, ensure_ascii=False, separators=(",", ":"))
        payload.append((now_unix, j, tag))
    return payload

def insert_batch(client, rows):
    if rows:
        client.insert(CH_TABLE, rows, column_names=["timestamp_unix", "data_value", "data_tag"])

def main():
    mc = make_minio_client(S3_ENDPOINT)
    if not any(b.name == S3_BUCKET for b in mc.list_buckets()):
        raise RuntimeError(f"Bucket '{S3_BUCKET}' não encontrado em {S3_ENDPOINT}")

    wait_clickhouse()
    ch = ch_client()
    ensure_table(ch)

    empty = True
    for obj_name in list_objects(mc, S3_BUCKET):
        empty = False
        print(f"[ingestor] Lendo: {obj_name}")
        df = read_csv_from_minio(mc, S3_BUCKET, obj_name)
        df = df.convert_dtypes()
        df = df.astype(object).where(pd.notnull(df), None)
        rows = rows_to_payload(df, obj_name)
        print(f"[ingestor] Inserindo {len(rows)} linhas em {CH_TABLE}…")
        for i in range(0, len(rows), BATCH_SIZE):
            insert_batch(ch, rows[i:i+BATCH_SIZE])
        print("[ingestor] OK.")
    if empty:
        print("[ingestor] Nenhum .csv encontrado no bucket. Envie arquivos para s3://ingestion/")

if __name__ == "__main__":
    main()
