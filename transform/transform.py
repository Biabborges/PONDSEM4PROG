import os
import json
import sys
from typing import Tuple, List, Optional
from datetime import datetime
import uuid

import pandas as pd
import clickhouse_connect
from .schemas import Track

CH_HOST   = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT   = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER   = os.getenv("CLICKHOUSE_USER", "chuser")
CH_PASS   = os.getenv("CLICKHOUSE_PASSWORD", "chpass")
CH_DB     = os.getenv("CLICKHOUSE_DATABASE", "default")

SRC_TABLE = os.getenv("SRC_TABLE", "working_g1")
DST_TABLE = os.getenv("DST_TABLE", "working_g2")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))


def ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS, database=CH_DB
    )


def load_rows(client, limit: int = 10000, offset: int = 0) -> List[Tuple[int, str, str]]:
    q = f"""
        SELECT timestamp_unix, data_value, data_tag
        FROM {SRC_TABLE}
        ORDER BY timestamp_unix
        LIMIT {limit} OFFSET {offset}
    """
    return client.query(q).result_rows


def _safe_load_json(s: Optional[str]):
    try:
        return json.loads(s) if s else None
    except Exception:
        return None


def enrich_json(data_json: str) -> str:
    """
    Espera JSON 'flat' compatível com Track.
    Valida via pydantic e adiciona campos derivados.
    """
    try:
        payload = _safe_load_json(data_json)
        if payload is None:
            raise ValueError("JSON vazio ou inválido")

        track = Track.model_validate(payload)
        d = dict(payload)

        now_year = datetime.now().year
        d["duration_min"]   = track.duration_min
        d["decade"]         = track.decade
        d["age_years"]      = now_year - track.release_year
        d["tempo_bucket"]   = track.tempo_bucket
        d["energy_bucket"]  = track.energy_bucket
        d["is_english"]     = track.is_english
        d["is_spanish"]     = track.is_spanish
        d["label_group"]    = track.label_group
        d["region"]         = track.region

        # Flags úteis
        d["is_long_track"]  = int((track.duration_sec or 0) >= 420)  # >= 7min
        d["is_popular"]     = None if track.popularity is None else int(track.popularity >= 70)
        d["is_high_energy"] = None if track.energy is None else int(track.energy >= 70)

        d["status_validacao"] = "valido"
        return json.dumps(d, ensure_ascii=False, separators=(",", ":"))

    except Exception as e:
        return json.dumps(
            {"_raw": data_json, "status_validacao": "erro", "erro": str(e)},
            ensure_ascii=False, separators=(",", ":")
        )


def calculate_and_store_metrics(
    df: pd.DataFrame, 
    client, 
    execution_id: uuid.UUID, 
    batch_num: int
):
    """
    Calcula métricas de qualidade de um DataFrame e as armazena no ClickHouse.
    """
    batch_id = f"{execution_id}-{batch_num}"
    
    rows_processed = len(df)
    df_valid = df[df['status_validacao'] == 'valido'].copy()
    rows_valid = len(df_valid)
    rows_invalid = rows_processed - rows_valid
    
    key_columns = ['track_id', 'artist_name', 'album_name'] 
    existing_key_columns = [col for col in key_columns if col in df_valid.columns]
    duplicate_rows = df_valid.duplicated(subset=existing_key_columns).sum() if existing_key_columns else 0

    nulls_per_column = df_valid.isnull().sum()
    null_counts_dict = {col: int(count) for col, count in nulls_per_column.items() if count > 0}
    
    status = 'SUCCESS'
    status_message = 'Batch processed successfully.'
    
    if rows_processed > 0 and (rows_invalid / rows_processed) > 0.1:
        status = 'FAILURE'
        status_message = f"Data quality check failed: {rows_invalid} invalid rows ({rows_invalid/rows_processed:.2%})."
        
    metrics_data = {
        'execution_id': str(execution_id),
        'batch_id': batch_id,
        'pipeline_name': 'transform-v1',
        'execution_timestamp': datetime.now(),
        'rows_processed': int(rows_processed),
        'rows_valid': int(rows_valid),
        'rows_invalid': int(rows_invalid),
        'duplicate_rows': int(duplicate_rows),
        'null_counts': null_counts_dict,
        'status': status,
        'status_message': status_message,
    }

    col_order = [
        'execution_id', 'batch_id', 'pipeline_name', 'execution_timestamp',
        'rows_processed', 'rows_valid', 'rows_invalid', 'duplicate_rows',
        'null_counts', 'status', 'status_message'
    ]
    data_row = [metrics_data[col] for col in col_order]

    client.insert(
        "default.pipeline_metrics",
        [data_row],
        column_names=col_order
    )
    
    print(f"  [metrics] Lote {batch_num}: {rows_valid} linhas válidas, {rows_invalid} inválidas. Status: {status}")

    if status == 'FAILURE':
        raise ValueError(status_message)


def write_rows(client, rows: List[Tuple[int, str, str]], execution_id: uuid.UUID, batch_num: int):
    enriched = [(ts, enrich_json(data_value), tag) for ts, data_value, tag in rows]
    
    if enriched:
        enriched_dicts = [_safe_load_json(data_val) for _, data_val, _ in enriched]
        df = pd.DataFrame([d for d in enriched_dicts if d is not None])
        
        calculate_and_store_metrics(df, client, execution_id, batch_num)
        
    if enriched:
        data_as_lists = [list(row) for row in enriched]
        client.insert(
            DST_TABLE,
            data_as_lists,
            column_names=["timestamp_unix", "data_value", "data_tag"]
        )


def main():
    client = ch_client()
    offset = 0
    total_written = 0
    batch_num = 1
    execution_id = uuid.uuid4()
    
    print(f"[transform] Iniciando execução. ID: {execution_id}", flush=True)

    while True:
        rows = load_rows(client, limit=BATCH_SIZE, offset=offset)
        if not rows:
            break

        print(f"[transform] Lote {batch_num} (offset={offset}) -> {len(rows)} linhas…", flush=True)
        write_rows(client, rows, execution_id, batch_num)
        
        total_written += len(rows)
        offset += BATCH_SIZE
        batch_num += 1

    if total_written == 0:
        print("[transform] Nada a transformar (tabela de origem vazia).", flush=True)
    else:
        print(f"[transform] OK. Total de linhas processadas: {total_written}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)