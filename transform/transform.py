import os
import json
import sys
from typing import Tuple, List, Optional
from datetime import datetime

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
            {"_raw": _safe_load_json(data_json), "status_validacao": "erro", "erro": str(e)},
            ensure_ascii=False, separators=(",", ":")
        )


def write_rows(client, rows: List[Tuple[int, str, str]]):
    enriched = [(ts, enrich_json(data_value), tag) for ts, data_value, tag in rows]
    if enriched:
        client.insert(
            DST_TABLE,
            enriched,
            column_names=["timestamp_unix", "data_value", "data_tag"]
        )


def main():
    client = ch_client()
    offset = 0
    total_written = 0

    while True:
        rows = load_rows(client, limit=BATCH_SIZE, offset=offset)
        if not rows:
            break

        print(f"[transform] Lote offset={offset} -> {len(rows)} linhas…", flush=True)
        write_rows(client, rows)
        total_written += len(rows)
        offset += BATCH_SIZE

    if total_written == 0:
        print("[transform] Nada a transformar (tabela Bronze vazia).", flush=True)
    else:
        print(f"[transform] OK. Linhas processadas: {total_written}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
