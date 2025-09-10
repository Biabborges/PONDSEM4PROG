from curses import raw
import os, json, time
from typing import Tuple, List
import clickhouse_connect
from datetime import datetime
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
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASS,
        database=CH_DB
    )

def load_rows(limit: int = 10000, offset: int = 0) -> List[Tuple[int, str, str]]:
    client = ch_client()
    q = f"""
        SELECT timestamp_unix, data_value, data_tag
        FROM {SRC_TABLE}
        ORDER BY timestamp_unix
        LIMIT {limit} OFFSET {offset}
    """
    return client.query(q).result_rows

def _safe_load_json(s: str):
    try:
        return json.loads(s) if s else None
    except Exception:
        return None

def enrich_json(data_json: str) -> str:
    """
    Espera JSON 'flat' com campos do Track.
    Aplica validação/limpeza e adiciona dimensões derivadas.
    """
    try:
        raw = _safe_load_json(data_json)
        if raw is None:
            raise ValueError("JSON vazio ou inválido")

        track = Track.parse_obj(raw)
        
        d = dict(raw)

        now_year = datetime.now().year
        d["duration_min"]  = track.duration_min
        d["decade"]        = track.decade
        d["age_years"]     = now_year - track.release_year
        d["tempo_bucket"]  = track.tempo_bucket
        d["energy_bucket"] = track.energy_bucket
        d["is_english"]    = track.is_english
        d["is_spanish"]    = track.is_spanish
        d["label_group"]   = track.label_group
        d["region"]        = track.region

        # Flags úteis
        d["is_long_track"] = 1 if track.duration_sec >= 420 else 0  # >= 7min
        d["is_popular"]    = None if track.popularity is None else int(track.popularity >= 70)
        d["is_high_energy"] = None if track.energy is None else int(track.energy >= 70)

        d["status_validacao"] = "valido"
        return json.dumps(d, ensure_ascii=False, separators=(",", ":"))

    except Exception as e:
        return json.dumps(
            {"_raw": _safe_load_json(data_json), "status_validacao": "erro", "erro": str(e)},
            ensure_ascii=False, separators=(",", ":")
        )

def write_rows(rows: List[Tuple[int, str, str]]):
    client = ch_client()
    enriched = []
    for ts, data_value, tag in rows:
        enriched.append((ts, enrich_json(data_value), tag))
    if enriched:
        client.insert(DST_TABLE, enriched, column_names=["timestamp_unix","data_value","data_tag"])

def main():
    offset = 0
    total_written = 0
    while True:
        rows = load_rows(limit=BATCH_SIZE, offset=offset)
        if not rows:
            break
        print(f"[transform] Lote offset={offset} -> {len(rows)} linhas…")
        write_rows(rows)
        total_written += len(rows)
        offset += BATCH_SIZE
    if total_written == 0:
        print("[transform] Nada a transformar (tabela Bronze vazia).")
    else:
        print(f"[transform] OK. Linhas processadas: {total_written}")

if __name__ == "__main__":
    main()
