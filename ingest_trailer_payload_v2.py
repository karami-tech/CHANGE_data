#!/usr/bin/env python3
"""
Ingest trailer payloads into PostgreSQL (robust to log headers and key casing).

Enhancements vs v1:
- Accepts files that contain log headers plus JSON objects; extracts JSON blocks by brace depth.
- Case-insensitive keys: 'G1a' -> 'g1a', 'O2a' -> 'o2a', etc.
- Expands composite 'y2a_e': [a,b,c,e] -> individual booleans y2a,y2b,y2c,y2e.
- Skips empty dicts in 'value' list.
- All else as before (upsert on (trailer_id,msg_index,t_ms), coercion of boolean-ish values).

Usage
  python ingest_trailer_payload_v2.py <file_with_json_or_logs>
  cat log.txt | python ingest_trailer_payload_v2.py
DB envs:
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS
"""

import os, sys, json, re
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Optional, Iterable

import psycopg2
import psycopg2.extras as extras

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "cahngedb")
DB_USER = os.getenv("DB_USER", "changeuser")
DB_PASS = os.getenv("DB_PASS", "1change@")

TABLE_COLUMNS: Dict[str, List[str]] = {
    "ebs11":  ["g1a","g1b","g1c","g1e","g1f","g1g","g1h","g1i","g1j"],
    "ebs12":  ["y1a","y1b","y1e","y1f","y1g","y1i"],
    "ebs21":  ["y2a","y2b","y2c","y2e","y2f","y2g","y2h","y2i"],
    "ebs22":  ["y3a","y3b"],
    "ebs23":  ["y8a","y8b","y8c","y8f1l","y8f1r","y8f2l","y8f2r","y8f3l","y8f3r",
               "y8g1l","y8g1r","y8g2l","y8g2r","y8g3l","y8g3r","y8h"],
    "ebs25":  ["y4g","y4h"],
    "ebs25bp":["y4a","y4b","y4c","y4d","y4e","y4f"],
    "ebs26":  ["y5a","y5b"],
    "gps":    ["o2a","o2b","o2c","o2d"],
    "imu":    ["o1a","o1b","o1c"],
    "lights": ["r1a","r1b"],
    "rge21":  ["y6a","y6b"],
    "rge22":  ["y7b1","y7b2","y7b3"],
    "rge23":  ["y9b1l","y9b1r","y9b2l","y9b2r","y9b3l","y9b3r"],
    "distance":["y10a","y10b"],
}

BOOL_COLS = {
    "ebs11":  {"g1a","g1b","g1c","g1e","g1f","g1g"},
    "ebs12":  {"y1a","y1b","y1e","y1f","y1g"},
    "ebs21":  {"y2a","y2b","y2c","y2e"},
    "ebs23":  {"y8a","y8b","y8c"},
    "ebs25":  {"y4g","y4h"},
    "lights": {"r1a","r1b"},
    "rge21":  {"y6a","y6b"},
}

ROUTES: List[Tuple[str, re.Pattern]] = [
    ("ebs25bp", re.compile(r"^y4[abcdef]$")),  # y4a..y4f
    ("ebs25",   re.compile(r"^y4[gh]$")),      # y4g..y4h
    ("ebs11",   re.compile(r"^g1[a-z]")),
    ("ebs12",   re.compile(r"^y1")),
    ("ebs21",   re.compile(r"^y2")),
    ("ebs22",   re.compile(r"^y3")),
    ("ebs23",   re.compile(r"^y8")),
    ("ebs26",   re.compile(r"^y5")),
    ("gps",     re.compile(r"^o2")),
    ("imu",     re.compile(r"^o1")),
    ("lights",  re.compile(r"^r1")),
    ("rge21",   re.compile(r"^y6")),
    ("rge22",   re.compile(r"^y7b")),
    ("rge23",   re.compile(r"^y9b")),
    ("distance",re.compile(r"^y10")),
]

BASE_COLS = ["trailer_id","msg_index","time_arrived","t_ms"]

def normalize_t_ms(t: Any) -> int:
    # Accept float or int; return int milliseconds
    try:
        t_float = float(t)
        t_int = int(t_float)
    except Exception:
        raise ValueError(f"Invalid T value: {t!r}")
    if t_int < 10**11:  # seconds
        return t_int * 1000
    return t_int

def coerce_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        if v == 1 or v == 1.0: return True
        if v == 0 or v == 0.0: return False
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1","true","t","yes","on"}: return True
        if s in {"0","false","f","no","off"}: return False
    try:
        return bool(int(v))
    except Exception:
        return None

def route_key(key: str) -> Optional[str]:
    for table, pat in ROUTES:
        if pat.match(key):
            if key in TABLE_COLUMNS.get(table, []):
                return table
    return None

def expand_composites(item: Dict[str, Any]) -> Dict[str, Any]:
    """Expand composite keys like y2a_e -> y2a,y2b,y2c,y2e booleans."""
    out = dict(item)
    key = "y2a_e"
    if key in out and isinstance(out[key], list):
        arr = out[key]
        # Expect 4 values for a,b,c,e
        mappings = [("y2a", 0), ("y2b", 1), ("y2c", 2), ("y2e", 3)]
        for k, i in mappings:
            if i < len(arr):
                out[k] = arr[i]
        del out[key]
    return out

def lower_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    return { (k.lower() if isinstance(k,str) else k): v for k,v in d.items() }

def prepare_rows_by_table(payload: Dict[str, Any]) -> Tuple[Dict[str, List[Dict[str, Any]]], int, int]:
    # normalize top-level keys to lowercase
    payload_l = lower_keys(payload)
    trailer_id = int(payload_l.get("sid") or payload_l.get("trailer_id"))
    msg_index  = int(payload_l.get("index") or payload_l.get("msg_index"))
    values = payload_l.get("value") or payload_l.get("values") or []
    if not isinstance(values, list):
        raise ValueError("payload['value'] must be a list of readings")
    time_arrived = datetime.now(timezone.utc)

    by_table: Dict[str, List[Dict[str, Any]]] = {t: [] for t in TABLE_COLUMNS.keys()}
    for item in values:
        if not isinstance(item, dict):
            continue
        # normalize keys and expand composites
        item_l = lower_keys(item)
        if "t" not in item_l:
            continue
        item_l = expand_composites(item_l)
        # remove empty dicts
        keys = [k for k in item_l.keys() if k != "t"]
        if not keys:
            continue

        t_ms = normalize_t_ms(item_l["t"])

        # Group keys by table
        keys_by_table: Dict[str, List[str]] = {}
        for k in keys:
            tbl = route_key(k)
            if tbl:
                keys_by_table.setdefault(tbl, []).append(k)

        for tbl, klist in keys_by_table.items():
            row = { "trailer_id": trailer_id, "msg_index": msg_index,
                    "time_arrived": time_arrived, "t_ms": t_ms }
            for k in klist:
                v = item_l.get(k)
                if tbl in BOOL_COLS and k in BOOL_COLS[tbl]:
                    v = coerce_bool(v)
                row[k] = v
            by_table[tbl].append(row)

    by_table = {t: rows for t, rows in by_table.items() if rows}
    return by_table, trailer_id, msg_index

def upsert_packet_index_log(cur, trailer_id: int, msg_index: int, time_arrived: datetime):
    cur.execute(
        """
        INSERT INTO packet_index_log (trailer_id, msg_index, time_arrived)
        VALUES (%s, %s, %s)
        ON CONFLICT (trailer_id, msg_index) DO NOTHING;
        """,
        (trailer_id, msg_index, time_arrived)
    )

def batch_insert(cur, table: str, rows: List[Dict[str, Any]]):
    cols = BASE_COLS + TABLE_COLUMNS[table]
    data = [[r.get(c) for c in cols] for r in rows]
    columns_sql = ", ".join(cols)
    template = "(" + ", ".join(["%s"] * len(cols)) + ")"
    sql = f"""
        INSERT INTO {table} ({columns_sql})
        VALUES %s
        ON CONFLICT (trailer_id, msg_index, t_ms) DO NOTHING;
    """
    extras.execute_values(cur, sql, data, template=template, page_size=1000)

def ingest_packet(conn, payload: Dict[str, Any]):
    by_table, trailer_id, msg_index = prepare_rows_by_table(payload)
    if not by_table:
        return 0
    time_arrived = datetime.now(timezone.utc)
    inserted = 0
    with conn.cursor() as cur:
        upsert_packet_index_log(cur, trailer_id, msg_index, time_arrived)
        for table, rows in by_table.items():
            batch_insert(cur, table, rows)
            inserted += len(rows)
    conn.commit()
    return inserted

def extract_json_blocks(text: str) -> List[Dict[str, Any]]:
    """Extract top-level JSON objects from a log file that may include headers."""
    blocks = []
    buf = []
    depth = 0
    in_json = False
    for ch in text:
        if ch == '{':
            depth += 1
            in_json = True
        if in_json:
            buf.append(ch)
        if ch == '}':
            depth -= 1
            if in_json and depth == 0:
                try:
                    obj = json.loads("".join(buf))
                    blocks.append(obj)
                except json.JSONDecodeError:
                    pass
                buf = []
                in_json = False
    return blocks

def read_inputs(path: Optional[str]) -> Iterable[Dict[str, Any]]:
    text = sys.stdin.read() if not path else open(path, "r", encoding="utf-8", errors="ignore").read()
    text = text.strip()
    if not text:
        return []
    # Try direct JSON
    try:
        obj = json.loads(text)
        if isinstance(obj, list):
            return obj
        elif isinstance(obj, dict):
            return [obj]
    except json.JSONDecodeError:
        pass
    # Try NDJSON
    rows = []
    ndjson_ok = True
    for line in text.splitlines():
        t = line.strip()
        if not t:
            continue
        try:
            rows.append(json.loads(t))
        except json.JSONDecodeError:
            ndjson_ok = False
            break
    if ndjson_ok and rows:
        return rows
    # Fallback: extract bracketed JSON blocks from logs
    return extract_json_blocks(text)

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else None
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    try:
        total = 0
        for payload in read_inputs(path):
            total += ingest_packet(conn, payload)
        print(f"Ingestion complete. Rows inserted: {total}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
