#!/usr/bin/env python3
"""
Create trailer telemetry tables in PostgreSQL with:
- msg_index (no quoted identifiers)
- t_ts as a GENERATED column from t_ms (epoch ms)
- distance.y10a/y10b as DOUBLE PRECISION (was BIGINT)
- safe migration: will ALTER distance.y10a/y10b to DOUBLE PRECISION if needed

DB: cahngedb | User: changeuser | Pass: 1change@
"""

import sys
from typing import List
try:
    import psycopg2
except ImportError:
    print("This script requires psycopg2. Install: pip install psycopg2-binary")
    raise

TABLES = [
    "ebs11","ebs12","ebs21","ebs22","ebs23",
    "ebs25","ebs25bp","ebs26","gps","imu",
    "lights","rge21","rge22","rge23","distance",
    "packet_index_log"
]

DDL_CREATE: List[str] = [
# ebs11
"""
CREATE TABLE IF NOT EXISTS ebs11 (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  g1a BOOLEAN, g1b BOOLEAN, g1c BOOLEAN, g1e BOOLEAN, g1f BOOLEAN, g1g BOOLEAN,
  g1h DOUBLE PRECISION, g1i DOUBLE PRECISION, g1j DOUBLE PRECISION,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_ebs11_trailer_time ON ebs11 (trailer_id, t_ms);
""",
# ebs12
"""
CREATE TABLE IF NOT EXISTS ebs12 (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  y1a BOOLEAN, y1b BOOLEAN, y1e BOOLEAN, y1f BOOLEAN, y1g BOOLEAN,
  y1i DOUBLE PRECISION,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_ebs12_trailer_time ON ebs12 (trailer_id, t_ms);
""",
# ebs21
"""
CREATE TABLE IF NOT EXISTS ebs21 (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  y2a BOOLEAN, y2b BOOLEAN, y2c BOOLEAN, y2e BOOLEAN,
  y2f DOUBLE PRECISION, y2g DOUBLE PRECISION, y2h DOUBLE PRECISION, y2i DOUBLE PRECISION,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_ebs21_trailer_time ON ebs21 (trailer_id, t_ms);
""",
# ebs22
"""
CREATE TABLE IF NOT EXISTS ebs22 (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  y3a DOUBLE PRECISION, y3b DOUBLE PRECISION,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_ebs22_trailer_time ON ebs22 (trailer_id, t_ms);
""",
# ebs23
"""
CREATE TABLE IF NOT EXISTS ebs23 (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  y8a BOOLEAN, y8b BOOLEAN, y8c BOOLEAN,
  y8f1l DOUBLE PRECISION, y8f1r DOUBLE PRECISION,
  y8f2l DOUBLE PRECISION, y8f2r DOUBLE PRECISION,
  y8f3l DOUBLE PRECISION, y8f3r DOUBLE PRECISION,
  y8g1l DOUBLE PRECISION, y8g1r DOUBLE PRECISION,
  y8g2l DOUBLE PRECISION, y8g2r DOUBLE PRECISION,
  y8g3l DOUBLE PRECISION, y8g3r DOUBLE PRECISION,
  y8h DOUBLE PRECISION,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_ebs23_trailer_time ON ebs23 (trailer_id, t_ms);
""",
# ebs25
"""
CREATE TABLE IF NOT EXISTS ebs25 (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  y4g BOOLEAN, y4h BOOLEAN,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_ebs25_trailer_time ON ebs25 (trailer_id, t_ms);
""",
# ebs25bp
"""
CREATE TABLE IF NOT EXISTS ebs25bp (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  y4a DOUBLE PRECISION, y4b DOUBLE PRECISION, y4c DOUBLE PRECISION,
  y4d DOUBLE PRECISION, y4e DOUBLE PRECISION, y4f DOUBLE PRECISION,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_ebs25bp_trailer_time ON ebs25bp (trailer_id, t_ms);
""",
# ebs26
"""
CREATE TABLE IF NOT EXISTS ebs26 (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  y5a DOUBLE PRECISION, y5b DOUBLE PRECISION,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_ebs26_trailer_time ON ebs26 (trailer_id, t_ms);
""",
# gps
"""
CREATE TABLE IF NOT EXISTS gps (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  o2a DOUBLE PRECISION, o2b DOUBLE PRECISION, o2c DOUBLE PRECISION, o2d DOUBLE PRECISION,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_gps_trailer_time ON gps (trailer_id, t_ms);
""",
# imu
"""
CREATE TABLE IF NOT EXISTS imu (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  o1a DOUBLE PRECISION, o1b DOUBLE PRECISION, o1c DOUBLE PRECISION,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_imu_trailer_time ON imu (trailer_id, t_ms);
""",
# lights
"""
CREATE TABLE IF NOT EXISTS lights (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  r1a BOOLEAN, r1b BOOLEAN,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_lights_trailer_time ON lights (trailer_id, t_ms);
""",
# rge21
"""
CREATE TABLE IF NOT EXISTS rge21 (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  y6a BOOLEAN, y6b BOOLEAN,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_rge21_trailer_time ON rge21 (trailer_id, t_ms);
""",
# rge22
"""
CREATE TABLE IF NOT EXISTS rge22 (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  y7b1 DOUBLE PRECISION, y7b2 DOUBLE PRECISION, y7b3 DOUBLE PRECISION,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_rge22_trailer_time ON rge22 (trailer_id, t_ms);
""",
# rge23
"""
CREATE TABLE IF NOT EXISTS rge23 (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  y9b1l DOUBLE PRECISION, y9b1r DOUBLE PRECISION,
  y9b2l DOUBLE PRECISION, y9b2r DOUBLE PRECISION,
  y9b3l DOUBLE PRECISION, y9b3r DOUBLE PRECISION,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_rge23_trailer_time ON rge23 (trailer_id, t_ms);
""",
# distance (DOUBLE PRECISION)
"""
CREATE TABLE IF NOT EXISTS distance (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  t_ms         BIGINT NOT NULL,
  t_ts         TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(t_ms/1000.0)) STORED,
  y10a DOUBLE PRECISION, y10b DOUBLE PRECISION,
  UNIQUE (trailer_id, msg_index, t_ms)
);
CREATE INDEX IF NOT EXISTS idx_distance_trailer_time ON distance (trailer_id, t_ms);
""",
# packet-only log
"""
CREATE TABLE IF NOT EXISTS packet_index_log (
  trailer_id   INT NOT NULL,
  msg_index    INT NOT NULL,
  time_arrived TIMESTAMPTZ NOT NULL,
  UNIQUE (trailer_id, msg_index)
);
CREATE INDEX IF NOT EXISTS idx_packet_index_time ON packet_index_log (trailer_id, msg_index);
"""
]

# Migration block: if distance.y10a/y10b exist and are not double precision, alter them.
MIGRATE_DISTANCE = """
DO $$
DECLARE
  t1 text;
  t2 text;
BEGIN
  SELECT data_type INTO t1 FROM information_schema.columns
  WHERE table_schema='public' AND table_name='distance' AND column_name='y10a';
  SELECT data_type INTO t2 FROM information_schema.columns
  WHERE table_schema='public' AND table_name='distance' AND column_name='y10b';
  IF t1 IS NOT NULL AND t1 <> 'double precision' THEN
    EXECUTE 'ALTER TABLE distance ALTER COLUMN y10a TYPE DOUBLE PRECISION USING y10a::double precision';
  END IF;
  IF t2 IS NOT NULL AND t2 <> 'double precision' THEN
    EXECUTE 'ALTER TABLE distance ALTER COLUMN y10b TYPE DOUBLE PRECISION USING y10b::double precision';
  END IF;
END $$;
"""

# Safe rename for legacy "index" -> msg_index
RENAME_BLOCKS = [
f"""
DO $$ BEGIN
IF EXISTS (
  SELECT 1 FROM information_schema.columns
  WHERE table_schema = 'public' AND table_name = '{tbl}' AND column_name = 'index'
) AND NOT EXISTS (
  SELECT 1 FROM information_schema.columns
  WHERE table_schema = 'public' AND table_name = '{tbl}' AND column_name = 'msg_index'
) THEN
  EXECUTE 'ALTER TABLE {tbl} RENAME COLUMN \"index\" TO msg_index';
END IF;
END $$;
""" for tbl in TABLES
]

def main(host: str = "localhost", port: int = 5432,
         dbname: str = "cahngedb",
         user: str = "changeuser",
         password: str = "1change@"):
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for stmt in DDL_CREATE:
                cur.execute(stmt)
                lines = [l for l in stmt.splitlines() if l.strip().upper().startswith("CREATE TABLE IF NOT EXISTS")]
                if lines:
                    tbl = lines[0].split()[4]
                    print(f"Ensured table exists: {tbl}")
            # Rename legacy "index" if present
            for stmt in RENAME_BLOCKS:
                cur.execute(stmt)
            # Migrate distance types if needed
            cur.execute(MIGRATE_DISTANCE)
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if len(sys.argv) != 6:
            print("Usage: python create_trailer_tables_v4.py <host> <port> <dbname> <user> <password>")
            sys.exit(1)
        host = sys.argv[1]
        port = int(sys.argv[2])
        dbname = sys.argv[3]
        user = sys.argv[4]
        password = sys.argv[5]
        main(host, port, dbname, user, password)
    else:
        main()
