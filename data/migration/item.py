#!/usr/bin/env python3
"""
migrate_item.py  –  v1.4  (04 Jun 2025)
────────────────────────────────────────────────────────────────────────────
Streams the “item master + purchasing intelligence” query from SQL-Server
into MySQL (streaming, temp-table friendly, oversized-column safe).
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DataError
from sqlalchemy.types import CHAR, DATE, DECIMAL, INTEGER, VARCHAR

from data_access.nav_database import get_engine as get_src_engine

# ─────────── MySQL connection ────────────────────────────────────────────
MYSQL_URL = (
    "mysql+pymysql://root:joshua444@127.0.0.1:3306/"
    "my_project_db?charset=utf8mb4"
)
TARGET_TABLE = "item"

# ─────────── chunk & file settings ───────────────────────────────────────
CHUNK_ROWS  = 20_000
WRITE_CHUNK = 1_000
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_FILE     = PROJECT_ROOT / "sql" / "item" / "item_us.sql"

# ─────────── dtype map ───────────────────────────────────────────────────
dtype_map: Dict[str, object] = {
    "row_index":                INTEGER(),
    "item_no":                  VARCHAR(30),
    "Description":              VARCHAR(255),
    "inventory_posting_group":  VARCHAR(50),
    "unit_cost":                DECIMAL(18, 6),

    "lead_time_calculation":    VARCHAR(20),
    "global_dimension_1_code":  VARCHAR(50),
    "replenishment_system":     VARCHAR(20),
    "revision_no":              VARCHAR(20),

    "item_source":              VARCHAR(30),
    "common_item_no":           VARCHAR(30),
    "hts":                      VARCHAR(20),

    "item_category_code":       VARCHAR(20),
    "parent_category_code":     VARCHAR(20),
    "item_category_description":VARCHAR(100),

    "last_9m_output_qty":       DECIMAL(18, 4),
    "last_9m_purchase_qty":     DECIMAL(18, 4),
    "open_purchase_qty":        DECIMAL(18, 4),

    "make_buy":                 VARCHAR(15),

    "last_vendor_name":         VARCHAR(255),
    "last_vendor_country":      CHAR(2),
    "last_purchase_qty":        DECIMAL(18, 4),
    "last_unit_cost":           DECIMAL(18, 6),
    "last_order_date":          DATE(),
    "last_mfg_part_no":         VARCHAR(255),   # ← widened

    "raw_mat_flag":             CHAR(3),
    "item_index":               VARCHAR(40),
}

# ─────────────────────────────────────────────────────────────────────────
def read_sql_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def stream_and_write_chunks(src: Engine, tgt: Engine, query: str) -> int:
    total_rows, first_chunk = 0, True
    conn_src = src.raw_connection()
    try:
        cur = conn_src.cursor()
        cur.execute(query)
        while cur.description is None:          # skip SET / DDL result-sets
            if not cur.nextset():
                raise RuntimeError("Batch ended without a row-set.")

        cols = [c[0] for c in cur.description]
        while True:
            rows = cur.fetchmany(CHUNK_ROWS)
            if not rows:
                break
            df = pd.DataFrame.from_records(rows, columns=cols)

            with tgt.begin() as conn_tgt:
                conn_tgt.exec_driver_sql("SET foreign_key_checks = 0;")
                try:
                    df.to_sql(
                        TARGET_TABLE,
                        conn_tgt,
                        if_exists="replace" if first_chunk else "append",
                        index=False,
                        chunksize=WRITE_CHUNK,
                        dtype=dtype_map,
                    )
                except DataError:
                    logging.exception(
                        "MySQL DataError inserting chunk starting at row %s",
                        f"{total_rows:,}",
                    )
                    raise
                conn_tgt.exec_driver_sql("SET foreign_key_checks = 1;")

            first_chunk = False
            total_rows += len(df)
            logging.info("… processed %s rows so far", f"{total_rows:,}")
    finally:
        conn_src.close()

    return total_rows


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
    )

    if not SQL_FILE.exists():
        logging.error("SQL file not found: %s", SQL_FILE)
        sys.exit(1)

    item_sql = read_sql_file(SQL_FILE)
    logging.info("Loaded SQL from %s", SQL_FILE)

    src_engine = get_src_engine()                       # SQL-Server
    tgt_engine = create_engine(MYSQL_URL, pool_pre_ping=True)

    logging.info("Starting streaming migration…")
    rows = stream_and_write_chunks(src_engine, tgt_engine, item_sql)

    if rows == 0:
        logging.error("No rows processed.")
    else:
        logging.info("✓ Migration complete (%s rows).", f"{rows:,}")


if __name__ == "__main__":
    main()
