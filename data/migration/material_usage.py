#!/usr/bin/env python3
"""
migrate_material_usage.py  –  v1.0  (04 Jun 2025)
────────────────────────────────────────────────────────────────────────────
Streams the **material-usage** query (consumption | sale | scrap) from
SQL-Server into MySQL with:

• Streaming fetches (20 000-row windows) → minimal RAM  
• Safe executemany inserts (1 000-row batches) → avoids MySQL’s 65 535-parameter limit  
• Oversize-proof column widths (generous VARCHAR / DECIMAL / DATE)  
• Ctrl-C friendly; clear surfacing of MySQL DataError messages
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
from sqlalchemy.types import (
    CHAR,
    DATE,
    DECIMAL,
    INTEGER,
    VARCHAR,
)

from data_access.nav_database import get_engine as get_src_engine


# ───────────── MySQL connection (edit if required) ───────────────────────
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "joshua444"
MYSQL_DB   = "my_project_db"
CHARSET    = "utf8mb4"

MYSQL_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset={CHARSET}"
)

TARGET_TABLE = "material_usage"            # ← final table in MySQL

# ───────────── chunk & file settings ─────────────────────────────────────
CHUNK_ROWS  = 20_000        # rows fetched from SQL-Server per window
WRITE_CHUNK = 1_000         # rows per executemany() batch into MySQL

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_FILE     = PROJECT_ROOT / "sql" / "ledger" / "material_usage.sql"

# ───────────── explicit dtype map (safe, slightly oversized) ─────────────
dtype_map: Dict[str, object] = {
    # identifiers & descriptors
    "subsidiary": CHAR(5),
    "entry_no": INTEGER(),
    "item_no": VARCHAR(50),
    "posting_date": DATE(),
    "location_code": VARCHAR(50),
    "order_no": VARCHAR(50),
    "document_no": VARCHAR(50),
    "issue_type": VARCHAR(6),                # 'C', 'SALE', 'S'

    # quantities & costs
    "qty_issued": DECIMAL(18, 4),
    "total_cost_usd": DECIMAL(18, 4),
    "unit_cost": DECIMAL(18, 4),
    "total_root_cost_usd": DECIMAL(18, 4),
    "unit_cost_root": DECIMAL(18, 4),

    # dimensions
    "department": VARCHAR(50),
}

# ─────────────────────────────────────────────────────────────────────────
def read_sql_file(path: Path) -> str:
    """Return the SQL query text (UTF-8)."""
    return path.read_text(encoding="utf-8")


def stream_and_write_chunks(
    src_engine: Engine,
    tgt_engine: Engine,
    query: str,
) -> int:
    """Pipe data from SQL-Server → MySQL in streaming/batched mode."""
    total_rows  = 0
    first_chunk = True

    try:
        for chunk in pd.read_sql_query(
            sql=query,
            con=src_engine.execution_options(stream_results=True),
            chunksize=CHUNK_ROWS,
        ):
            with tgt_engine.begin() as conn:          # one TX per chunk
                conn.exec_driver_sql("SET foreign_key_checks = 0;")

                try:
                    chunk.to_sql(
                        name=TARGET_TABLE,
                        con=conn,
                        if_exists="replace" if first_chunk else "append",
                        index=False,
                        chunksize=WRITE_CHUNK,
                        dtype=dtype_map,
                    )
                except DataError:
                    logging.exception(
                        "MySQL DataError while inserting chunk starting at row %s",
                        f"{total_rows:,}"
                    )
                    raise

                conn.exec_driver_sql("SET foreign_key_checks = 1;")

            first_chunk = False
            total_rows += len(chunk)
            logging.info("… processed %s rows so far", f"{total_rows:,}")

    except KeyboardInterrupt:
        logging.warning("Migration aborted by user (%s rows written).", f"{total_rows:,}")
        return total_rows

    return total_rows


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
    )

    if not SQL_FILE.exists():
        logging.error("SQL file not found: %s", SQL_FILE)
        sys.exit(1)

    usage_sql = read_sql_file(SQL_FILE)
    logging.info("Loaded SQL from %s", SQL_FILE)

    src_engine = get_src_engine()
    tgt_engine = create_engine(MYSQL_URL, pool_pre_ping=True)

    logging.info("Starting streaming migration…")
    rows = stream_and_write_chunks(src_engine, tgt_engine, usage_sql)

    if rows == 0:
        logging.error("No rows processed.")
    else:
        logging.info("✓ Migration complete (%s rows).", f"{rows:,}")


if __name__ == "__main__":
    main()
