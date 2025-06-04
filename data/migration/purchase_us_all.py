#!/usr/bin/env python3
"""
migrate_purchase_all_us.py  –  v1.4  (04 Jun 2025)
────────────────────────────────────────────────────────────────────────────
Streams the big procurement query from SQL-Server into MySQL with:

• Streaming fetches (20 000 rows each)  → low memory
• Safe executemany inserts (1 000 rows) → avoids MySQL 65 535-parameter cap
• Oversize-proof column widths (TEXT for long free-text fields)
• Graceful handling of Ctrl-C and real MySQL/DataError messages
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
    SMALLINT,
    TEXT,          # ← NEW: for very long strings
    VARCHAR,
)

from data_access.nav_database import get_engine as get_src_engine


# ─────────────────────── MySQL connection settings ─────────────────────
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

TARGET_TABLE = "purchase_all_us"

# ─────——— chunk settings ────────────────────────────────────
CHUNK_ROWS  = 20_000   # rows fetched from SQL-Server at a time
WRITE_CHUNK = 1_000    # rows per executemany() into MySQL

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_FILE     = PROJECT_ROOT / "sql" / "purchase" / "purchase_all_us.sql"

# ────────────── Explicit dtype map (widened text columns) ─────────────
dtype_map: Dict[str, object] = {
    # IDs & codes
    "order_date": DATE(),
    "status": VARCHAR(10),
    "document_type": VARCHAR(20),
    "document_no": VARCHAR(50),
    "line_no": INTEGER(),
    "buy_from_vendor_no": VARCHAR(20),
    "vendor_name": VARCHAR(200),      # was 100
    "vendor_country": CHAR(2),
    "vendor_posting_group": VARCHAR(20),
    "type": VARCHAR(10),
    "item_no": VARCHAR(20),
    "cost_center": VARCHAR(50),
    "location_code": CHAR(10),

    # dates
    "expected_receipt_date": DATE(),
    "promised_receipt_date": DATE(),
    "posting_date": DATE(),
    "requested_receipt_date": DATE(),
    "planned_receipt_date": DATE(),
    "order_confirmation_date": DATE(),

    # numerics
    "qty_per_unit_of_measure": DECIMAL(18, 6),
    "quantity": DECIMAL(18, 4),
    "outstanding_quantity": DECIMAL(18, 4),
    "unit_cost": DECIMAL(18, 6),
    "avg_price_1y": DECIMAL(18, 6),
    "avg_price_2y": DECIMAL(18, 6),
    "avg_price_1y_vendor": DECIMAL(18, 6),
    "avg_price_2y_vendor": DECIMAL(18, 6),
    "baseline_unit_cost": DECIMAL(18, 6),
    "baseline_unit_cost_vendor": DECIMAL(18, 6),
    "price_var_pct": DECIMAL(18, 6),
    "savings_value": DECIMAL(18, 6),
    "price_var_pct_vendor": DECIMAL(18, 6),
    "savings_value_vendor": DECIMAL(18, 6),
    "last_unit_cost": DECIMAL(18, 6),
    "total": DECIMAL(18, 6),
    "quantity_delivered": DECIMAL(18, 4),

    # flags & enums
    "uom_sanity_flag": VARCHAR(5),
    "single_source_flag": CHAR(3),
    "high_volume_po_flag": CHAR(3),
    "high_volume_spend_flag": CHAR(3),
    "first_purchase": CHAR(3),
    "country_change": CHAR(3),
    "china_change": CHAR(3),
    "on_time_flag": SMALLINT(),

    # lead-time / lateness
    "days_late_early": INTEGER(),
    "bus_days_late": INTEGER(),
    "promised_lead_time_days": INTEGER(),
    "actual_lead_time_days": INTEGER(),

    # free-text  (widened)
    "description": TEXT(),                 # was VARCHAR(255)
    "manufacturer_part_no": TEXT(),        # was VARCHAR(50)
    "manufacturer_code": VARCHAR(100),     # was 50
    "assigned_user_id": VARCHAR(50),
    "purchaser_code": VARCHAR(50),

    # indices
    "subsidiary": CHAR(5),
    "item_index": VARCHAR(50),      # 25 → 50 just to be safe
    "vendor_index": VARCHAR(50),
}

# ────────────────────────────────────────────────────────────────────────
def read_sql_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def stream_and_write_chunks(
    src_engine: Engine,
    tgt_engine: Engine,
    query: str,
) -> int:
    """Stream SQL-Server data and write to MySQL in safe batches."""
    total_rows  = 0
    first_chunk = True

    try:
        for chunk in pd.read_sql_query(
            sql=query,
            con=src_engine.execution_options(stream_results=True),
            chunksize=CHUNK_ROWS,
        ):
            # one MySQL transaction per chunk
            with tgt_engine.begin() as conn:
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
                except DataError as e:
                    # Surface the exact column / value that overflows
                    logging.exception("MySQL DataError on chunk starting at row %s", f"{total_rows:,}")
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

    purchase_sql = read_sql_file(SQL_FILE)
    logging.info("Loaded SQL from %s", SQL_FILE)

    src_engine = get_src_engine()
    tgt_engine = create_engine(MYSQL_URL, pool_pre_ping=True)

    logging.info("Starting streaming migration…")
    rows = stream_and_write_chunks(src_engine, tgt_engine, purchase_sql)

    if rows == 0:
        logging.error("No rows processed.")
    else:
        logging.info("✓ Migration complete (%s rows).", f"{rows:,}")


if __name__ == "__main__":
    main()
