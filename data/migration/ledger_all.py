#!/usr/bin/env python3
"""
migrate_ledger_all_stream.py
────────────────────────────────────────────────────────────────────────────
• Streams a large SELECT from SQL Server to a temp CSV in 50 k–row chunks
  (fast, low-memory, keeps the TCP socket active).
• Logs progress after every chunk.
• Bulk-loads the CSV into MySQL `ledger_all` with LOAD DATA LOCAL INFILE.

Dependencies: pandas, SQLAlchemy, pymysql, pyodbc.
"""
import csv
import logging
import os
import tempfile
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.types import CHAR, VARCHAR, SMALLINT, DATE, DECIMAL

from data_access.nav_database import get_engine as get_src_engine

# ─────────────────────── MySQL settings ────────────────────────────────
MYSQL_URL  = "mysql+pymysql://root:joshua444@127.0.0.1:3306/my_project_db?charset=utf8mb4"
TABLE_NAME = "ledger_all"

# ─────——— chunk + file locations ————————————————————————————————
CHUNK_ROWS   = 50_000            # smaller chunk ⇒ fewer timeouts
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_FILE     = PROJECT_ROOT / "sql" / "ledger" / "ledger_all.sql"

# ──────────────────────────────────────────────────────────────────────────
# Explicit column-type mapping to ensure DECIMAL precision is preserved
# ──────────────────────────────────────────────────────────────────────────
dtype_map = {
    "Subsidiary": CHAR(length=4),
    "Entry No_": VARCHAR(length=50),
    "Item No_": CHAR(length=20),
    "Posting Date": DATE(),
    "Entry Type": SMALLINT(),
    "Document No_": VARCHAR(length=50),
    "Location Code": CHAR(length=10),
    "Quantity": DECIMAL(18,4),
    "SUM_Cost_Amount_Actual_USD": DECIMAL(18,6),
    "SUM_Cost_Amount_Expected_USD": DECIMAL(18,6),
    "SUM_Root_Cost_Actual_USD": DECIMAL(18,6),
    "SUM_Root_Cost_Expected_USD": DECIMAL(18,6),
}

def stream_sql_to_csv(engine: Engine, query: str, csv_path: Path) -> int:
    """Run *query* and append each 50 k-row chunk to *csv_path*."""
    total_rows = 0
    first_chunk = True
    for chunk in pd.read_sql_query(
        sql=query,
        con=engine.execution_options(stream_results=True),
        chunksize=CHUNK_ROWS,
    ):
        mode = "w" if first_chunk else "a"
        with open(csv_path, mode, newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
            if first_chunk:
                writer.writerow(chunk.columns)  # write header once
                first_chunk = False
            writer.writerows(chunk.values)

        total_rows += len(chunk)
        logging.info(f"… streamed {total_rows:,d} rows so far")
    return total_rows

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s"
    )

    # 1 ── load the SELECT
    if not SQL_FILE.exists():
        logging.error("SQL file not found: %s", SQL_FILE)
        return
    query = SQL_FILE.read_text(encoding="utf-8")
    logging.info("Loaded SQL from %s", SQL_FILE)

    # 2 ── temp CSV
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    csv_path = Path(tmp.name)
    tmp.close()
    logging.info("Streaming SQL rows into %s …", csv_path)

    # 3 ── stream SQL Server → CSV
    src_engine = get_src_engine()
    rows = stream_sql_to_csv(src_engine, query, csv_path)
    if rows == 0:
        logging.error("Source query returned zero rows – aborting.")
        os.remove(csv_path)
        return
    logging.info(f"Finished streaming {rows:,d} rows to CSV")

    # 4 ── connect to MySQL (client flag for LOCAL INFILE)
    tgt_engine = create_engine(
        MYSQL_URL,
        pool_pre_ping=True,
        connect_args={"local_infile": 1},
    )

    # 5 ── recreate empty target table with precise DECIMAL definitions
    with tgt_engine.begin() as conn:
        logging.info("Recreating table `%s` …", TABLE_NAME)
        header_df = pd.read_csv(csv_path, nrows=0)
        header_df.to_sql(
            TABLE_NAME,
            conn,
            if_exists="replace",
            index=False,
            dtype=dtype_map
        )

    # 6 ── LOAD DATA
    load_sql = text(f"""
        LOAD DATA LOCAL INFILE :file
        INTO TABLE {TABLE_NAME}
        FIELDS TERMINATED BY ',' ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES;
    """)
    with tgt_engine.begin() as conn:
        logging.info("Executing LOAD DATA …")
        conn.exec_driver_sql("SET foreign_key_checks = 0;")
        conn.execute(load_sql, {"file": str(csv_path)})
        conn.exec_driver_sql("SET foreign_key_checks = 1;")
    logging.info(f"✓ Bulk load complete ({rows:,d} rows).")

    # 7 ── cleanup
    os.remove(csv_path)
    logging.info("Temp CSV removed – migration finished.")

if __name__ == "__main__":
    main()
