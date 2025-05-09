#!/usr/bin/env python3
"""
material_usage.py
────────────────────────────────────────────────────────────────────────────
Pull data defined in sql/ledger/material_usage.sql from the SQL-Server data-warehouse
(IPG-DW-PROTOTYPE) and push it into the `material_usage` table of your local
MySQL schema `my_project_db`.

Uses a streaming approach for handling large data volumes:
• Streams data from SQL Server to a temp CSV in chunks
• Logs progress during streaming
• Bulk-loads the CSV into MySQL with LOAD DATA LOCAL INFILE
"""

# ── Standard library ──────────────────────────────────────────────────────
from pathlib import Path
import logging
import csv
import os
import tempfile

# ── Third-party ───────────────────────────────────────────────────────────
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.types import VARCHAR, CHAR, DATE, DECIMAL

# ── Internal imports (source-side helpers) ────────────────────────────────
from data_access.nav_database import get_engine as get_src_engine

# ──────────────────────────────────────────────────────────────────────────
# TARGET-SIDE (MySQL) CONNECTION DETAILS
# ──────────────────────────────────────────────────────────────────────────
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "joshua444"
MYSQL_DB   = "my_project_db"
CHARSET    = "utf8mb4"

# include &local_infile=1 to ensure the driver sends the LOCAL_INFILE flag
MYSQL_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    f"?charset={CHARSET}&local_infile=1"
)

# ──────────────────────────────────────────────────────────────────────────
# PATH TO THE SQL FILE YOU WANT TO RUN ON SQL SERVER
# ──────────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]   # climb two levels
SQL_FILE     = PROJECT_ROOT / "sql" / "ledger" / "material_usage.sql"
CHUNK_ROWS   = 50_000  # smaller chunk ⇒ fewer timeouts

# Column type mapping for preserving precision
dtype_map = {
    # e.g. "Item No_": CHAR(length=20),
    #      "Posting Date": DATE(),
    #      "Quantity": DECIMAL(18,4),
}

def read_sql_file(path: Path) -> str:
    """Return the contents of a .sql file as a string."""
    return path.read_text(encoding="utf-8")

def stream_sql_to_csv(engine: Engine, query: str, csv_path: Path) -> int:
    """Run *query* and append each chunk to *csv_path*."""
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
                writer.writerow(chunk.columns)
                first_chunk = False
            writer.writerows(chunk.values)

        total_rows += len(chunk)
        logging.info(f"… streamed {total_rows:,d} rows so far")
    return total_rows

def main() -> None:
    # ── Logging setup ────────────────────────────────────────────────────
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
    )

    # ── Load and validate SQL file ───────────────────────────────────────
    if not SQL_FILE.exists():
        logging.error("SQL file not found: %s", SQL_FILE)
        return
    material_usage_sql = read_sql_file(SQL_FILE)
    logging.info("Loaded SQL from %s", SQL_FILE)

    # ── Create temp CSV file ────────────────────────────────────────────
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    csv_path = Path(tmp.name)
    tmp.close()
    logging.info("Streaming SQL rows into %s …", csv_path)

    # ── Stream data from SQL Server to CSV ───────────────────────────────
    src_engine = get_src_engine()
    rows = stream_sql_to_csv(src_engine, material_usage_sql, csv_path)
    if rows == 0:
        logging.error("Source query returned zero rows – aborting.")
        os.remove(csv_path)
        return
    logging.info(f"Finished streaming {rows:,d} rows to CSV")

    # ── Connect to MySQL with LOCAL INFILE support ────────────────────────
    tgt_engine = create_engine(
        MYSQL_URL,
        pool_pre_ping=True,
        connect_args={"local_infile": True},
    )

    # ── Pre-flight check: ensure server allows LOCAL INFILE ─────────────
    with tgt_engine.connect() as conn:
        var = conn.execute(text("SHOW GLOBAL VARIABLES LIKE 'local_infile';")).fetchone()
        if var is None or var[1].lower() != "on":
            logging.error(
                "MySQL server local_infile is OFF – cannot use LOAD DATA LOCAL INFILE.\n"
                "Either enable `local_infile=1` in your my.cnf under [mysqld], "
                "or place the CSV in secure_file_priv and use `LOAD DATA INFILE` instead."
            )
            raise RuntimeError("MySQL server local_infile=OFF")

    # ── Create/recreate empty target table with proper column types ─────
    table_name = "material_usage"
    with tgt_engine.begin() as conn:
        logging.info(f"Recreating table `{table_name}` …")
        header_df = pd.read_csv(csv_path, nrows=0)
        header_df.to_sql(
            table_name,
            conn,
            if_exists="replace",
            index=False,
            dtype=dtype_map
        )

    # ── LOAD DATA from CSV to MySQL ─────────────────────────────────────
    load_sql = text(f"""
        LOAD DATA LOCAL INFILE :file
        INTO TABLE {table_name}
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

    # ── Cleanup ────────────────────────────────────────────────────────
    os.remove(csv_path)
    logging.info("Temp CSV removed – migration finished.")

if __name__ == "__main__":
    main()
