#!/usr/bin/env python3
"""
migrate_ledger.py
────────────────────────────────────────────────────────────────────────────
Pull data defined in sql/item/item.sql from the SQL-Server data-warehouse
(IPG-DW-PROTOTYPE) and push it into the `item` table of your local
MySQL schema `my_project_db`.

Project layout expected:

project_root/
├── data_access/
│   └── nav_database.py       ← defines get_engine(), load_and_process_table()
├── sql/
│   └── item/
│       └── item.sql          ← the SELECT … query to run on SQL Server
└── data/
    └── migration/
        └── migrate_ledger.py ← this script (any folder is fine)
"""

# ── Standard library ──────────────────────────────────────────────────────
from pathlib import Path
import logging

# ── Third-party ───────────────────────────────────────────────────────────
import pandas as pd
from sqlalchemy import create_engine

# ── Internal imports (source-side helpers) ────────────────────────────────
from data_access.nav_database import get_engine as get_src_engine
from data_access.nav_database import load_and_process_table

# ──────────────────────────────────────────────────────────────────────────
# TARGET-SIDE (MySQL) CONNECTION DETAILS
# ──────────────────────────────────────────────────────────────────────────
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "joshua444"
MYSQL_DB   = "my_project_db"
CHARSET    = "utf8mb4"

MYSQL_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    f"?charset={CHARSET}"
)

# ──────────────────────────────────────────────────────────────────────────
# PATH TO THE SQL FILE YOU WANT TO RUN ON SQL SERVER
# ──────────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]   # climb two levels
SQL_FILE     = PROJECT_ROOT / "sql" / "item" / "item.sql"


def read_sql_file(path: Path) -> str:
    """Return the contents of a .sql file as a string."""
    return path.read_text(encoding="utf-8")


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
    item_sql = read_sql_file(SQL_FILE)
    logging.info("Loaded SQL from %s", SQL_FILE)

    # ── Pull data from SQL Server ────────────────────────────────────────
    src_engine = get_src_engine()  # from nav_database.py
    df: pd.DataFrame = load_and_process_table(
        query=item_sql,
        engine=src_engine,
        rename_cols=None,
        additional_processing=None,
    )
    if df is None or df.empty:
        logging.error("No rows returned from source query. Aborting.")
        return
    logging.info("Pulled %s rows.", len(df))

    # ── Push data into MySQL in one txn & conn ───────────────────────────
    tgt_engine = create_engine(MYSQL_URL, pool_pre_ping=True)

    with tgt_engine.connect() as conn:
        with conn.begin():
            # disable FKs for this transaction
            conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS = 0;")

            logging.info("Writing to MySQL table `item` …")
            df.to_sql(
                name="item",
                con=conn,             # reuse this connection
                if_exists="replace",  # use 'append' to keep existing data
                index=False,
                chunksize=10_000,
                method="multi"
            )

            # re-enable FKs
            conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS = 1;")

    logging.info("✓ Migration complete.")


if __name__ == "__main__":
    main()
