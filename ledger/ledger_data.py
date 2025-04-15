# File: ledger/ledger_data.py

import os
import sys

# Add project root to path first
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from utils.config_utils import (
    configure_logging,
    read_sql_file,
    get_database_engine,
    load_and_process_data,
    set_pandas_display_options
)

logger = configure_logging()

# Get the SQL query from the ledger_all.sql file
ledger_query = read_sql_file('ledger_all.sql')

def get_all_ledger_data():
    """Returns a DataFrame containing all ledger data."""
    engine = get_database_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None
    return load_and_process_data(query=ledger_query, engine=engine, logger=logger)

if __name__ == "__main__":
    set_pandas_display_options()
    ledger_df = get_all_ledger_data()
    if ledger_df is not None:
        print("All Ledger Data Preview:")
        print(ledger_df.head(10))
        print("\nColumn Names:", ledger_df.columns.tolist())
        print("\nTotal records:", len(ledger_df))