# File: bom/bom_data.py

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

# Get the SQL query from the bom.sql file
bom_query = read_sql_file('sql/bom/bom.sql')

def get_all_bom_data():
    """Returns a DataFrame containing all BOM data."""
    engine = get_database_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None
    return load_and_process_data(query=bom_query, engine=engine, logger=logger)

if __name__ == "__main__":
    set_pandas_display_options()
    bom_df = get_all_bom_data()
    if bom_df is not None:
        print("All BOM Data Preview:")
        print(bom_df.head(10))
        print("\nColumn Names:", bom_df.columns.tolist())
        print("\nTotal records:", len(bom_df))
