# File: vendor/vendor_data.py

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

# Get the SQL query from the vendor_all.sql file
vendor_query = read_sql_file('vendor_all.sql')

def get_all_vendor_data():
    """Returns a DataFrame containing all vendor data."""
    engine = get_database_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None
    return load_and_process_data(query=vendor_query, engine=engine, logger=logger)

if __name__ == "__main__":
    set_pandas_display_options()
    vendor_df = get_all_vendor_data()
    if vendor_df is not None:
        print("All Vendor Data Preview:")
        print(vendor_df.head(10))
        print("\nColumn Names:", vendor_df.columns.tolist())
        print("\nTotal records:", len(vendor_df))