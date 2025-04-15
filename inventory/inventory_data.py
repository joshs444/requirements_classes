# File: inventory/inventory_data.py

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

# Get the SQL query from the inventory_all.sql file
inventory_query = read_sql_file('inventory_all.sql')

def get_all_inventory_data():
    """Returns a DataFrame containing all inventory data."""
    engine = get_database_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None
    return load_and_process_data(query=inventory_query, engine=engine, logger=logger)

if __name__ == "__main__":
    set_pandas_display_options()
    inventory_df = get_all_inventory_data()
    if inventory_df is not None:
        print("All Inventory Data Preview:")
        print(inventory_df.head(10))
        print("\nColumn Names:", inventory_df.columns.tolist())
        print("\nTotal records:", len(inventory_df))
