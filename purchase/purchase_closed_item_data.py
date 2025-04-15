# File: purchase/purchase_closed_item_data.py

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

# Get the SQL query from the purchase_closed_item.sql file
purchase_closed_item_query = read_sql_file('purchase_closed_item.sql')

def get_all_purchase_closed_item_data():
    """Returns a DataFrame containing all closed purchase item data."""
    engine = get_database_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None
    return load_and_process_data(query=purchase_closed_item_query, engine=engine, logger=logger)

if __name__ == "__main__":
    set_pandas_display_options()
    purchase_closed_item_df = get_all_purchase_closed_item_data()
    if purchase_closed_item_df is not None:
        print("All Closed Purchase Item Data Preview:")
        print(purchase_closed_item_df.head(10))
        print("\nColumn Names:", purchase_closed_item_df.columns.tolist())
        print("\nTotal records:", len(purchase_closed_item_df)) 