# File: sales/sales_open_data.py

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

# Get the SQL query from the sales_open.sql file
sales_open_query = read_sql_file('sales_open.sql')

def get_all_sales_open_data():
    """Returns a DataFrame containing all open sales data."""
    engine = get_database_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None
    return load_and_process_data(query=sales_open_query, engine=engine, logger=logger)

if __name__ == "__main__":
    set_pandas_display_options()
    sales_open_df = get_all_sales_open_data()
    if sales_open_df is not None:
        print("All Open Sales Data Preview:")
        print(sales_open_df.head(10))
        print("\nColumn Names:", sales_open_df.columns.tolist())
        print("\nTotal records:", len(sales_open_df))