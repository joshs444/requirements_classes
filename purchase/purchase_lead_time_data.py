# File: purchase/purchase_lead_time_data.py

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

# Get the SQL query from the purchase_lead_time.sql file
purchase_lead_time_query = read_sql_file('purchase_lead_time.sql')

def get_all_purchase_lead_time_data():
    """Returns a DataFrame containing purchase lead time data."""
    engine = get_database_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None
    return load_and_process_data(query=purchase_lead_time_query, engine=engine, logger=logger)

if __name__ == "__main__":
    set_pandas_display_options()
    purchase_lead_time_df = get_all_purchase_lead_time_data()
    if purchase_lead_time_df is not None:
        print("Purchase Lead Time Data Preview:")
        print(purchase_lead_time_df.head(10))
        print("\nColumn Names:", purchase_lead_time_df.columns.tolist())
        print("\nTotal records:", len(purchase_lead_time_df)) 