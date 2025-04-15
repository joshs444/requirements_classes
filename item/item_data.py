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

# Get the SQL query from the item.sql file
item_query = read_sql_file('item.sql')

def get_all_item_data():
    """Returns a DataFrame containing all item data."""
    engine = get_database_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None
    return load_and_process_data(query=item_query, engine=engine, logger=logger)

if __name__ == "__main__":
    set_pandas_display_options()
    item_df = get_all_item_data()
    if item_df is not None:
        print("All Item Data Preview:")
        print(item_df.head(10))
        print("\nColumn Names:", item_df.columns.tolist())
        print("\nTotal records:", len(item_df))