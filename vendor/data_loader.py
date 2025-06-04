import os
import sys
import logging
import pandas as pd

# Add project root to path first
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import (
    configure_logging,
    read_sql_file,
    get_database_engine,
    load_and_process_data,
    set_pandas_display_options
)

logger = configure_logging()

# Get project root for consistent SQL file path resolution
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# SQL query with full path
vendor_query = read_sql_file(os.path.join(project_root, 'sql', 'vendor', 'vendor.sql'))

def get_all_vendor_data():
    """Returns a DataFrame containing all vendor data."""
    engine = get_database_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None
    return load_and_process_data(query=vendor_query, engine=engine, logger=logger)

if __name__ == "__main__":
    set_pandas_display_options()
    
    # Test vendor data loader
    print("\n=== VENDOR DATA ===")
    vendor_df = get_all_vendor_data()
    if vendor_df is not None:
        print("All Vendor Data Preview:")
        print(vendor_df.head(10))
        print("\nColumn Names:", vendor_df.columns.tolist())
        print("\nTotal records:", len(vendor_df)) 