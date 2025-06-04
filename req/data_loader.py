import os
import sys
import logging
import pandas as pd

# Add project root to path first
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_access.req_database import get_engine, load_and_process_table
from utils.config_utils import configure_logging, set_pandas_display_options

logger = configure_logging()

# Get project root for consistent SQL file path resolution
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Read SQL query for requisition data
req_query_path = os.path.join(project_root, 'sql', 'req', 'req.sql')
with open(req_query_path, 'r', encoding='utf-8') as file:
    req_query = file.read()

def get_req_data():
    """Returns a DataFrame containing requisition data."""
    engine = get_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None

    try:
        df = load_and_process_table(query=req_query, engine=engine)
        logger.info("Loaded %d requisition records", len(df) if df is not None else 0)
        return df
    except Exception as e:
        logger.error("Error during query execution or processing: %s", e)
        return None

if __name__ == "__main__":
    set_pandas_display_options()

    print("\n=== REQUISITION DATA ===")
    req_df = get_req_data()
    if req_df is not None:
        print("Requisition Data Preview:")
        print(req_df.head(5))
        print("\nTotal records:", len(req_df)) 