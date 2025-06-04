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
from data_access.req_database import get_engine, load_and_process_table

logger = configure_logging()

# Get project root for consistent SQL file path resolution
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# SQL queries with full paths
purchase_all_query = read_sql_file(os.path.join(project_root, 'sql', 'purchase', 'purchase_all.sql'))
purchase_lead_time_query = read_sql_file(os.path.join(project_root, 'sql', 'purchase', 'purchase_lead_time.sql'))
purchase_receipt_query = read_sql_file(os.path.join(project_root, 'sql', 'purchase', 'purchase_receipt.sql'))

# For requisition data
pr_query_path = os.path.join(project_root, 'sql', 'req', 'req.sql')
with open(pr_query_path, 'r', encoding='utf-8') as file:
    pr_query = file.read()

def get_all_purchase_data():
    """Returns a DataFrame containing all purchase data."""
    engine = get_database_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None
    return load_and_process_data(query=purchase_all_query, engine=engine, logger=logger)

def get_all_purchase_lead_time_data():
    """Returns a DataFrame containing purchase lead time data."""
    engine = get_database_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None
    return load_and_process_data(query=purchase_lead_time_query, engine=engine, logger=logger)

def get_all_purchase_receipt_data():
    """Returns a DataFrame containing all purchase receipt data."""
    engine = get_database_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None
    return load_and_process_data(query=purchase_receipt_query, engine=engine, logger=logger)

def get_pr_data():
    """
    Returns a DataFrame containing purchase requisition data for items that are
    on hold or pending approval.
    """
    engine = get_engine()
    if not engine:
        logger.error("Could not get database engine.")
        return None

    try:
        df = load_and_process_table(query=pr_query, engine=engine)
        logger.info("Loaded %d requisition records", len(df) if df is not None else 0)
        return df
    except Exception as e:
        logger.error("Error during query execution or processing: %s", e)
        return None

if __name__ == "__main__":
    set_pandas_display_options()
    
    # Test all data loaders
    print("\n=== PURCHASE DATA ===")
    purchase_df = get_all_purchase_data()
    if purchase_df is not None:
        print("All Purchase Data Preview:")
        print(purchase_df.head(5))
        print("\nTotal records:", len(purchase_df))
    
    print("\n=== PURCHASE LEAD TIME DATA ===")
    lead_time_df = get_all_purchase_lead_time_data()
    if lead_time_df is not None:
        print("Purchase Lead Time Data Preview:")
        print(lead_time_df.head(5))
        print("\nTotal records:", len(lead_time_df))
    
    print("\n=== PURCHASE RECEIPT DATA ===")
    receipt_df = get_all_purchase_receipt_data()
    if receipt_df is not None:
        print("Purchase Receipt Data Preview:")
        print(receipt_df.head(5))
        print("\nTotal records:", len(receipt_df))
    
    print("\n=== REQUISITION DATA ===")
    pr_df = get_pr_data()
    if pr_df is not None:
        print("PR Data Preview:")
        print(pr_df.head(5))
        print("\nTotal records:", len(pr_df)) 