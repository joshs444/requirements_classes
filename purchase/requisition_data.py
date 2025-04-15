import sys
import os
import logging
import pandas as pd
from data_access.req_database import get_engine, load_and_process_table

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set display options to show all columns
pd.set_option('display.max_columns', None)

def read_sql_file(file_path):
    """
    Read SQL query from a file.

    Args:
        file_path (str): Path to the SQL file

    Returns:
        str: Contents of the SQL file
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

# Get the SQL query from the req.sql file
sql_file_path = os.path.join(project_root, 'sql', 'req.sql')
pr_query = read_sql_file(sql_file_path)

def get_pr_data():
    """
    Returns a DataFrame containing purchase requisition data for items that are
    on hold or pending approval.

    Returns:
        pandas.DataFrame or None: PR data, or None if an error occurs.
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
    # Set display options for better visibility
    pd.set_option('display.width', 200)

    pr_df = get_pr_data()
    if pr_df is not None:
        print("PR Data Preview:")
        print(pr_df.head(10))
        print("\nColumn Names:", pr_df.columns.tolist())
        print("\nTotal records:", len(pr_df))
    else:
        print("Failed to retrieve requisition data.")
