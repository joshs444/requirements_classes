import os
import sys
import logging
import pandas as pd
from data_access.nav_database import get_engine, load_and_process_table

# Define project root as a constant
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def add_project_root_to_path():
    """Add the project root directory to sys.path if not already present."""
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)

def configure_logging(level=logging.INFO):
    """Configure logging with a given level and return a logger."""
    logging.basicConfig(level=level)
    return logging.getLogger(__name__)

def read_sql_file(file_name):
    """Read an SQL query from a file in the 'sql' directory."""
    # Remove 'sql/' prefix if it's already in the file_name
    if file_name.startswith('sql/'):
        file_name = file_name[4:]  # Remove 'sql/' prefix
    
    sql_file_path = os.path.join(PROJECT_ROOT, 'sql', file_name)
    with open(sql_file_path, 'r', encoding='utf-8') as file:
        return file.read()

def get_database_engine():
    """Get the database engine from nav_database."""
    return get_engine()

def load_and_process_data(query, engine, logger):
    """Load and process data from a query, with error handling."""
    try:
        return load_and_process_table(query=query, engine=engine)
    except Exception as e:
        logger.error("Error during query execution or processing: %s", e)
        return None

def set_pandas_display_options():
    """Set pandas display options for better visibility."""
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200) 