import pandas as pd
from sqlalchemy import create_engine

# Database connection details
DB_TYPE = 'mssql+pyodbc'
DB_SERVER = 'ipgp-ox-dvsql02'
DB_NAME = 'PurchaseREQ_PRODCopy'
DB_USER = 'sql-prsRead'
DB_PASS = 'FhMEzNw7tcjTJGBDktko'
DB_DRIVER = 'ODBC Driver 17 for SQL Server'

# Build the connection string (URL-encode spaces in the driver name)
CONNECTION_STRING = (
    f"{DB_TYPE}://{DB_USER}:{DB_PASS}@{DB_SERVER}/{DB_NAME}"
    f"?driver={DB_DRIVER.replace(' ', '+')}"
)

def get_engine():
    return create_engine(CONNECTION_STRING)

def load_and_process_table(query, engine, rename_cols=None, additional_processing=None, **kwargs):
    """
    Runs a SQL query and returns a pandas DataFrame with optional processing.
    
    Args:
        query (str): SQL query to execute
        engine: SQLAlchemy engine
        rename_cols (dict, optional): Dictionary to rename columns {old_name: new_name}
        additional_processing (function, optional): Function to apply additional processing
        **kwargs: Additional arguments for the processing function
        
    Returns:
        DataFrame or None: Processed pandas DataFrame or None if error
    """
    try:
        df = pd.read_sql_query(query, con=engine)
        if rename_cols:
            df = df.rename(columns=rename_cols)
        if additional_processing:
            df = additional_processing(df, **kwargs)
        return df
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
        return None

