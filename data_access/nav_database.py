import pandas as pd
from sqlalchemy import create_engine

# Database connection details
DB_TYPE = 'mssql+pyodbc'
DB_HOST = 'IPGP-OX-AGP02'
DB_NAME = 'IPG-DW-PROTOTYPE'
DB_DRIVER = 'ODBC Driver 17 for SQL Server'  # Or 'SQL Server' if 17 isn’t installed
CONNECTION_STRING = f"{DB_TYPE}://{DB_HOST}/{DB_NAME}?driver={DB_DRIVER.replace(' ', '+')}&trusted_connection=yes"

def get_engine():
    return create_engine(CONNECTION_STRING)

def load_and_process_table(query, engine, rename_cols=None, additional_processing=None, **kwargs):
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

