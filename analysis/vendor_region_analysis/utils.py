"""Utility functions for vendor region analysis.

This module provides common utility functions used in vendor region analysis.
"""

import pandas as pd

def filter_country(df: pd.DataFrame, country: str) -> pd.DataFrame:
    """Filter a dataframe to only include rows from the specified country.
    
    Args:
        df: DataFrame containing vendor data with vendor_country column
        country: ISO country code to filter on
        
    Returns:
        DataFrame containing only rows with the specified country
    """
    return df[df["vendor_country"] == country].copy() 