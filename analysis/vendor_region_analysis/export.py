"""Export functions for vendor region analysis.

This module provides functions to export analysis results to Excel files.
"""

import os
from pathlib import Path
from typing import Dict

import pandas as pd
from item.item_data import get_all_item_data

def export_to_excel(tables: Dict[str, pd.DataFrame], 
                   country_code: str = "vendor",
                   output_dir: str = "output") -> Path:
    """Export analysis results to an Excel workbook.
    
    Args:
        tables: Dictionary of DataFrames to export to sheets
        country_code: Country code for the filename
        output_dir: Directory to save the Excel file
        
    Returns:
        Path to the created Excel file
    """
    os.makedirs(output_dir, exist_ok=True)
    outfile = Path(output_dir) / f"{country_code}_vendor_spend.xlsx"
    
    with pd.ExcelWriter(outfile) as xl:
        tables["vendor_info"].to_excel(xl, sheet_name="Vendor Info", index=False)
        tables["vendor_item_detail"].to_excel(xl, sheet_name="Vendor × Item Detail", index=False)
        tables["alternative_vendor_options"].to_excel(xl, sheet_name="Alternative Vendor Options", index=False)
    
    return outfile 