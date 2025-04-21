"""CLI entry-point for requisition data analysis.

This module provides a command-line interface for exporting requisition data to Excel.
"""

import argparse
import os
from pathlib import Path

from req.data_loader import get_req_data

def export_req_to_excel(req_df, output_dir: str = "output") -> Path:
    """Export requisition data to an Excel workbook.
    
    Args:
        req_df: DataFrame with requisition data
        output_dir: Directory to save the Excel file
        
    Returns:
        Path to the created Excel file
    """
    import pandas as pd
    
    os.makedirs(output_dir, exist_ok=True)
    outfile = Path(output_dir) / "requisition_data.xlsx"
    
    # Only include selected columns
    req_cols = ["PartNum", "OrderQty", "RequestDelivery", "UnitPrice", 
                "SubmitDate", "VendorID", "Department", "SubmitUser"]
    
    with pd.ExcelWriter(outfile) as xl:
        req_df[req_cols].to_excel(xl, sheet_name="Requisition Data", index=False)
    
    return outfile

def main():
    """Execute the requisition data export as a CLI application."""
    parser = argparse.ArgumentParser(description="Export requisition data to Excel")
    parser.add_argument("--output-dir", type=str, default="output", 
                        help="Directory for output files")
    args = parser.parse_args()

    # Get the requisition data
    req_df = get_req_data()
    
    if req_df.empty:
        raise SystemExit("Requisition data frame is empty – aborting export.")

    outfile = export_req_to_excel(req_df, args.output_dir)
    print(f"Exported requisition data workbook → {outfile.absolute()}")

if __name__ == "__main__":
    main() 