"""CLI entry-point for vendor region analysis.

This module provides a command-line interface for performing vendor region analysis.
"""

import argparse
from pathlib import Path

from purchase.repository import PurchaseRepository
from purchase.data_loader import get_all_purchase_data
from item.item_data import get_all_item_data
from analysis.vendor_region_analysis.analysis import analyse_vendor_exposure
from analysis.vendor_region_analysis.export import export_to_excel

def main():
    """Execute the vendor region analysis as a CLI application."""
    parser = argparse.ArgumentParser(description="Analyze vendor exposure by region")
    parser.add_argument("--country", type=str, default="CN", help="ISO country code to analyze")
    parser.add_argument("--output-dir", type=str, default="output", 
                       help="Directory for output files")
    parser.add_argument("--threshold", type=float, default=1000.0, 
                       help="Minimum spend threshold to include an item/vendor")
    args = parser.parse_args()

    # Get the purchase data and create a repository
    purchase_df = get_all_purchase_data()
    item_df = get_all_item_data()
    
    # Create the repository from the purchase data
    repo = PurchaseRepository(purchase_df)

    if purchase_df.empty:
        raise SystemExit("Purchase data frame is empty – aborting analysis.")

    cfg = {
        "country": args.country,
        "spend_threshold": args.threshold
    }

    tables = analyse_vendor_exposure(repo, item_df, cfg)
    outfile = export_to_excel(tables, args.country, args.output_dir)
    print(f"Exported {args.country} vendor exposure workbook → {outfile.absolute()}")

if __name__ == "__main__":
    main() 