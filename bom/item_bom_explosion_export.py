import os
import sys
import pandas as pd
from pathlib import Path

# Add the project root (parent directory of bom/) to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Imports from custom modules
from item.item_repository import ItemRepository
from bom.bom_repository import BomRepository
from bom.bom_explosion import BomExplosion

def item_bom_explosion(item_no: str, output_dir: str = "outputs") -> str:
    """
    Creates a BOM explosion for a given item number and exports it to Excel.
    
    Args:
        item_no: The item number to explode
        output_dir: Directory to save the Excel file
        
    Returns:
        str: Path to the created Excel file
    """
    # Get item index
    item_repo = ItemRepository.get_instance()
    item_table = item_repo.get_final_item_table()
    
    # Check if item_no exists in the correct column
    if item_no not in item_table['item_no'].values:
        raise ValueError(f"Item number {item_no} not found")
        
    item_index = item_table[item_table['item_no'] == item_no]['item_index'].iloc[0]
    
    # Get BOM data
    bom_repo = BomRepository.get_instance()
    bom_data = bom_repo.load_configured_bom_data()
    
    # Run explosion
    explosion = BomExplosion(top_level_indices=[item_index], bom_data=bom_data)
    hierarchy_df = explosion.create_bom_hierarchy()
    
    # Create mapping for item numbers
    item_no_map = item_table.set_index('item_index')['item_no'].to_dict()
    
    # Convert indices to item numbers
    result_df = pd.DataFrame()
    result_df['Top Level Item'] = hierarchy_df['production_index'].map(item_no_map)
    result_df['Level'] = hierarchy_df['level']
    result_df['Parent Item'] = hierarchy_df['parent_index'].map(item_no_map)
    result_df['Component'] = hierarchy_df['child_index'].map(item_no_map)
    result_df['Qty Per'] = hierarchy_df['qty_per']
    result_df['Total Qty'] = hierarchy_df['total_quantity']
    
    # Create output directory if it doesn’t exist
    Path(output_dir).mkdir(exist_ok=True)
    
    # Export to Excel
    output_path = Path(output_dir) / f"item_bom_explosion_{item_no}.xlsx"
    result_df.to_excel(output_path, index=False)
    
    return str(output_path)

if __name__ == "__main__":
    # Example usage
    item_no = input("Enter item number: ")
    print(f"Processing item: {item_no}")  # Added message here
    try:
        output_path = item_bom_explosion(item_no)
        print(f"BOM explosion exported to: {output_path}")
    except Exception as e:
        print(f"Error: {e}")