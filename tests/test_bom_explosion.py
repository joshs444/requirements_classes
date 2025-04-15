import os
import sys
import pandas as pd
import pytest

# Add the project root (parent directory of tests/) to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from bom.bom_explosion import BomExplosion

def test_simple_multi_level_bom():
    """Test a multi-level BOM with correct hierarchy and quantity calculations."""
    bom_data = pd.DataFrame({
        'parent_index': ['A', 'A', 'B', 'C'],
        'child_index': ['B', 'C', 'D', 'E'],
        'total': [2, 3, 4, 5]
    })
    bom = BomExplosion(top_level_indices=['A'], bom_data=bom_data)
    result_df = bom.create_bom_hierarchy()
    
    # Round floating point values to avoid precision issues
    result_df['qty_per'] = result_df['qty_per'].round(6)
    result_df['total_quantity'] = result_df['total_quantity'].round(6)
    
    expected = {
        ('A', 0, 'A', 'B', 2.0, 2.0),
        ('A', 1, 'B', 'D', 4.0, 8.0),
        ('A', 0, 'A', 'C', 3.0, 3.0),
        ('A', 1, 'C', 'E', 5.0, 15.0)
    }
    result = set(tuple(row) for row in result_df[['production_index', 'level', 'parent_index', 'child_index', 'qty_per', 'total_quantity']].to_records(index=False))
    
    assert result == expected, "Hierarchy or quantities do not match expected output"
    assert set(result_df['order']) == set(range(1, len(result_df) + 1)), "'order' column should be consecutive integers"

def test_single_level_bom():
    """Test a single-level BOM with direct components only."""
    bom_data = pd.DataFrame({
        'parent_index': ['X', 'X'],
        'child_index': ['Y', 'Z'],
        'total': [1, 2]
    })
    bom = BomExplosion(top_level_indices=['X'], bom_data=bom_data)
    result_df = bom.create_bom_hierarchy()
    
    expected = {
        ('X', 0, 'X', 'Y', 1.0, 1.0),
        ('X', 0, 'X', 'Z', 2.0, 2.0)
    }
    result = set(tuple(row) for row in result_df[['production_index', 'level', 'parent_index', 'child_index', 'qty_per', 'total_quantity']].to_records(index=False))
    
    assert result == expected, "Single-level BOM does not match expected output"
    assert set(result_df['order']) == set(range(1, len(result_df) + 1)), "'order' column should be consecutive integers"

def test_multiple_top_level_items():
    """Test explosion with multiple top-level items."""
    bom_data = pd.DataFrame({
        'parent_index': ['A', 'B', 'X'],
        'child_index': ['B', 'C', 'Y'],
        'total': [2, 3, 1]
    })
    bom = BomExplosion(top_level_indices=['A', 'X'], bom_data=bom_data)
    result_df = bom.create_bom_hierarchy()
    
    expected = {
        ('A', 0, 'A', 'B', 2.0, 2.0),
        ('A', 1, 'B', 'C', 3.0, 6.0),
        ('X', 0, 'X', 'Y', 1.0, 1.0)
    }
    result = set(tuple(row) for row in result_df[['production_index', 'level', 'parent_index', 'child_index', 'qty_per', 'total_quantity']].to_records(index=False))
    
    assert result == expected, "Multiple top-level items do not match expected output"
    assert set(result_df['order']) == set(range(1, len(result_df) + 1)), "'order' column should be consecutive integers"

def test_circular_reference():
    """Test handling of circular references in BOM data."""
    bom_data = pd.DataFrame({
        'parent_index': ['A', 'B', 'C'],
        'child_index': ['B', 'C', 'A'],
        'total': [1, 1, 1]
    })
    bom = BomExplosion(top_level_indices=['A'], bom_data=bom_data)
    result_df = bom.create_bom_hierarchy()
    
    expected = {
        ('A', 0, 'A', 'B', 1.0, 1.0),
        ('A', 1, 'B', 'C', 1.0, 1.0)
    }
    result = set(tuple(row) for row in result_df[['production_index', 'level', 'parent_index', 'child_index', 'qty_per', 'total_quantity']].to_records(index=False))
    
    assert result == expected, "Circular reference not handled correctly"
    assert set(result_df['order']) == set(range(1, len(result_df) + 1)), "'order' column should be consecutive integers"

def test_zero_quantities():
    """Test BOM with zero quantities, which should be excluded."""
    bom_data = pd.DataFrame({
        'parent_index': ['A', 'A'],
        'child_index': ['B', 'C'],
        'total': [0, 2]
    })
    bom = BomExplosion(top_level_indices=['A'], bom_data=bom_data)
    result_df = bom.create_bom_hierarchy()
    
    expected = {
        ('A', 0, 'A', 'C', 2.0, 2.0)
    }
    result = set(tuple(row) for row in result_df[['production_index', 'level', 'parent_index', 'child_index', 'qty_per', 'total_quantity']].to_records(index=False))
    
    assert result == expected, "Zero quantities not handled correctly"
    assert set(result_df['order']) == set(range(1, len(result_df) + 1)), "'order' column should be consecutive integers"

def test_non_numeric_quantities():
    """Test BOM with non-numeric quantities, expecting conversion to 0.0."""
    bom_data = pd.DataFrame({
        'parent_index': ['A', 'A'],
        'child_index': ['B', 'C'],
        'total': ['two', 3]
    })
    bom = BomExplosion(top_level_indices=['A'], bom_data=bom_data)
    result_df = bom.create_bom_hierarchy()
    
    expected = {
        ('A', 0, 'A', 'C', 3.0, 3.0)
    }
    result = set(tuple(row) for row in result_df[['production_index', 'level', 'parent_index', 'child_index', 'qty_per', 'total_quantity']].to_records(index=False))
    
    assert result == expected, "Non-numeric quantities not handled correctly"
    assert set(result_df['order']) == set(range(1, len(result_df) + 1)), "'order' column should be consecutive integers"

def test_empty_bom_data():
    """Test handling of an empty BOM DataFrame."""
    bom_data = pd.DataFrame(columns=['parent_index', 'child_index', 'total'])
    bom = BomExplosion(top_level_indices=['A'], bom_data=bom_data)
    result_df = bom.create_bom_hierarchy()
    
    assert result_df.empty, "Result should be empty for empty BOM data"
    assert list(result_df.columns) == ['order', 'production_index', 'level', 'parent_index', 'child_index', 'qty_per', 'total_quantity'], "Output columns incorrect"

def test_missing_columns():
    """Test initialization with missing required columns."""
    bom_data = pd.DataFrame({
        'parent_index': ['A'],
        'child_index': ['B']
        # 'total' is missing
    })
    with pytest.raises(ValueError):
        BomExplosion(top_level_indices=['A'], bom_data=bom_data)

def test_top_level_not_in_bom():
    """Test when top-level index is not in BOM data."""
    bom_data = pd.DataFrame({
        'parent_index': ['X'],
        'child_index': ['Y'],
        'total': [1]
    })
    bom = BomExplosion(top_level_indices=['A'], bom_data=bom_data)
    result_df = bom.create_bom_hierarchy()
    
    assert result_df.empty, "Result should be empty when top-level index is not in BOM"

def test_deep_hierarchy():
    """Test a deep BOM hierarchy with multiple levels."""
    bom_data = pd.DataFrame({
        'parent_index': ['A', 'B', 'C'],
        'child_index': ['B', 'C', 'D'],
        'total': [1, 1, 1]
    })
    bom = BomExplosion(top_level_indices=['A'], bom_data=bom_data)
    result_df = bom.create_bom_hierarchy()
    
    expected = {
        ('A', 0, 'A', 'B', 1.0, 1.0),
        ('A', 1, 'B', 'C', 1.0, 1.0),
        ('A', 2, 'C', 'D', 1.0, 1.0)
    }
    result = set(tuple(row) for row in result_df[['production_index', 'level', 'parent_index', 'child_index', 'qty_per', 'total_quantity']].to_records(index=False))
    
    assert result == expected, "Deep hierarchy levels or quantities incorrect"
    assert set(result_df['order']) == set(range(1, len(result_df) + 1)), "'order' column should be consecutive integers"