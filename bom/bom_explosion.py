import pandas as pd
from collections import defaultdict


class BomExplosion:
    def __init__(self, top_level_indices, bom_data):
        """
        Initialize the SimplifiedBomExplosion class with top-level indices and BOM data.

        Args:
            top_level_indices (list): List of top-level item indices (e.g., integers from your database).
            bom_data (pd.DataFrame): DataFrame with 'parent_index', 'child_index', 'total' columns.
        """
        if bom_data is None:
            raise ValueError("BOM data must be provided.")

        # Ensure required columns are present
        required_columns = {'parent_index', 'child_index', 'total'}
        if not required_columns.issubset(bom_data.columns):
            raise ValueError(f"BOM data must contain columns: {required_columns}")

        self.bom_data = bom_data.copy()
        self.bom_data['total'] = pd.to_numeric(self.bom_data['total'], errors='coerce').fillna(0.0)

        # Create BOM dictionary for efficient lookup
        self.bom_dict = defaultdict(list)
        for _, row in self.bom_data.iterrows():
            parent = row['parent_index']
            child = row['child_index']
            qty = row['total']
            if pd.notna(qty):
                self.bom_dict[parent].append((child, qty))

        self.top_level_indices = top_level_indices
        self.hierarchy = []

    def build_indented_bom(self, main_number, top_level_index):
        """
        Build the BOM hierarchy iteratively for a single top-level item.

        Args:
            main_number: Production index (typically the same as top_level_index).
            top_level_index: Top-level item index to explode.
        """
        stack = []
        children = self.bom_dict.get(top_level_index, [])
        for child, qty_per in reversed(children):
            if qty_per > 0:
                total_qty = qty_per * 1.0  # Top-level quantity multiplier is 1.0
                stack.append((child, top_level_index, 0, qty_per, total_qty, {top_level_index}))

        while stack:
            current_index, parent_index, level, qty_per, total_qty, path = stack.pop()
            if current_index in path:
                continue  # Skip circular references to prevent infinite loops
            new_path = path.copy()
            new_path.add(current_index)
            self.hierarchy.append({
                'production_index': main_number,
                'level': level,
                'parent_index': parent_index,
                'child_index': current_index,
                'qty_per': qty_per,
                'total_quantity': total_qty
            })
            for child, child_qty_per in reversed(self.bom_dict.get(current_index, [])):
                if child_qty_per > 0:
                    child_total_qty = child_qty_per * total_qty
                    stack.append((child, current_index, level + 1, child_qty_per, child_total_qty, new_path))

    def create_bom_hierarchy(self):
        """
        Create the BOM hierarchy DataFrame for all top-level indices.

        Returns:
            pd.DataFrame: The fully expanded BOM hierarchy.
        """
        self.hierarchy = []
        for index in self.top_level_indices:
            self.build_indented_bom(index, index)
        df = pd.DataFrame(self.hierarchy)
        if df.empty:
            df = pd.DataFrame(columns=['production_index', 'level', 'parent_index', 'child_index', 'qty_per', 'total_quantity'])
        df.insert(0, 'order', range(1, len(df) + 1))
        return df