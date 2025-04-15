# File: inventory/inventory_repository.py

import pandas as pd
from .inventory_data import get_inventory_data
from item.item_repository import ItemRepository

class InventoryRepository:
    # Class variable for singleton pattern
    _instance = None

    @classmethod
    def get_instance(cls):
        """Returns the singleton instance of InventoryRepository."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        """Initializes the repository with an empty cache."""
        self._configured_inventory_data = None

    def load_configured_inventory_data(self):
        """
        Configures the inventory data by aggregating quantities and merging with item data.

        Returns:
            A DataFrame with 'item_no', 'item_index', and 'total_quantity'.
        """
        # Get the ItemRepository instance and final item table
        item_repo = ItemRepository.get_instance()
        final_item_table = item_repo.get_final_item_table()

        # Aggregate inventory directly
        inv_agg = get_inventory_data().groupby("item_no", as_index=False)["quantity"].sum().rename(columns={"quantity": "total_quantity"})

        # Merge with final item table and fill missing quantities
        final_inv = pd.merge(
            final_item_table[["item_no", "item_index"]],
            inv_agg,
            on="item_no",
            how="left"
        ).fillna({"total_quantity": 0})[["item_no", "item_index", "total_quantity"]]

        return final_inv

    def get_configured_inventory_data(self):
        """
        Returns the cached configured inventory data, loading it if necessary.

        Returns:
            A DataFrame containing the configured inventory data.
        """
        if self._configured_inventory_data is None:
            self._configured_inventory_data = self.load_configured_inventory_data()
        return self._configured_inventory_data

    def get_configured_data(self):
        """Alias for get_configured_inventory_data to match DataLoader interface"""
        return self.get_configured_inventory_data()

    def refresh(self):
        """
        Forces a reload of the configured inventory data and returns it.

        Returns:
            A DataFrame containing the freshly loaded configured inventory data.
        """
        self._configured_inventory_data = self.load_configured_inventory_data()
        return self._configured_inventory_data