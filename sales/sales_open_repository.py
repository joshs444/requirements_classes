# File: sales/sales_open_repository.py

import pandas as pd
from .sales_open_data import get_sales_open_data
from item.item_repository import ItemRepository

class SalesOpenRepository:
    # Class variable for singleton pattern
    _instance = None

    @classmethod
    def get_instance(cls):
        """Returns the singleton instance of SalesOpenRepository."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        """Initializes the repository with an empty cache."""
        self._configured_sales_data = None

    def load_configured_sales_data(self):
        """
        Loads raw sales data and adds an 'item_index' column by referencing
        the final item table's 'item_index' for each item number.

        Returns:
            A DataFrame containing the original sales columns plus 'item_index'.
        """
        # Get the ItemRepository instance
        item_repo = ItemRepository.get_instance()

        # Get the final item table
        final_item_table = item_repo.get_final_item_table()

        # Create a mapping dictionary from item_no to item_index
        item_index_map = (
            final_item_table
            .set_index("item_no")["item_index"]
            .astype(int)
            .to_dict()
        )

        # Load raw sales data
        sales_df = get_sales_open_data().copy()

        # Add 'item_index' column
        sales_df["item_index"] = sales_df["item_no"].map(item_index_map).fillna(0).astype(int)

        return sales_df

    def get_configured_sales_data(self):
        """
        Returns the cached configured sales data, loading it if necessary.

        Returns:
            A DataFrame containing the configured sales data.
        """
        if self._configured_sales_data is None:
            self._configured_sales_data = self.load_configured_sales_data()
        return self._configured_sales_data

    def get_configured_data(self):
        """Alias for get_configured_sales_data to match DataLoader interface"""
        return self.get_configured_sales_data()

    def refresh(self):
        """
        Forces a reload of the configured sales data and returns it.

        Returns:
            A DataFrame containing the freshly loaded configured sales data.
        """
        self._configured_sales_data = self.load_configured_sales_data()
        return self._configured_sales_data

    def get_unique_sales_indexes(self):
        """
        Returns a list of unique sales indexes from the configured sales data.
        
        Returns:
            list: List of unique sales indexes
        """
        sales_data = self.get_configured_sales_data()
        return sorted(sales_data['item_index'].unique().tolist())