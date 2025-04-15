# File: bom/bom_repository.py

import pandas as pd
from .bom_data import get_bom_data
from item.item_repository import ItemRepository
import logging

# Configure logging
logger = logging.getLogger(__name__)

class BomRepository:
    _instance = None

    @classmethod
    def get_instance(cls):
        """Returns the singleton instance of BomRepository."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        """Initializes the repository with an empty cache."""
        self._configured_bom_data = None

    def load_configured_bom_data(self):
        """
        Loads and processes the configured BOM data, ensuring parent_index
        and child_index columns are returned as integers.
        """
        # Access final item table via ItemRepository
        item_repo = ItemRepository.get_instance()
        final_item_table = item_repo.get_final_item_table()

        # Create mappings (item_index is already integer from ItemRepository)
        item_index_map = final_item_table.set_index("item_no")["item_index"].to_dict()
        item_po_map = final_item_table.set_index("item_no")["purchase_output"].to_dict()

        # Load and filter BOM data
        bom_df = get_bom_data()
        bom_df = bom_df[bom_df["production_bom_no"].map(item_po_map) == "Output"]

        # Add index columns and fill missing values
        bom_df["parent_index"] = bom_df["production_bom_no"].map(item_index_map).fillna(0)
        bom_df["child_index"] = bom_df["component_no"].map(item_index_map).fillna(0)

        # Ensure 'total' is numeric if present
        if "total" in bom_df.columns:
            bom_df["total"] = pd.to_numeric(bom_df["total"], errors="coerce")
        else:
            logger.warning("'total' column not found in BOM data")

        # Select final columns
        final_columns = ["production_bom_no", "component_no", "total", "parent_index", "child_index"]
        final_bom = bom_df[final_columns]

        logger.info("BomRepository: Successfully loaded and processed BOM data with integer indices.")
        return final_bom

    def get_configured_bom_data(self):
        """Returns cached BOM data, loading it if necessary."""
        if self._configured_bom_data is None:
            logger.info("BomRepository: Loading and caching BOM data...")
            self._configured_bom_data = self.load_configured_bom_data()
        else:
            logger.info("BomRepository: Returning cached BOM data.")
        return self._configured_bom_data

    def get_configured_data(self):
        """Alias for get_configured_bom_data to match DataLoader interface"""
        return self.get_configured_bom_data()

    def refresh(self):
        """Forces a reload of the BOM data."""
        logger.info("BomRepository: Refreshing BOM data...")
        self._configured_bom_data = self.load_configured_bom_data()
        return self._configured_bom_data