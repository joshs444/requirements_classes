import pandas as pd
from .ledger_data import get_item_ledger_data
from item.item_repository import ItemRepository
import logging
from enum import IntEnum
from utils.time_utils import TimeUtils

# Configure logging
logger = logging.getLogger(__name__)

class EntryType(IntEnum):
    PURCHASE = 0
    SALE = 1
    POSITIVE_ADJUSTMENT = 2
    NEGATIVE_ADJUSTMENT = 3
    TRANSFER = 4
    CONSUMPTION = 5
    OUTPUT = 6

class LedgerRepository:
    _instance = None

    @classmethod
    def get_instance(cls):
        """Returns the singleton instance of LedgerRepository."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        """Initializes the repository with an empty cache."""
        self._configured_ledger_data = None

    def load_configured_ledger_data(self):
        """
        Loads raw ledger data and adds an 'item_index' column by merging with the final item table.

        Returns:
            pandas.DataFrame: Ledger data with 'item_index' added.
        """
        try:
            # Get the ItemRepository instance and final item table
            item_repo = ItemRepository.get_instance()
            final_item_table = item_repo.get_final_item_table()

            # Load and merge ledger data directly
            merged_df = pd.merge(
                get_item_ledger_data(),
                final_item_table[["item_no", "item_index"]],
                on="item_no",
                how="left"
            ).assign(item_index=lambda x: x["item_index"].fillna(0).astype(int))

            return merged_df
        except Exception as e:
            logger.error(f"Failed to load configured ledger data: {e}")
            raise

    def get_configured_ledger_data(self):
        """
        Returns the cached configured ledger data, loading it if necessary.

        Returns:
            pandas.DataFrame: The configured ledger data.
        """
        if self._configured_ledger_data is None:
            self._configured_ledger_data = self.load_configured_ledger_data()
        return self._configured_ledger_data

    def get_configured_data(self):
        """Alias for get_configured_ledger_data to match DataLoader interface."""
        return self.get_configured_ledger_data()

    def refresh(self):
        """
        Forces a reload of the configured ledger data and returns it.

        Returns:
            pandas.DataFrame: Freshly loaded configured ledger data.
        """
        self._configured_ledger_data = self.load_configured_ledger_data()
        return self._configured_ledger_data

    def filter_ledger_data(self, entry_types=None, start_date=None, end_date=None, days=None, time_period=None, **kwargs):
        """
        Filters ledger data with support for predefined time periods.

        Args:
            entry_types (list, optional): List of entry types to filter by (e.g., [EntryType.PURCHASE, EntryType.SALE]).
            start_date (str or datetime, optional): Start date for filtering.
            end_date (str or datetime, optional): End date for filtering.
            days (int, optional): Number of days prior to today to filter.
            time_period (str, optional): Predefined time period like "ytd", "year", "quarter", or custom days.
            **kwargs: Additional column-value filters (e.g., item_no="ABC123").

        Returns:
            pandas.DataFrame: Filtered ledger data.
        """
        df = self.get_configured_ledger_data()

        # Handle predefined time periods
        if time_period:
            start_date = TimeUtils.get_start_date(time_period)

        # Filter by entry types if provided
        if entry_types is not None:
            df = df[df['entry_type'].isin(entry_types)]

        # Handle date filtering
        if days is not None and start_date is None:
            start_date = pd.to_datetime('today') - pd.Timedelta(days=days)
        if start_date is not None:
            start_date = pd.to_datetime(start_date)
            df = df[df['posting_date'] >= start_date]
        if end_date is not None:
            end_date = pd.to_datetime(end_date)
            df = df[df['posting_date'] <= end_date]

        # Apply additional filters from kwargs
        for key, value in kwargs.items():
            if key in df.columns:
                if isinstance(value, list):
                    df = df[df[key].isin(value)]
                else:
                    df = df[df[key] == value]

        return df