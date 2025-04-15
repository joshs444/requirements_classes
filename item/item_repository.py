# File: item/item_repository.py

import pandas as pd
from ledger.ledger_data import get_item_ledger_data
from purchase.purchase_data import get_purchase_data
from .item_data import get_item_data
import logging

# Configure logging
logger = logging.getLogger(__name__)

class ItemRepository:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._final_item_table = None

    def load_aggregated_ledger_data(self):
        try:
            # Load and filter ledger data
            ledger_df = get_item_ledger_data()
            ledger_df["posting_date"] = pd.to_datetime(ledger_df["posting_date"])
            nine_months_ago = pd.Timestamp.today() - pd.DateOffset(months=9)
            ledger_agg = ledger_df[
                (ledger_df["posting_date"] >= nine_months_ago) &
                (ledger_df["entry_type"].isin([0, 6]))
            ].pivot_table(
                index="item_no",
                columns="entry_type",
                values="quantity",
                aggfunc=lambda x: sum(abs(x)),
                fill_value=0
            ).reset_index().rename(columns={6: "output_9_months", 0: "purchase_9_months"})
            ledger_agg.columns.name = None

            # Load and aggregate purchase data
            purchase_agg = get_purchase_data()[
                (get_purchase_data()["status"] == "OPEN") &
                (get_purchase_data()["type"] == 2) &
                (get_purchase_data()["document_type"] == 1)
            ].groupby("item_no", as_index=False)["outstanding_quantity"].sum().rename(columns={"outstanding_quantity": "open_purchases"})

            # Merge and fill missing values
            merged_df = pd.merge(ledger_agg, purchase_agg, on="item_no", how="outer").fillna({"output_9_months": 0, "purchase_9_months": 0, "open_purchases": 0})
            return merged_df
        except Exception as e:
            logger.error(f"Failed to load aggregated ledger data: {e}")
            raise

    def load_final_item_data(self):
        try:
            aggregated_df = self.load_aggregated_ledger_data()
            item_df = get_item_data()

            if item_df is None:
                raise ValueError("Item data is None")
            if aggregated_df is None:
                raise ValueError("Aggregated ledger data is None")

            # Merge and fill missing values
            merged = pd.merge(item_df, aggregated_df, on="item_no", how="left").fillna({"purchase_9_months": 0, "output_9_months": 0, "open_purchases": 0})

            # Compute purchase_output
            merged["item_source_mapped"] = merged["item_source"].replace({
                "Made In-House": "Output",
                "Third Party Purchase": "Purchase",
                "Interco Purchase": "Purchase"
            })
            merged["main"] = merged["item_source_mapped"]
            merged.loc[merged["main"].str.strip() == "", "main"] = merged["replenishment_system"]
            merged["purchase_output"] = merged.apply(
                lambda row: "Purchase" if row["open_purchases"] > 0 or (row["main"] == "Output" and row["purchase_9_months"] > row["output_9_months"])
                else "Output" if row["main"] == "Purchase" and row["output_9_months"] > row["purchase_9_months"]
                else row["main"],
                axis=1
            )

            # Select columns and ensure item_index
            final_columns = item_df.columns.tolist() + ["open_purchases", "purchase_9_months", "output_9_months", "purchase_output"]
            final_result = merged[final_columns].copy()

            if "row_index" in final_result.columns:
                final_result = final_result.rename(columns={"row_index": "item_index"})
            if "item_index" not in final_result.columns:
                raise KeyError("Missing 'item_index' or 'row_index' in item data")

            final_result["item_index"] = final_result["item_index"].astype(int)
            return final_result
        except Exception as e:
            logger.error(f"Failed to load final item data: {e}")
            raise

    def get_final_item_table(self):
        if self._final_item_table is None:
            logger.info("ItemRepository: Loading final item table...")
            self._final_item_table = self.load_final_item_data()
        return self._final_item_table

    def get_configured_data(self):
        """Alias for get_final_item_table to match DataLoader interface"""
        return self.get_final_item_table()

    def refresh(self):
        logger.info("ItemRepository: Refreshing final item table...")
        self._final_item_table = self.load_final_item_data()
        return self._final_item_table

    def get_item_details(self, item_no):
        item_table = self.get_final_item_table()
        return item_table[item_table['item_no'] == item_no]