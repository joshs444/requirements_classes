# purchase/repository.py
"""
Repository layer – **only** owns and returns raw purchase data.

You can swap the constructor to accept a SQL connection, a CSV path,
or anything else.  Down‑stream code never touches the storage details.
"""

from __future__ import annotations

import pandas as pd


class PurchaseRepository:
    """Lightweight data‑access wrapper for purchase history."""

    REQUIRED_COLUMNS = {
        "status",
        "order_date",
        "unit_cost",
        "quantity_delivered",
        "outstanding_quantity",
        "item_no",
        "vendor_name",
        "vendor_country",
        "type",
        "document_no",
        "line_no",
    }

    def __init__(self, purchase_df: pd.DataFrame):
        if purchase_df is None or purchase_df.empty:
            raise ValueError("purchase_df cannot be None or empty")

        missing = self.REQUIRED_COLUMNS - set(purchase_df.columns)
        if missing:
            raise KeyError(
                f"Missing required column(s): {', '.join(sorted(missing))}"
            )

        # Store a cleaned copy
        self._df = purchase_df.copy()
        self._df["order_date"] = pd.to_datetime(self._df["order_date"])

    # ── Public “read” helpers ────────────────────────────────────────────
    def all(self) -> pd.DataFrame:
        """Return **all** purchase rows (never mutate this! use .copy())."""
        return self._df

    def open(self) -> pd.DataFrame:
        """Return only rows with status == 'OPEN'."""
        return self._df.query("status == 'OPEN'").copy()
