# purchase/queries.py
"""
Query layer – ad‑hoc look‑ups & read‑only helpers.

These helpers stay free of side‑effects so they can be re‑used from
notebooks, dashboards or scheduled jobs without surprises.
"""

from __future__ import annotations

from typing import Sequence

import pandas as pd

from .repository import PurchaseRepository
from utils.time_utils import TimeUtils


class PurchaseQueries:
    def __init__(self, repo: PurchaseRepository):
        # One *read‑only* snapshot for all helpers
        self.df = repo.all()

    # ── Date helpers ────────────────────────────────────────────────────
    @staticmethod
    def _apply_period(
        frame: pd.DataFrame,
        time_period: str | int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """Filter by explicit dates or by a named period (ytd, 90, etc.)."""
        if time_period:
            start_date, end_date = TimeUtils.get_period_dates(time_period)

        if start_date:
            frame = frame[frame["order_date"] >= pd.to_datetime(start_date)]
        if end_date:
            frame = frame[frame["order_date"] <= pd.to_datetime(end_date)]

        return frame

    # ── High‑level look‑ups ────────────────────────────────────────────
    def items_last_purchased_from_country(
        self,
        country: str,
        *,
        time_period: str | int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        One row per item – most recent PO from ``country`` within the window.
        """
        df = self._apply_period(self.df, time_period, start_date, end_date)
        df = df[df["vendor_country"] == country]
        df = df.sort_values("order_date", ascending=False).drop_duplicates("item_no")
        return df[["item_no", "order_date", "vendor_name"]].reset_index(drop=True)

    def identify_items_from_multiple_countries(
        self,
        country: str,
        *,
        time_period: str | int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        Flag items that have *also* been sourced outside ``country``.
        """
        df = self._apply_period(self.df, time_period, start_date, end_date)

        # How many unique countries per item?
        country_counts = (
            df.groupby("item_no")["vendor_country"].nunique().reset_index()
        ).rename(columns={"vendor_country": "country_count"})

        # Items bought (at least once) from the main country
        items_from_country = df.loc[df["vendor_country"] == country, "item_no"].unique()
        result = country_counts[country_counts["item_no"].isin(items_from_country)]
        result["multi_country"] = result["country_count"].gt(1).map({True: "Yes", False: "No"})
        return result[["item_no", "multi_country"]]

    def filter_by_type(self, item_type: str) -> pd.DataFrame:
        """Quick filter for GL / Item / FA lines."""
        return self.df[self.df["type"] == item_type].copy()

    def get_vendors_for_item_excluding_countries(
        self,
        item_no: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        exclude_countries: str | Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """
        All vendors that supplied ``item_no`` in the window, excluding
        any in ``exclude_countries``.
        """
        df = self.df[self.df["item_no"] == item_no]
        df = self._apply_period(df, None, start_date, end_date)

        if exclude_countries:
            if isinstance(exclude_countries, str):
                exclude_countries = [exclude_countries]
            df = df[~df["vendor_country"].isin(exclude_countries)]

        return (
            df[["vendor_name", "vendor_country"]]
            .drop_duplicates()
            .sort_values("vendor_name")
            .reset_index(drop=True)
        )

    def get_most_recent_purchase_data(
        self,
        *,
        item_no: str | None = None,
        vendor_name: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: list[str] | None = None,
        group_by: str = "both",  # 'item', 'vendor', 'both'
    ) -> pd.DataFrame:
        """
        Latest PO lines for a given item/vendor combo.
        """
        df = self._apply_period(self.df, None, start_date, end_date)

        if item_no is not None:
            df = df[df["item_no"] == item_no]
        if vendor_name is not None:
            df = df[df["vendor_name"] == vendor_name]

        df = df.sort_values("order_date", ascending=False)

        if group_by == "item":
            df = df.drop_duplicates("item_no")
        elif group_by == "vendor":
            df = df.drop_duplicates("vendor_name")
        elif group_by == "both":
            df = df.drop_duplicates(["item_no", "vendor_name"])
        else:
            raise ValueError("group_by must be 'item', 'vendor', or 'both'")

        if fields is None:
            fields = df.columns.tolist()

        core = ["item_no", "vendor_name", "order_date"]
        extra = [c for c in fields if c not in core]
        return df[core + extra].reset_index(drop=True)
