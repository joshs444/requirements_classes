from __future__ import annotations

from typing import Iterable, Union, Sequence
from datetime import datetime, timedelta

import pandas as pd

from .repository import PurchaseRepository


class PurchaseAnalytics:
    """Business metrics computed from a PurchaseRepository."""

    def __init__(self, repo: PurchaseRepository):
        self.df = repo.all()

    # ── Internal helpers ─────────────────────────────────────────────────
    @staticmethod
    def _require_cols(frame: pd.DataFrame, cols: Iterable[str]) -> None:
        missing = set(cols) - set(frame.columns)
        if missing:
            raise KeyError(f"Missing column(s): {', '.join(sorted(missing))}")

    # ── Core value metrics ────────────────────────────────────────────────
    def delivered_value(self, frame: pd.DataFrame | None = None) -> float:
        f = frame if frame is not None else self.df
        self._require_cols(f, ["unit_cost", "quantity_delivered"])
        return (f["unit_cost"] * f["quantity_delivered"]).sum()

    def open_value(self, frame: pd.DataFrame | None = None) -> float:
        f = frame if frame is not None else self.df
        f = f.query("status == 'OPEN'")
        self._require_cols(f, ["unit_cost", "outstanding_quantity"])
        return (f["unit_cost"] * f["outstanding_quantity"]).sum()

    def total_value(self, frame: pd.DataFrame | None = None) -> float:
        f = frame if frame is not None else self.df
        return self.delivered_value(f) + self.open_value(f)

    # ── Grouped value roll‑ups ───────────────────────────────────────────
    def group_value(
        self,
        by: Union[str, Sequence[str]],
        kind: str = "total",
        frame: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """
        Sum delivered / open / total value by one or more columns.

        Parameters
        ----------
        by
            Column name or list of column names to group by.
        kind
            'delivered', 'open', or 'total'.
        frame
            Optional pre‑filtered dataframe.
        """
        f = frame if frame is not None else self.df.copy()
        self._require_cols(
            f,
            ["unit_cost", "quantity_delivered", "outstanding_quantity", "status"],
        )

        f["delivered"] = f["unit_cost"] * f["quantity_delivered"]
        f["open"] = 0.0
        mask_open = f["status"] == "OPEN"
        f.loc[mask_open, "open"] = (
            f.loc[mask_open, "unit_cost"] * f.loc[mask_open, "outstanding_quantity"]
        )
        f["total"] = f["delivered"] + f["open"]

        if kind not in {"delivered", "open", "total"}:
            raise ValueError("kind must be 'delivered', 'open', or 'total'")

        return f.groupby(by)[kind].sum().reset_index()

    # ── Weighted-average unit cost metrics ───────────────────────────────
    def weighted_avg_unit_cost(
        self,
        by: Union[str, Sequence[str]] = ("item_no",),
        *,
        lookback_days: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        Compute weighted‑average unit cost:

            Σ(unit_cost * quantity) / Σ(quantity)

        • `by`: column or sequence of columns to group by.
        • date window: either `lookback_days` or explicit `start_date` / `end_date`.
        • only considers rows where `type == 'Item'`.
        """
        f = self.df.query("type == 'Item'").copy()

        # apply date filters
        if lookback_days is not None:
            end_dt = pd.to_datetime(end_date) if end_date else pd.to_datetime(datetime.utcnow().date())
            start_dt = end_dt - timedelta(days=lookback_days)
            f = f[f["order_date"] >= start_dt]
            f = f[f["order_date"] <= end_dt]
        else:
            if start_date:
                f = f[f["order_date"] >= pd.to_datetime(start_date)]
            if end_date:
                f = f[f["order_date"] <= pd.to_datetime(end_date)]

        self._require_cols(f, ["unit_cost", "quantity"])

        group_cols = [by] if isinstance(by, str) else list(by)
        grouped = f.groupby(group_cols, dropna=False)
        avg = grouped.apply(
            lambda d: (d["unit_cost"] * d["quantity"]).sum() / d["quantity"].sum()
        )
        return avg.reset_index(name="avg_unit_cost")

    def avg_unit_cost_per_item_last_year(self) -> pd.DataFrame:
        """Average unit cost per item over the past 12 months."""
        return self.weighted_avg_unit_cost(
            by="item_no",
            lookback_days=365,
        )

    def avg_unit_cost_vendor_item_last_2yrs(self) -> pd.DataFrame:
        """Average unit cost per (vendor, item) over the past 24 months."""
        return self.weighted_avg_unit_cost(
            by=("buy_from_vendor_no", "vendor_name", "item_no"),
            lookback_days=730,
        )
