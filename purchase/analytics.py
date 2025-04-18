# purchase/analytics.py
"""
Analytics layer – pure, testable business calculations & KPIs.
"""

from __future__ import annotations

from typing import Iterable, Union

import pandas as pd

from .repository import PurchaseRepository


class PurchaseAnalytics:
    """Business metrics computed from a PurchaseRepository."""

    def __init__(self, repo: PurchaseRepository):
        self.df = repo.all()

    # ── Core value metrics ──────────────────────────────────────────────
    @staticmethod
    def _require_cols(frame: pd.DataFrame, cols: Iterable[str]) -> None:
        missing = set(cols) - set(frame.columns)
        if missing:
            raise KeyError(f"Missing column(s): {', '.join(sorted(missing))}")

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

    # ── Grouped value roll‑ups ──────────────────────────────────────────
    def group_value(
        self,
        by: Union[str, list[str]],
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
        # open value only for rows whose status is OPEN
        f["open"] = 0.0
        mask_open = f["status"] == "OPEN"
        f.loc[mask_open, "open"] = (
            f.loc[mask_open, "unit_cost"] * f.loc[mask_open, "outstanding_quantity"]
        )
        f["total"] = f["delivered"] + f["open"]

        if kind not in {"delivered", "open", "total"}:
            raise ValueError("kind must be 'delivered', 'open', or 'total'")

        return f.groupby(by)[kind].sum().reset_index()
