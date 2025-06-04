#!/usr/bin/env python
"""
CLI tool: Export requisition data to Excel with actionable purchasing insights.

Key features
------------
• Loads requisition, vendor, item, and purchase‑history tables, then builds
  cost‑benchmark look‑ups (avg unit cost last year and best vendor cost 2 yrs).  
• Produces an Excel workbook with four sheets:
    – Requisition Data  
    – CN (mainland China **and Hong Kong**)  
    – Item‑Vendor Summary  (no tariffs)  
    – Item‑Vendor Summary (Tariff)  (all prices tariff‑adjusted; HK treated as CN)  
• Highlights rows in red if potential savings >= 250 and in green if potential savings <= -250.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd
import xlsxwriter.utility as xl_util

try:
    import yaml
except ImportError:
    yaml = None

# ── Path & internal imports ────────────────────────────────────────────
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from req.data_loader import get_req_data
from vendor.data_loader import get_all_vendor_data
from item.item_data import get_all_item_data
from purchase.data_loader import get_all_purchase_data
from purchase.repository import PurchaseRepository
from purchase.analytics import PurchaseAnalytics

# ── Constants ───────────────────────────────────────────────────────────
SAVINGS_THRESHOLD = 250.0
CN_CODES = {"CN", "HK"}  # treat HK like CN everywhere

TARIFF_RATES = {
    **{abbr: 0.10 for abbr in
       ("AU", "BR", "KH", "CA", "CL", "CO", "DK", "EU", "FR", "DE", "IN", "ID",
        "IL", "IT", "JP", "MY", "MX", "PK", "PH", "SG", "KR", "LK", "CH", "TW",
        "TH", "TR", "UK", "VN")},
    "CN": 1.45,
    "HK": 1.45,
    "US": 0.00,
}

def _load_40pct_prefixes() -> tuple[str, ...]:
    cfg = project_root / "config" / "tariff_exclusions.yml"
    if yaml and cfg.exists():
        try:
            return tuple((yaml.safe_load(cfg.read_text()) or {}).get("prefixes", []))
        except Exception:
            pass
    return (
        "8517.13.00", "8471", "8517.62.00", "8473.30", "8528.52.00", "8542",
        "8486", "8524", "8523.51.00", "8541.10.00", "8541.21.00", "8541.29.00",
        "8541.30.00", "8541.49.10", "8541.49.70", "8541.49.80", "8541.49.95",
        "8541.51.00", "8541.59.00", "8541.90.00",
    )

CN_40PCT_PREFIXES = _load_40pct_prefixes()

# ── Core helpers ────────────────────────────────────────────────────────
def _normalize_cn(code: str | None) -> str | None:
    return "CN" if code in CN_CODES else code


def _build_purchase_analytics() -> PurchaseAnalytics:
    hist = get_all_purchase_data()
    if hist is None or hist.empty:
        raise RuntimeError("Purchase history empty.")
    return PurchaseAnalytics(PurchaseRepository(hist))


def _compute_cost_lookups(
    analytics: PurchaseAnalytics, vendor_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    item_avg = analytics.avg_unit_cost_per_item_last_year() \
        .rename(columns={"avg_unit_cost": "avg_unit_cost_last_year"})

    vendor_item_avg = analytics.avg_unit_cost_vendor_item_last_2yrs()
    idx = vendor_item_avg.groupby("item_no")["avg_unit_cost"].idxmin()
    best_vendor = (
        vendor_item_avg.loc[idx]
        .rename(columns={
            "avg_unit_cost": "best_vendor_avg_cost_2yrs",
            "vendor_name": "best_vendor_name_2yrs",
        })[["item_no", "best_vendor_avg_cost_2yrs", "best_vendor_name_2yrs"]]
        .merge(
            vendor_df[["name", "country_region_code"]]
            .rename(columns={
                "name": "best_vendor_name_2yrs",
                "country_region_code": "best_vendor_country_2yrs",
            }),
            on="best_vendor_name_2yrs",
            how="left",
        )
    )
    return item_avg, best_vendor

# ── Summary builder ─────────────────────────────────────────────────────
def _make_summary(
    req_df: pd.DataFrame,
    item_avg: pd.DataFrame,
    best_vendor: pd.DataFrame,
    *,
    with_tariff: bool,
) -> pd.DataFrame:
    df = (
        req_df.groupby(
            ["PartNum", "VendorID", "vendor_name", "country_region_code", "SubmitUser"],
            as_index=False,
        )
        .agg(
            total_qty=("OrderQty", "sum"),
            avg_req_unit_price=("UnitPrice", "mean"),
            hts_code=("hts_code", "first"),
        )
        .merge(item_avg, left_on="PartNum", right_on="item_no", how="left")
        .merge(best_vendor, left_on="PartNum", right_on="item_no", how="left")
    )

    df["hts_40pct"] = df["hts_code"].astype(str).str.startswith(CN_40PCT_PREFIXES, na=False)

    # ── Tariff adjustments ──────────────────────────────────────────
    if with_tariff:
        vendor_cn = df["country_region_code"].apply(_normalize_cn)
        alt_cn = df["best_vendor_country_2yrs"].apply(_normalize_cn)

        df["tariff_rate_vendor"] = vendor_cn.map(TARIFF_RATES).fillna(0.0)
        df["tariff_rate_alt"] = alt_cn.map(TARIFF_RATES).fillna(0.0)

        df.loc[(vendor_cn == "CN") & df["hts_40pct"], "tariff_rate_vendor"] = 0.40
        df.loc[(alt_cn == "CN") & df["hts_40pct"],   "tariff_rate_alt"]   = 0.40

        df["avg_req_unit_price"]          *= 1 + df["tariff_rate_vendor"]
        df["avg_unit_cost_last_year"]     *= 1 + df["tariff_rate_vendor"]
        df["best_vendor_avg_cost_2yrs"]   *= 1 + df["tariff_rate_alt"]

    # ── Savings calculations ─────────────────────────────────────────
    df["savings_per_unit_avg"] = df["avg_req_unit_price"] - df["avg_unit_cost_last_year"]
    df["savings_per_unit_alt"] = df["avg_req_unit_price"] - df["best_vendor_avg_cost_2yrs"]

    # choose benchmark for displayed potential_savings column
    if with_tariff:
        df["savings_per_unit"] = df["savings_per_unit_alt"]
    else:
        df["savings_per_unit"] = df[["savings_per_unit_avg", "savings_per_unit_alt"]].max(axis=1)

    df["potential_savings"] = df["savings_per_unit"] * df["total_qty"]

    df.sort_values("potential_savings", ascending=False, inplace=True)

    cols_common = [
        "PartNum", "VendorID", "vendor_name", "country_region_code", "SubmitUser",
        "total_qty", "hts_code", "hts_40pct",
        "avg_req_unit_price", "best_vendor_avg_cost_2yrs", "best_vendor_name_2yrs",
        "best_vendor_country_2yrs", "savings_per_unit", "potential_savings",
    ]
    if not with_tariff:
        cols_common.insert(cols_common.index("best_vendor_avg_cost_2yrs"),
                           "avg_unit_cost_last_year")

    return df[cols_common]

# ── Excel helpers ──────────────────────────────────────────────────────
def _write_summary_sheet(*, df: pd.DataFrame, sheet_name: str, writer, threshold: float):
    df.to_excel(writer, sheet_name=sheet_name, index=False)

    wb, ws = writer.book, writer.sheets[sheet_name]
    red_fmt = wb.add_format({"bg_color": "#FFC7CE"})
    green_fmt = wb.add_format({"bg_color": "#C6EFCE"})

    r0 = 1
    rng = xl_util.xl_range(r0, 0, r0 + len(df) - 1, len(df.columns) - 1)
    sav_col = xl_util.xl_col_to_name(df.columns.get_loc("potential_savings"))

    # Highlight red if potential_savings >= threshold
    ws.conditional_format(rng, {
        "type": "formula",
        "criteria": f'=${sav_col}{r0+1}>={threshold}',
        "format": red_fmt,
    })

    # Highlight green if potential_savings <= -threshold
    ws.conditional_format(rng, {
        "type": "formula",
        "criteria": f'=${sav_col}{r0+1}<=-{threshold}',
        "format": green_fmt,
    })

# ── Excel export orchestrator ──────────────────────────────────────────
def export_to_excel(
    req_df: pd.DataFrame,
    item_avg: pd.DataFrame,
    best_vendor: pd.DataFrame,
    output_dir: str,
) -> Path:
    os.makedirs(output_dir, exist_ok=True)
    outfile = Path(output_dir) / "requisition_data.xlsx"

    detail_cols = [
        "PartNum", "OrderQty", "RequestDelivery", "UnitPrice", "LastDirectCost",
        "SubmitDate", "SubmitUser", "VendorID", "vendor_name",
        "country_region_code", "hts_code",
        "avg_unit_cost_last_year",
        "best_vendor_avg_cost_2yrs", "best_vendor_name_2yrs",
        "best_vendor_country_2yrs",
    ]

    with pd.ExcelWriter(outfile, engine="xlsxwriter") as writer:
        req_df[detail_cols].to_excel(writer, sheet_name="Requisition Data", index=False)
        req_df.loc[req_df["country_region_code"].isin(CN_CODES), detail_cols] \
              .to_excel(writer, sheet_name="CN", index=False)

        _write_summary_sheet(
            df=_make_summary(req_df, item_avg, best_vendor, with_tariff=False),
            sheet_name="Item-Vendor Summary",
            writer=writer,
            threshold=SAVINGS_THRESHOLD,
        )
        _write_summary_sheet(
            df=_make_summary(req_df, item_avg, best_vendor, with_tariff=True),
            sheet_name="Item-Vendor Summary (Tariff)",
            writer=writer,
            threshold=SAVINGS_THRESHOLD,
        )

    return outfile

# ── Data prep helper ───────────────────────────────────────────────────
def _prepare_frames():
    req_df   = get_req_data()
    vendor_df = get_all_vendor_data()
    item_df  = get_all_item_data()
    for name, df in [("Requisition", req_df), ("Vendor", vendor_df), ("Item", item_df)]:
        if df is None or df.empty:
            raise SystemExit(f"{name} data frame empty – aborting.")

    req_df = (
        req_df.merge(
            vendor_df[["no", "country_region_code", "name"]]
            .rename(columns={"name": "vendor_name"}),
            left_on="VendorID", right_on="no", how="left",
        )
        .merge(
            item_df[["item_no", "hts"]].rename(columns={"hts": "hts_code"}),
            left_on="PartNum", right_on="item_no", how="left",
        )
    )

    item_avg, best_vendor = _compute_cost_lookups(_build_purchase_analytics(), vendor_df)

    req_df = (
        req_df.merge(item_avg,    left_on="PartNum", right_on="item_no", how="left")
              .merge(best_vendor, left_on="PartNum", right_on="item_no", how="left")
              .drop(columns=[c for c in ("no", "item_no_x", "item_no_y") if c in req_df.columns])
    )
    return req_df, item_avg, best_vendor

# ── CLI entrypoint ─────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Export requisition data to Excel")
    parser.add_argument("-o", "--output-dir", default="output", help="Output directory")
    outfile = export_to_excel(*_prepare_frames(), output_dir=parser.parse_args().output_dir)
    print(f"Exported workbook → {outfile.resolve()}")

if __name__ == "__main__":
    main()