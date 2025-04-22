#!/usr/bin/env python
"""
CLI tool: Export requisition data to Excel with actionable purchasing insights.

What it does
------------
1. Loads requisition, vendor, item, and historical purchase data from the project’s
   data‑loader modules.
2. Computes:  
   • average unit cost per item for the past year  
   • lowest average unit cost & vendor for each item over the past 2 years  
3. Creates an Excel workbook containing:
   • **Requisition Data** – full detail plus cost benchmarks  
   • **CN** – subset of the detail where the current vendor is in mainland China **or
     Hong Kong**  
   • **Item‑Vendor Summary** – unit cost benchmarks & potential savings (no tariffs)  
   • **Item‑Vendor Summary (Tariff)** – the same summary after applying country
     tariff rates, Section‑301 HTS overrides, and China/HK 40 % prefixes
4. Highlights rows with actionable savings and flags “Moved From China” opportunities.
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
    import yaml  # used to read 40 % HTS prefix list
except ImportError:  # pragma: no cover
    yaml = None

# ── Project import path setup ───────────────────────────────────────────
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

# ── Internal imports ────────────────────────────────────────────────────
from req.data_loader import get_req_data
from vendor.data_loader import get_all_vendor_data
from item.item_data import get_all_item_data
from purchase.data_loader import get_all_purchase_data
from purchase.repository import PurchaseRepository
from purchase.analytics import PurchaseAnalytics

# ── Constants ───────────────────────────────────────────────────────────
SAVINGS_THRESHOLD = 250.0
CN_CODES = {"CN", "HK"}  # treat Hong Kong exactly like mainland China

# Base tariff rates (multiplier − 1)
TARIFF_RATES = {
    "AU": 0.10, "BR": 0.10, "KH": 0.10, "CA": 0.10, "CL": 0.10, "CN": 1.45,
    "CO": 0.10, "DK": 0.10, "EU": 0.10, "FR": 0.10, "DE": 0.10, "HK": 1.45,
    "IN": 0.10, "ID": 0.10, "IL": 0.10, "IT": 0.10, "JP": 0.10, "MY": 0.10,
    "MX": 0.10, "PK": 0.10, "PH": 0.10, "SG": 0.10, "KR": 0.10, "LK": 0.10,
    "CH": 0.10, "TW": 0.10, "TH": 0.10, "TR": 0.10, "UK": 0.10, "US": 0.00,
    "VN": 0.10,
}


def _load_40pct_prefixes() -> tuple[str, ...]:
    """Prefixes that trigger 40 % duty for China & Hong Kong under Section‑301."""
    cfg = project_root / "config" / "tariff_exclusions.yml"
    if yaml and cfg.exists():
        try:
            with cfg.open() as f:
                data = yaml.safe_load(f) or {}
            return tuple(data.get("prefixes", []))
        except Exception:  # pragma: no cover
            pass
    # fallback default list
    return (
        "8517.13.00", "8471", "8517.62.00", "8473.30", "8528.52.00", "8542",
        "8486", "8524", "8523.51.00", "8541.10.00", "8541.21.00", "8541.29.00",
        "8541.30.00", "8541.49.10", "8541.49.70", "8541.49.80", "8541.49.95",
        "8541.51.00", "8541.59.00", "8541.90.00",
    )


CN_40PCT_PREFIXES = _load_40pct_prefixes()


# ── Helper functions ────────────────────────────────────────────────────
def _build_purchase_analytics() -> PurchaseAnalytics:
    purchases = get_all_purchase_data()
    if purchases is None or purchases.empty:
        raise RuntimeError("Purchase data frame is empty – cannot compute averages.")
    return PurchaseAnalytics(PurchaseRepository(purchases))


def _compute_cost_lookups(
    analytics: PurchaseAnalytics, vendor_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Per‑item average last year + best vendor‑item average (2 yrs) with country."""
    item_avg = (
        analytics.avg_unit_cost_per_item_last_year()
        .rename(columns={"avg_unit_cost": "avg_unit_cost_last_year"})
    )

    vendor_item_avg = analytics.avg_unit_cost_vendor_item_last_2yrs()
    idx = vendor_item_avg.groupby("item_no")["avg_unit_cost"].idxmin()
    best_vendor = (
        vendor_item_avg.loc[idx]
        .rename(
            columns={
                "avg_unit_cost": "best_vendor_avg_cost_2yrs",
                "vendor_name": "best_vendor_name_2yrs",
            }
        )[["item_no", "best_vendor_avg_cost_2yrs", "best_vendor_name_2yrs"]]
        .merge(
            vendor_df[["name", "country_region_code"]]
            .rename(
                columns={
                    "name": "best_vendor_name_2yrs",
                    "country_region_code": "best_vendor_country_2yrs",
                }
            ),
            on="best_vendor_name_2yrs",
            how="left",
        )
    )
    return item_avg, best_vendor


def _normalize_cn(code: str | None) -> str | None:
    """Return 'CN' for 'CN' or 'HK'; otherwise pass through."""
    return "CN" if code in CN_CODES else code


# ── Excel export ────────────────────────────────────────────────────────
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

        # Mainland China + Hong Kong subset
        req_df.loc[req_df["country_region_code"].isin(CN_CODES), detail_cols].to_excel(
            writer, sheet_name="CN", index=False
        )

        summary_plain = _make_summary(req_df, item_avg, best_vendor, with_tariff=False)
        _write_summary_sheet(
            df=summary_plain,
            sheet_name="Item-Vendor Summary",
            writer=writer,
            threshold=SAVINGS_THRESHOLD,
        )

        summary_tariff = _make_summary(req_df, item_avg, best_vendor, with_tariff=True)
        _write_summary_sheet(
            df=summary_tariff,
            sheet_name="Item-Vendor Summary (Tariff)",
            writer=writer,
            threshold=SAVINGS_THRESHOLD,
        )

    return outfile


# ── Summary generation ─────────────────────────────────────────────────
def _make_summary(
    req_df: pd.DataFrame,
    item_avg: pd.DataFrame,
    best_vendor: pd.DataFrame,
    *,
    with_tariff: bool,
) -> pd.DataFrame:
    """
    Build Item‑Vendor summary.
    If `with_tariff=True`, tariff multipliers are applied to **all three** price
    inputs before savings logic: requisition price, avg last year, best vendor 2 yrs.
    """
    df = (
        req_df.groupby(
            ["PartNum", "VendorID", "vendor_name",
             "country_region_code", "SubmitUser"],
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

    df["hts_40pct"] = df["hts_code"].astype(str).str.startswith(
        CN_40PCT_PREFIXES, na=False
    )

    if with_tariff:
        vendor_cn = df["country_region_code"].apply(_normalize_cn)
        alt_cn = df["best_vendor_country_2yrs"].apply(_normalize_cn)

        df["tariff_rate_vendor"] = vendor_cn.map(TARIFF_RATES).fillna(0.0)
        df["tariff_rate_alt"] = alt_cn.map(TARIFF_RATES).fillna(0.0)

        vendor_40_mask = (vendor_cn == "CN") & df["hts_40pct"]
        alt_40_mask = (alt_cn == "CN") & df["hts_40pct"]
        df.loc[vendor_40_mask, "tariff_rate_vendor"] = 0.40
        df.loc[alt_40_mask, "tariff_rate_alt"] = 0.40

        df["avg_req_unit_price"] *= 1 + df["tariff_rate_vendor"]
        df["avg_unit_cost_last_year"] *= 1 + df["tariff_rate_vendor"]
        df["best_vendor_avg_cost_2yrs"] *= 1 + df["tariff_rate_alt"]

    df["benchmark_cost"] = df[
        ["avg_unit_cost_last_year", "best_vendor_avg_cost_2yrs"]
    ].min(axis=1, skipna=True)

    df["savings_per_unit"] = df["avg_req_unit_price"] - df["benchmark_cost"]
    df["potential_savings"] = df["savings_per_unit"] * df["total_qty"]

    cheaper_avg = (
        df["avg_unit_cost_last_year"].notna()
        & df["avg_req_unit_price"].gt(df["avg_unit_cost_last_year"])
    )
    cheaper_alt = (
        df["best_vendor_avg_cost_2yrs"].notna()
        & df["avg_req_unit_price"].gt(df["best_vendor_avg_cost_2yrs"])
        & df["vendor_name"].ne(df["best_vendor_name_2yrs"])
    )
    china_move = (
        cheaper_alt
        & df["best_vendor_country_2yrs"].apply(_normalize_cn).eq("CN")
        & df["country_region_code"].apply(_normalize_cn).ne("CN")
    )

    df["status_flag"] = np.select(
        [china_move, cheaper_avg, cheaper_alt],
        ["Moved From China", "Cheaper Average", "Cheaper Alternative Vendor"],
        default="",
    )

    low_val = df["potential_savings"].abs() < SAVINGS_THRESHOLD
    blank_mask = (
        low_val
        & df["status_flag"].isin(["Cheaper Average", "Cheaper Alternative Vendor"])
    )
    df.loc[blank_mask, "status_flag"] = ""
    default_idx = (
        df["potential_savings"].ge(SAVINGS_THRESHOLD) & df["status_flag"].eq("")
    )
    df.loc[default_idx, "status_flag"] = "Cheaper Average"

    df.sort_values("potential_savings", ascending=False, inplace=True)

    return df[
        [
            "PartNum", "VendorID", "vendor_name", "country_region_code", "SubmitUser",
            "total_qty", "hts_code", "hts_40pct",
            "avg_req_unit_price", "avg_unit_cost_last_year",
            "best_vendor_avg_cost_2yrs", "best_vendor_name_2yrs",
            "best_vendor_country_2yrs", "savings_per_unit",
            "potential_savings", "status_flag",
        ]
    ]


# ── Excel writing helpers ──────────────────────────────────────────────
def _write_summary_sheet(*, df: pd.DataFrame, sheet_name: str, writer, threshold: float):
    df.to_excel(writer, sheet_name=sheet_name, index=False)

    wb = writer.book
    ws = writer.sheets[sheet_name]

    red_fmt = wb.add_format({"bg_color": "#FFC7CE"})
    yellow_fmt = wb.add_format({"bg_color": "#FFEB9C"})

    r0 = 1
    rng = xl_util.xl_range(r0, 0, r0 + len(df) - 1, len(df.columns) - 1)
    flag_col = xl_util.xl_col_to_name(df.columns.get_loc("status_flag"))
    sav_col = xl_util.xl_col_to_name(df.columns.get_loc("potential_savings"))

    ws.conditional_format(
        rng,
        {"type": "formula",
         "criteria": f'=${flag_col}{r0+1}="Moved From China"',
         "format": yellow_fmt},
    )
    ws.conditional_format(
        rng,
        {"type": "formula",
         "criteria": (
             f"=AND(${sav_col}{r0+1}>={threshold},"
             f'${flag_col}{r0+1}<>"Moved From China")'
         ),
         "format": red_fmt},
    )


# ── CLI entry‑point ─────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Export requisition data to Excel")
    parser.add_argument("-o", "--output-dir", default="output", help="Output directory")
    outfile = export_to_excel(
        *_prepare_frames(),
        output_dir=parser.parse_args().output_dir,
    )
    print(f"Exported workbook → {outfile.resolve()}")


# ── Data preparation helper ────────────────────────────────────────────
def _prepare_frames():
    req_df = get_req_data()
    vendor_df = get_all_vendor_data()
    item_df = get_all_item_data()
    for name, df in [("Requisition", req_df), ("Vendor", vendor_df), ("Item", item_df)]:
        if df is None or df.empty:
            raise SystemExit(f"{name} data frame is empty – aborting.")

    req_df = (
        req_df.merge(
            vendor_df[["no", "country_region_code", "name"]].rename(
                columns={"name": "vendor_name"}
            ),
            left_on="VendorID",
            right_on="no",
            how="left",
        )
        .merge(
            item_df[["item_no", "hts"]].rename(columns={"hts": "hts_code"}),
            left_on="PartNum",
            right_on="item_no",
            how="left",
        )
    )

    analytics = _build_purchase_analytics()
    item_avg, best_vendor = _compute_cost_lookups(analytics, vendor_df)

    req_df = (
        req_df.merge(item_avg, left_on="PartNum", right_on="item_no", how="left")
        .merge(best_vendor, left_on="PartNum", right_on="item_no", how="left")
        .drop(columns=[c for c in ("no", "item_no_x", "item_no_y") if c in req_df.columns])
    )

    return req_df, item_avg, best_vendor


if __name__ == "__main__":
    main()
