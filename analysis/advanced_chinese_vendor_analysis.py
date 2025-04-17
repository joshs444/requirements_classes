"""Vendor Exposure Analysis Tool.

This module provides functions to analyze purchase data, focusing on vendors from a specified country. It calculates spend metrics, identifies sourcing risks, and suggests actions based on alternative vendor options.

### Main Components:
- **Vendor Spend Calculation**: Computes open and delivered spend for vendors.
- **Item-Level Analysis**: Provides detailed insights into items with sourcing action recommendations.
- **Alternative Vendor Identification**: Identifies alternative vendors and compares prices.
- **Configurable Parameters**: Allows customization of country, spend thresholds, and other settings.

The analysis results are returned as dataframes and can be exported to an Excel workbook for easy sharing and review.

### To Use:
- Call `analyse_china_exposure` with purchase and item dataframes.
- Optionally, provide a configuration dictionary to customize the analysis.
- Export results using `_export_to_excel`.

**Note**: The tool is flexible and can analyze vendors from any country by adjusting the configuration, despite the function name suggesting a China-specific focus.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
import yaml

from purchase.purchase_repository import PurchaseRepository
from purchase.purchase_all_data import get_all_purchase_data
from utils.time_utils import TimeUtils
from item.item_data import get_all_item_data

# ────────────────────────────────────────────────────────────────────────────
# 0. Configuration helpers
# ────────────────────────────────────────────────────────────────────────────
_DEFAULT_CFG: Dict = {
    "country": "CN",                     # ISO country code to analyze
    "spend_threshold": 1_000,            # Minimum $ exposure to keep a row
    "tariff_prefix_file": Path("config/tariff_exclusions.yml"),
    # Price‑delta buckets
    "shift_now_threshold": 0.25,         # ≤25% premium counts as shift now
    "near_parity_threshold": 0.10,       # ≤10% premium counts as near‑parity
}

def _load_yaml_prefixes(path: Path) -> Tuple[str, ...]:
    """Load HTS prefixes from YAML; fall back to hard‑coded defaults."""
    default_prefixes = (
        "8517.13.00", "8471", "8517.62.00", "8473.3",
        "8528.52.00", "8542", "8486", "8524", "8523.51.00",
        "8541.10.00", "8541.21.00", "8541.29.00", "8541.30.00",
        "8541.49.10", "8541.49.70", "8541.49.80", "8541.49.95",
        "8541.51.00", "8541.59.00", "8541.90.00",
    )
    try:
        content = yaml.safe_load(path.read_text())
        return tuple(content.get("prefixes", default_prefixes))
    except FileNotFoundError:
        return default_prefixes

# ────────────────────────────────────────────────────────────────────────────
# 1. Generic helpers
# ────────────────────────────────────────────────────────────────────────────
def _filter_country(df: pd.DataFrame, country: str) -> pd.DataFrame:
    return df[df["vendor_country"] == country].copy()

# ────────────────────────────────────────────────────────────────────────────
# 2. Core calculators
# ────────────────────────────────────────────────────────────────────────────
def _get_vendor_spend(repo: PurchaseRepository,
                      start_date: pd.Timestamp,
                      end_date: pd.Timestamp,
                      *,
                      cfg: Dict) -> pd.DataFrame:
    df_all = repo.get_purchase_data()
    china = _filter_country(df_all, cfg["country"])
    past_year = china.query("@start_date <= order_date <= @end_date")

    open_sp = (repo.group_by_and_sum("vendor_name", "open", data=china)
               .rename(columns={"open_value": "all_open_spend"}))
    delivered = (repo.group_by_and_sum("vendor_name", "delivered", data=past_year)
                 .rename(columns={"delivered_value": "delivered_spend_past_year"}))

    out = open_sp.merge(delivered, how="outer").fillna(0)
    out["total_value"] = out["all_open_spend"] + out["delivered_spend_past_year"]
    out = (out[out["total_value"] >= cfg["spend_threshold"]]
           .sort_values("total_value", ascending=False)
           .drop(columns="total_value"))
    return out.reset_index(drop=True)


def _get_vendor_item_spend(repo: PurchaseRepository,
                           item_df: pd.DataFrame,
                           start_date: pd.Timestamp,
                           end_date: pd.Timestamp,
                           *,
                           cfg: Dict) -> pd.DataFrame:
    df_all = repo.get_purchase_data()
    china_items = _filter_country(repo.filter_by_type("Item"), cfg["country"])
    past_year = china_items.query("@start_date <= order_date <= @end_date")

    open_sp = (repo.group_by_and_sum(["vendor_name", "item_no"], "open", data=china_items)
               .rename(columns={"open_value": "all_open_spend"}))
    deliv_sp = (repo.group_by_and_sum(["vendor_name", "item_no"], "delivered", data=past_year)
                .rename(columns={"delivered_value": "delivered_spend_past_year"}))
    df = open_sp.merge(deliv_sp, how="outer").fillna(0)
    df["total_value"] = df["all_open_spend"] + df["delivered_spend_past_year"]
    df = df[df["total_value"] >= cfg["spend_threshold"]].drop(columns="total_value")

    multi = repo.identify_items_from_multiple_countries(cfg["country"])
    df = (df.merge(multi, on="item_no", how="left")
          .assign(alternative_vendor=lambda d: d["multi_country"].fillna("No"))
          .drop(columns=["multi_country"]))

    meta_cols = ["item_no", "description", "hts", "item_category_code"]
    df = df.merge(item_df[meta_cols], on="item_no", how="left")

    prefixes = _load_yaml_prefixes(cfg["tariff_prefix_file"])
    df["tariff_exclusion"] = (
        df["hts"].astype(str).str.startswith(prefixes).fillna(False)
          .map({True: "Yes", False: "No"})
    )

    recent_cols = ["item_no", "vendor_name", "order_date", "unit_cost",
                   "assigned_user_id", "cost_center"]
    recent = repo.get_most_recent_purchase_data(
        data=china_items, fields=recent_cols, group_by="both"
    )
    df = (df.merge(recent[recent_cols], on=["item_no", "vendor_name"], how="left")
          .rename(columns={
              "order_date": "last_purchase_date",
              "unit_cost": "last_unit_price"
          })
          .fillna({"assigned_user_id": "Unassigned", "cost_center": "Unassigned"}))

    all_recent = repo.get_most_recent_purchase_data(
        data=df_all,
        fields=["item_no", "vendor_name", "unit_cost"],
        group_by="both"
    )
    non_cn = all_recent.merge(
        df_all[["vendor_name", "vendor_country"]].drop_duplicates(),
        on="vendor_name", how="left"
    )
    non_cn = non_cn[non_cn["vendor_country"] != cfg["country"]]
    best_alt = (non_cn.sort_values("unit_cost")
                      .drop_duplicates("item_no")
                      .rename(columns={
                          "vendor_name": "alt_vendor_name",
                          "unit_cost":   "alt_unit_cost",
                          "vendor_country": "alt_vendor_country"
                      }))
    df = df.merge(
        best_alt[["item_no", "alt_vendor_name", "alt_unit_cost", "alt_vendor_country"]],
        on="item_no", how="left"
    )
    df["cost_delta_pct"] = (
        (df["alt_unit_cost"] - df["last_unit_price"]) / df["last_unit_price"]
    ).round(3)

    def _sourcing_action(r):
        if r["alternative_vendor"] == "No":
            return "Develop alt source"
        if pd.isna(r["cost_delta_pct"]):
            return "Analyse"
        if r["cost_delta_pct"] <= cfg["shift_now_threshold"]:
            return "Shift now"
        if r["cost_delta_pct"] <= cfg["near_parity_threshold"]:
            return "Quote / negotiate"
        return "Monitor"

    df["sourcing_action"] = df.apply(_sourcing_action, axis=1)

    return df.reset_index(drop=True)

# ────────────────────────────────────────────────────────────────────────────
# 3. Vendor‐info roll‐up
# ────────────────────────────────────────────────────────────────────────────
def _build_vendor_action_plan(by_item: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    group = by_item.groupby("vendor_name")

    summarize = pd.DataFrame({
        "all_open_spend":           group["all_open_spend"].sum(),
        "delivered_spend_12m":      group["delivered_spend_past_year"].sum(),
        "single_source_open_spend": group.apply(
            lambda d: d.loc[d["alternative_vendor"] == "No", "all_open_spend"].sum()
        ),
        "multi_source_open_spend":  group.apply(
            lambda d: d.loc[d["alternative_vendor"] == "Yes", "all_open_spend"].sum()
        ),
        "tariff_excl_pct":          group.apply(
            lambda d: (
                (d["tariff_exclusion"] == "Yes").mul(d["all_open_spend"]).sum()
            ) / d["all_open_spend"].sum() if d["all_open_spend"].sum() > 0 else 0
        ),
        "sku_count":                group["item_no"].nunique(),
        "single_source_count":      group.apply(
            lambda d: (d["alternative_vendor"] == "No").sum()
        ),
        "alt_cheaper":              group.apply(
            lambda d: (d["sourcing_action"] == "Shift now").sum()
        ),
        "est_savings":              group.apply(
            lambda d: abs(d["cost_delta_pct"].clip(upper=0)
                           .mul(d["all_open_spend"] + d["delivered_spend_past_year"]).sum()
            )
        ),
    }).reset_index()

    owner_map = group["assigned_user_id"].agg(
        lambda s: s.mode().iat[0] if not s.mode().empty else "Unassigned"
    )
    summarize["action_owner"] = summarize["vendor_name"].map(owner_map)

    return summarize

# ────────────────────────────────────────────────────────────────────────────
# 4. Public façade with pivoted Alternative Vendor Options
# ────────────────────────────────────────────────────────────────────────────
def analyse_china_exposure(purchase_df: pd.DataFrame,
                           item_df: pd.DataFrame,
                           cfg: Dict | None = None) -> Dict[str, pd.DataFrame]:
    cfg = {**_DEFAULT_CFG, **(cfg or {})}
    start_date, end_date = TimeUtils.get_period_dates("year")
    repo = PurchaseRepository(purchase_df)

    # Vendor and item spend
    by_vendor   = _get_vendor_spend(repo, start_date, end_date, cfg=cfg)
    by_item     = _get_vendor_item_spend(repo, item_df, start_date, end_date, cfg=cfg)
    vendor_info = _build_vendor_action_plan(by_item, cfg)

    # Vendor × Item Detail
    vendor_item_detail = by_item[[
        "vendor_name", "item_no", "description",
        "all_open_spend", "delivered_spend_past_year",
        "hts", "item_category_code", "assigned_user_id",
        "cost_center", "tariff_exclusion", "alternative_vendor",
        "last_purchase_date", "last_unit_price", "sourcing_action"
    ]].rename(columns={
        "all_open_spend": "open_spend",
        "delivered_spend_past_year": "past_year_spend",
        "item_category_code": "item_category",
        "assigned_user_id": "assigned_user",
        "last_purchase_date": "last_purchased_date"
    }).reset_index(drop=True)

    # Build pivoted Alternative Vendor Options
    base = by_item.query("sourcing_action != 'Develop alt source'")[[
        "vendor_name", "item_no", "description",
        "all_open_spend", "delivered_spend_past_year",
        "last_purchase_date", "last_unit_price"
    ]].rename(columns={
        "all_open_spend": "open_spend",
        "delivered_spend_past_year": "past_year_spend"
    }).reset_index(drop=True)

    alt_all = repo.get_most_recent_purchase_data(
        data=purchase_df,
        fields=["item_no", "vendor_name", "order_date", "unit_cost", "vendor_country"],
        group_by="both"
    ).rename(columns={
        "order_date": "last_purchase_date",
        "unit_cost": "last_unit_price",
        "vendor_name": "alternative_vendor"
    })
    alt_all = alt_all[alt_all["vendor_country"] != cfg["country"]]
    alt_all = alt_all.sort_values(["item_no", "last_unit_price"])

    # Assign alt_rank: "preferred" for the first (lowest cost), then 2,3,...
    alt_all["alt_rank"] = alt_all.groupby("item_no").cumcount().apply(lambda x: "preferred" if x == 0 else x + 1)

    alt_pivot = alt_all.pivot(
        index="item_no",
        columns="alt_rank",
        values=["alternative_vendor", "last_unit_price", "last_purchase_date"]
    )
    alt_pivot.columns = [f"{col[0]}_{col[1]}" for col in alt_pivot.columns]
    alt_pivot = alt_pivot.reset_index()

    alternative_vendor_options = base.merge(alt_pivot, on="item_no", how="left")

    # Get ordered suffixes for column ordering
    all_suffixes = set(alt_all["alt_rank"])
    ordered_suffixes = sorted(all_suffixes, key=lambda x: (x != "preferred", int(x) if str(x).isdigit() else 0))

    # Calculate price percent differences
    for suffix in ordered_suffixes:
        if f"last_unit_price_{suffix}" in alternative_vendor_options.columns:
            alternative_vendor_options[f"price_percent_diff_{suffix}"] = (
                (alternative_vendor_options[f"last_unit_price_{suffix}"] - alternative_vendor_options["last_unit_price"]) / alternative_vendor_options["last_unit_price"]
            ).round(3)

    # Remove columns that are entirely blank
    alternative_vendor_options = alternative_vendor_options.dropna(axis=1, how='all')

    # Reorder columns: base columns followed by interleaved alternative vendor columns
    base_cols = ['vendor_name', 'item_no', 'description', 'open_spend', 'past_year_spend', 'last_purchase_date', 'last_unit_price']
    alt_cols = []
    for suffix in ordered_suffixes:
        for col_type in ['alternative_vendor', 'last_purchase_date', 'last_unit_price', 'price_percent_diff']:
            col_name = f"{col_type}_{suffix}" if col_type!='price_percent_diff' else f"price_percent_diff_{suffix}"
            if col_name in alternative_vendor_options.columns:
                alt_cols.append(col_name)
    all_cols = base_cols + alt_cols
    alternative_vendor_options = alternative_vendor_options[all_cols]

    return {
        "vendor_info": vendor_info,
        "vendor_item_detail": vendor_item_detail,
        "alternative_vendor_options": alternative_vendor_options,
    }

# ────────────────────────────────────────────────────────────────────────────
# 5. Excel exporter
# ────────────────────────────────────────────────────────────────────────────
def _export_to_excel(tables: Dict[str, pd.DataFrame], output_dir: str = "output") -> Path:
    os.makedirs(output_dir, exist_ok=True)
    outfile = Path(output_dir) / "china_vendor_spend.xlsx"
    with pd.ExcelWriter(outfile) as xl:
        tables["vendor_info"].to_excel(xl, sheet_name="Vendor Info", index=False)
        tables["vendor_item_detail"].to_excel(xl, sheet_name="Vendor × Item Detail", index=False)
        tables["alternative_vendor_options"].to_excel(xl, sheet_name="Alternative Vendor Options", index=False)
    return outfile

# ────────────────────────────────────────────────────────────────────────────
# 6. CLI entry‑point
# ────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    purchase_df = get_all_purchase_data()
    item_df = get_all_item_data()

    if purchase_df.empty:
        raise SystemExit("Purchase data frame is empty – aborting analysis.")

    tables = analyse_china_exposure(purchase_df, item_df)
    outfile = _export_to_excel(tables)
    print(f"Exported China exposure workbook → {outfile.absolute()}")