"""Core calculation functions for vendor region spend analysis.

This module provides functions to calculate vendor spend metrics, analyze item-level
data, and identify alternative vendors.
"""

import pandas as pd
from typing import Dict

from purchase.repository import PurchaseRepository
from purchase.analytics import PurchaseAnalytics
from purchase.queries import PurchaseQueries
from analysis.vendor_region_analysis.config import load_yaml_prefixes
from analysis.vendor_region_analysis.utils import filter_country

def get_vendor_spend(repo: PurchaseRepository,
                     analytics: PurchaseAnalytics,
                     queries: PurchaseQueries,
                     start_date: pd.Timestamp,
                     end_date: pd.Timestamp,
                     *,
                     cfg: Dict) -> pd.DataFrame:
    """Calculate vendor spend metrics.
    
    Args:
        repo: PurchaseRepository with purchase data
        analytics: PurchaseAnalytics instance
        queries: PurchaseQueries instance
        start_date: Start date for the analysis period
        end_date: End date for the analysis period
        cfg: Configuration dictionary
        
    Returns:
        DataFrame with vendor spend metrics
    """
    df_all = repo.all()
    target_country = filter_country(df_all, cfg["country"])
    past_year = target_country.query("@start_date <= order_date <= @end_date")

    # Using the new analytics module to get values
    open_spend = analytics.group_value(by="vendor_name", kind="open", frame=target_country)
    open_spend = open_spend.rename(columns={"open": "all_open_spend"})
    
    delivered_spend = analytics.group_value(by="vendor_name", kind="delivered", frame=past_year)
    delivered_spend = delivered_spend.rename(columns={"delivered": "delivered_spend_past_year"})

    out = open_spend.merge(delivered_spend, how="outer").fillna(0)
    out["total_value"] = out["all_open_spend"] + out["delivered_spend_past_year"]
    out = (out[out["total_value"] >= cfg["spend_threshold"]]
           .sort_values("total_value", ascending=False)
           .drop(columns="total_value"))
    return out.reset_index(drop=True)


def get_vendor_item_spend(repo: PurchaseRepository,
                          analytics: PurchaseAnalytics,
                          queries: PurchaseQueries,
                          item_df: pd.DataFrame,
                          start_date: pd.Timestamp,
                          end_date: pd.Timestamp,
                          *,
                          cfg: Dict) -> pd.DataFrame:
    """Calculate vendor item spend with detailed analysis.
    
    Args:
        repo: PurchaseRepository with purchase data
        analytics: PurchaseAnalytics instance
        queries: PurchaseQueries instance
        item_df: DataFrame with item metadata
        start_date: Start date for the analysis period
        end_date: End date for the analysis period
        cfg: Configuration dictionary
        
    Returns:
        DataFrame with vendor item spend analysis
    """
    df_all = repo.all()
    # Use queries for filtering by type
    items_only = queries.filter_by_type("Item")
    country_items = filter_country(items_only, cfg["country"])
    past_year = country_items.query("@start_date <= order_date <= @end_date")

    # Using new analytics module for grouped values
    open_spend = analytics.group_value(by=["vendor_name", "item_no"], kind="open", frame=country_items)
    open_spend = open_spend.rename(columns={"open": "all_open_spend"})
    
    delivered_spend = analytics.group_value(by=["vendor_name", "item_no"], kind="delivered", frame=past_year)
    delivered_spend = delivered_spend.rename(columns={"delivered": "delivered_spend_past_year"})
    
    df = open_spend.merge(delivered_spend, how="outer").fillna(0)
    df["total_value"] = df["all_open_spend"] + df["delivered_spend_past_year"]
    df = df[df["total_value"] >= cfg["spend_threshold"]].drop(columns="total_value")

    # Use queries for multi-country identification
    multi = queries.identify_items_from_multiple_countries(
        country=cfg["country"]
    )
    df = (df.merge(multi, on="item_no", how="left")
          .assign(alternative_vendor=lambda d: d["multi_country"].fillna("No"))
          .drop(columns=["multi_country"]))

    meta_cols = ["item_no", "description", "hts", "item_category_code"]
    df = df.merge(item_df[meta_cols], on="item_no", how="left")

    prefixes = load_yaml_prefixes(cfg["tariff_prefix_file"])
    df["tariff_exclusion"] = (
        df["hts"].astype(str).str.startswith(prefixes).fillna(False)
          .map({True: "Yes", False: "No"})
    )

    # Use queries for most recent purchase data
    recent_cols = ["item_no", "vendor_name", "order_date", "unit_cost",
                   "assigned_user_id", "cost_center"]
    recent = queries.get_most_recent_purchase_data(
        item_no=None, vendor_name=None, fields=recent_cols,
        group_by="both"
    )
    df = (df.merge(recent[recent_cols], on=["item_no", "vendor_name"], how="left")
          .rename(columns={
              "order_date": "last_purchase_date",
              "unit_cost": "last_unit_price"
          })
          .fillna({"assigned_user_id": "Unassigned", "cost_center": "Unassigned"}))

    # Get most recent purchase data for all vendors
    all_recent = queries.get_most_recent_purchase_data(
        item_no=None, vendor_name=None, fields=["item_no", "vendor_name", "unit_cost"],
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
        return "Monitor"

    df["sourcing_action"] = df.apply(_sourcing_action, axis=1)

    return df.reset_index(drop=True) 