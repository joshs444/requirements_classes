"""Core calculation functions for vendor region spend analysis.

This module provides functions to calculate vendor spend metrics, analyze item-level
data, and identify alternative vendors.
"""

import pandas as pd
from typing import Dict

from purchase.purchase_repository import PurchaseRepository
from analysis.vendor_region_analysis.config import load_yaml_prefixes
from analysis.vendor_region_analysis.utils import filter_country

def get_vendor_spend(repo: PurchaseRepository,
                     start_date: pd.Timestamp,
                     end_date: pd.Timestamp,
                     *,
                     cfg: Dict) -> pd.DataFrame:
    """Calculate vendor spend metrics.
    
    Args:
        repo: PurchaseRepository with purchase data
        start_date: Start date for the analysis period
        end_date: End date for the analysis period
        cfg: Configuration dictionary
        
    Returns:
        DataFrame with vendor spend metrics
    """
    df_all = repo.get_purchase_data()
    target_country = filter_country(df_all, cfg["country"])
    past_year = target_country.query("@start_date <= order_date <= @end_date")

    open_sp = (repo.group_by_and_sum("vendor_name", "open", data=target_country)
               .rename(columns={"open_value": "all_open_spend"}))
    delivered = (repo.group_by_and_sum("vendor_name", "delivered", data=past_year)
                 .rename(columns={"delivered_value": "delivered_spend_past_year"}))

    out = open_sp.merge(delivered, how="outer").fillna(0)
    out["total_value"] = out["all_open_spend"] + out["delivered_spend_past_year"]
    out = (out[out["total_value"] >= cfg["spend_threshold"]]
           .sort_values("total_value", ascending=False)
           .drop(columns="total_value"))
    return out.reset_index(drop=True)


def get_vendor_item_spend(repo: PurchaseRepository,
                          item_df: pd.DataFrame,
                          start_date: pd.Timestamp,
                          end_date: pd.Timestamp,
                          *,
                          cfg: Dict) -> pd.DataFrame:
    """Calculate vendor item spend with detailed analysis.
    
    Args:
        repo: PurchaseRepository with purchase data
        item_df: DataFrame with item metadata
        start_date: Start date for the analysis period
        end_date: End date for the analysis period
        cfg: Configuration dictionary
        
    Returns:
        DataFrame with vendor item spend analysis
    """
    df_all = repo.get_purchase_data()
    country_items = filter_country(repo.filter_by_type("Item"), cfg["country"])
    past_year = country_items.query("@start_date <= order_date <= @end_date")

    open_sp = (repo.group_by_and_sum(["vendor_name", "item_no"], "open", data=country_items)
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

    prefixes = load_yaml_prefixes(cfg["tariff_prefix_file"])
    df["tariff_exclusion"] = (
        df["hts"].astype(str).str.startswith(prefixes).fillna(False)
          .map({True: "Yes", False: "No"})
    )

    recent_cols = ["item_no", "vendor_name", "order_date", "unit_cost",
                   "assigned_user_id", "cost_center"]
    recent = repo.get_most_recent_purchase_data(
        data=country_items, fields=recent_cols, group_by="both"
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
        return "Monitor"

    df["sourcing_action"] = df.apply(_sourcing_action, axis=1)

    return df.reset_index(drop=True) 