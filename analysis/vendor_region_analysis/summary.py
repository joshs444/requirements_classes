"""Vendor action plan summary functions.

This module provides functions to build and manage vendor action plan summaries.
"""

import pandas as pd
from typing import Dict

def build_vendor_action_plan(by_item: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    """Build a vendor action plan summary from item-level data.
    
    Args:
        by_item: DataFrame with item-level spend data
        cfg: Configuration dictionary
        
    Returns:
        DataFrame with vendor-level action plan summary
    """
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