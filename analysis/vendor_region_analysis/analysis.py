"""Main analysis function for vendor region analysis.

This module provides the main analysis function that orchestrates the vendor
region analysis process.
"""

from typing import Dict

import pandas as pd

from purchase.purchase_repository import PurchaseRepository
from utils.time_utils import TimeUtils
from analysis.vendor_region_analysis.config import DEFAULT_CFG
from analysis.vendor_region_analysis.calculators import get_vendor_spend, get_vendor_item_spend
from analysis.vendor_region_analysis.summary import build_vendor_action_plan

def analyse_vendor_exposure(purchase_df: pd.DataFrame,
                            item_df: pd.DataFrame,
                            cfg: Dict | None = None) -> Dict[str, pd.DataFrame]:
    """Analyze vendor exposure for a specific region/country.
    
    Args:
        purchase_df: DataFrame with purchase data
        item_df: DataFrame with item data
        cfg: Optional configuration dictionary to override defaults
        
    Returns:
        Dictionary of DataFrames with analysis results
    """
    cfg = {**DEFAULT_CFG, **(cfg or {})}
    start_date, end_date = TimeUtils.get_period_dates("year")
    repo = PurchaseRepository(purchase_df)

    # Vendor and item spend
    by_vendor = get_vendor_spend(repo, start_date, end_date, cfg=cfg)
    by_item = get_vendor_item_spend(repo, item_df, start_date, end_date, cfg=cfg)
    vendor_info = build_vendor_action_plan(by_item, cfg)

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

# Create an alias for backward compatibility
analyse_china_exposure = analyse_vendor_exposure 