import pandas as pd
import os
from purchase.purchase_repository import PurchaseRepository
from purchase.purchase_all_data import get_all_purchase_data
from utils.time_utils import TimeUtils
from item.item_data import get_all_item_data

def get_chinese_vendor_spend(purchase_data):
    """
    Calculate total open (all time) and delivered (past year) purchase spend by Chinese vendors.
    
    Args:
        purchase_data (pd.DataFrame): The purchase data to analyze.
    
    Returns:
        pd.DataFrame: DataFrame with vendor_name, all_open_spend, delivered_spend_past_year
    """
    try:
        start_date, end_date = TimeUtils.get_period_dates("year")
        
        if purchase_data is None or purchase_data.empty:
            print("Purchase data is None or empty")
            return pd.DataFrame()
            
        repo = PurchaseRepository(purchase_data)
        
        all_china_data = repo.get_purchase_data()
        all_china_data = all_china_data[all_china_data['vendor_country'] == 'CN']
        
        past_year_china_data = all_china_data[(all_china_data['order_date'] >= start_date) & 
                                              (all_china_data['order_date'] <= end_date)]
        
        if all_china_data.empty:
            print("No vendors from China found in the data")
            return pd.DataFrame()
            
        open_spend = repo.group_by_and_sum('vendor_name', value_type='open', data=all_china_data)
        open_spend = open_spend.rename(columns={'open_value': 'all_open_spend'})
        
        delivered_spend = repo.group_by_and_sum('vendor_name', value_type='delivered', data=past_year_china_data)
        delivered_spend = delivered_spend.rename(columns={'delivered_value': 'delivered_spend_past_year'})
        
        merged_spend = pd.merge(open_spend, delivered_spend, on='vendor_name', how='outer')
        merged_spend = merged_spend.fillna(0)
        
        merged_spend['total_value'] = merged_spend['all_open_spend'] + merged_spend['delivered_spend_past_year']
        merged_spend = merged_spend[merged_spend['total_value'] >= 1000]
        merged_spend = merged_spend.sort_values('total_value', ascending=False)
        
        return merged_spend.drop(columns=['total_value'])
        
    except Exception as e:
        print(f"Error calculating Chinese vendor spend: {e}")
        return pd.DataFrame()

def get_chinese_vendor_item_spend(purchase_data, item_data):
    """Get spend data for Chinese vendors grouped by vendor and item
    
    Args:
        purchase_data (pd.DataFrame): The purchase data to analyze.
        item_data (pd.DataFrame): The item data to merge with purchase data.
    
    Returns:
        pd.DataFrame: DataFrame with detailed spend data by vendor and item
    """
    try:
        start_date, end_date = TimeUtils.get_period_dates("year")
        
        if purchase_data is None or purchase_data.empty:
            print("Purchase data is None or empty")
            return pd.DataFrame()
            
        repo = PurchaseRepository(purchase_data)
        
        item_only_data = repo.filter_by_type('Item')
        all_china_data = item_only_data[item_only_data['vendor_country'] == 'CN']
        
        past_year_china_data = all_china_data[(all_china_data['order_date'] >= start_date) & 
                                              (all_china_data['order_date'] <= end_date)]
        
        if all_china_data.empty:
            print("No item data from China found")
            return pd.DataFrame()
            
        open_spend = repo.group_by_and_sum(['vendor_name', 'item_no'], value_type='open', data=all_china_data)
        open_spend = open_spend.rename(columns={'open_value': 'all_open_spend'})
        
        delivered_spend = repo.group_by_and_sum(['vendor_name', 'item_no'], value_type='delivered', data=past_year_china_data)
        delivered_spend = delivered_spend.rename(columns={'delivered_value': 'delivered_spend_past_year'})
        
        merged_spend = pd.merge(open_spend, delivered_spend, on=['vendor_name', 'item_no'], how='outer')
        merged_spend = merged_spend.fillna(0)
        
        merged_spend['total_value'] = merged_spend['all_open_spend'] + merged_spend['delivered_spend_past_year']
        merged_spend = merged_spend[merged_spend['total_value'] >= 1000]
        merged_spend = merged_spend.sort_values('total_value', ascending=False)
        
        multi_country_items = repo.identify_items_from_multiple_countries('CN')
        merged_spend = pd.merge(merged_spend, multi_country_items, on='item_no', how='left')
        merged_spend['alternative_vendor'] = merged_spend['multi_country'].fillna('No')
        
        if item_data is not None:
            item_info = item_data[['item_no', 'description', 'hts', 'item_category_code']]
            merged_spend = pd.merge(merged_spend, item_info, on='item_no', how='left')
        
        prefixes = (
            "8517.13.00", "8471", "8517.62.00", "8473.3", "8528.52.00", "8542", "8486",
            "8524", "8523.51.00", "8541.10.00", "8541.21.00", "8541.29.00", "8541.30.00",
            "8541.49.10", "8541.49.70", "8541.49.80", "8541.49.95", "8541.51.00",
            "8541.59.00", "8541.90.00"
        )
        merged_spend['tariff_exclusion'] = merged_spend['hts'].str.startswith(prefixes).fillna(False).map({True: 'Yes', False: 'No'})
        
        recent_costs = repo.get_most_recent_purchase_data(data=all_china_data, fields=['item_no', 'vendor_name', 'order_date', 'unit_cost'], group_by='both')
        merged_spend = pd.merge(
            merged_spend,
            recent_costs[['item_no', 'vendor_name', 'order_date', 'unit_cost']],
            on=['item_no', 'vendor_name'],
            how='left'
        )
        merged_spend = merged_spend.rename(columns={'order_date': 'last_purchase_date', 'unit_cost': 'last_unit_price'})
        
        # Get most recent assigned user for each item
        recent_assigned_users = repo.get_most_recent_purchase_data(data=all_china_data, 
                                                                  fields=['item_no', 'assigned_user_id'], 
                                                                  group_by='item')
        if 'assigned_user_id' in recent_assigned_users.columns:
            merged_spend = pd.merge(
                merged_spend,
                recent_assigned_users[['item_no', 'assigned_user_id']],
                on='item_no',
                how='left'
            )
            merged_spend['assigned_user_id'] = merged_spend['assigned_user_id'].fillna('Unassigned')
        else:
            merged_spend['assigned_user_id'] = 'Unassigned'
        
        # Get most recent cost_center for each item
        recent_cost_centers = repo.get_most_recent_purchase_data(data=all_china_data,
                                                               fields=['item_no', 'cost_center'],
                                                               group_by='item')
        if 'cost_center' in recent_cost_centers.columns:
            merged_spend = pd.merge(
                merged_spend,
                recent_cost_centers[['item_no', 'cost_center']],
                on='item_no',
                how='left'
            )
            merged_spend['cost_center'] = merged_spend['cost_center'].fillna('Unassigned')
        else:
            merged_spend['cost_center'] = 'Unassigned'
            
        return merged_spend.drop(columns=['total_value', 'multi_country'])
        
    except Exception as e:
        print(f"Error calculating Chinese vendor-item spend: {e}")
        return pd.DataFrame()

def get_alternative_vendor_options(item_result, repo):
    """Generate a DataFrame with alternative vendor options for items with vendors from other countries.
    
    Args:
        item_result (pd.DataFrame): DataFrame containing item-level spend data from Chinese vendors.
        repo (PurchaseRepository): Repository instance to access purchase data.
    
    Returns:
        pd.DataFrame: DataFrame with alternative vendor details.
    """
    alt_items_df = item_result[item_result['alternative_vendor'] == 'Yes'].copy()
    alt_vendor_list = []
    for _, row in alt_items_df.iterrows():
        item_no = row['item_no']
        original_vendor = row['vendor_name']
        original_price = row['last_unit_price']
        assigned_user_id = row['assigned_user_id'] if 'assigned_user_id' in row else 'Unassigned'
        cost_center = row['cost_center'] if 'cost_center' in row else 'Unassigned'
        alt_vendors = repo.get_vendors_for_item_excluding_countries(item_no, exclude_countries='CN')
        for _, alt_row in alt_vendors.iterrows():
            alt_vendor = alt_row['vendor_name']
            alt_country = alt_row['vendor_country'] if 'vendor_country' in alt_row else 'Unknown'
            recent_cost = repo.get_most_recent_purchase_data(item_no=item_no, vendor_name=alt_vendor, fields=['item_no', 'vendor_name', 'order_date', 'unit_cost'], group_by='both')
            if not recent_cost.empty:
                alt_price = recent_cost['unit_cost'].iloc[0]
                alt_date = recent_cost['order_date'].iloc[0]
                percent_diff = ((alt_price - original_price) / original_price) if original_price != 0 else None
                alt_vendor_list.append({
                    'item': item_no,
                    'description': row['description'],
                    'current_vendor': original_vendor,
                    'current_vendor_country': 'CN',
                    'open_spend': row['all_open_spend'],
                    'delivered_spend_past_year': row['delivered_spend_past_year'],
                    'last_order_date': row['last_purchase_date'],
                    'most_recent_unit_price': original_price,
                    'alternative_vendor': alt_vendor,
                    'alternative_vendor_country': alt_country,
                    'alternative_last_order_date': alt_date,
                    'alternative_unit_price': alt_price,
                    'percent_difference': percent_diff,
                    'assigned_user_id': assigned_user_id,
                    'cost_center': cost_center
                })
    return pd.DataFrame(alt_vendor_list)

if __name__ == "__main__":
    purchase_data = get_all_purchase_data()
    item_data = get_all_item_data()
    vendor_result = get_chinese_vendor_spend(purchase_data)
    item_result = get_chinese_vendor_item_spend(purchase_data, item_data)
    
    if not vendor_result.empty and not item_result.empty:
        # Calculate tariff exclusion percentage
        exclusion_yes = item_result[item_result['tariff_exclusion'] == 'Yes']
        if not exclusion_yes.empty:
            exclusion_by_vendor = exclusion_yes.groupby('vendor_name')['all_open_spend'].sum().reset_index()
            exclusion_by_vendor = exclusion_by_vendor.rename(columns={'all_open_spend': 'all_open_spend_excluded'})
            vendor_result = pd.merge(vendor_result, exclusion_by_vendor, on='vendor_name', how='left')
            vendor_result['all_open_spend_excluded'] = vendor_result['all_open_spend_excluded'].fillna(0)
            vendor_result['open_spend_tariff_exclusion_pct'] = (vendor_result['all_open_spend_excluded'] / vendor_result['all_open_spend']).round(3)
        else:
            vendor_result['open_spend_tariff_exclusion_pct'] = 0.0
        
        vendor_export = vendor_result[['vendor_name', 
                                       'all_open_spend', 
                                       'delivered_spend_past_year',
                                       'open_spend_tariff_exclusion_pct']]
        
        # Generate alternative vendor options
        repo = PurchaseRepository(purchase_data)
        alt_vendor_df = get_alternative_vendor_options(item_result, repo)
        
        # Count unique items per vendor
        items_per_vendor = item_result.groupby('vendor_name')['item_no'].nunique().reset_index()
        items_per_vendor = items_per_vendor.rename(columns={'item_no': 'unique_item_count'})
        
        # Count single-source items (items only purchased from China, not from any other country)
        single_source_items = item_result[item_result['alternative_vendor'] == 'No'].groupby('vendor_name')['item_no'].nunique().reset_index()
        single_source_items = single_source_items.rename(columns={'item_no': 'single_source_item_count'})
        
        # Add counts to vendor_export
        vendor_export = pd.merge(vendor_export, items_per_vendor, on='vendor_name', how='left')
        vendor_export = pd.merge(vendor_export, single_source_items, on='vendor_name', how='left')
        vendor_export['unique_item_count'] = vendor_export['unique_item_count'].fillna(0).astype(int)
        vendor_export['single_source_item_count'] = vendor_export['single_source_item_count'].fillna(0).astype(int)
        
        # Update vendor_export to include new columns
        vendor_export = vendor_export[['vendor_name', 
                                       'all_open_spend', 
                                       'delivered_spend_past_year',
                                       'open_spend_tariff_exclusion_pct',
                                       'unique_item_count',
                                       'single_source_item_count']]
        
        # Print results
        print("\nPurchase Spend by Chinese Vendors (Open: All Time, Delivered: Past Year):")
        print("="*90)
        print(vendor_export.to_string(index=False))
        
        # Export to Excel
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)
        excel_path = os.path.join(output_dir, 'chinese_vendor_spend.xlsx')
        with pd.ExcelWriter(excel_path) as writer:
            vendor_export.to_excel(writer, sheet_name='By Vendor', index=False)
            item_result.to_excel(writer, sheet_name='By Vendor and Item', index=False)
            alt_vendor_df.to_excel(writer, sheet_name='Alternative Vendor Options', index=False)
        
        print(f"\nExported results to {excel_path}")
    else:
        print("No data available for Chinese vendor spend analysis")