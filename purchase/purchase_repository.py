import pandas as pd
from utils.time_utils import TimeUtils

class PurchaseRepository:
    def __init__(self, purchase_data=None):
        """Initialize with purchase data and split into open/closed based on 'status' column.

        Args:
            purchase_data (pd.DataFrame, optional): The purchase data from purchase_all.sql. Defaults to None.
        
        Raises:
            ValueError: If purchase_data is None or empty.
            KeyError: If 'status' column is missing.
        """
        if purchase_data is None or purchase_data.empty:
            raise ValueError("Purchase data cannot be None or empty")
        
        self.purchase_data = purchase_data.copy()
        self.purchase_data['order_date'] = pd.to_datetime(self.purchase_data['order_date'])
        
        if 'status' not in self.purchase_data.columns:
            raise KeyError("'status' column is required")
            
        self.open_data = self.purchase_data[self.purchase_data['status'] == 'OPEN'].copy()

    def get_purchase_data(self):
        """Return all purchase data.

        Returns:
            pd.DataFrame: Complete purchase data.
        """
        return self.purchase_data
            
    def calculate_delivered_value(self, data=None):
        """Calculate delivered value (unit_cost * quantity_delivered) for all lines.
        
        Args:
            data (pd.DataFrame, optional): Data to calculate delivered value for. 
                                          If None, uses all available data. Defaults to None.
        
        Returns:
            float: Total delivered value
        """
        if data is None:
            data = self.get_purchase_data()
            
        # Ensure the required columns exist
        if 'unit_cost' not in data.columns or 'quantity_delivered' not in data.columns:
            raise KeyError("Required columns 'unit_cost' and/or 'quantity_delivered' missing")
            
        # Calculate delivered value for each line
        data = data.copy()
        data['delivered_value'] = data['unit_cost'] * data['quantity_delivered']
        
        # Return the sum
        return data['delivered_value'].sum()
        
    def calculate_open_value(self, data=None):
        """Calculate open value (unit_cost * outstanding_quantity) for all open lines.
        
        Args:
            data (pd.DataFrame, optional): Data to calculate open value for. 
                                          If None, uses all available data. Defaults to None.
        
        Returns:
            float: Total open value
        """
        if data is None:
            data = self.get_purchase_data()
        
        # Filter to only include OPEN status
        data = data[data['status'] == 'OPEN'].copy()
        
        # If no open data exists, return 0
        if data.empty:
            return 0.0
            
        # Ensure the required columns exist
        if 'unit_cost' not in data.columns or 'outstanding_quantity' not in data.columns:
            raise KeyError("Required columns 'unit_cost' and/or 'outstanding_quantity' missing")
            
        # Calculate open value for each line
        data['open_value'] = data['unit_cost'] * data['outstanding_quantity']
        
        # Return the sum
        return data['open_value'].sum()
    
    def calculate_total_value(self, data=None):
        """Calculate total value (delivered + open) for all lines.
        
        Args:
            data (pd.DataFrame, optional): Data to calculate total value for. 
                                          If None, uses all available data. Defaults to None.
        
        Returns:
            float: Total value
        """
        if data is None:
            data = self.get_purchase_data()
            
        delivered = self.calculate_delivered_value(data)
        open_value = self.calculate_open_value(data)
        
        return delivered + open_value
    
    def group_by_and_sum(self, group_by, value_type='total', data=None):
        """Group data by specified column(s) and calculate values.
        
        Args:
            group_by (str or list): Column(s) to group by (e.g., 'vendor_country', 'vendor_name', 'item_no')
            value_type (str, optional): Type of value to calculate ('delivered', 'open', or 'total'). 
                                        Defaults to 'total'.
            data (pd.DataFrame, optional): Data to group and calculate. 
                                          If None, uses all available data. Defaults to None.
        
        Returns:
            pd.DataFrame: Grouped data with calculated values
        """
        if data is None:
            data = self.get_purchase_data()
            
        # Create a copy to avoid modifying the original data
        df = data.copy()
        
        # Calculate values
        df['delivered_value'] = df['unit_cost'] * df['quantity_delivered']
        
        # For open value, we need to filter for open status first
        open_df = df[df['status'] == 'OPEN'].copy()
        if not open_df.empty:
            open_df['open_value'] = open_df['unit_cost'] * open_df['outstanding_quantity']
            # Merge the open values back to the main dataframe
            df = df.merge(open_df[['document_no', 'line_no', 'open_value']], 
                          on=['document_no', 'line_no'], how='left')
        else:
            df['open_value'] = 0
            
        # Fill NaN values with 0
        df['open_value'] = df['open_value'].fillna(0)
        df['total_value'] = df['delivered_value'] + df['open_value']
        
        # Select the value column based on the value_type
        if value_type == 'delivered':
            value_col = 'delivered_value'
        elif value_type == 'open':
            value_col = 'open_value'
        elif value_type == 'total':
            value_col = 'total_value'
        else:
            raise ValueError("value_type must be 'delivered', 'open', or 'total'")
        
        # Group by the specified column(s) and sum the values
        return df.groupby(group_by)[value_col].sum().reset_index() 

    def get_items_last_purchased_from_country(self, country, time_period=None, start_date=None, end_date=None):
        """Get items last purchased from a specific country within a time period.
        
        Args:
            country (str): Country to filter by
            time_period (str/int, optional): Predefined time period like "ytd", "year", "quarter", or custom days
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: Items with their last purchase date from the country
        """
        df = self.purchase_data.copy()
        # 'order_date' is already datetime from __init__
        
        if time_period:
            start_date, end_date = TimeUtils.get_period_dates(time_period)
        
        if start_date:
            df = df[df['order_date'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['order_date'] <= pd.to_datetime(end_date)]
            
        return (df[df['vendor_country'] == country]
                .sort_values('order_date', ascending=False)
                .drop_duplicates('item_no')
                [['item_no', 'order_date', 'vendor_name']])

    def identify_items_from_multiple_countries(self, country, time_period=None, start_date=None, end_date=None):
        """Identify items purchased from more than one country in a specified period.
        
        Args:
            country (str): Main country to filter items by
            time_period (str/int, optional): Predefined time period like "ytd", "year", "quarter", or custom days
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: Items with 'multi_country' column showing 'Yes' if purchased from multiple countries, 'No' if not
        """
        df = self.purchase_data.copy()
        # 'order_date' is already datetime from __init__
        
        if time_period:
            start_date, end_date = TimeUtils.get_period_dates(time_period)
        
        if start_date:
            df = df[df['order_date'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['order_date'] <= pd.to_datetime(end_date)]
        
        # Count unique countries per item
        country_counts = df.groupby('item_no')['vendor_country'].nunique().reset_index()
        country_counts.columns = ['item_no', 'country_count']
        
        # Filter to items from the specified country
        items_from_country = df[df['vendor_country'] == country]['item_no'].unique()
        result = country_counts[country_counts['item_no'].isin(items_from_country)].copy()
        
        # Mark items purchased from multiple countries
        result['multi_country'] = result['country_count'].apply(lambda x: 'Yes' if x > 1 else 'No')
        
        return result[['item_no', 'multi_country']] 

    def filter_by_type(self, item_type, data=None):
        """Filter purchase data by item type.
        
        Args:
            item_type (str): Type to filter by ('GL', 'Item', or 'FA')
            data (pd.DataFrame, optional): Data to filter. If None, uses all available data.
            
        Returns:
            pd.DataFrame: Filtered purchase data
        """
        if data is None:
            data = self.get_purchase_data()
            
        return data[data['type'] == item_type].copy() 

    def get_vendors_for_item_excluding_countries(self, item_no, start_date=None, end_date=None, exclude_countries=None, data=None):
        """Identify vendors that supplied a specific item within a date range, excluding specified countries.

        Args:
            item_no (str): Item number to filter by.
            start_date (str, optional): Start date in YYYY-MM-DD format.
            end_date (str, optional): End date in YYYY-MM-DD format.
            exclude_countries (str or list, optional): Country or list of countries to exclude.
            data (pd.DataFrame, optional): Data to filter. If None, uses all available purchase data.

        Returns:
            pd.DataFrame: Unique vendors with columns 'vendor_name' and 'vendor_country', sorted by vendor_name.
        """
        if data is None:
            data = self.get_purchase_data().copy()
        else:
            data = data.copy()

        df = data[data['item_no'] == item_no].copy()
        # 'order_date' is already datetime from __init__

        if start_date:
            df = df[df['order_date'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['order_date'] <= pd.to_datetime(end_date)]

        if exclude_countries:
            if isinstance(exclude_countries, str):
                exclude_countries = [exclude_countries]
            df = df[~df['vendor_country'].isin(exclude_countries)]

        vendors = df[['vendor_name', 'vendor_country']].drop_duplicates().sort_values('vendor_name')
        return vendors

    def get_most_recent_unit_cost(self, item_no=None, vendor_name=None, start_date=None, end_date=None, data=None):
        """Get the most recent unit_cost for specified item and/or vendor within an optional date range.

        Args:
            item_no (str, optional): Item number to filter by.
            vendor_name (str, optional): Vendor name to filter by.
            start_date (str, optional): Start date in YYYY-MM-DD format.
            end_date (str, optional): End date in YYYY-MM-DD format.
            data (pd.DataFrame, optional): Data to use. If None, uses all available purchase data.

        Returns:
            pd.DataFrame: DataFrame with columns 'item_no', 'vendor_name', 'order_date', 'unit_cost'
                          for the most recent records based on the filters.
        """
        if data is None:
            data = self.get_purchase_data().copy()
        else:
            data = data.copy()

        # 'order_date' is already datetime from __init__

        if item_no is not None:
            data = data[data['item_no'] == item_no]
        if vendor_name is not None:
            data = data[data['vendor_name'] == vendor_name]
        if start_date is not None:
            data = data[data['order_date'] >= pd.to_datetime(start_date)]
        if end_date is not None:
            data = data[data['order_date'] <= pd.to_datetime(end_date)]

        data = data.sort_values('order_date', ascending=False)
        data = data.drop_duplicates(subset=['item_no', 'vendor_name'])
        result = data[['item_no', 'vendor_name', 'order_date', 'unit_cost']].sort_values(['item_no', 'vendor_name'])
        return result