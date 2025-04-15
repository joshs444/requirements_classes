import pandas as pd

class TimeUtils:
    # Dictionary of predefined time periods
    PREDEFINED_PERIODS = {
        "ytd": lambda: (pd.to_datetime(f"{pd.to_datetime('today').year}-01-01"), pd.to_datetime("today")),
        "year": lambda: (pd.to_datetime("today") - pd.Timedelta(days=365), pd.to_datetime("today")),
        "quarter": lambda: (pd.to_datetime("today") - pd.Timedelta(days=90), pd.to_datetime("today")),
        "month": lambda: (pd.to_datetime("today") - pd.Timedelta(days=30), pd.to_datetime("today")),
        "week": lambda: (pd.to_datetime("today") - pd.Timedelta(days=7), pd.to_datetime("today"))
    }

    @staticmethod
    def get_period_dates(time_period):
        """Get start and end dates for a given time period.
        
        Args:
            time_period (str or int): Predefined period like 'ytd', 'year', 'quarter', 'month', 'week',
                                      or an integer for custom days.
        
        Returns:
            tuple: (start_date, end_date) as pandas Timestamps
        
        Raises:
            ValueError: If time_period is invalid
        """
        if isinstance(time_period, str):
            if time_period in TimeUtils.PREDEFINED_PERIODS:
                return TimeUtils.PREDEFINED_PERIODS[time_period]()
            else:
                raise ValueError(f"Invalid predefined time_period: {time_period}. "
                                 f"Supported: {list(TimeUtils.PREDEFINED_PERIODS.keys())}")
        elif isinstance(time_period, int):
            end = pd.to_datetime("today")
            start = end - pd.Timedelta(days=time_period)
            return start, end
        else:
            raise ValueError("time_period must be a string (predefined period) or an integer (custom days)")

    @staticmethod
    def get_date_range(start_date, end_date=None, freq='D'):
        """Generate a sequence of dates between start_date and end_date.
        
        Args:
            start_date (str or datetime): Start date
            end_date (str or datetime, optional): End date. If None, uses today. Defaults to None.
            freq (str, optional): Frequency of the date sequence (e.g., 'D' for daily). Defaults to 'D'.
        
        Returns:
            pd.DatetimeIndex: Sequence of dates
        """
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date) if end_date else pd.to_datetime("today")
        return pd.date_range(start=start, end=end, freq=freq)

    @staticmethod
    def is_date_in_range(date, start_date, end_date=None):
        """Check if a date falls within a given range.
        
        Args:
            date (str or datetime): Date to check
            start_date (str or datetime): Start of range
            end_date (str or datetime, optional): End of range. If None, uses today. Defaults to None.
        
        Returns:
            bool: True if date is in range, False otherwise
        """
        date = pd.to_datetime(date)
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date) if end_date else pd.to_datetime("today")
        return start <= date <= end

    @staticmethod
    def aggregate_time_series(df, date_col, value_col, freq='M', agg_func='sum'):
        """Aggregate a time series DataFrame to a specified frequency.
        
        Args:
            df (pd.DataFrame): DataFrame with a datetime column
            date_col (str): Name of the datetime column
            value_col (str): Name of the column to aggregate
            freq (str, optional): Frequency to aggregate to (e.g., 'M' for monthly). Defaults to 'M'.
            agg_func (str, optional): Aggregation function (e.g., 'sum', 'mean'). Defaults to 'sum'.
        
        Returns:
            pd.DataFrame: Aggregated DataFrame
        """
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        df.set_index(date_col, inplace=True)
        aggregated = df[value_col].resample(freq).agg(agg_func)
        return aggregated.reset_index()

    @staticmethod
    def get_quarter(date):
        """Get the quarter of a given date.
        
        Args:
            date (str or datetime): Date to get the quarter for
        
        Returns:
            int: Quarter number (1-4)
        """
        date = pd.to_datetime(date)
        return (date.month - 1) // 3 + 1

    @staticmethod
    def get_fiscal_year(date, fiscal_start_month=1):
        """Get the fiscal year of a given date.
        
        Args:
            date (str or datetime): Date to get the fiscal year for
            fiscal_start_month (int, optional): Month when the fiscal year starts. Defaults to 1 (January).
        
        Returns:
            int: Fiscal year
        """
        date = pd.to_datetime(date)
        year = date.year
        if date.month < fiscal_start_month:
            year -= 1
        return year