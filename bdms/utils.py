from typing import List, Tuple, Dict

import zipfile
import polars as pl
import pandas as pd
import urllib.request
import warnings
from itertools import product
from datetime import datetime, timedelta

from bdms.enums import *


def get_base_path(
        trading_type: str, 
        market_data_type: str, 
        time_period: str, 
        symbol: str, 
        interval: str = None
    ) -> str:
    """
    Get the base path for storing market data.
    
    Parameters:
    ----------
    trading_type: str
        Trading type (spot, um, cm).
    market_data_type: str
        Market data type (trades, aggTrades, klines).
    time_period: str
        Time period (daily, monthly).
    symbol: str
        Trading symbol (e.g. "BTCUSDT").
    interval: str
        Interval for the klines data. See enums.INTERVALS and 
        enums.DAILY_INTERVALS for valid intervals.
    
    Returns:
    -------
    path: str
        Base path for storing the market data.
    """   
    assert time_period in TIME_PERIODS
    assert trading_type in TRADING_TYPES
    assert market_data_type in TYPES_MAP[trading_type]["monthly"] + \
        TYPES_MAP[trading_type]["daily"]
    
    if "klines" in market_data_type.lower():
        assert interval is not None, "Interval must be specified for klines."
        assert interval in INTERVALS_MAP[time_period], \
            "Invalid interval. Choose from enum.INTERVALS_MAP."
    else:
        assert interval is None, "Interval must be None for non-klines data."
        
    trading_type_path = 'data/spot'
    if trading_type in ["um", "cm"]:
        trading_type_path = f'data/futures/{trading_type}'
    
    path = (
        f'{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/'
        + (f'{interval}/' if interval is not None else '') 
    )
    return path


def get_file_basename(
        symbol: str,
        market_data_type: str,
        date: str,
        time_period: str,
        interval: str = None
    ) -> str:
    """
    Get the file name for storing market data.
    
    Parameters:
    ----------
    symbol: str
        Trading symbol (e.g. "BTCUSDT").
    market_data_type: str
        Market data type (trades, aggTrades, klines).
    date: str
        Date in the format "YYYY-MM-DD".
    time_period: str
        Time period (daily, monthly).
    interval: str
        Interval for the klines data. See enums.INTERVALS and 
        enums.DAILY_INTERVALS for valid intervals.
        
    Returns:
    -------
    file_name: str
        File name for storing the market data.
    """
    assert (interval is None) != ("klines" in market_data_type.lower()), \
        "Interval must be specified for klines data."
    
    date = date[:-3] if time_period == "monthly" else date
    if interval is not None:
        file_name = f"{symbol}-{interval}-{date}"
    else:
        file_name = f"{symbol}-{market_data_type}-{date}"
        
    return file_name
        

def download_file(url: str, path: str) -> bool:
    """
    Download a file from a URL and save it to a local path.
    
    Parameters:
    ----------
    url: str
        URL of the file to download.
    path: str
        Local path to save the file.
    
    Returns:
    -------
    success: bool
        True if the file was downloaded successfully, False otherwise.
    """
    try:
        dl_file = urllib.request.urlopen(url)
        blocksize = 1024 * 1024
            
        with open(path, 'wb') as out_file:
            while True:
                buffer = dl_file.read(blocksize)   
                if not buffer:
                    break
                out_file.write(buffer)
        out_file.close()
        
    except urllib.error.HTTPError:
        warnings.warn("File not found: {}".format(url))
        return False
    
    return True


def load_csv_from_zip(path: str) -> pl.DataFrame:
    """
    Loads single CSV file from a ZIP file into a Polars DataFrame.
    
    Parameters:
    ----------
    path: str
        Path to the ZIP file.
    
    Returns:
    -------
    df: pl.DataFrame
        DataFrame containing the data from the CSV file.
    """
    # Check if the file is a valid ZIP file
    if not zipfile.is_zipfile(path):
        raise ValueError(f"{path} is not a valid ZIP file.")

    with zipfile.ZipFile(path, 'r') as zip_ref:
        # Find the CSV file in the archive
        csv_file = None
        for file_name in zip_ref.namelist():
            if file_name.endswith('.csv'):
                csv_file = file_name
                break
        
        if not csv_file:
            raise FileNotFoundError("No CSV file found in the ZIP archive.")
        
        # Determine if CSV file has header by looking at first row
        with zip_ref.open(csv_file) as file:
            first_row = pl.read_csv(file, has_header=False, n_rows=1)
            has_header = all([d == pl.String for d in first_row.dtypes])
            
        # Open the CSV file in the archive and load it into a DataFrame
        with zip_ref.open(csv_file) as file:
            df = pl.read_csv(
                file, 
                has_header=has_header,
                null_values=["", "null", "NULL", "None", "none", "NaN", "nan"],
            )
        
    # Close the ZIP file
    zip_ref.close()
    
    return df


def load_df_with_unkwon_format(path: str) -> pl.DataFrame:
    """
    Load a DataFrame from a file with an either parquet, csv or zip format.
    
    Parameters:
    ----------
        path: str
            Path to the file.
            
    Returns:
    -------
        df: pl.DataFrame
            DataFrame containing the data from the file.
    """
    file_ext = path.split(".")[-1].lower()
    if file_ext == "parquet":
        df = pl.read_parquet(path)
    elif file_ext == "csv":
        df = pl.read_csv(path)
    elif file_ext == "zip":
        df = load_csv_from_zip(path)
    else:
        raise ValueError(f"Invalid file format {file_ext}.")
    
    return df


def get_last_trade_id(filepath: str) -> int:
    """
    Reads the last row of a file to get the last_id.
    
    Parameters:
    ----------
    filepath: str
        Path to the file.
        
    Returns:
    -------
    last_id: int
        The last trade/aggTrade ID in the file
    """
    storage_format = filepath.split(".")[-1]
    if storage_format == "parquet":
        metadata = pl.scan_parquet(filepath).collect()

        # Total number of rows in the file
        num_rows = metadata.shape[0]

        # Read only the last row
        last_row = pl.read_parquet(
            filepath, row_index_offset=num_rows-1, n_rows=1
        ).to_pandas()
    elif storage_format == "csv":
        last_row = pl.scan_csv(filepath).tail(1).collect().to_pandas()
    else:
        raise ValueError("Invalid storage format. Choose from csv, parquet.")
    
    # Get the last trade ID
    last_id = int(last_row.values[0][0])
    
    return last_id


def extract_dates_from_filenames(
        filenames: List[str], 
    ) -> List[datetime.date]:
    """
    Extracts the date from a list of filenames. Only valid for this use case.
    It uses a hacky way to extract the date from the filenames, based on the 
    fact that all data on binance occurres in the second millenium.
    
    Parameters:
    ----------
    filenames: List[str]
        List of filenames.
    
    Returns:
    -------
    dates: List[datetime.date]
        List of dates extracted from the filenames.
    """    
    dates = []
    for f in filenames:
        c = "".join(f.split(".")[:-1])
        # Find all 20xx-xx-xx in the string
        date = c[c.find("20"):]
        # Add day if missing
        date = date + "-01" if len(date) == 7 else date
        dates.append(
            datetime.strptime(date, "%Y-%m-%d").date()
        )

    return dates


def check_date_range(
        dates: List[datetime.date],
        mode: str = "monthly"
    ) -> None:
    """
    Check if the dates in the list are coherent and in order. Coherent means
    that the dates are in sequence without any gaps.
    
    Parameters:
    ----------  
    dates: List[datetime.date]
        List of dates.
    mode: str
        Time period (daily, monthly).
        
    Raises:
    -------
    ValueError: If the date range is invalid.
    """
    assert mode in ("daily", "monthly"), \
        "Invalid mode. Choose from daily, monthly."
        
    for d1, d2 in zip(dates[:-1], dates[1:]):        
        if mode == "monthly":
            if d2.day != 1 or d1.day != 1:
                return (
                    f"Invalid date {d2} or {d1}. "
                    f"Dates must be the first of the month."
                )
            if (d2 - d1).days > 31:
                return (
                    f"Invalid date range. {d2} is more than 31 days after {d1}."
                )
            if (d2 - d1).days < 28:
                return (
                    f"Invalid date range. {d2} is less than 28 days after {d1}."
                )
        else:
            if (d2 - d1).days != 1:
                return (
                    f"Invalid date range. {d2} is not the day after {d1}."
                )
    return None


def generate_daily_date_range(
        start_date: str,
        end_date: str,
        include_end_date: bool = False
    ) -> List[datetime.date]:
    """
    Generate a list of daily dates between the start and end dates.

    Parameters:
    ----------
    start_date: str
        Start date in the format "YYYY-MM-DD".
    end_date: str
        End date in the format "YYYY-MM-DD".
    include_end_date: bool
        Whether to include the end date in the date range. Default is False.
        
    Returns:
    -------
    dates: List[datetime.date]
        List of daily dates in the format.
    """
    # Convert the start and end dates to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    if start_date > end_date:
        raise ValueError("start_date must be earlier than or equal to end_date")
    
    # Generate the daily dates
    dates = []
    current_date = start_date
    while current_date < end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)
        
    if include_end_date:
        dates.append(current_date)
        
    # Verify the date range
    check_date_range(dates, mode="daily")
    
    return dates


def generate_monthly_date_range(
        start_date: str,
        end_date: str,
        include_end_date: bool = False
    ) -> List[datetime.date]:
    """
    Generate a list of monthly dates between the start and end dates.
    
    Parameters:
    ----------
    start_date: str
        Start date in the format "YYYY-MM-DD".
    end_date: str
        End date in the format "YYYY-MM-DD".
    include_end_date: bool
        Whether to include the end date in the date range. Default is False.
        
    Returns:
    -------
    dates: List[datetime.date]
        List of monthly dates in the format.
    """
    # Convert the start and end dates to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    if start_date > end_date:
        raise ValueError("start_date must be earlier than or equal to end_date")
    
    # Generate the monthly dates
    dates = []
    current_date = start_date.replace(day=1)
    while current_date < end_date.replace(day=1):
        dates.append(current_date)
        current_date = (current_date + timedelta(days=32)).replace(day=1)
        
    if include_end_date:
        dates.append(current_date)
        
    # Verify the date range 
    check_date_range(dates, mode="monthly")
    
    return dates


def split_date_range(
        start_date: str, 
        end_date: str
    ) -> Tuple[List[datetime.date], List[datetime.date]]:
    """
    Splits a date range into monthly dates (full months) and daily dates 
    (remaining days). No matter what start date is provided, if the date range
    is greater than a month, the start date is set to the first day of the first
    month in the range. If the date range is less than a month, the start date 
    is set to the provided start date.

    Parameters:
    ----------
    start_date: str
        Start date in the format "YYYY-MM-DD".
    end_date: str
        End date in the format "YYYY-MM-DD".
        
    Returns:
    -------
    monthly_dates: List[datetime.date]
        List of monthly dates in the format.
    daily_dates: List[datetime.date]
        List of daily dates in the format.
    """
    # Generate monthly dates
    monthly_dates = generate_monthly_date_range(start_date, end_date, True)
    
    # Set the start date to the first day of the last month
    start_date = monthly_dates[-1].strftime("%Y-%m-%d")
    
    # Pop the last month to exclude the end date if it is the first day of 
    # the month
    monthly_dates.pop()
    
    # Generate daily dates
    daily_dates = generate_daily_date_range(start_date, end_date, False)

    return monthly_dates, daily_dates


def intersect_dates(
        monthly_dates: List[datetime.date], 
        daily_dates: List[datetime.date]
    ) -> Dict[str, List[datetime.date]]:
    """
    Filters the daily dates to exclude any dates that are within months covered
    by the monthly dates. The results are sorted and returned.
    
    Parameters:
    ----------
    monthly_dates: List[datetime.date]
        List of monthly dates.
    daily_dates: List[datetime.date]
        List of daily dates.
        
    Returns:
    -------
    Dict[str, List[datetime.date]]:
        Dictionary containing the filtered monthly and daily dates.
    """
    # Get the set of months in the monthly dates
    monthly_months = set([date.strftime("%Y-%m") for date in monthly_dates])

    # Filter all days that are prior to the first monthly date
    _daily_dates = daily_dates
    if len(monthly_dates):
        _daily_dates = [
            date for date in daily_dates if date >= monthly_dates[0]
        ]
    
    # Warn in case there were daily dates before the first monthly date
    if len(_daily_dates) != len(daily_dates):
        warnings.warn(
            f"Daily data before the first monthly date. Removing these dates."
        )
    
    # Filter daily dates to exclude any dates within months covered by 
    # monthly_dates
    filtered_daily_dates = [
        date for date in _daily_dates 
        if date.strftime("%Y-%m") not in monthly_months
    ]

    continuous_dates = {
        "monthly": monthly_dates, 
        "daily": filtered_daily_dates
    }
    return continuous_dates


def get_valid_combinations(
    trading_types: List[str], 
    market_data_types: List[str],
    ) -> List[Tuple[str, str]]:
    """
    Get valid combinations of trading types and market data types.
    
    Parameters:
    ----------
    trading_types: List[str]
        List of trading types (spot, um, cm).
    market_data_types: List[str]
        List of market data types.
        
    Returns:
    -------
    valid_combinations: List[Tuple[str, str]]
        List of valid combinations of trading types and market data types.
    """
    assert all(t in TRADING_TYPES for t in trading_types), \
        "Invalid trading type. Choose from spot, um, cm."
    assert all(t in MARKET_DATA_TYPES for t in market_data_types), \
        "Invalid market data type."
    assert (("um" not in trading_types) or ("cm" not in trading_types)) or \
        any(t in FUTURE_TYPES for t in market_data_types), \
        "Futures data requires um or cm trading type."
        
    spot_combinations = []
    if "spot" in trading_types:
        valid_data_types = list(set(market_data_types).intersection(SPOT_TYPES))
        spot_combinations = list(product(["spot"], valid_data_types))
        
    um_combinations = []
    if "um" in trading_types:
        valid_data_types = list(set(market_data_types).intersection(
            set(DAILY_FUTURES_TYPES + MONTHLY_FUTURES_TYPES)
        ))
        um_combinations = list(product(["um"], valid_data_types))
        
    cm_combinations = []
    if "cm" in trading_types:
        valid_data_types = list(set(market_data_types).intersection(
            set(MONTHLY_FUTURES_TYPES + DAILY_FUTURES_TYPES)
        ))
        cm_combinations = list(product(["cm"], valid_data_types))
        
    return spot_combinations + um_combinations + cm_combinations

if __name__ == "__main__":
    pass