from typing import List

import io
import os
import gc
import json
import zipfile
import warnings
import urllib.request
import urllib.error
import polars as pl
import numpy as np
from tqdm import tqdm
from datetime import datetime
from multiprocessing import Pool, cpu_count

from bdms.enums import (
    TYPES_MAP, START_DATE_MAP, END_DATE, INTERVALS_MAP,
    BASE_URL, SPOT_COLUMNS_MAP, FUTURES_COLUMNS_MAP,
    DTYPE_MAP,
)
from bdms.utils import (
    get_base_path, get_file_basename, get_valid_combinations,
    generate_daily_date_range, generate_monthly_date_range, 
    split_date_range,  download_file, load_csv_from_zip
)


def download_and_process_file(
        url: str,
        zip_path: str,
        file_path: str,
        cols: List[str],
    ) -> None:
    """Downloads the zip file and processes it to the desired format."""
    try:
        storage_format = file_path.split(".")[-1].lower()
        
        # Download the file 
        success = download_file(url, zip_path)
        if not success: 
            return
        
        # Extract the zip file and get the csv file
        df = load_csv_from_zip(zip_path)
        df.columns = cols
        
        # Set all ignore values to 0 for compatibility. Older files have
        # an ignore column with not all values set to 0. 
        if "ignore" in df.columns:
            df = df.with_columns(pl.lit(0).alias("ignore"))
            
        # Convert timestamp to unix if it is string. Only BookDepth files have
        # been observed to have the column "timestamp" as datetime. All other
        # files have the column "timestamp" in unix format.
        if "timestamp" in df.columns and df["timestamp"].dtype == pl.String:
            df = df.with_columns(
                pl.col("timestamp").str.strptime(
                    pl.Datetime("ms"), "%Y-%m-%d %H:%M:%S"
                ).cast(pl.Int64)
            )
        
        # Convert all time related columns to microseconds, for forward 
        # compatibility. Binance switched from milliseconds to microseconds
        # in 2025.
        for col in df.columns:
            if "time" in col:
                df = df.with_columns(
                    # Converts milliseconds to microseconds but leaves 
                    # microseconds as they are.
                    pl.col(col) * (1 + (pl.col(col) % 10**6 != 0) * 999)
                )
            
        # Set the data types
        df = df.with_columns(
            [(df[col].cast(DTYPE_MAP[col])) for col in df.columns]
        )
        
          # Write the file to the desired format
        if storage_format == "parquet":
            df.write_parquet(file_path)
            os.remove(zip_path)
        elif storage_format == "csv":
            df.write_csv(file_path)
            os.remove(zip_path)
        elif storage_format == "zip":
            buffer = io.StringIO()
            df.write_csv(buffer)
            with zipfile.ZipFile(zip_path, "w") as z:
                z.writestr(
                    f"{os.path.basename(file_path)}".replace(".zip", ".csv"),
                    buffer.getvalue()
                )               
    except Exception as e:
        warnings.warn(f"Error processing {url}: {e}", category=RuntimeWarning)
    finally:
        gc.collect()


def populate_database(
        root_dir: str,
        symbols: List[str],
        trading_types: List[str],
        market_data_types: List[str],
        intervals: List[str] = None,
        start_date: str = None,
        end_date: str = END_DATE,
        storage_format: str = "zip",
        n_jobs: int = -1
    ) -> None:
    """
    Populates the root directory with selected market data from the Binance 
    database https://data.binance.vision/. It creates a directory structure
    similar to the database structure. The data will automatically be downloaded
    from the monthly endpoint for the bulk of the data, and the daily endpoint
    for the remaining data that the specified date range covers, if available.
    
    Note: The available dates are not easy to determine, so it will be tried to
    download the data for the specified date range. If the data is not available
    for the specified date range, the function will give a warning.
    
    WARNING: Depending on selected data you might need a lot of disk space, RAM,
    and time. Especially trades are heavy in terms of data. If resources are 
    limited, consider limiting the number of parallel jobs.
     
    Parameters:
    ----------
    root_dir: str
        Root directory to populate with the data.
    symbols: List[str]
        List of trading symbols (e.g. ["BTCUSDT", "ETHUSDT"]). Must be in
        uppercase.
    trading_types: List[str]
        Trading types (spot, um, cm).
    market_data_types: List[str] 
        Market data types. Can be (trades, aggTrades, klines) for all trading
        types, and additionally (bookTicker, fundingRate, indexPriceKlines,
        markPriceKlines, premiumIndexKlines) for um and cm.
    intervals: List[str]
        Kline intervals. Default is None. Required if market_data_type is
        klines, else ignored. See enums.INTERVALS.
    start_date: str
        Start date for the data. Default is None. If None, the start date
        is set to the first date in the database for the given trading_type.
        Inclusive.
    end_date: str
        End date for the data. Default is the current date. Exclusive.
    storage_format: str
        Storage format for the data (csv, parquet, zip). Default is zip.
        Note: Parquet has the lowest file size.
    n_jobs: int
        Number of parallel jobs to run. Default is -1, which uses all available
        CPUs.
        
    Raises:
    -------
        ValueError: 
            If the storage format is invalid.
        ValueError: 
            If the interval is not provided for klines.
        Warning:
            If the file is not found in the database.
    """        
    assert storage_format in ["zip", "csv", "parquet"], \
        "Invalid storage format. Choose from zip, csv, parquet."
            
    # Check if the start date is provided
    start_date_provided = start_date is not None

    # Get valid combinations of trading types and market data types
    combinations = get_valid_combinations(trading_types, market_data_types)

    jobs = []
    # Iterate over the combinations
    for trading_type, market_data_type in combinations:
               
        # Get the start date and check if the start date is valid
        _start_date = datetime.strptime(
            START_DATE_MAP[trading_type][market_data_type], "%Y-%m-%d"
        ).date()
        if start_date_provided:
            provided_start_date = datetime.strptime(
                start_date, "%Y-%m-%d"
            ).date()
            if provided_start_date < _start_date:
                warnings.warn(
                    f"({trading_type}: {market_data_type}) only available "
                    f"from {_start_date}. Setting start date to {_start_date}.",
                    RuntimeWarning
                )
                start_date = str(_start_date)
        else:
            start_date = str(_start_date)
        
        # Determine if data can be drawn from daily and monthly endpoints
        has_daily = market_data_type in TYPES_MAP[trading_type]["daily"]
        has_monthly = market_data_type in TYPES_MAP[trading_type]["monthly"]
        
        # Generate the date ranges 
        monthly_dates, daily_dates = [], []
        if has_daily and has_monthly:
            monthly_dates, daily_dates = split_date_range(start_date, end_date)
        elif has_daily:
            daily_dates = generate_daily_date_range(start_date, end_date)
        elif has_monthly:
            monthly_dates = generate_monthly_date_range(start_date, end_date)
        
        # Iterate over the dates
        n_monthly = len(monthly_dates)
        for i, date in enumerate(monthly_dates + daily_dates):
            time_period = "monthly" if i < n_monthly else "daily"
            
            # Iterate over the symbols
            for symbol in symbols:
                
                # Determine the intervals
                if "klines" not in market_data_type.lower():
                    _intervals = [None]
                elif intervals is not None:
                    _intervals = intervals.copy()
                else:
                    raise ValueError("Intervals must be provided for klines.")
                    
                # Iterate over the intervals
                for interval in _intervals:
                    # Skip if the interval is not valid
                    if interval is not None:
                        if interval not in INTERVALS_MAP[time_period]:
                            continue
                    if interval == "1s" and trading_type != "spot":
                        continue
                    
                    # Get the base path and create the save path
                    base_path = get_base_path(
                        trading_type, market_data_type, 
                        time_period, symbol, interval
                    )
                    save_path = os.path.join(root_dir, base_path)
                    os.makedirs(save_path, exist_ok=True)
                    
                    # Define the zip and csv file path
                    basename = get_file_basename(
                        symbol, market_data_type, str(date), 
                        time_period, interval
                    )
                    zip_path = f"{save_path}{basename}.zip"
                    file_path = f"{save_path}{basename}.{storage_format}"
                    
                    # Skip if the file in target format already exists
                    if os.path.exists(file_path):
                        continue
                        
                    # Get the column names for this market data type
                    if trading_type != "spot":
                        cols = FUTURES_COLUMNS_MAP[market_data_type]
                    else:
                        cols = SPOT_COLUMNS_MAP[market_data_type]
                    
                    # Define the URL 
                    url = f"{BASE_URL}{base_path}{basename}.zip"
                    
                    # Add the job to the list
                    jobs.append((url, zip_path, file_path, cols))
    
    # Check if there are any jobs
    if not jobs:
        print("No valid combinations found.")
        return
    
    # Shuffle jobs to avoid processing large files in sequence, potentially
    # running out of memory.
    np.random.shuffle(jobs)
    
    # Run the jobs in parallel
    pbar = tqdm(total=len(jobs), desc="Downloading data", smoothing=0)
    n_jobs = min(cpu_count(), len(jobs)) if n_jobs == -1 else n_jobs
    pool = Pool(n_jobs, maxtasksperchild=1)
    for job in jobs:
        pool.apply_async(
            download_and_process_file, args=job, 
            callback=lambda _: pbar.update(1)
        )
    pool.close()
    pool.join()
    pbar.close()
                                

def get_all_symbols(type: str) -> List[str]:
    """
    Get all trading symbols from Binance for a specific trading type.
    
    Parameters:
    ----------
    type: str
        Trading type (spot, um, cm).
        
    Returns:
    -------
    List[str]
        List of trading symbols.
    """
    if type == 'um':
        response = urllib.request.urlopen(
            "https://fapi.binance.com/fapi/v1/exchangeInfo"
        ).read()
    elif type == 'cm':
        response = urllib.request.urlopen(
            "https://dapi.binance.com/dapi/v1/exchangeInfo"
        ).read()
    elif type == 'spot':
        response = urllib.request.urlopen(
            "https://api.binance.com/api/v3/exchangeInfo"
        ).read()
    else:
        raise ValueError("Invalid trading type {}".format(type))
    
    symbols = list(
        map(lambda symbol: symbol['symbol'], json.loads(response)['symbols'])
    )
    
    return symbols

    
if __name__ == "__main__":
    pass