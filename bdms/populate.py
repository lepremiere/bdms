from typing import List

import os
import gc
import json
import warnings
import urllib.request
import urllib.error
import warnings
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

from bdms.enums import *
from bdms.utils import (
    get_base_path, get_file_basename, 
    split_date_range, get_valid_combinations,
    download_file, load_csv_from_zip, get_cols,
    
)


def download_and_process_file(
        url: str,
        zip_path: str,
        file_path: str,
        cols: List[str],
    ) -> None:
    """Downloads the zip file and processes it to the desired format."""
    storage_format = file_path.split(".")[-1].lower()
    
    # Download the file 
    if not download_file(url, path=zip_path):
        return
    
    # Store data in different format if not zip via Thread
    if storage_format != "zip":
        # Extract the zip file and get the csv file
        df = load_csv_from_zip(zip_path)
        df.columns = cols
        
        if storage_format == "parquet":
            df.write_parquet(file_path)
        else:
            df.write_csv(file_path)
            
        # Delete the zip file and the df
        os.remove(zip_path)
        del df
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
    for the remaining data that the specified date range covers.
    
    WARNING: Depending on selected data you might need a lot of disk space.
     
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
        If zip, the data is stored as received from the database.
        If csv or parquet, the native zip file is extracted and stored in the
        selected format. Note: Parquet has the lowest file size.
    n_jobs: int
        Number of parallel jobs to run. Default is -1, which uses all available
        CPUs.
    """        
    assert storage_format in ["zip", "csv", "parquet"], \
        "Invalid storage format. Choose from zip, csv, parquet."
            
    # Check if the start date is provided
    start_date_provided = start_date is not None
    
    # Get all the combinations 
    combinations = get_valid_combinations(
        symbols, trading_types, market_data_types, intervals
    )

    jobs = []
    # Iterate over the combinations
    for symbol, trading_type, market_data_type, interval in combinations:
        if not start_date_provided:
            start_date = START_DATE_MAP[trading_type]
        
        # Split date range into monthly and daily periods
        monthly_dates, daily_dates = split_date_range(start_date, end_date)

        # Iterate over the dates
        for date in monthly_dates + daily_dates:
            time_period = "monthly" if date in monthly_dates else "daily"
            
            # Get the base path and create the save path
            base_path = get_base_path(
                trading_type, market_data_type, time_period, symbol, interval
            )
            save_path = os.path.join(root_dir, base_path)
            os.makedirs(save_path, exist_ok=True)
            
            # Define the zip and csv file path
            basename = get_file_basename(
                symbol, market_data_type, date, time_period, interval
            )
            zip_path = f"{save_path}{basename}.zip"
            file_path = f"{save_path}{basename}.{storage_format}"
            
            # Skip if the file in target format already exists
            if os.path.exists(file_path):
                warnings.warn(f"File already exists: {file_path}")
                continue
                
            # Define the URL and columns
            url = f"{BASE_URL}{base_path}{basename}.zip"
            cols = get_cols(trading_type, market_data_type)
            
            # Add the job to the list
            jobs.append((url, zip_path, file_path, cols))
    
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
    
