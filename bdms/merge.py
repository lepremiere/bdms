from typing import List

import os
import gc
import warnings
import polars as pl
import pandas as pd
import pyarrow.parquet as pq
from itertools import product
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

from bdms.enums import *
from bdms.utils import (
    get_base_path, 
    extract_dates_from_filenames, 
    check_date_range,
    intersect_dates,
    load_df_with_unkwon_format, 
    get_valid_combinations,
)

def concatenate_dfs_on_disk(paths: List[str], output_file: str) -> None:
    """
    Concatenates the dataframes stored in the files specified by the paths. The
    files are read and written sequentially to the output file. The output file
    format is determined by the extension of the output file. Supported formats
    are csv and parquet.
    
    Parameters:
    ----------
    paths: List[str]
        List of file paths to concatenate.
    output_file: str
        Output file path.
        
    Raises:
    ------
    ValueError:
        If the input or output format is invalid.
    """
    assert all([os.path.exists(p) for p in paths]), "File does not exist."
    assert all([p.split(".")[-1] in ["zip", "csv", "parquet"] for p in paths]),\
        "Invalid file format. Choose from zip, csv, parquet."
    
    storage_format = output_file.split(".")[-1].lower()
    assert storage_format in ["csv", "parquet"], \
        "Invalid output format. Choose from csv, parquet."
    try:
        with open(output_file, mode="wb") as f:
            if storage_format == "csv":
                first_file = True
                for path in paths:             
                    df = load_df_with_unkwon_format(path)                
                    df.write_csv(f, include_header=first_file)
                    first_file = False
                    
            elif storage_format == "parquet":
                # Load the first file and get the schema
                table = load_df_with_unkwon_format(paths[0]).to_arrow()
                writer = pq.ParquetWriter(
                    f, table.schema , compression="zstd", 
                    use_dictionary=False, compression_level=5,
                )
                with writer:
                    writer.write_table(table)
                    for path in paths[1:]:
                        table = load_df_with_unkwon_format(path).to_arrow()
                        writer.write_table(table)  
                writer.close() 
            else:
                raise ValueError(f"Invalid storage format {storage_format}.")
    except Exception as e:
        warnings.warn(f"Error: {e}. Removing {output_file}.")
        os.remove(output_file)  
    finally:
        gc.collect()
            

def merge_database(
        root_dir: str,
        symbols: List[str],
        trading_types: List[str],
        market_data_types: List[str],
        intervals: List[str] = None,
        data_base_format: str = "zip",
        output_format: str = "parquet",
        check_continuous: bool = True,
        n_jobs: int = -1
    ) -> None:
    """
    Merges monthly and daily data for the specified symbols, trading types,
    market data types, and intervals. The data is read from the database and
    written into a "merged" directory in the root directory. The directory 
    structure is similar to the database structure but without the monthly and
    daily subdirectories, as well as there is no subfolder for the symbol. The
    data selected for this operation is based on the "data_base_format" 
    parameter. The output format is determined by the "output_format" parameter.
    
    Parameters:
    ----------
    root_dir: str
        Root directory of the database.
    symbols: List[str]
        List of trading symbols (e.g. ["BTCUSDT", "ETHUSDT"]). Must be in
        uppercase.
    trading_type: List[str]
        Trading types (spot, um, cm).
    market_data_type: List[str]
        Market data types (trades, aggTrades, klines).
    interval: List[str]
        Intervals for the klines data. See enums.INTERVALS and 
        enums.DAILY_INTERVALS for valid intervals.
    data_base_format: str
        Storage format of the target data in the database (zip, csv, parquet). 
        Default is zip.
    output_format: str
        Storage format for the merged data (csv, parquet). Default is parquet.
    check_continuous: bool
        Check if the monthly and daily data is continuous. Default is True.
    n_jobs: int
        Number of parallel jobs to run. Default is the number of CPUs
        
    Raises:
    ------
    AssertionError:
        If the storage or output format or output format is invalid.
    Warning:
        If no valid combinations are found or if the monthly and daily data is
        not continuous or if no files are found for a specific combination.
    """
    assert data_base_format in ["zip", "csv", "parquet"], \
        "Invalid storage format. Choose from zip, csv, parquet."
    assert output_format in ["csv", "parquet"], \
        "Invalid output format. Choose from csv, parquet."
    
    # Get all the combinations 
    combinations = get_valid_combinations(trading_types, market_data_types)
    combinations = product(symbols, combinations)
    
    jobs = []
    # Iterate over the combinations
    for symbol, (trading_type, market_data_type) in combinations:           
        # Determine the intervals
        if "klines" not in market_data_type.lower():
            _intervals = [None]
        elif intervals is not None:
            _intervals = intervals.copy()
        else:
            raise ValueError("Intervals must be provided for klines.")
        
        # Iterate over the intervals
        for interval in _intervals:
            
            # Get the base paths
            path_monthly, path_daily = None, None
            base_path_monthly = get_base_path(
                trading_type, market_data_type, "monthly", symbol, interval
            )
            base_path_daily = get_base_path(
                trading_type, market_data_type, "daily", symbol, interval
            )
            
            # Get the full paths
            path_daily = os.path.join(root_dir, base_path_daily)
            path_monthly = os.path.join(root_dir, base_path_monthly)
            
            # Get the save path and create the directory
            base_save_path = base_path_monthly.replace("data/", "merged/")
            base_save_path = base_save_path.replace("monthly/", "")
            save_path = os.path.join(
                root_dir, base_save_path.replace(f"{symbol}/", "")
            )
            os.makedirs(save_path, exist_ok=True)
            
            # Get all the files in the directory and according dates
            monthly_files, daily_files = [], []
            monthly_dates, daily_dates = [], []
            has_monthly, has_daily = False, False
            
            if os.path.exists(path_monthly):
                # Get the monthly files
                all_monthly_files = os.listdir(path_monthly)
                monthly_files = [
                    f for f in all_monthly_files if f.endswith(data_base_format)
                ]
                # Get available dates from the files and check the date 
                # range if needed
                monthly_dates = extract_dates_from_filenames(monthly_files)
                if check_continuous:
                    check_date_range(monthly_dates, "monthly")
                    
                has_monthly = True if monthly_files else False
                    
            if os.path.exists(path_daily):
                # Get the daily files
                all_daily_files = os.listdir(path_daily)
                daily_files = [
                    f for f in all_daily_files if f.endswith(data_base_format)
                ]
                # Get available dates from the files and check the date 
                # range if needed
                daily_dates = extract_dates_from_filenames(daily_files)
                if check_continuous:
                    check_date_range(daily_dates, "daily")
                    
                has_daily = True if daily_files else False
                
            # Intersect the dates and filter for continuous dates
            dates = intersect_dates(monthly_dates, daily_dates)
            monthly_dates, daily_dates = dates["monthly"], dates["daily"]

            # Check if the first daily date is immediately after the last 
            # monthly date. If not, warn the user and skip this combination
            if has_monthly and has_daily and check_continuous:
                last_month = monthly_dates[-1]
                target_date = last_month.replace(
                    month=(last_month.month+1)%12 or 12, 
                    year=last_month.year+(last_month.month+1)%12
                ) 
                if daily_dates[0] != target_date:
                    warnings.warn(
                        f"Monthly and daily data is not continuous. Skipping"
                        f" {symbol}, {trading_type}, {market_data_type}."
                        f" Populate the database again from {target_date}."
                    )
                    continue
            
            # Create jobs
            paths = [os.path.join(path_monthly, f) for f in monthly_files]
            paths += [os.path.join(path_daily, f) for f in daily_files]
            
            # Check if any files are available
            if not paths:
                warnings.warn(
                    f"No files found for {symbol}, {trading_type}, "
                    f"{market_data_type}."
                )
                continue
            
            # Create job
            output_file = os.path.join(save_path, f"{symbol}.{output_format}")
            jobs.append((paths, output_file))
     
    # Check if there are any jobs
    if not jobs:
        warnings.warn("No valid combinations found.")
        return

    # Run the jobs in parallel
    pbar = tqdm(total=len(jobs), desc="Merging data")
    n_jobs = min(cpu_count(), len(jobs)) if n_jobs == -1 else n_jobs

    pool = Pool(n_jobs, maxtasksperchild=1)
    for job in jobs:
        pool.apply_async(
            concatenate_dfs_on_disk, args=job,
            callback=lambda _: pbar.update(1)
        )
    pool.close()
    pool.join()
    pbar.close()