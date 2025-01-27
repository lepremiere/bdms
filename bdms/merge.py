from typing import List

import os
import gc
import warnings
import pyarrow.parquet as pq
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from datetime import datetime

from bdms.enums import *
from bdms.utils import (
    get_base_path, extract_dates_from_filenames, check_date_range,
    load_df_with_unkwon_format, get_valid_combinations
)

def concatenate_dfs_on_disk(paths: List[str], output_file: str) -> None:
    """
    Concatenates the dataframes stored in the files specified by the paths. The
    files are read and written sequentially to the output file. The output file
    format is determined by the extension of the output file. Supported formats
    are csv and parquet.
    """
    storage_format = output_file.split(".")[-1].lower()

    with open(output_file, mode="wb") as f:
        if storage_format == "csv":
            first_file = True
            last_id = 0
            for path in paths:
                df = load_df_with_unkwon_format(path)
                
                # Check if the data is continuous
                current_id = int(df[0, 0])
                if current_id != (last_id + 1):
                    warnings.warn(
                        f"Data is not continuous from {path} on. Removing data."
                    )
                    f.close()
                    os.remove(output_file)
                    break
                
                # Write the data to the output file
                df.write_csv(f, include_header=first_file)
                first_file = False
                last_id = int(df[-1, 0])
                
        # Work around for parquet files since polars does not support writing
        # to parquet files yet
        elif storage_format == "parquet":
            # Load the first file and get the schema
            arrow_table = load_df_with_unkwon_format(paths[0]).to_arrow() 
            writer = pq.ParquetWriter(f, arrow_table.schema, compression="zstd")
            with writer:
                writer.write_table(arrow_table)
                last_id = int(arrow_table.take([0, 0].to_pandas().iloc[0, 0]))
                for path in paths[1:]:
                    arrow_table = load_df_with_unkwon_format(path).to_arrow()
                    
                    # Check if the data is continuous
                    current_id = int(
                        arrow_table.take([0, 0].to_pandas().iloc[0, 0])
                    )
                    if current_id != (last_id + 1):
                        warnings.warn(
                            f"Data is not continuous from {path} on. "
                            f"Removing data."
                        )
                        f.close()
                        os.remove(output_file)
                        break
                    
                    # Write the data to the output file
                    writer.write_table(arrow_table)   
        else:
            raise ValueError(f"Invalid storage format {storage_format}.")
    f.close()
    gc.collect()
        

def merge_data(
        root_dir: str,
        symbols: List[str],
        trading_types: List[str],
        market_data_types: List[str],
        intervals: List[str] = None,
        data_base_format: str = "zip",
        output_format: str = "parquet",
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
    n_jobs: int
        Number of parallel jobs to run. Default is the number of CPUs
    """
    assert data_base_format in ["zip", "csv", "parquet"], \
        "Invalid storage format. Choose from zip, csv, parquet."
    assert output_format in ["csv", "parquet"], \
        "Invalid output format. Choose from csv, parquet."
    
    # Get all the combinations 
    combinations = get_valid_combinations(
        symbols, trading_types, market_data_types, intervals
    )
    
    jobs = []
    # Iterate over the combinations
    for symbol, trading_type, market_data_type, interval in combinations:
        # Get the base path and create the save path
        base_path_monthly = get_base_path(
            trading_type, market_data_type, "monthly", symbol, interval
        )
        base_path_daily = get_base_path(
            trading_type, market_data_type, "daily", symbol, interval
        )
        stored_path_monthly = os.path.join(root_dir, base_path_monthly)
        stored_path_daily = os.path.join(root_dir, base_path_daily)
        
        # Set the output directory
        base_save_path = base_path_monthly.replace("data/", "merged/")
        base_save_path = base_save_path.replace("monthly/", "")
        save_path = os.path.join(
            root_dir, base_save_path.replace(f"{symbol}/", "")
        )
        os.makedirs(save_path, exist_ok=True)
                
        # Get all the files in the directory and filter for file type
        all_monthly_files = os.listdir(stored_path_monthly)
        monthly_files = [
            f for f in all_monthly_files if f.endswith(data_base_format)
        ]
        all_daily_files = os.listdir(stored_path_daily)
        daily_files = [
            f for f in all_daily_files if f.endswith(data_base_format)
        ]
                
        # Get available dates from the files and check the date range
        monthly_dates = extract_dates_from_filenames(monthly_files)
        daily_dates = extract_dates_from_filenames(daily_files)
        
        # Filter days/files that are after the last month
        last_month = monthly_dates[-1]
        daily_dates = [d for d in daily_dates if d > last_month]
        daily_files = [
            f for f, d in zip(daily_files, daily_dates) if d > last_month
        ]

        # Check if the date ranges are valid
        msg = check_date_range(monthly_dates, "monthly")
        if msg is not None:
            warnings.warn(
                msg + f" Skipping {symbol}, {trading_type}, {market_data_type}."
            )
            continue
            
        msg = check_date_range(daily_dates, "daily")
        if msg is not None:
            warnings.warn(
                msg + f" Skipping {symbol}, {trading_type}, {market_data_type}."
            )
            continue
        
        # Check if the first daily date is immediately after the last monthly 
        # date. If not, warn the user and skip this combination
        target_date = last_month.replace(
            month=(last_month.month+1)%12 or 12, 
            year=last_month.year+(last_month.month+1)%12
        ) 
        if daily_dates[0] != target_date:
            warnings.warn(
                f"Monthly and daily data is not continuous. "
                f"Skipping {symbol}, {trading_type}, {market_data_type}."
                f" Populate the database again from {target_date}."
            )
            continue
        
        # Create jobs
        paths = [os.path.join(stored_path_monthly, f) for f in monthly_files]
        paths += [os.path.join(stored_path_daily, f) for f in daily_files]
        output_file = os.path.join(save_path, f"{symbol}.{output_format}")
        jobs.append((paths, output_file))
    
    # Check if there are any valid jobs
    if not jobs:
        warnings.warn("No valid combinations found.")
        return
    
    # Run the jobs in parallel
    pbar = tqdm(total=len(symbols), desc="Merging data")
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