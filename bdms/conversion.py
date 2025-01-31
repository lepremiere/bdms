import os
import gc
import warnings 
import numpy as np
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import polars as pl
from bdms.utils import load_csv_from_zip
   
    
def convert_single_file(
        input_file: str, 
        output_format: str, 
        delete_original: bool = False
    ) -> None:
    """
    Converts a single file to the specified output format. The converted file is
    stored in the same folder as the input file.
    
    Parameters:
    ----------
    input_file: str
        Path to the file to convert.
    output_format: str
        Format to convert the file to (csv, parquet).
    delete_original: bool
        If True, the original file is deleted after conversion.
    """
    input_format = input_file.split(".")[-1].lower()
    output_file = input_file.replace(input_format, output_format)
    assert os.path.isfile(input_file), f"Invalid file {input_file}."
    assert input_format in ["zip", "csv", "parquet"], \
        f"Invalid input format {input_format}."
    assert output_format in ["csv", "parquet"], \
        f"Invalid output format {output_format}."
        
    # Catch any errors that occur during the conversion, eg empty files
    try:
        if input_format == "zip":
            df = load_csv_from_zip(input_file).lazy()
        elif input_format == "csv":
            df = pl.scan_csv(input_file)
        else:
            df = pl.scan_parquet(input_file)
    except Exception as e:
        warnings.warn(f"\nError loading file {input_file}. {e}")
    
    # Write the dataframe to the output file
    if output_format == "csv":
        df.sink_csv(output_file)
    else:
        df.sink_parquet(output_file)
    
    # Delete the original file
    if delete_original:
        os.remove(input_file)

    # Clear the dataframe from memory
    del df
    gc.collect()
    

def convert_files(
        folder: str, 
        input_format: str, 
        output_format: str,
        walk: bool = False,
        delete_original: bool = False,
        n_jobs: int = -1
    ) -> None:
    """
    Converts all the files in 'input_format' in the specified folder to
    'output_format'. The converted files are stored in the same folder.
    
    Parameters:
    ----------
    folder: str
        Folder containing the files to convert.
    input_format: str
        Format of the files to convert (zip, csv, parquet).
    output_format: str
        Format to convert the files to (csv, parquet).
    walk: bool
        If True, the conversion is done recursively in all subfolders.
    delete_original: bool
        If True, the original files are deleted after conversion.
    n_jobs: int
        Number of parallel jobs to run. Default is the number of CPUs.
    """
    assert os.path.isdir(folder), f"Invalid folder {folder}."
    assert input_format in ["csv", "parquet"], \
        f"Invalid input format {input_format}."
    assert output_format in ["csv", "parquet"], \
        f"Invalid output format {output_format}."
                
    # Find all the files in the folder and its subfolders if walk is True
    paths = []
    for root, _, files in os.walk(folder):
        paths.extend([
            os.path.join(root, f) for f in files if f.endswith(input_format)
        ]) 
        # Stop walking if walk is False
        if not walk: 
            break
    
    # Check if any files were found
    if not paths:
        warnings.warn(f"No files found in {root}.")
    
    # Shuffle the paths to avoid processing the files in order, avoiding
    # processing large files in sequence.
    np.random.shuffle(paths)
    
    # Convert the files
    n_jobs = min(cpu_count(), len(paths)) if n_jobs == -1 else n_jobs
    pool = Pool(n_jobs, maxtasksperchild=1)
    pbar = tqdm(total=len(paths), desc="Converting Files", position=0)
    for path in paths:
        input_file = os.path.join(root, path)
        pool.apply_async(
            convert_single_file, 
            args=(input_file, output_format, delete_original),
            callback=lambda _: pbar.update(1)
        )
    pool.close()
    pool.join()
    pbar.close()
        

if __name__ == "__main__":
    pass