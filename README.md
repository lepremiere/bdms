
# Binance Data Management System (BDMS)

The Binance Data Management System (BDMS) is a Python-based framework designed to streamline the management of cryptocurrency trading data from Binance. This system enables efficient downloading, conversion, merging, and updating to support research, algorithmic trading, and data-driven decision-making.

**Note**: This project is currently under development and may not be fully functional. Please check back for updates!

---

## 🚀 Features

- **Automated Data Population**: Download historical data from [Binance](https://data.binance.vision/) and create your own database.
- **Data Merging**: Combine the natively distributed data to create a unified dataset.
- **Data Format Conversion**: Seamlessly convert data between CSV, Parquet, and ZIP formats.
- **Real-Time Data Updates**: Update datasets using Binance's API to include the latest trades.
- **Scalable Processing**: Utilize multi-threading for efficient parallel processing.

---

## 📦 Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/lepremiere/bdms.git
   cd bdms
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Ensure you have API access keys from Binance for real-time updates.

---

## 🔧 Usage

### 1. Data Population
Download historical data directly from Binance:
```python
import warnings
from bdms import populate_database

warnings.filterwarnings("ignore", category=UserWarning) 
if __name__ == "__main__":
    populate_database(
        root_dir="C:/Binance",
        symbols=["BTCUSDT", "ETHUSDT"],
        trading_types=["spot"],
        market_data_types=["klines", "aggTrades"],
        intervals=["1h"],
        start_date="2023-01-01",
        end_date="2023-12-31",
        storage_format="parquet",
    )
```
- Does not support _bookTicker_ and _liquidationSnapshot_ data types since they are no longer supported by Binance. 
- Automatically determines all valid combinations of the types and intervals provided.
- Sets the _start_date_ automatically to earliest available date if not provided or specified to early. _end_date_ is set to the current date if not provided.
- Tries to download all valid combinations for a given date range. If a combination is not available, it throws a _UserWarning_ and skips the file. Therefore, suppressing _UserWarning_ is recommended. 

### 2. Format Conversion
Convert files from one format to another:
```python
from bdms.conversion import convert_files

convert_files(
    folder="../Binance",
    input_format="csv",
    output_format="parquet",
    walk=True,                # Recursively search for files
    delete_original=False,    # Keep original files after conversion
)
```

### 3. Merging Data
Merge all available data for a given combination into a single file:
```python
from bdms.merge import merge_data

merge_data(
    root_dir="../Binance",
    symbols=["BTCUSDT"],
    trading_types=["spot"],
    market_data_types=["klines"],
    intervals=["1h"],
    output_format="parquet",
)
```
- Automatically determines all valid combinations of the types and intervals provided.

### 4. Real-Time Updates
Update aggregate trade data:
```python
from bdms.update import update_aggTrades

update_aggTrades(
    api_key="your_api_key",
    api_secret="your_api_secret",
    path="../BTCUSDT.parquet",
    write_interval=1000             # Write trades to file every 1000 trades
)
```
- Supports updating _aggTrades_ and single files only.
- The filename must contain the symbol.

---

## 🛠 Project Structure

- **`populate.py`**: Handles downloading and structuring historical data.
- **`conversion.py`**: Converts data between supported formats.
- **`merge.py`**: Merges data into unified files for analysis.
- **`update.py`**: Updates datasets with real-time trade data.
- **`utils.py`**: Utilities for common tasks like file handling and validation.
- **`enums.py`**: Contains constants for Binance API configurations.

---

## 🖇 Dependencies

- Python 3.8+
- Required libraries:
    - `numpy`
    - `pandas`
    - `polars`
    - `pyarrow`
    - `tqdm`
    - `multiprocessing`
    - `python-binance`

Install all dependencies using the provided `requirements.txt`.

---

## 📖 Documentation

Documentation will be available soon. Stay tuned!

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Submit a pull request for review.

---

## 🛡 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.


---

## 📬 Contact

**[GitHub](https://github.com/lepremiere)**
**[Kaggle](https://www.kaggle.com/lepremiere)**
**[LinkedIn](https://www.linkedin.com/in/henry-huick-4ba7a52a8/)**
**[XING](https://www.xing.com/profile/Henry_Huick)**

---

## ⭐ Acknowledgements

- Data provided by [Binance](https://data.binance.vision/).
- This repository is based on the code of the Binance-Team @ [binance-public-data](https://github.com/binance/binance-public-data). Big thanks to them!

If you found this project useful, please consider giving it a star 🌟!
