
# Binance Data Management System (BDMS)

The Binance Data Management System (BDMS) is a Python-based framework designed to streamline the management of cryptocurrency trading data from Binance. This system enables efficient downloading, conversion, merging, and updating to support research, algorithmic trading, and data-driven decision-making.

**Note**: This project is currently under development and may not be fully functional. Please check back for updates!

---

## 🚀 Features

- **Automated Data Population**: Download historical data from [Binance Vision](https://data.binance.vision/) and create your own database.
- **Data Merging**: Combine daily and monthly data to create a unified dataset.
- **Data Format Conversion**: Seamlessly convert data between CSV, Parquet, and ZIP formats.
- **Real-Time Data Updates**: Update datasets using Binance's API to include the latest trades.
- **Scalable Processing**: Utilize multi-threading for efficient parallel processing.
- **Flexible Integration**: Designed with modularity in mind to integrate easily into existing systems.

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
from bdms.populate import populate_database

populate_database(
    root_dir="data/",
    symbols=["BTCUSDT", "ETHUSDT"],
    trading_types=["spot"],
    market_data_types=["klines", "trades"],
    intervals=["1h"],
    start_date="2023-01-01",
    end_date="2023-12-31",
    storage_format="parquet",
)
```

### 2. Format Conversion
Convert files from one format to another:
```python
from bdms.conversion import convert_files

convert_files(
    folder="data/",
    input_format="csv",
    output_format="parquet",
    walk=True,
)
```

### 3. Merging Data
Merge daily and monthly data:
```python
from bdms.merge import merge_data

merge_data(
    root_dir="data/",
    symbols=["BTCUSDT"],
    trading_types=["spot"],
    market_data_types=["klines"],
    intervals=["1h"],
    output_format="parquet",
)
```

### 4. Real-Time Updates
Update aggregate trade data:
```python
from bdms.update import update_aggTrades

update_aggTrades(
    api_key="your_api_key",
    api_secret="your_api_secret",
    path="data/BTCUSDT.parquet",
)
```

---

## 🛠 Project Structure

- **`populate.py`**: Handles downloading and structuring historical data.
- **`conversion.py`**: Converts data between supported formats.
- **`merge.py`**: Merges data into unified files for analysis.
- **`update.py`**: Updates datasets with real-time trade data.
- **`utils.py`**: Utilities for common tasks like file handling and validation.
- **`enums.py`**: Contains constants for Binance API configurations.

---


## 🔮 Upcoming Features

- **Advanced Analytics**: Add built-in functions for technical analysis and trend prediction.
- **Backtesting Module**: Integrate a backtesting module for algorithmic trading strategies.
- **Visualization Tools**: Provide graphical representations of market trends and patterns.
- **Improved Error Handling**: Enhance error detection and reporting across all modules.
- **Additional Data Sources**: Extend support for data from other exchanges beyond Binance.
- **Cloud Integration**: Add support for cloud storage and processing options.

Feel free to suggest additional features via [issues](https://github.com/lepremiere/bdms/issues)!

---

## 🖇 Dependencies

- Python 3.8+
- Required libraries:
  - `pandas`
  - `polars`
  - `pyarrow`
  - `tqdm`
  - `multiprocessing`
  - `binance`

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

- Data provided by [Binance Vision](https://data.binance.vision/).
- Libraries: `polars`, `pyarrow`, `tqdm`, `binance`.

If you found this project useful, please consider giving it a star 🌟!
