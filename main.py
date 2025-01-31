import bdms
import warnings 

warnings.filterwarnings("ignore", category=UserWarning)

if __name__ == "__main__":
    symbols = [
        "BTCUSDT", "BNBUSDT", "ADAUSDT", 
        "ETHUSDT", "XRPUSDT", 
    ]
    market_data_types = [
        "aggTrades", "klines", "fundingRate", 
        "indexPriceKlines", "markPriceKlines", 
        "premiumIndexKlines", "bookDepth", "metrics",
    ]
    intervals = ["1m", "5m", "1h", "1d"]
    
    bdms.populate_database(
        root_dir="D:/Binance",
        symbols=symbols, 
        trading_types=["spot", "um"], 
        market_data_types=market_data_types,
        intervals=intervals,
        storage_format="parquet",
    )
    
    bdms.merge_database(
        root_dir="D:/Binance",
        symbols=symbols, 
        trading_types=["spot", "um"], 
        market_data_types=market_data_types,
        intervals=intervals,
        data_base_format="parquet",
        output_format="parquet",
        start_date="2020-01-01",
    )