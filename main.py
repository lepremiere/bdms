import bdms
import warnings 

warnings.filterwarnings("ignore", category=UserWarning)

if __name__ == "__main__":
    symbols = [
        "BTCUSDT", "BNBUSDT", "ADAUSDT", 
        "ETHUSDT", "XRPUSDT", 
    ]
    download_market_data_types = [
        "aggTrades", "klines", "fundingRate", 
        "indexPriceKlines", "markPriceKlines", 
        "premiumIndexKlines", "bookDepth", "metrics",
    ]
    intevals = ["1m", "5m", "1h", "1d"]
    
    bdms.populate_database(
        root_dir="D:/Binance",
        symbols=symbols, 
        trading_types=["um"], 
        market_data_types=download_market_data_types,
        intervals=intevals,
        storage_format="parquet",
    )
    
    agg_market_data_types = [
        "klines", "fundingRate", 
        "indexPriceKlines", "markPriceKlines", 
        "premiumIndexKlines", "bookDepth", "metrics",
    ]
    bdms.merge_database(
        root_dir="D:/Binance",
        symbols=symbols, 
        trading_types=["spot", "um"], 
        market_data_types=agg_market_data_types,
        intervals=intevals,
        data_base_format="parquet",
        output_format="parquet",
    )