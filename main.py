import bdms

if __name__ == "__main__":
    symbols = [
        "BTCUSDT", "BNBUSDT", "ADAUSDT", 
        "ETHUSDT", "XRPUSDT", "SOLUSDT"
    ]
    download_market_data_types = [
        "aggTrades", "klines", "fundingRate", 
        "indexPriceKlines", "markPriceKlines", 
        "premiumIndexKlines", "bookDepth", "metrics",
    ]
    bdms.populate_database(
        root_dir="D:/Binance1",
        symbols=symbols, 
        trading_types=["spot"], 
        market_data_types=download_market_data_types,
        intervals=["1m"],
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
        trading_types=["spot"], 
        market_data_types=agg_market_data_types,
        intervals=["1m"],
        data_base_format="parquet",
        output_format="parquet",
    )