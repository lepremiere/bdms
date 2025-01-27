from bdms.merge import merge_data

if __name__ == "__main__":
    symbols = [
        "BTCUSDT", "BNBUSDT", "ADAUSDT", 
        "ETHUSDT", "XRPUSDT", "SOLUSDT"
    ]
    market_data_types = [
        "klines", "fundingRate", 
        "indexPriceKlines", "markPriceKlines", 
        "premiumIndexKlines", "aggTrades"
    ]
    merge_data(
        root_dir="D:/Binance",
        symbols=["BTCUSDT"], 
        trading_types=["spot"], 
        market_data_types=["aggTrades"],
        data_base_format="parquet",
        output_format="parquet",
    )

