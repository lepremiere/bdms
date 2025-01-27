import bdms

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
    bdms.populate_database(
        root_dir="D:/Binance",
        symbols=symbols, 
        trading_types=["spot", "um"], 
        market_data_types=market_data_types,
        intervals=["1m"],
        storage_format="parquet",
    )
    # bdms.merge_trading_data(
    #     symbols=["ADAUSDT"], 
    #     trading_type="spot", 
    #     market_data_type="aggTrades",
    #     time_period="monthly",
    #     root_dir="D:/Binance",
    #     output_dir="D:/Binance",
    #     start_date="2024-12-01",
    #     end_date="2024-12-01",
    #     data_base_format="parquet",
    #     output_format="parquet",
    # )
    # bdms.update_aggTrades(
    #     api_key=api_key,
    #     api_secret=api_secret,
    #     path="D:/Binance/ADAUSDT.parquet",
    # )

