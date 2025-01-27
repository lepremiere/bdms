from datetime import datetime


BASE_URL = 'https://data.binance.vision/'

# Kline intervals
INTERVALS = [
    "1s", "1m", "3m", "5m", "15m", "30m", 
    "1h", "2h", "4h", "6h", "8h", "12h", 
    "1d", "3d", "1w", "1mo"
]

# API endpoints
TRADING_TYPES = ["spot", "um", "cm"]
MARKET_DATA_TYPES = ["trades", "aggTrades", "klines"]
FUTURES_DATA_TYPES = [
    "bookTicker", "fundingRate", "indexPriceKlines",
    "markPriceKlines", "premiumIndexKlines"
]
TIME_PERIODS = ["daily", "monthly"]

# Default start and end dates for (monthly, daily)
START_DATE_MAP = {
    "spot": "2017-08-15",
    "um": "2020-01-01",
    "cm": "2020-08-01"
}
END_DATE = str(datetime.date(datetime.now()))

# Column names
AGGTRADES_COLUMNS = [
    "agg_id", "price", "quantity", "first_trade_id", "last_trade_id",
    "timestamp", "is_buyer_maker", "is_best_match"
]
TRADES_COLUMNS = [
    "id", "price", "quantity", "quote_quantity", "timestamp", 
    "is_buyer_maker", "is_best_match"
]
KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume",
    "ignore"
]
BOOKTICKER_COLUMNS = [
    "update_id", "best_bid_price", "best_bid_qty", "best_ask_price",
    "best_ask_qty", "transaction_time", "event_time"
]
FUNDINGRATE_COLUMNS = [
    "calc_time", "funding_interval_hours", "last_funding_rate"
]
SPOT_COLUMNS_MAP = {
    "aggTrades": AGGTRADES_COLUMNS,
    "trades": TRADES_COLUMNS,
    "klines": KLINE_COLUMNS,
}
FUTURES_COLUMNS_MAP = {
    "aggTrades": AGGTRADES_COLUMNS[:-1],
    "trades": TRADES_COLUMNS[:-1],
    "klines": KLINE_COLUMNS,
    "bookTicker": BOOKTICKER_COLUMNS,
    "fundingRate": FUNDINGRATE_COLUMNS,
    "indexPriceKlines": KLINE_COLUMNS,
    "markPriceKlines": KLINE_COLUMNS,
    "premiumIndexKlines": KLINE_COLUMNS
}