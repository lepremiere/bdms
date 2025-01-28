from datetime import datetime


BASE_URL = 'https://data.binance.vision/'

# Kline intervals
INTERVALS_MAP = {
    "daily": [
        "1m", "3m", "5m", "15m", "30m", 
        "1h", "2h", "4h", "6h", "8h", "12h", 
        "1d"
    ],
    "monthly": [
        "1s", "1m", "3m", "5m", "15m", "30m", 
        "1h", "2h", "4h", "6h", "8h", "12h", 
        "1d", "3d", "1w", "1mo"
    ]
}

# API endpoints
TIME_PERIODS = ["daily", "monthly"]
TRADING_TYPES = ["spot", "um", "cm"]
SPOT_TYPES = ["trades", "aggTrades", "klines"]
DAILY_FUTURES_TYPES = [
    "aggTrades", "bookDepth", "bookTicker", "indexPriceKlines",
    "klines", "liquidationSnapshot", "markPriceKlines", "metrics",
    "premiumIndexKlines", "trades"
]
MONTHLY_FUTURES_TYPES = [
    "aggTrades", "bookTicker", "fundingRate", "indexPriceKlines",
    "klines", "markPriceKlines", "premiumIndexKlines", "trades"
]
FUTURE_TYPES = DAILY_FUTURES_TYPES + MONTHLY_FUTURES_TYPES
MARKET_DATA_TYPES = set(
    SPOT_TYPES + DAILY_FUTURES_TYPES + MONTHLY_FUTURES_TYPES
)
TYPES_MAP = {
    "spot": {
        "daily": SPOT_TYPES,
        "monthly": SPOT_TYPES
    },
    "um": {
        "daily": DAILY_FUTURES_TYPES,
        "monthly": MONTHLY_FUTURES_TYPES
    },
    "cm": {
        "daily": DAILY_FUTURES_TYPES,
        "monthly": MONTHLY_FUTURES_TYPES
    }
}


# Default start and end dates for (monthly, daily)
START_DATE_MAP = {
    "spot": {
        "klines": "2017-08-15",
        "trades": "2017-08-15",
        "aggTrades": "2017-08-15"
    },
    "um": {
        "klines": "2020-01-01",
        "trades": "2020-01-01",
        "aggTrades": "2020-01-01",
        "bookDepth": "2023-01-01",
        "fundingRate": "2020-01-01",
        "metrics": "2021-12-01",
        "indexPriceKlines": "2020-01-01",
        "markPriceKlines": "2020-01-01",
        "premiumIndexKlines": "2020-01-01",
    },
    "cm": {
        "klines": "2020-08-01",
        "trades": "2020-08-01",
        "aggTrades": "2020-08-01",
        "bookDepth": "2023-01-01",
        "fundingRate": "2020-01-01",
        "metrics": "2021-12-01",
        "indexPriceKlines": "2020-01-01",
        "markPriceKlines": "2020-01-01",
        "premiumIndexKlines": "2020-01-01",
    }
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
BOOKDEPTH_COLUMNS = [
    "timestamp", "percentage", "depth", "notional"
]
BOOKTICKER_COLUMNS = [
    "update_id", "best_bid_price", "best_bid_qty", "best_ask_price",
    "best_ask_qty", "transaction_time", "event_time"
]
FUNDINGRATE_COLUMNS = [
    "calc_time", "funding_interval_hours", "last_funding_rate"
]
LIQUIDATION_COLUMNS = [
    "time", "side", "order_type", "time_in_force", "original_quantity",
    "price", "average_price", "order_status", "last_fill_quantity", 
    "accumulated_fill_quantity"
]
METRICS_COLUMNS = [
    "create_time", "symbol", "sum_open_interest", "sum_open_interest_value",
    "count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio",
    "count_long_short_ratio", "sum_taker_long_short_vol_ratio"
]

SPOT_COLUMNS_MAP = {
    "aggTrades": AGGTRADES_COLUMNS,
    "trades": TRADES_COLUMNS,
    "klines": KLINE_COLUMNS,
}
FUTURES_COLUMNS_MAP = {
    "aggTrades": AGGTRADES_COLUMNS[:-1],
    "bookDepth": BOOKDEPTH_COLUMNS,
    "bookTicker": BOOKTICKER_COLUMNS,
    "fundingRate": FUNDINGRATE_COLUMNS,
    "indexPriceKlines": KLINE_COLUMNS,
    "klines": KLINE_COLUMNS,
    "liquidationSnapshot": LIQUIDATION_COLUMNS,
    "markPriceKlines": KLINE_COLUMNS,
    "metrics": METRICS_COLUMNS,
    "premiumIndexKlines": KLINE_COLUMNS,
    "trades": TRADES_COLUMNS
}