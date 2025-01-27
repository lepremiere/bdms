import os
import polars as pl
import pyarrow.parquet as pq
from datetime import datetime
from binance.client import Client
from bdms.utils import get_last_trade_id


def update_aggTrades(
        api_key: str, 
        api_secret: str, 
        path: str,
        write_invterval: int=1000,
        client_kwargs: dict={},
    ) -> None:    
    """
    Update the aggregate trades data for a trading pair.
    
    Parameters:
    ----------
    api_key: str
        Binance API key.
    api_secret: str
        Binance API secret.
    path: str
        Path where the file is saved.
    write_interval: int
        Write to the file after this number of trades are fetched.
    client_kwargs: dict
        Additional keyword arguments to pass to the Binance client.   
    """
    # Check if the file exists
    if os.path.exists(path):
        last_id = get_last_trade_id(path)
        print(f"Last ID: {last_id}")
    else:
        raise ValueError(f"File does not exist: {path}")
    
    # Get the symbol and file type from the file name
    symbol, file_type = os.path.basename(path).split(".")
    print(f"Symbol: {symbol}, File type: {file_type}")
    
    # Initialize the Binance client
    client = Client(api_key, api_secret, **client_kwargs)
        

    num_fetched = 0
    try:
        # Fetch aggregate trades from the Binance API
        aggregate_trades = client.aggregate_trade_iter(
            symbol=symbol,
            last_id=last_id  # Fetch from the last ID
        )
        
        trades = []
        for trade in aggregate_trades:
            trades.append(
                {
                    "agg_id": trade["a"],
                    "price": float(trade["p"]),
                    "quantity": float(trade["q"]),
                    "first_id": trade["f"],
                    "last_id": trade["l"],
                    "timestamp": trade["T"],
                    "is_buyer_maker": bool(trade["m"]),
                    "is_best_match": bool(trade["M"]),
                }
            )
            
            # Write to the CSV file if the list reaches 10000
            if len(trades) == write_invterval:
                df = pl.DataFrame(trades)
                
                # Append to file
                with open(path, mode="ab") as f:
                    if file_type == "csv":
                        df.write_csv(f, include_header=False)
                    else:
                        arrow_table = df.to_arrow()
                        writer = pq.ParquetWriter(
                            f, arrow_table.schema, compression="zstd"
                        )
                        with writer:
                            writer.write_table(arrow_table)
                                
                # Get weights used in the last request
                used_weights = client.response.headers["x-mbx-used-weight-1m"]
                
                # Print a message
                last_id, last_time = trade["l"], trade["T"]
                last_time = datetime.fromtimestamp(last_time / 1000)
                num_fetched += write_invterval
                msg = (
                    f"Trades fetched: {num_fetched}, "
                    f"Used weights 1m: {used_weights}, "
                    f"Last ID: {last_id}, Last Time: {last_time}"
                )
                print(msg, end="\r", flush=True)
                
                trades = []
            
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if trades:
            df = pl.DataFrame(trades)

            # Append to file
            with open(path, mode="a", encoding="utf-8") as f:
                if file_type == "csv":
                        df.write_csv(f, include_header=False)
                else:
                    arrow_table = df.to_arrow()
                    writer = pq.ParquetWriter(
                        f, arrow_table.schema, compression="zstd"
                    )
                    with writer:
                        writer.write_table(arrow_table)
            num_fetched += len(trades)

        print(f"\nAppended {num_fetched} new trades to {path}.")

if __name__ == "__main__":
    pass
