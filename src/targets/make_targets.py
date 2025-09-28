import pandas as pd
import pathlib


btcP  = pathlib.Path("data/stage/btc.csv")
outP  = pathlib.Path("data/targets/targets_v1.csv")

btc     = pd.read_csv(btcP, parse_dates=["date"]).set_index("date")
targets = pd.DataFrame(index=btc.index)
close   = btc["close"]

days = [1, 7, 30]

for d in days:
    targets[f"ret_{d}d_ahead"] = (close.shift(-d) / close - 1) * 100.0
    targets[f"up_{d}d_ahead"] = (targets[f"ret_{d}d_ahead"] > 0).astype(int)

targets.to_csv(outP, index_label="date")