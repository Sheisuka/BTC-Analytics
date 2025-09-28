import pandas as pd
import pathlib


days = [1, 7, 30]

defBtcP  = pathlib.Path("data/stage/btc.csv")
defOutP  = pathlib.Path("data/targets/targets_v1.csv")

def make_targets(btcP=defBtcP, days=days):
    btc     = pd.read_csv(btcP, parse_dates=["date"]).set_index("date")
    targets = pd.DataFrame(index=btc.index)
    close   = btc["close"]

    for d in days:
        targets[f"ret_{d}d_ahead"] = (close.shift(-d) / close - 1) * 100.0
        targets[f"up_{d}d_ahead"]  = (targets[f"ret_{d}d_ahead"] > 0).astype(int)

    return targets

if __name__ == "__main__":
    targets = make_targets()
    targets.to_csv(defOutP, index_label="date")