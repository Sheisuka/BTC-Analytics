import pandas as pd
import pathlib


stagePath    = pathlib.Path("data/stage")
featuresPath = pathlib.Path("data/features")

btc_csv  = "btc.csv"
spx_csv  = "spx.csv"

btc = pd.read_csv(stagePath / "btc.csv",  parse_dates=["date"]).set_index("date")
spx = pd.read_csv(stagePath / "spx.csv",  parse_dates=["date"]).set_index("date")

spx = spx.reindex(btc.index)

assert btc.index.is_monotonic_increasing and btc.index.is_unique
assert spx.index.is_monotonic_increasing and spx.index.is_unique

features = pd.DataFrame(index=btc.index)


features["btc_close"] = btc["close"]

features["btc_ret_1d"]  = btc["close"].pct_change(1)
features["btc_ret_7d"]  = btc["close"].pct_change(7)
features["btc_ret_30d"] = btc["close"].pct_change(30)

for n in (7, 20, 60):
    features[f"btc_ma{n}"] = btc["close"].rolling(n, min_periods=n).mean()

features["btc_vol_20"]       = features["btc_ret_1d"].rolling(20, min_periods=20).std()
features["btc_dist_to_ma20"] = btc["close"] / features["btc_ma20"] - 1

features["btc_rel_vol_20"] = btc["volume"] / btc["volume"].rolling(20, min_periods=20).mean()

spx_td = spx[spx["is_trading_day"] == True][["close"]].copy() # признаки только по сесиям

spx_td                  = spx_td.rename({"close": "spx_close"})
spx_td["spx_ret_1d"]    = spx_td["spx_close"].pct_change(1)
spx_td["spx_ret_5d"]    = spx_td["spx_close"].pct_change(5)
spx_td["spx_ret_21d"]   = spx_td["spx_close"].pct_change(21)
spx_td["spx_ma20"]      = spx_td["spx_close"].rolling(20, min_periods=20).mean()
spx_td["spx_dist_ma20"] = spx_td["spx_close"] / spx_td["spx_ma20"] - 1

spx_td = spx_td.reindex(btc.index).ffill()

features = features.join(spx_td)
features.to_csv(featuresPath / "basic.csv")