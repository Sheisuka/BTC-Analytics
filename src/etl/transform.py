import pandas as pd
import pathlib


raw   = pathlib.Path("data/raw")
stage = pathlib.Path("data/stage")

btc_csv  = "btc_ohlc13_25.csv"
spx_csv  = "spx_ohlc13_25.csv"
fng_csv  = "fng18_25.csv"
effr_csv = "effr54_25.csv"

btc  = pd.read_csv(raw / btc_csv, dtype={"TIMESTAMP": "int"}).drop_duplicates()
spx  = pd.read_csv(raw / spx_csv, parse_dates=["Date"])
fng  = pd.read_csv(raw / fng_csv, parse_dates=["timestamp"], dayfirst=True)
effr = pd.read_csv(raw / effr_csv, parse_dates=["observation_date"])

btc["TIMESTAMP"] = pd.to_datetime(btc["TIMESTAMP"], unit="s")

mapper = {
    "date": "date", "Date": "date", "observation_date": "date", "timestamp": "date", "TIMESTAMP": "date",
    "open": "open", "Open": "open", "OPEN": "open",
    "high": "high", "High": "high", "HIGH": "high",
    "low": "low",  "Low": "low", "LOW": "low",
    "close": "close", "Close": "close", "CLOSE": "close",
    "volume": "volume", "Volume": "volume", "VOLUME": "volume",
    "QUOTE_VOLUME": "volume_usd"
}

btc  = btc.rename(columns=mapper)
spx  = spx.rename(columns=mapper)
fng  = fng.rename(columns=mapper)
effr = effr.rename(columns=mapper)

btc  = btc.set_index("date").sort_index().asfreq(freq="D")
spx  = spx.set_index("date").sort_index().asfreq(freq="D", method="ffill")
fng  = fng.set_index("date").sort_index().asfreq(freq="D")
effr = effr.set_index("date").sort_index().asfreq(freq="D")

btc_stg  = btc[["open", "close", "high", "low", "volume", "volume_usd"]]
spx_stg  = spx[["open", "close", "high", "low"]]
fng_stg  = fng[["value", "classification"]]
effr_stg = effr[["DFF"]]

assert btc_stg.index.is_monotonic_increasing and btc_stg.index.is_unique
assert spx_stg.index.is_monotonic_increasing and spx_stg.index.is_unique
assert fng_stg.index.is_monotonic_increasing and fng_stg.index.is_unique
assert effr_stg.index.is_monotonic_increasing and effr_stg.index.is_unique

btc_stg.to_csv(stage / "btc.csv", index_label="date")
spx_stg.to_csv(stage / "spx.csv", index_label="date")
fng_stg.to_csv(stage / "fng.csv", index_label="date")
effr_stg.to_csv(stage / "effr.csv", index_label="date")

print(f"""
    BTC:  from {btc_stg.index[0]} | to {btc_stg.index[-1]}
    SPX:  from {spx_stg.index[0]} | to {spx_stg.index[-1]}
    FNG:  from {fng_stg.index[0]} | to {fng_stg.index[-1]}
    EFFR: from {effr_stg.index[0]} | to {effr_stg.index[-1]}
""")