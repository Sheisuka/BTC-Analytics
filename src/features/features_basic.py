import numpy as np
import pandas as pd
import pathlib


stagePath    = pathlib.Path("data/stage")
featuresPath = pathlib.Path("data/features/components")

btc  = pd.read_csv(stagePath / "btc.csv",  parse_dates=["date"]).set_index("date")
spx  = pd.read_csv(stagePath / "spx.csv",  parse_dates=["date"]).set_index("date")
fng  = pd.read_csv(stagePath / "fng.csv",  parse_dates=["date"]).set_index("date")
effr = pd.read_csv(stagePath / "effr.csv", parse_dates=["date"]).set_index("date")

spx  = spx.reindex(btc.index)
fng  = fng.reindex(btc.index)
effr = effr.reindex(btc.index).ffill()

assert btc.index.is_monotonic_increasing and btc.index.is_unique

features = pd.DataFrame(index=btc.index)

# ── BTC: базовые ценовые признаки ──

features["btc_close"] = btc["close"]

features["btc_ret_1d"]  = btc["close"].pct_change(1)
features["btc_ret_7d"]  = btc["close"].pct_change(7)
features["btc_ret_30d"] = btc["close"].pct_change(30)

for n in (7, 20, 60):
    features[f"btc_ma{n}"] = btc["close"].rolling(n, min_periods=n).mean()

features["btc_vol_20"]       = features["btc_ret_1d"].rolling(20, min_periods=20).std()
features["btc_dist_to_ma20"] = btc["close"] / features["btc_ma20"] - 1

features["btc_rel_vol_20"] = btc["volume"] / btc["volume"].rolling(20, min_periods=20).mean()

# ── BTC: RSI (14 дней, метод Уайлдера) ──
# RSI = 100 - 100/(1 + avg_gain/avg_loss)
delta  = btc["close"].diff()
gain   = delta.clip(lower=0)
loss   = (-delta).clip(lower=0)
avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
features["btc_rsi_14"] = 100 - 100 / (1 + avg_gain / avg_loss)

# ── BTC: MACD histogram ──
# MACD = EMA_12 - EMA_26, signal = EMA_9(MACD), histogram = MACD - signal
ema12 = btc["close"].ewm(span=12, min_periods=12, adjust=False).mean()
ema26 = btc["close"].ewm(span=26, min_periods=26, adjust=False).mean()
macd_line = ema12 - ema26
macd_signal = macd_line.ewm(span=9, min_periods=9, adjust=False).mean()
features["btc_macd_hist"] = macd_line - macd_signal

# ── BTC: ширина полос Боллинджера (нормализованная) ──
# bw = (upper - lower) / ma20 = 2*2*std20 / ma20
std20 = btc["close"].rolling(20, min_periods=20).std()
features["btc_bb_width"] = (4 * std20) / features["btc_ma20"]

# ── BTC: скользящая экспонента Хёрста (R/S, окно 126 дней) ──
def _rs_hurst(x):
    """R/S-оценка экспоненты Хёрста для массива x."""
    N = len(x)
    if N < 20:
        return np.nan
    min_n, max_n = 10, N // 2
    n_values = np.unique(np.logspace(np.log10(min_n), np.log10(max_n), 15).astype(int))
    log_n, log_rs = [], []
    for n in n_values:
        n_blocks = N // n
        if n_blocks < 1:
            continue
        rs_list = []
        for b in range(n_blocks):
            block = x[b*n:(b+1)*n]
            cumdev = np.cumsum(block - block.mean())
            R = cumdev.max() - cumdev.min()
            S = block.std(ddof=1)
            if S > 0:
                rs_list.append(R / S)
        if rs_list:
            log_n.append(np.log(n))
            log_rs.append(np.log(np.mean(rs_list)))
    if len(log_n) < 2:
        return np.nan
    H = np.polyfit(log_n, log_rs, 1)[0]
    return H

btc_log_ret = np.log(btc["close"] / btc["close"].shift(1))
features["btc_hurst_126"] = btc_log_ret.rolling(126, min_periods=126).apply(
    lambda w: _rs_hurst(w.values), raw=False
)

# ── S&P 500 ──

spx_td = spx[spx["is_trading_day"] == True][["close"]].copy()

spx_td                  = spx_td.rename(columns={"close": "spx_close"})
spx_td["spx_ret_1d"]    = spx_td["spx_close"].pct_change(1)
spx_td["spx_ret_5d"]    = spx_td["spx_close"].pct_change(5)
spx_td["spx_ret_21d"]   = spx_td["spx_close"].pct_change(21)
spx_td["spx_ma20"]      = spx_td["spx_close"].rolling(20, min_periods=20).mean()
spx_td["spx_dist_ma20"] = spx_td["spx_close"] / spx_td["spx_ma20"] - 1

spx_td = spx_td.reindex(btc.index).ffill()

features = features.join(spx_td)

# ── Скользящая корреляция BTC-SPX (60 дней) ──
features["corr_btc_spx_60"] = features["btc_ret_1d"].rolling(60, min_periods=60).corr(
    spx_td["spx_ret_1d"]
)

# ── Fear & Greed Index ──
features["fng_value"]      = fng["value"]
features["fng_ma7"]        = fng["value"].rolling(7, min_periods=7).mean()
features["fng_change_7d"]  = fng["value"].diff(7)

# ── Federal Funds Rate ──
features["effr_rate"]       = effr["DFF"]
features["effr_change_30d"] = effr["DFF"].diff(30)

features.to_csv(featuresPath / "basic.csv")