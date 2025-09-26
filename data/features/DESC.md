# Features

## Торговые данные

- btc_close — цена закрытия BTC (USD) на дату t.

- btc_ret_1d — дневная доходность BTC: close_t / close_{t-1} - 1.

- btc_ret_7d — 7-дневная доходность BTC: close_t / close_{t-7} - 1.

- btc_ret_30d — 30-дневная доходность BTC: close_t / close_{t-30} - 1.

- btc_ma7 — скользящее среднее цены BTC за 7 дней.

- btc_ma20 — скользящее среднее цены BTC за 20 дней.

- btc_ma60 — скользящее среднее цены BTC за 60 дней.

- btc_vol_20 — волатильность BTC: std(btc_ret_1d_{t-19…t}), окно 20.

- btc_dist_to_ma20 — отклонение цены BTC от MA20: close_t / MA20_t - 1.

- btc_rel_vol_20 — относительный объём BTC: volume_t / volume_ma20.

- close — цена закрытия S&P 500 по торговым сессиям; на не-торговые дни протягивается.

- spx_ret_1d — дневная доходность S&P 500: spx_close_t / spx_close_{t-1} - 1.

- spx_ret_5d — ~недельная доходность S&P 500: spx_close_t / spx_close_{t-5} - 1.

- spx_ret_21d — ~месячная доходность S&P 500: spx_close_t / spx_close_{t-21} - 1.

- spx_ma20 — скользящее среднее цены S&P 500 за 20 сессий.

- spx_dist_ma20 — отклонение S&P 500 от MA20: spx_close_t / spx_MA20_t - 1.