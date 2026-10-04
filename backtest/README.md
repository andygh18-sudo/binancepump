# V15.8 Replay / Backtest Engine

`backtest/v158_replay.py` replays the current V15.8 microstructure design without importing or modifying the live scanner.

## What it tests

Current Fastest-Pump hard gates:
- at least 3 trades in 10s
- buy pressure >= 0.57
- trade acceleration >= 1.25
- CVD >= 0.08 OR buy-pressure slope >= 0.025
- 1m price >= 0.05%
- 5m price >= 0.30%
- 10s price >= -0.75%
- 60s price >= -1.00%
- exhaustion < 65

Replayable V15.8 advisory layers:
- adaptive trade/volume anomaly
- executable depth/sweep cost
- absorption/replenishment persistence

Profiles: `v157`, `adaptive`, `sweep`, `replenishment`, `full`. Profiles never bypass or weaken production gates.

## Event input

Use JSONL or CSV with normalized fields. Trade example:
`{"ts":1791107000000,"event":"trade","symbol":"GLMRUSDT","price":0.12,"qty":5000,"is_buyer_maker":false,"exhaustion_score":20}`

Depth snapshot example:
`{"ts":1791107000100,"event":"depth","symbol":"GLMRUSDT","bids":[[0.1199,10000],[0.1198,15000]],"asks":[[0.1201,5000],[0.1202,7000]]}`

`ts` may be milliseconds, microseconds, or seconds. Depth must be genuine historical data; the engine does not synthesize an order book from OHLC.

Run:
`python backtest/v158_replay.py --mode events --input data/replay/glmr/ --profile full --output data/v158_backtest_results.json`

If historical exhaustion is unavailable, explicitly use `--allow-missing-exhaustion`. That is a less strict reconstruction and must not be treated as an exact production replay.

## Recorded-snapshot mode

The live learning stream can be evaluated without raw order-book history:
`python backtest/v158_replay.py --mode snapshot --input data/v157_observations.jsonl --output data/v158_snapshot_backtest.json`

This evaluates recorded Fastest-Pump signals, not reconstructed signals.

## Evaluation

For every signal the engine records signal timestamp/price, MFE/MAE at 30s, 1m, 3m, 5m, 10m and 30m, plus time to +2%, +5% and +10%. Compare `v157 -> adaptive -> sweep -> replenishment -> full`.

## Look-ahead protection

Signals are generated sequentially from information available at the replay timestamp. Outcome windows are calculated only after the signal. Adaptive baselines use only prior observations.

## Data requirement

Binance publishes historical Spot aggTrades through Binance Public Data. Full V15.8 microstructure replay also requires genuine historical L2/depth events; missing depth must not be fabricated.
