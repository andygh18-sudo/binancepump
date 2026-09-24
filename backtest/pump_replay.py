#!/usr/bin/env python3
"""
Pump Replay / Backtest Engine v1

Replays Binance 1-minute Spot klines and evaluates the scanner's
Accumulation / Pre-Pump concept before subsequent price expansions.

This is deliberately a historical proxy: 1m klines contain price, volume,
trade count and taker-buy volume, but not the live scanner's 5-second
order-book state. Results therefore measure whether the signal logic would
have appeared early, not whether the exact live score would have been identical.
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

BASE = os.getenv("BINANCE_REST_BASE", "https://data-api.binance.vision")
OUT = "data/backtest_results.json"

COLS = ["open_time","open","high","low","close","volume","close_time",
        "quote_volume","trades","taker_buy_base","taker_buy_quote","ignore"]

def ms(dt):
    return int(pd.Timestamp(dt, tz="UTC").timestamp() * 1000)

def fetch_klines(symbol, start, end):
    rows, cursor = [], ms(start)
    stop = ms(end)
    while cursor < stop:
        p = {"symbol": symbol, "interval": "1m", "startTime": cursor,
             "endTime": stop, "limit": 1000}
        r = requests.get(BASE + "/api/v3/klines", params=p, timeout=20)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        nxt = int(batch[-1][0]) + 60000
        if nxt <= cursor:
            break
        cursor = nxt
        time.sleep(0.08)
        if len(batch) < 1000:
            break
    if not rows:
        return pd.DataFrame(columns=COLS)
    df = pd.DataFrame(rows, columns=COLS).drop_duplicates("open_time")
    for c in COLS:
        if c != "ignore":
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    return df.sort_values("open_time").reset_index(drop=True)

def prepare(df):
    x = df.copy()
    x["ret_1m"] = x["close"].pct_change() * 100
    x["ret_5m"] = x["close"].pct_change(5) * 100
    x["ret_15m"] = x["close"].pct_change(15) * 100
    x["vol_1m"] = x["quote_volume"]
    x["vol_avg_5m"] = x["vol_1m"].rolling(5, min_periods=3).mean()
    x["vol_avg_15m"] = x["vol_1m"].rolling(15, min_periods=5).mean()
    x["vol_avg_60m"] = x["vol_1m"].rolling(60, min_periods=20).mean()
    x["vol_ratio"] = x["vol_1m"] / x["vol_avg_5m"].replace(0, pd.NA)
    x["vol_ratio_15"] = x["vol_1m"] / x["vol_avg_15m"].replace(0, pd.NA)
    x["trade_avg_5m"] = x["trades"].rolling(5, min_periods=3).mean()
    x["trade_avg_15m"] = x["trades"].rolling(15, min_periods=5).mean()
    x["trade_accel"] = x["trades"] / x["trade_avg_15m"].replace(0, pd.NA)
    x["buy_pressure"] = x["taker_buy_quote"] / x["quote_volume"].replace(0, pd.NA)
    x["range_pct"] = (x["high"] / x["low"] - 1) * 100
    return x

def signal(row):
    bp = float(row.buy_pressure) if pd.notna(row.buy_pressure) else 0
    vr = float(row.vol_ratio) if pd.notna(row.vol_ratio) else 0
    ta = float(row.trade_accel) if pd.notna(row.trade_accel) else 0
    p1 = float(row.ret_1m) if pd.notna(row.ret_1m) else 0
    p5 = float(row.ret_5m) if pd.notna(row.ret_5m) else 0

    # Historical proxy for the live accumulation detector.
    buy = max(0, min((bp - .50) / .25, 1)) * 30
    trade = max(0, min((ta - 1) / 2, 1)) * 25
    vol = max(0, min(vr / 2, 1)) * 20
    price = max(0, min((p1 + .25) / 2.0, 1)) * 10
    activity = 5 if row.trades >= 10 else 0
    extension = max(0, min((p1 - 2.0) * 8, 20))
    accum = max(0, min(round(buy + trade + vol + price + activity - extension), 100))

    quality = (accum >= 50 and row.trades >= 10 and bp >= .55
               and ta >= 1.25 and p1 < 4)
    astage = "ACCUMULATION ALERT" if accum >= 70 and quality else (
        "ACCUMULATION WATCH" if accum >= 50 and quality else "MONITOR")

    # Historical proxy for the normal pump score.
    raw = min(max(p1, 0) * 10, 20)
    raw += min(max(vr - 1, 0) * 14, 28)
    raw += min(max(ta - 1, 0) * 12, 18)
    raw += max(min((bp - .5) * 50, 12), -12)
    pscore = max(0, min(100, round(raw)))
    stage = ("CONFIRMED PUMP" if pscore >= 82 else
             "BREAKOUT" if pscore >= 70 else
             "EARLY MOMENTUM" if pscore >= 55 else
             "PRE-PUMP" if pscore >= 45 else
             "BUILDING" if pscore >= 30 else "QUIET")
    return {
        "accumulation_score": accum,
        "accumulation_stage": astage,
        "accumulation_quality": bool(quality),
        "pump_score": pscore,
        "pump_stage": stage,
        "buy_pressure": bp,
        "volume_ratio": vr,
        "trade_accel": ta,
        "ret_1m": p1,
        "ret_5m": p5,
    }

def evaluate(df, symbol):
    x = prepare(df)
    if len(x) < 70:
        return {"symbol": symbol, "status": "insufficient_data"}

    signals = []
    for i in range(60, len(x)):
        s = signal(x.iloc[i])
        if s["accumulation_quality"] and s["accumulation_score"] >= 50:
            future = x.iloc[i+1:]
            entry = float(x.iloc[i].close)
            gains = {}
            for minutes in (15, 30, 60, 240):
                f = future.iloc[:minutes]
                gains[f"{minutes}m"] = round((float(f.high.max()) / entry - 1) * 100, 2) if len(f) else None
            signals.append({
                "time": x.iloc[i].time.isoformat(),
                "price": entry,
                **s,
                "future_max_gain": gains,
            })

    # Deduplicate into alert episodes: only the first alert in a 30-minute
    # episode counts as a new detection.
    episodes = []
    for s in signals:
        t = pd.Timestamp(s["time"])
        if not episodes or (t - pd.Timestamp(episodes[-1]["time"])).total_seconds() >= 1800:
            episodes.append(s)

    # Identify actual pump episodes independently of the detector.
    pumps = []
    for i in range(60, len(x) - 240):
        entry = float(x.iloc[i].close)
        g60 = (float(x.iloc[i+1:i+61].high.max()) / entry - 1) * 100
        g240 = (float(x.iloc[i+1:i+241].high.max()) / entry - 1) * 100
        if max(g60, g240) >= 10:
            if not pumps or (x.iloc[i].time - pd.Timestamp(pumps[-1]["time"])).total_seconds() >= 1800:
                pumps.append({"time": x.iloc[i].time.isoformat(), "price": entry,
                              "gain_60m": round(g60,2), "gain_240m": round(g240,2)})

    return {
        "symbol": symbol,
        "status": "ok",
        "bars": len(x),
        "start": x.time.iloc[0].isoformat(),
        "end": x.time.iloc[-1].isoformat(),
        "accumulation_episodes": episodes[:100],
        "pump_episodes": pumps[:100],
        "detections": len(episodes),
        "pump_episodes_count": len(pumps),
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="NOMUSDT,NILUSDT")
    ap.add_argument("--start", default="2026-09-20T00:00:00Z")
    ap.add_argument("--end", default="2026-09-25T00:00:00Z")
    ap.add_argument("--output", default=OUT)
    args = ap.parse_args()

    results = []
    for symbol in [s.strip().upper() for s in args.symbols.split(",") if s.strip()]:
        print(f"Replaying {symbol} {args.start} -> {args.end}")
        try:
            df = fetch_klines(symbol, args.start, args.end)
            results.append(evaluate(df, symbol))
        except Exception as e:
            results.append({"symbol": symbol, "status": "error", "error": str(e)})

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "Pump Replay / Backtest v1",
        "data_source": BASE,
        "method": "Binance 1m Spot klines; accumulation is a historical proxy",
        "results": results,
    }
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(payload, f, indent=2)
    print(json.dumps(payload, indent=2))

if __name__ == "__main__":
    main()
