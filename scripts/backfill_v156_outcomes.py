#!/usr/bin/env python3
"""V15.6 historical outcome backfill from Binance public Spot 1m klines."""

import json
import os
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path("data")
OUTCOMES = ROOT / "v156_outcomes.jsonl"
SUMMARY = ROOT / "v156_outcome_summary.json"
BASE_URL = os.getenv("BINANCE_REST_BASE", "https://data-api.binance.vision")
INTERVAL = "1m"
CHECKPOINTS = (60, 180, 300, 600, 1800, 3600)
TARGETS = (1, 2, 3, 5, 10)
REQUEST_LIMIT = 1000
REQUEST_SLEEP = float(os.getenv("V156_BACKFILL_SLEEP", "0.12"))
MAX_SYMBOLS_PER_RUN = int(os.getenv("V156_BACKFILL_MAX_SYMBOLS", "150"))
ONLY_UNRESOLVED = os.getenv("V156_BACKFILL_ONLY_UNRESOLVED", "1") == "1"


def load_events():
    events = {}
    if not OUTCOMES.exists():
        return events
    with OUTCOMES.open(encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            eid = r.get("event_id")
            if eid:
                events[eid] = r
    return events


def fetch_klines(symbol, start_ms, end_ms):
    rows = []
    cursor = int(start_ms)
    end_ms = int(end_ms)
    while cursor <= end_ms:
        params = urlencode({
            "symbol": symbol,
            "interval": INTERVAL,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": REQUEST_LIMIT,
        })
        url = f"{BASE_URL}/api/v3/klines?{params}"
        req = Request(url, headers={"User-Agent": "Binance-Pump-V15.6-Backfill/1.0"})
        batch = None
        for attempt in range(5):
            try:
                with urlopen(req, timeout=20) as resp:
                    batch = json.loads(resp.read().decode("utf-8"))
                break
            except Exception:
                if attempt == 4:
                    raise
                time.sleep(1.0 + attempt * 1.5)
        if not batch:
            break
        rows.extend(batch)
        last_open = int(batch[-1][0])
        next_cursor = last_open + 60_000
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(batch) < REQUEST_LIMIT:
            break
        time.sleep(REQUEST_SLEEP)
    return rows


def candle_records(rows):
    out = []
    for k in rows:
        try:
            out.append({
                "open_ts": int(k[0]) / 1000.0,
                "close_ts": int(k[6]) / 1000.0,
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "trades": int(k[8]),
                "taker_buy_base": float(k[9]),
            })
        except (IndexError, TypeError, ValueError):
            continue
    return out


def checkpoint_price(candles, target_ts):
    for c in candles:
        if c["close_ts"] >= target_ts:
            return c["close"], c["close_ts"]
    return None, None


def update_event(e, candles):
    try:
        signal_ts = float(e["signal_ts"])
        base = float(e["signal_price"])
    except (KeyError, TypeError, ValueError):
        return False
    if base <= 0:
        return False

    cps = dict(e.get("checkpoints") or {})
    first_hits = dict(e.get("first_hit_seconds") or {})
    mfe = float(e.get("mfe_pct", 0) or 0)
    mae = float(e.get("mae_pct", 0) or 0)
    future = [c for c in candles if c["close_ts"] > signal_ts]
    if not future:
        return False

    changed = False
    for c in future:
        elapsed = c["close_ts"] - signal_ts
        high_ret = (c["high"] / base - 1.0) * 100.0
        low_ret = (c["low"] / base - 1.0) * 100.0
        mfe = max(mfe, high_ret)
        mae = min(mae, low_ret)
        for target in TARGETS:
            key = str(target)
            if key not in first_hits and high_ret >= target:
                first_hits[key] = round(max(0.0, elapsed), 2)
                changed = True

    for cp in CHECKPOINTS:
        key = str(cp)
        if key in cps:
            continue
        target_ts = signal_ts + cp
        price, actual_ts = checkpoint_price(future, target_ts)
        if price is None:
            continue
        ret = (price / base - 1.0) * 100.0
        cps[key] = {
            "checkpoint_seconds": cp,
            "actual_ts": actual_ts,
            "lag_seconds": round(actual_ts - target_ts, 2),
            "source": "BINANCE_1M_KLINES",
            "price": price,
            "return_pct": round(ret, 4),
            "hit_1pct": ret >= 1,
            "hit_2pct": ret >= 2,
            "hit_3pct": ret >= 3,
            "hit_5pct": ret >= 5,
            "hit_10pct": ret >= 10,
        }
        changed = True

    e["mfe_pct"] = round(mfe, 4)
    e["mae_pct"] = round(mae, 4)
    e["first_hit_seconds"] = first_hits
    e["checkpoints"] = cps

    if "3600" in cps:
        e["resolved"] = True
        e["resolved_ts"] = cps["3600"]["actual_ts"]
        e["final_return_pct"] = cps["3600"]["return_pct"]
        e["confirmed_3pct_10m"] = bool(cps.get("600", {}).get("hit_3pct"))
        e["persistence_1m_10m"] = all(
            float(cps.get(str(cp), {}).get("return_pct", -999)) >= 0
            for cp in (60, 180, 300, 600)
        )
        e["resolution_source"] = "BINANCE_1M_BACKFILL"
        changed = True

    return changed


def write_events(events):
    tmp = OUTCOMES.with_suffix(".jsonl.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for e in sorted(events.values(), key=lambda x: (float(x.get("signal_ts", 0)), x["event_id"])):
            f.write(json.dumps(e, separators=(",", ":"), allow_nan=False) + "\n")
    tmp.replace(OUTCOMES)


def summary(events):
    vals = list(events.values())
    resolved = [e for e in vals if e.get("resolved")]

    def cp(sec):
        rows = [e.get("checkpoints", {}).get(str(sec)) for e in vals]
        rows = [r for r in rows if r]
        if not rows:
            return {"samples": 0}
        return {
            "samples": len(rows),
            "avg_return_pct": round(sum(float(r["return_pct"]) for r in rows) / len(rows), 4),
            "hit_1pct": sum(bool(r["hit_1pct"]) for r in rows),
            "hit_2pct": sum(bool(r["hit_2pct"]) for r in rows),
            "hit_3pct": sum(bool(r["hit_3pct"]) for r in rows),
            "hit_5pct": sum(bool(r["hit_5pct"]) for r in rows),
            "hit_10pct": sum(bool(r["hit_10pct"]) for r in rows),
        }

    return {
        "version": "V15.6",
        "signals": len(vals),
        "resolved": len(resolved),
        "open": len(vals) - len(resolved),
        "backfilled_resolved": sum(e.get("resolution_source") == "BINANCE_1M_BACKFILL" for e in resolved),
        "checkpoints": {
            "1m": cp(60), "3m": cp(180), "5m": cp(300),
            "10m": cp(600), "30m": cp(1800), "60m": cp(3600),
        },
        "resolved_mfe_avg_pct": round(sum(float(e.get("mfe_pct", 0)) for e in resolved) / len(resolved), 4) if resolved else None,
        "resolved_mae_avg_pct": round(sum(float(e.get("mae_pct", 0)) for e in resolved) / len(resolved), 4) if resolved else None,
    }


def main():
    events = load_events()
    if not events:
        print("No V15.6 outcome events found.")
        return

    candidates = []
    for e in events.values():
        if ONLY_UNRESOLVED and e.get("resolved"):
            continue
        try:
            age = time.time() - float(e["signal_ts"])
        except Exception:
            continue
        if age < 60:
            continue
        candidates.append(e)

    candidates.sort(key=lambda e: float(e.get("signal_ts", 0)))
    symbols = sorted({str(e.get("symbol", "")).upper() for e in candidates if e.get("symbol")})
    symbols = symbols[:MAX_SYMBOLS_PER_RUN]

    changed = 0
    for symbol in symbols:
        group = [e for e in candidates if str(e.get("symbol", "")).upper() == symbol]
        if not group:
            continue
        start = int(min(float(e["signal_ts"]) for e in group) * 1000) - 60_000
        end = int(max(float(e["signal_ts"]) for e in group) * 1000) + 3_600_000 + 120_000
        try:
            rows = candle_records(fetch_klines(symbol, start, min(end, int(time.time() * 1000))))
        except Exception as exc:
            print(f"BACKFILL_ERROR {symbol}: {exc}")
            continue
        for e in group:
            if update_event(e, rows):
                changed += 1

    write_events(events)
    SUMMARY.write_text(json.dumps(summary(events), indent=2), encoding="utf-8")
    print(json.dumps({
        "events": len(events),
        "candidate_events": len(candidates),
        "symbols_processed": len(symbols),
        "events_changed": changed,
        "summary": summary(events),
    }, indent=2))


if __name__ == "__main__":
    main()
