"""30-minute persistence monitor for V15.8 Fastest-Pump candidates.

This is an additive confirmation/learning lane. It does not alter V15.8
candidate scoring, candidate gates, BUY decisions, or the existing Fastest-Pump
alert selector. State is persisted between short GitHub Actions worker runs.
"""
import json
import os
import tempfile
from pathlib import Path

WINDOW_SECONDS = int(os.getenv("PUMP_PERSISTENCE_WINDOW_SECONDS", "1800"))
CHECKPOINT_SECONDS = (300, 600, 900, 1200, 1500, 1800)
STATE_PATH = Path(os.getenv("PUMP_PERSISTENCE_STATE_PATH", "data/v158_pump_persistence_30m.json"))
MAX_COMPLETED = 500


def load_state(path=STATE_PATH):
    try:
        with Path(path).open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data.setdefault("active", {})
            data.setdefault("completed", [])
            return data
    except (OSError, ValueError, TypeError):
        pass
    return {"schema_version": 1, "updated": 0.0, "active": {}, "completed": []}


def save_state(state, path=STATE_PATH):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    state["updated"] = float(state.get("updated", 0.0) or 0.0)
    fd, tmp = tempfile.mkstemp(prefix=target.name + ".", suffix=".tmp", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, separators=(",", ":"), allow_nan=False)
        os.replace(tmp, target)
    finally:
        try:
            if os.path.exists(tmp):
                os.unlink(tmp)
        except OSError:
            pass


def _num(row, key, default=0.0):
    try:
        value = float(row.get(key, default) or default)
        return value if value == value and abs(value) != float("inf") else default
    except (TypeError, ValueError):
        return default


def _sample(row, now):
    return {
        "ts": float(now),
        "price": _num(row, "price"),
        "fast_score": _num(row, "_fast_pump_score"),
        "buy_pressure": _num(row, "buy_pressure"),
        "volume_ratio": _num(row, "volume_ratio"),
        "trade_accel": _num(row, "trade_accel"),
        "cvd": _num(row, "_fast_cvd", _num(row, "cvd")),
        "exhaustion": _num(row, "_fast_dynamic_exhaustion", _num(row, "exhaustion_score")),
        "price_only": bool(row.get("_persistence_price_only", False)),
    }


def _ret_pct(price, entry):
    return ((price / entry) - 1.0) * 100.0 if entry > 0 and price > 0 else 0.0


def _confirmation_checks(ep, sample):
    entry = _num(ep, "entry_price")
    price = _num(sample, "price")
    ret = _ret_pct(price, entry)
    peak = max(_num(ep, "peak_price", entry), price)
    peak_drawdown = _ret_pct(price, peak)
    checks = {
        "price_progress": ret >= 0.50,
        "pullback_control": peak_drawdown >= -1.50,
        "buy_pressure": _num(sample, "buy_pressure") >= 0.52,
        "volume_persistence": _num(sample, "volume_ratio") >= 0.90,
        "trade_activity": _num(sample, "trade_accel") >= 1.00,
        "order_flow": _num(sample, "cvd") >= -0.05,
    }
    return checks, ret, peak_drawdown


def _finish_episode(state, symbol, ep, now, outcome, stale_age=None):
    """Close an episode once, using only the last observed market price."""
    entry = _num(ep, "entry_price")
    price = _num(ep, "last_price", entry)
    peak = _num(ep, "peak_price", entry)
    ret = _ret_pct(price, entry)
    peak_dd = _ret_pct(price, peak)
    completed = {
        k: v for k, v in ep.items()
        if k not in ("samples", "_candidate_sample")
    }
    completed.update({
        "end_ts": float(now),
        "final_return_pct": round(ret, 4),
        "max_return_pct": round(_ret_pct(peak, entry), 4),
        "outcome": outcome,
        "sample_count": len(ep.get("samples", [])),
    })
    if stale_age is not None:
        completed["stale_seconds"] = round(float(stale_age), 1)
    state["completed"].append(completed)
    state["completed"] = state["completed"][-MAX_COMPLETED:]
    state["active"].pop(symbol, None)
    if outcome == "STALE_SYMBOL":
        return None
    return {
        "type": "OUTCOME",
        "outcome": outcome,
        "symbol": symbol,
        "entry_price": entry,
        "price": price,
        "final_return_pct": round(ret, 4),
        "max_return_pct": round(_ret_pct(peak, entry), 4),
        "peak_drawdown_pct": round(peak_dd, 4),
        "elapsed_minutes": round(max(0.0, now - _num(ep, "start_ts", now)) / 60.0, 1),
        "sample_count": len(ep.get("samples", [])),
        "confirmed": bool(ep.get("confirmed")),
    }


def _new_episode(symbol, sample, now):
    price = _num(sample, "price")
    return {
        "symbol": symbol,
        "start_ts": float(now),
        "entry_price": price,
        "peak_price": price,
        "trough_price": price,
        "last_ts": float(now),
        "last_price": price,
        "confirmed": False,
        "failure_alerted": False,
        "checkpoints_seen": [],
        "samples": [],
        "stage": "MONITORING",
    }


def update_monitor(rows, candidates, state, now, path=STATE_PATH):
    """Update episodes, expiring them by wall-clock and closing stale symbols."""
    now = float(now)
    state.setdefault("active", {})
    state.setdefault("completed", [])
    rows_by_symbol = {
        str(r.get("symbol", "")).strip().upper(): r
        for r in (rows or [])
        if isinstance(r, dict) and r.get("symbol")
    }
    candidate_by_symbol = {
        str(r.get("symbol", "")).strip().upper(): r
        for r in (candidates or [])
        if isinstance(r, dict) and r.get("symbol")
    }
    alerts = []

    # Active episodes are pinned until the 30-minute window has elapsed.
    # Missing symbols are price-polled by the worker; a brief data/API outage
    # records a gap but must not silently truncate an episode at 120 seconds.
    stale_seconds = max(30, int(os.getenv("PUMP_PERSISTENCE_STALE_SECONDS", "120")))

    # Arm from the existing qualifying candidate set only.
    for symbol, row in candidate_by_symbol.items():
        sample = _sample(row, now)
        ep = state["active"].get(symbol)
        if ep is None:
            if sample["price"] <= 0:
                continue
            state["active"][symbol] = _new_episode(symbol, sample, now)
            ep = state["active"][symbol]
        ep["_candidate_sample"] = sample

    for symbol, ep in list(state["active"].items()):
        row = rows_by_symbol.get(symbol)
        candidate_sample = ep.pop("_candidate_sample", None)
        current_row_exists = row is not None or candidate_sample is not None
        start_ts = _num(ep, "start_ts", now)
        elapsed = max(0.0, now - start_ts)
        last_ts = _num(ep, "last_ts", start_ts)
        stale_age = max(0.0, now - last_ts)

        # A missing symbol stays pinned. The worker attempts a direct ticker-price
        # request for it; if that request also fails, preserve the episode and wait
        # for the next cycle instead of mislabelling it STALE_SYMBOL.
        if not current_row_exists:
            ep["data_gap_since"] = ep.get("data_gap_since", last_ts)
            ep["data_gap_seconds"] = round(max(0.0, now - _num(ep, "data_gap_since", last_ts)), 1)
            if elapsed >= WINDOW_SECONDS:
                # Never publish a nominal 30-minute outcome using an old price.
                ep["stage"] = "AWAITING_FINAL_PRICE"
            continue

        sample = candidate_sample or _sample(row, now)
        price = _num(sample, "price")
        if price <= 0:
            continue

        entry = _num(ep, "entry_price")
        ep["last_ts"] = now
        ep["last_price"] = price
        ep.pop("data_gap_since", None)
        ep.pop("data_gap_seconds", None)
        ep["peak_price"] = max(_num(ep, "peak_price", entry), price)
        ep["trough_price"] = min(_num(ep, "trough_price", entry), price)
        ep.setdefault("samples", []).append(sample)
        if len(ep["samples"]) > 400:
            ep["samples"] = ep["samples"][-400:]

        elapsed = max(0.0, now - _num(ep, "start_ts", now))
        ret = _ret_pct(price, entry)
        peak_dd = _ret_pct(price, _num(ep, "peak_price", entry))
        ep["elapsed_seconds"] = int(elapsed)
        ep["return_pct"] = round(ret, 4)
        ep["peak_drawdown_pct"] = round(peak_dd, 4)
        ep["max_return_pct"] = round(_ret_pct(_num(ep, "peak_price", entry), entry), 4)

        for checkpoint in CHECKPOINT_SECONDS:
            if elapsed >= checkpoint and checkpoint not in ep["checkpoints_seen"]:
                ep["checkpoints_seen"].append(checkpoint)
                ep.setdefault("checkpoint_results", {})[str(checkpoint // 60)] = {
                    "return_pct": round(ret, 4),
                    "peak_drawdown_pct": round(peak_dd, 4),
                    "buy_pressure": None if sample.get("price_only") else round(_num(sample, "buy_pressure"), 4),
                    "volume_ratio": None if sample.get("price_only") else round(_num(sample, "volume_ratio"), 4),
                    "trade_accel": None if sample.get("price_only") else round(_num(sample, "trade_accel"), 4),
                    "cvd": None if sample.get("price_only") else round(_num(sample, "cvd"), 4),
                }

        sample_span = (
            _num(ep["samples"][-1], "ts") - _num(ep["samples"][0], "ts")
            if len(ep.get("samples", [])) >= 2 else 0.0
        )
        metric_samples = [s for s in ep.get("samples", []) if not s.get("price_only")]
        metric_span = (
            _num(metric_samples[-1], "ts") - _num(metric_samples[0], "ts")
            if len(metric_samples) >= 2 else 0.0
        )
        if (not sample.get("price_only") and not ep.get("confirmed") and elapsed >= 300
                and len(metric_samples) >= 5 and metric_span >= 240):
            checks, ret, peak_dd = _confirmation_checks(ep, sample)
            passed = sum(bool(v) for v in checks.values())
            if ret >= 0.50 and passed >= 4:
                ep["confirmed"] = True
                ep["stage"] = "SUSTAINED_PUMP_CONFIRMED"
                ep["confirmed_ts"] = now
                ep["confirmation_checks"] = checks
                alerts.append({
                    "type": "CONFIRMED", "symbol": symbol,
                    "entry_price": entry, "price": price,
                    "return_pct": ret, "peak_drawdown_pct": peak_dd,
                    "elapsed_minutes": round(elapsed / 60.0, 1),
                    "checks_passed": passed, "checks_total": len(checks),
                    "score": _num(sample, "fast_score"),
                })

        if ep.get("confirmed") and not ep.get("failure_alerted"):
            if ret <= -1.50 or peak_dd <= -2.50:
                ep["failure_alerted"] = True
                ep["stage"] = "CONFIRMED_PUMP_DETERIORATED"
                alerts.append({
                    "type": "DETERIORATED", "symbol": symbol,
                    "entry_price": entry, "price": price,
                    "return_pct": ret, "peak_drawdown_pct": peak_dd,
                    "elapsed_minutes": round(elapsed / 60.0, 1),
                    "score": _num(sample, "fast_score"),
                })

        if elapsed >= WINDOW_SECONDS:
            max_ret = _ret_pct(_num(ep, "peak_price", entry), entry)
            if max_ret >= 3.0 and ret >= 1.0:
                outcome = "SUSTAINED_PUMP"
            elif max_ret >= 1.0 and (ret < 0.25 or peak_dd <= -2.0):
                outcome = "FAILED_BURST"
            else:
                outcome = "NO_CLEAR_PUMP"
            alert = _finish_episode(state, symbol, ep, now, outcome)
            if alert:
                alert["peak_drawdown_pct"] = round(peak_dd, 4)
                alerts.append(alert)

    # Surface state only for rows that are present in this scan.
    for symbol, row in rows_by_symbol.items():
        ep = state["active"].get(symbol)
        if ep:
            row["pump_persistence_stage"] = ep.get("stage", "MONITORING")
            row["pump_persistence_minutes"] = round(_num(ep, "elapsed_seconds") / 60.0, 1)
            row["pump_persistence_return_pct"] = _num(ep, "return_pct")
            row["pump_persistence_peak_drawdown_pct"] = _num(ep, "peak_drawdown_pct")
            row["pump_persistence_confirmed"] = bool(ep.get("confirmed"))
            row["pump_persistence_window_minutes"] = WINDOW_SECONDS // 60
        else:
            row.setdefault("pump_persistence_stage", "NOT_MONITORED")
            row.setdefault("pump_persistence_confirmed", False)

    state["updated"] = now
    save_state(state, path)
    return alerts
