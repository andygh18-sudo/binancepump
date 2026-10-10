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


def update_monitor(rows, candidates, state, now, path=STATE_PATH):
    """Update episodes from the current scan. Returns newly triggered alerts."""
    now = float(now)
    state.setdefault("active", {})
    state.setdefault("completed", [])
    rows_by_symbol = {
        str(r.get("symbol", "")).upper(): r
        for r in (rows or [])
        if isinstance(r, dict) and r.get("symbol")
    }
    candidate_by_symbol = {
        str(r.get("symbol", "")).upper(): r
        for r in (candidates or [])
        if isinstance(r, dict) and r.get("symbol")
    }
    alerts = []

    # Arm only from the existing Fastest-Pump qualifying candidate set.
    for symbol, row in candidate_by_symbol.items():
        sample = _sample(row, now)
        ep = state["active"].get(symbol)
        if ep is None:
            if sample["price"] <= 0:
                continue
            ep = {
                "symbol": symbol,
                "start_ts": now,
                "entry_price": sample["price"],
                "peak_price": sample["price"],
                "trough_price": sample["price"],
                "last_ts": now,
                "last_price": sample["price"],
                "confirmed": False,
                "failure_alerted": False,
                "checkpoints_seen": [],
                "samples": [],
                "stage": "MONITORING",
            }
            state["active"][symbol] = ep
        # Candidate values are preferred for the current loop because they
        # include the full Fastest-Pump microstructure features.
        ep["_candidate_sample"] = sample

    for symbol, ep in list(state["active"].items()):
        row = rows_by_symbol.get(symbol)
        candidate_sample = ep.pop("_candidate_sample", None)
        if row is None and candidate_sample is None:
            continue
        sample = candidate_sample or _sample(row, now)
        price = _num(sample, "price")
        if price <= 0:
            continue

        entry = _num(ep, "entry_price")
        ep["last_ts"] = now
        ep["last_price"] = price
        ep["peak_price"] = max(_num(ep, "peak_price", entry), price)
        ep["trough_price"] = min(_num(ep, "trough_price", entry), price)
        ep["samples"].append(sample)
        # Keep a bounded per-episode sample list; checkpoints/outcome stats remain.
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
                    "buy_pressure": round(_num(sample, "buy_pressure"), 4),
                    "volume_ratio": round(_num(sample, "volume_ratio"), 4),
                    "trade_accel": round(_num(sample, "trade_accel"), 4),
                    "cvd": round(_num(sample, "cvd"), 4),
                }

        # Confirm only after a full 5-minute observation, with price progress
        # plus at least 3 of the 5 supporting conditions (4 of 6 total).
        sample_span = (
            _num(ep["samples"][-1], "ts") - _num(ep["samples"][0], "ts")
            if len(ep.get("samples", [])) >= 2 else 0.0
        )
        # Require repeated observations across the window, not a single stale
        # candidate reappearing after a long gap.
        if not ep.get("confirmed") and elapsed >= 300 and len(ep.get("samples", [])) >= 5 and sample_span >= 240:
            checks, ret, peak_dd = _confirmation_checks(ep, sample)
            passed = sum(bool(v) for v in checks.values())
            if ret >= 0.50 and passed >= 4:
                ep["confirmed"] = True
                ep["stage"] = "SUSTAINED_PUMP_CONFIRMED"
                ep["confirmed_ts"] = now
                ep["confirmation_checks"] = checks
                alerts.append({
                    "type": "CONFIRMED",
                    "symbol": symbol,
                    "entry_price": entry,
                    "price": price,
                    "return_pct": ret,
                    "peak_drawdown_pct": peak_dd,
                    "elapsed_minutes": round(elapsed / 60.0, 1),
                    "checks_passed": passed,
                    "checks_total": len(checks),
                    "score": _num(sample, "fast_score"),
                })

        # Only send a failure alert after a confirmed sustained move breaks down.
        if ep.get("confirmed") and not ep.get("failure_alerted"):
            if ret <= -1.50 or peak_dd <= -2.50:
                ep["failure_alerted"] = True
                ep["stage"] = "CONFIRMED_PUMP_DETERIORATED"
                alerts.append({
                    "type": "DETERIORATED",
                    "symbol": symbol,
                    "entry_price": entry,
                    "price": price,
                    "return_pct": ret,
                    "peak_drawdown_pct": peak_dd,
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
            completed = {
                k: v for k, v in ep.items()
                if k not in ("samples", "_candidate_sample")
            }
            completed.update({
                "end_ts": now,
                "final_return_pct": round(ret, 4),
                "max_return_pct": round(max_ret, 4),
                "outcome": outcome,
                "sample_count": len(ep.get("samples", [])),
            })
            state["completed"].append(completed)
            state["completed"] = state["completed"][-MAX_COMPLETED:]
            state["active"].pop(symbol, None)

    # Expose monitor status to the dashboard's latest.json rows.
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
