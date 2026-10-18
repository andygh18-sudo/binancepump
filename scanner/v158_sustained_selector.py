"""Persistent global Top-2 selector for V15.8 sustained-pump candidates."""
import json
import os
import time


def _f(obj, key, default=0.0):
    try:
        value = float(obj.get(key, default) or default)
        return value if value == value and abs(value) != float("inf") else default
    except (AttributeError, TypeError, ValueError):
        return default


def _score(row):
    base = max(0, min(100, _f(row, "_sustained_pump_score", _f(row, "sustained_pump_score"))))
    follow = max(0, min(100, 50 + _f(row, "price_5m") * 5 + _f(row, "price_10m") * 2 + _f(row, "price_15m")))
    buy = max(0, min(100, (_f(row, "buy_pressure") - .45) * 200))
    accel = max(0, min(100, (_f(row, "trade_accel") - .5) * 35))
    volume = max(0, min(100, (_f(row, "volume_ratio") - .5) * 30))
    cvd = max(0, min(100, 50 + _f(row, "cvd") * 50))
    penalty = max(0, min(30, (_f(row, "exhaustion_score") - 35) * .6))
    return round(max(0, min(100, base * .4 + follow * .2 + buy * .15 +
                            accel * .08 + volume * .07 + cvd * .1 - penalty)), 2)


def _load(path):
    try:
        with open(path, encoding="utf-8") as source:
            state = json.load(source)
        if not isinstance(state, dict):
            raise ValueError("state is not an object")
    except (OSError, ValueError, TypeError):
        state = {}
    state["pool"] = state.get("pool") if isinstance(state.get("pool"), dict) else {}
    state["observations"] = state.get("observations") if isinstance(state.get("observations"), dict) else {}
    state["pending_alerts"] = state.get("pending_alerts") if isinstance(state.get("pending_alerts"), dict) else {}
    # Only successfully delivered alerts count as notified. Legacy top2 is ranking
    # state, not evidence that a Telegram notification was delivered.
    state["alerted_top2"] = sorted({str(s).upper() for s in state.get("alerted_top2", [])})
    return state


def _save(state, path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as target:
        json.dump(state, target, indent=2, allow_nan=False)
    os.replace(tmp, path)


def update_global_top2(candidates, observed_symbols=None, now=None, path=None):
    now = time.time() if now is None else float(now)
    path = path or os.getenv("SUSTAINED_PUMP_LEADERBOARD_FILE", "data/v158_sustained_pump_top2.json")
    ttl = max(60.0, float(os.getenv("SUSTAINED_PUMP_POOL_TTL_SECONDS", "1800")))
    margin = max(0.0, float(os.getenv("SUSTAINED_PUMP_REPLACEMENT_MARGIN", "5")))
    step = max(0.0, float(os.getenv("SUSTAINED_PUMP_ALERT_SCORE_STEP", "5")))
    min_observed = max(0.0, float(os.getenv("SUSTAINED_PUMP_MIN_OBSERVED_SECONDS", "0")))
    observation_gap = max(15.0, float(os.getenv("SUSTAINED_PUMP_OBSERVATION_GAP_SECONDS", "180")))
    retry_seconds = max(15.0, float(os.getenv("SUSTAINED_PUMP_ALERT_RETRY_SECONDS", "60")))

    state = _load(path)
    pool, observations = state["pool"], state["observations"]
    pending = state["pending_alerts"]
    prior = set(state["alerted_top2"])

    clean_candidates = []
    for row in candidates or []:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol or not symbol.isascii() or not symbol.isalnum():
            continue
        item = dict(row)
        item["symbol"] = symbol
        clean_candidates.append(item)
    eligible = {row["symbol"] for row in clean_candidates}
    observed = {str(symbol).strip().upper() for symbol in (observed_symbols or [])
                if str(symbol).strip() and str(symbol).strip().isascii() and str(symbol).strip().isalnum()}

    # A candidate's clock starts when it first qualifies, not merely because its
    # ticker happened to be present in the general scan before it qualified.
    for symbol, item in list(observations.items()):
        if not isinstance(item, dict) or now - _f(item, "last_seen") > observation_gap:
            observations.pop(symbol, None)
    for symbol in eligible:
        previous = observations.get(symbol, {})
        last_seen = _f(previous, "last_seen")
        first_seen = _f(previous, "first_seen", now) if last_seen and now - last_seen <= observation_gap else now
        observations[symbol] = {"first_seen": first_seen, "last_seen": now}
    for symbol in observed:
        if symbol in observations and symbol not in eligible:
            observations[symbol]["last_seen"] = now

    for symbol, item in list(pool.items()):
        if now - _f(item, "last_seen") > ttl or (symbol.upper() in observed and symbol.upper() not in eligible):
            pool.pop(symbol, None)
            pending.pop(symbol.upper(), None)

    for row in clean_candidates:
        symbol = row["symbol"]
        item = dict(row)
        item["last_seen"] = now
        item["global_rank_score"] = _score(item)
        previous = pool.get(symbol, {})
        item["last_alert_score"] = _f(previous, "last_alert_score")
        item["last_alert_at"] = _f(previous, "last_alert_at")
        pool[symbol] = item

    ranked = sorted(pool.values(), key=lambda row: (
        _f(row, "global_rank_score"), _f(row, "_sustained_pump_score"), _f(row, "price_15m")
    ), reverse=True)
    leaders = ranked[:2]
    weakest = min((_f(pool.get(symbol, {}), "global_rank_score") for symbol in prior if symbol in pool), default=0)
    alerts = []
    for row in leaders:
        symbol = row["symbol"]
        score = _f(row, "global_rank_score")
        observation = observations.get(symbol, {})
        observed_seconds = max(0.0, now - _f(observation, "first_seen", now))
        row["observed_seconds"] = round(observed_seconds, 1)
        is_new = symbol not in prior and (len(prior) < 2 or score >= weakest + margin)
        last_alert_at = _f(row, "last_alert_at")
        improved = last_alert_at > 0 and now - last_alert_at >= 300 and score >= _f(row, "last_alert_score") + step
        pending_item = pending.get(symbol, {})
        retry_ready = now - _f(pending_item, "last_attempt_at") >= retry_seconds
        if observed_seconds < min_observed:
            row["alert_block_reason"] = "observation_window_not_complete"
        elif not (is_new or improved):
            row["alert_block_reason"] = "already_notified_or_replacement_margin"
        elif pending_item and not retry_ready:
            row["alert_block_reason"] = "telegram_delivery_retry_cooldown"
        else:
            row["alert_block_reason"] = "ready_for_telegram"
            pending[symbol] = {"last_attempt_at": now, "score": score}
            alerts.append(row)

    state.update({
        "updated": now,
        "selector": "V15.8_GLOBAL_SUSTAINED_PUMP_TOP2",
        "pool_ttl_seconds": ttl,
        "min_observed_seconds": min_observed,
        "observation_gap_seconds": observation_gap,
        "top2": [row["symbol"] for row in leaders],
        "alerted_top2": sorted(prior & set(pool)),
        "observations": observations,
        "pending_alerts": pending,
        "pool": pool,
        "leaderboard": [{key: row.get(key, 0) for key in (
            "symbol", "global_rank_score", "_sustained_pump_score", "price_5m", "price_10m",
            "price_15m", "buy_pressure", "volume_ratio", "trade_accel", "exhaustion_score"
        )} for row in ranked[:20]],
    })
    _save(state, path)
    return alerts, ranked


def acknowledge_global_top2(symbol, score, delivered, now=None, path=None):
    """Persist notification history only after Telegram confirms successful delivery."""
    now = time.time() if now is None else float(now)
    path = path or os.getenv("SUSTAINED_PUMP_LEADERBOARD_FILE", "data/v158_sustained_pump_top2.json")
    state = _load(path)
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return
    state["pending_alerts"].pop(symbol, None)
    if delivered:
        item = state["pool"].get(symbol, {})
        item["last_alert_score"] = float(score or 0)
        item["last_alert_at"] = now
        state["pool"][symbol] = item
        state["alerted_top2"] = sorted(set(state["alerted_top2"]) | {symbol})
    state["updated"] = now
    _save(state, path)
