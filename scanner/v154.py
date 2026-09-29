"""V15.4 Early-Ignition state machine.

V15.4 deliberately exposes exactly three actionable stages:
    WATCH -> EARLY IGNITION -> CONFIRMED IGNITION

Early Ignition prioritizes short-term participation and price/flow structure.
TradingView multi-timeframe alignment is confirmation for Stage 3 rather than
a hard Stage-2 gate. Exhaustion and disqualifiers are veto/diagnostic
conditions, not additional signal stages. V15 remains unchanged; this engine
runs alongside it.
"""
import time
import uuid
import os

V154 = {
    "price_10s_min": float(os.getenv("V154_PRICE_10S_MIN", "0.10")),
    "price_60s_min": float(os.getenv("V154_PRICE_60S_MIN", "0.35")),
    "volume_ratio_min": float(os.getenv("V154_VOLUME_MIN", "1.50")),
    "trade_accel_min": float(os.getenv("V154_ACCEL_MIN", "2.00")),
    "buy_pressure_min": float(os.getenv("V154_BUY_MIN", "0.65")),
    "rs5_min": float(os.getenv("V154_RS5_MIN", "0.20")),
    "v15_min": float(os.getenv("V154_V15_MIN", "75")),
    "opportunity_min": float(os.getenv("V154_OPPORTUNITY_MIN", "65")),
    "confirmation_min": float(os.getenv("V154_CONFIRMATION_MIN", "70")),
    "accumulation_min": float(os.getenv("V154_ACCUMULATION_MIN", "65")),
    "tv_bull_tf_min": int(os.getenv("V154_TV_BULL_TF_MIN", "3")),
    "persistence_seconds": float(os.getenv("V154_PERSISTENCE_SECONDS", "30")),
    "persistence_min_seconds": float(os.getenv("V154_PERSISTENCE_MIN_SECONDS", "15")),
    "persistence_volume_min": float(os.getenv("V154_PERSISTENCE_VOLUME_MIN", "1.25")),
    "persistence_accel_min": float(os.getenv("V154_PERSISTENCE_ACCEL_MIN", "1.50")),
    "persistence_buy_min": float(os.getenv("V154_PERSISTENCE_BUY_MIN", "0.55")),
    "persistence_price_60s_min": float(os.getenv("V154_PERSISTENCE_PRICE_60S_MIN", "0.00")),
    "persistence_rs5_min": float(os.getenv("V154_PERSISTENCE_RS5_MIN", "0.00")),
    "persistence_v15_min": float(os.getenv("V154_PERSISTENCE_V15_MIN", "70")),
    "persistence_tv_bull_tf_min": int(os.getenv("V154_PERSISTENCE_TV_BULL_TF_MIN", "3")),
    "persistence_fail_count": int(os.getenv("V154_PERSISTENCE_FAIL_COUNT", "2")),
    "alert_cooldown": float(os.getenv("V154_ALERT_COOLDOWN", "180")),
    "exhaustion_price_60s": float(os.getenv("V154_EXHAUSTION_PRICE_60S", "1.50")),
    "exhaustion_accel_drop": float(os.getenv("V154_EXHAUSTION_ACCEL_DROP", "0.50")),
}

def _f(row, k, d=0.0):
    try:
        return float(row.get(k, d) or d)
    except (TypeError, ValueError):
        return d

def _i(row, k, d=0):
    try:
        return int(row.get(k, d) or d)
    except (TypeError, ValueError):
        return d

def evaluate(row, memory, now=None):
    now = time.time() if now is None else float(now)
    symbol = str(row.get("symbol", "")).upper()

    p10 = _f(row, "price_10s")
    p60 = _f(row, "price_60s")
    vol = _f(row, "volume_ratio")
    accel = _f(row, "trade_accel")
    buy = _f(row, "buy_pressure")
    rs5 = _f(row, "relative_strength_5m")
    v15 = _f(row, "v15_score")
    opp = _f(row, "v15_opportunity_score")
    conf = _f(row, "v15_confirmation_score")
    acc = _f(row, "accumulation_score")
    tvtf = _i(row, "tv_bullish_timeframes")
    btc_off = bool(row.get("v15_btc_risk_off", False))
    book = _f(row, "book_imbalance")
    spread = _f(row, "spread_bps")
    bridge = _f(row, "v15_reignition_bridge_score")
    bridge_trigger = bool(row.get("v15_reignition_bridge_trigger", False))
    exhaustion = _f(row, "exhaustion_score")
    pump = _f(row, "pump_momentum_score")

    participation = (
        vol >= V154["volume_ratio_min"]
        and accel >= V154["trade_accel_min"]
        and buy >= V154["buy_pressure_min"]
    )
    price = (
        p10 >= V154["price_10s_min"]
        and p60 >= V154["price_60s_min"]
        and rs5 >= V154["rs5_min"]
    )

    # Stage 2 structure: deliberately excludes TV so early ignition can fire
    # before higher-timeframe confirmation catches up.
    structure = (
        v15 >= V154["v15_min"]
        and opp >= V154["opportunity_min"]
        and conf >= V154["confirmation_min"]
        and acc >= V154["accumulation_min"]
        and not btc_off
    )

    # Stage 3 confirmation: all Stage-2 gates plus TV alignment.
    confirmation = structure and tvtf >= V154["tv_bull_tf_min"]

    failures = []
    if p10 <= 0:
        failures.append("PRICE_10S_NONPOSITIVE")
    if vol < V154["persistence_volume_min"]:
        failures.append("VOLUME_COLLAPSE")
    if accel < V154["persistence_accel_min"]:
        failures.append("ACCEL_COLLAPSE")
    if buy < V154["persistence_buy_min"]:
        failures.append("BUY_PRESSURE_COLLAPSE")
    if p60 <= V154["persistence_price_60s_min"]:
        failures.append("PRICE_60S_LOST")
    if rs5 <= V154["persistence_rs5_min"]:
        failures.append("RS5_LOST")
    if v15 < V154["persistence_v15_min"]:
        failures.append("V15_CONFIRMATION_LOST")

    trigger_v15 = memory.get("trigger_v15")
    if trigger_v15 is not None and v15 < trigger_v15 - 20:
        failures.append("V15_DROP")
    if rs5 < 0:
        failures.append("RS5_NEGATIVE")

    extension = (
        p60 > V154["exhaustion_price_60s"]
        and (
            (
                memory.get("trigger_accel", accel) > 0
                and accel < memory.get("trigger_accel", accel) * V154["exhaustion_accel_drop"]
            )
            or buy < 0.50
        )
    )
    if extension:
        failures.append("EXHAUSTION")

    early = participation and price and structure

    score = 0
    score += 25 if participation else (15 if vol >= 1.25 and accel >= 1.5 and buy >= 0.60 else 0)
    score += 25 if price else (15 if p60 >= 0.30 and rs5 >= 0 else 0)
    score += 25 if structure else (15 if v15 >= 70 and conf >= 65 and acc >= 60 and not btc_off else 0)
    score += 10 if tvtf >= 4 else 5 if tvtf >= 3 else 0
    score += 5 if rs5 >= 0.50 else 3 if rs5 >= 0.25 else 0
    score += 5 if bridge_trigger else 3 if bridge >= 60 else 0
    score += 5 if not btc_off else 0
    score = max(0, min(100, round(score)))

    status = memory.get("status", "IDLE")
    event_record = None

    # Stage 1 -> Stage 2: create a pending ignition event.
    if early and not extension and status in ("IDLE", "FAILED", "FAILED_PERSISTENCE", "CANCELLED", "COMPLETED"):
        last_alert = float(memory.get("last_alert", 0) or 0)
        if now - last_alert >= V154["alert_cooldown"]:
            event_id = uuid.uuid4().hex[:12]
            memory.clear()
            memory.update({
                "status": "PENDING",
                "stage": "EARLY IGNITION",
                "event_id": event_id,
                "symbol": symbol,
                "trigger_ts": now,
                "last_alert": now,
                "trigger_price": _f(row, "price"),
                "trigger_v154_score": score,
                "trigger_v15": v15,
                "trigger_accel": accel,
                "trigger_volume": vol,
                "trigger_buy": buy,
                "trigger_p10": p10,
                "trigger_p60": p60,
                "trigger_book": book,
                "trigger_spread": spread,
                "trigger_opp": opp,
                "trigger_conf": conf,
                "trigger_acc": acc,
                "trigger_tv_bull_tf": tvtf,
                "trigger_bridge": bridge,
                "trigger_bridge_trigger": bridge_trigger,
                "trigger_rs5": rs5,
                "trigger_exhaustion": exhaustion,
                "trigger_pump": pump,
                "failures": [],
                "last_update": now,
            })
            event_record = {
                "event": "TRIGGER",
                "event_id": event_id,
                "symbol": symbol,
                "ts": now,
                "v154_score": score,
                "stage": "EARLY IGNITION",
                "persistence_status": "PENDING",
                "price": _f(row, "price"),
                "trigger_fields": dict(memory),
            }

    # Stage 2 -> Stage 3: persistence confirmation.
    elif status == "PENDING":
        age = now - float(memory.get("trigger_ts", now))
        current_failures = failures
        memory["failures"] = current_failures
        memory["last_update"] = now

        if len(current_failures) >= V154["persistence_fail_count"] and age >= V154["persistence_min_seconds"]:
            memory["status"] = "FAILED_PERSISTENCE"
            memory["stage"] = "WATCH"
            event_record = {
                "event": "RESOLVED",
                "event_id": memory.get("event_id"),
                "symbol": symbol,
                "ts": now,
                "outcome": "FAILED_PERSISTENCE",
                "stage": "WATCH",
                "persistence_seconds": round(age, 1),
                "failures": current_failures,
                "max_price_60s": p60,
                "v154_score": score,
            }
        elif age >= V154["persistence_seconds"] and confirmation and tvtf >= V154["persistence_tv_bull_tf_min"] and not extension and len(current_failures) == 0:
            memory["status"] = "PERSISTENCE_CONFIRMED"
            memory["stage"] = "CONFIRMED IGNITION"
            event_record = {
                "event": "RESOLVED",
                "event_id": memory.get("event_id"),
                "symbol": symbol,
                "ts": now,
                "outcome": "PERSISTENCE_CONFIRMED",
                "stage": "CONFIRMED IGNITION",
                "persistence_seconds": round(age, 1),
                "failures": current_failures,
                "max_price_60s": p60,
                "v154_score": score,
                "tv_bullish_timeframes": tvtf,
                "confirmation_gate": True,
                "high_confidence": True,
            }

    status = memory.get("status", status)
    confirmed = status == "PERSISTENCE_CONFIRMED" and not extension
    early_stage = status == "PENDING" and early and not extension

    if confirmed:
        stage = "CONFIRMED IGNITION"
    elif early_stage:
        stage = "EARLY IGNITION"
    else:
        stage = "WATCH"

    if extension:
        stage = "WATCH"

    out = {
        "v154_score": score,
        "v154_early_buy": bool(early_stage or confirmed),
        "v154_stage": stage,
        "v154_participation_gate": participation,
        "v154_price_gate": price,
        "v154_structure_gate": structure,
        "v154_confirmation_gate": confirmation,
        "v154_high_confidence": bool(confirmed),
        "v154_tv_confirmation_gate": bool(tvtf >= V154["persistence_tv_bull_tf_min"]),
        "v154_persistence_status": memory.get("status", "IDLE"),
        "v154_persistence_failures": "|".join(memory.get("failures", failures)),
        "v154_persistence_gate": bool(memory.get("status") == "PERSISTENCE_CONFIRMED"),
        "v154_persistence_seconds": round(
            max(0, now - float(memory.get("trigger_ts", now)))
            if memory.get("status") == "PENDING" else 0,
            1,
        ),
        "v154_exhaustion_veto": extension,
        "v154_disqualifiers": "|".join(failures),
        "v154_book_imbalance": book,
        "v154_spread_bps": spread,
        "v154_reignition_bonus": 5 if bridge_trigger else 3 if bridge >= 60 else 0,
        "v154_alert": bool(event_record and event_record.get("event") == "TRIGGER"),
        "v154_event_id": memory.get("event_id", ""),
    }
    return out, event_record
