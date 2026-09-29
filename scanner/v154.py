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

V155 = {
    "enabled": os.getenv("V155_REIGNITION_ENABLED", "1") == "1",
    "watch_seconds": float(os.getenv("V155_REIGNITION_WATCH_SECONDS", "7200")),
    "min_v15": float(os.getenv("V155_MIN_V15", "80")),
    "min_volume": float(os.getenv("V155_MIN_VOLUME", "2.00")),
    "min_accel": float(os.getenv("V155_MIN_ACCEL", "2.00")),
    "min_buy": float(os.getenv("V155_MIN_BUY", "0.75")),
    "min_price_60s": float(os.getenv("V155_MIN_PRICE_60S", "0.35")),
    "min_rs5": float(os.getenv("V155_MIN_RS5", "0.20")),
    "min_tv_bull_tf": int(os.getenv("V155_MIN_TV_BULL_TF", "3")),
    "pre_watch_seconds": float(os.getenv("V155_PRE_WATCH_SECONDS", "1800")),
    "pre_min_v15": float(os.getenv("V155_PRE_MIN_V15", "45")),
    "pre_min_opp": float(os.getenv("V155_PRE_MIN_OPP", "40")),
    "pre_min_acc": float(os.getenv("V155_PRE_MIN_ACC", "45")),
    "pre_min_price_60s": float(os.getenv("V155_PRE_MIN_PRICE_60S", "0.15")),
    "pre_min_rs5": float(os.getenv("V155_PRE_MIN_RS5", "0.00")),
    "pre_min_tv_bull_tf": int(os.getenv("V155_PRE_MIN_TV_BULL_TF", "3")),
    "pre_min_volume": float(os.getenv("V155_PRE_MIN_VOLUME", "0.75")),
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

def _arm_reignition(memory):
    if not V155["enabled"]:
        return False
    return (
        _f(memory, "trigger_v15") >= V155["min_v15"]
        and _f(memory, "trigger_volume") >= V155["min_volume"]
        and _f(memory, "trigger_accel") >= V155["min_accel"]
        and _f(memory, "trigger_buy") >= V155["min_buy"]
        and _f(memory, "trigger_p60") >= V155["min_price_60s"]
        and _f(memory, "trigger_rs5") >= V155["min_rs5"]
        and _i(memory, "trigger_tv_bull_tf") >= V155["min_tv_bull_tf"]
    )

def _pre_ignition_setup(row, btc_off, early, extension):
    """Non-alerting setup watch for PUMP/PHA-like pre-pump conditions."""
    if btc_off or early or extension:
        return False
    v15 = _f(row, "v15_score")
    opp = _f(row, "v15_opportunity_score")
    acc = _f(row, "accumulation_score")
    p60 = _f(row, "price_60s")
    rs5 = _f(row, "relative_strength_5m")
    vol = _f(row, "volume_ratio")
    tvtf = _i(row, "tv_bullish_timeframes")
    # Require either strong higher-timeframe alignment or meaningful V15
    # structure, plus early price/flow improvement. This state never alerts.
    higher_tf = tvtf >= V155["pre_min_tv_bull_tf"]
    structure = v15 >= V155["pre_min_v15"] and opp >= V155["pre_min_opp"] and acc >= V155["pre_min_acc"]
    price_flow = p60 >= V155["pre_min_price_60s"] and rs5 >= V155["pre_min_rs5"] and vol >= V155["pre_min_volume"]
    return price_flow and (higher_tf or structure)
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

    reignition_active = (
        V155["enabled"]
        and memory.get("status") == "REIGNITION_WATCH"
        and now < float(memory.get("reignition_expires", 0) or 0)
        and not btc_off
    )

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
    pre_watch = _pre_ignition_setup(row, btc_off, early, extension)

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

    # PRE-IGNITION WATCH is intentionally non-alerting. It records a setup
    # and keeps it warm while short-term participation catches up.
    if pre_watch and status in ("IDLE", "FAILED", "FAILED_PERSISTENCE", "CANCELLED", "COMPLETED"):
        last_pre = float(memory.get("pre_watch_ts", 0) or 0)
        if status != "PRE_IGNITION_WATCH" or now - last_pre >= 300:
            memory["status"] = "PRE_IGNITION_WATCH"
            memory["stage"] = "PRE-IGNITION WATCH"
            memory["pre_watch_ts"] = now
            memory["pre_watch_expires"] = now + V155["pre_watch_seconds"]
            memory["pre_watch_price"] = _f(row, "price")
            memory["pre_watch_v15"] = v15
            memory["pre_watch_volume"] = vol
            memory["pre_watch_accel"] = accel
            memory["pre_watch_buy"] = buy
            memory["pre_watch_p60"] = p60
            memory["pre_watch_rs5"] = rs5
            memory["pre_watch_tv_bull_tf"] = tvtf
            event_record = {
                "event": "PRE_WATCH",
                "event_id": memory.get("event_id") or uuid.uuid4().hex[:12],
                "symbol": symbol,
                "ts": now,
                "stage": "PRE-IGNITION WATCH",
                "alert": False,
                "price": _f(row, "price"),
                "v15_score": v15,
                "volume_ratio": vol,
                "trade_accel": accel,
                "buy_pressure": buy,
                "price_60s": p60,
                "rs5": rs5,
                "tv_bullish_timeframes": tvtf,
            }
    # Stage 1 -> Stage 2: create a pending ignition event.
    if early and not extension and status in ("IDLE", "PRE_IGNITION_WATCH", "FAILED", "FAILED_PERSISTENCE", "CANCELLED", "COMPLETED", "REIGNITION_WATCH"):
        last_alert = float(memory.get("last_alert", 0) or 0)
        if now - last_alert >= V154["alert_cooldown"]:
            was_reignition = reignition_active
            prior_event_id = memory.get("reignition_source_event_id", "")
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
                "v155_reignition": was_reignition,
                "v155_reignition_source_event_id": prior_event_id,
                "failures": [],
                "last_update": now,
            })
            event_record = {
                "event": "TRIGGER",
                "event_id": event_id,
                "symbol": symbol,
                "ts": now,
                "v154_score": score,
                "stage": "REIGNITION EARLY IGNITION" if was_reignition else "EARLY IGNITION",
                "persistence_status": "PENDING",
                "v155_reignition": was_reignition,
                "v155_reignition_source_event_id": prior_event_id,
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
            old_event_id = memory.get("event_id", "")
            high_quality = _arm_reignition(memory)
            if high_quality:
                memory["status"] = "REIGNITION_WATCH"
                memory["stage"] = "RE-IGNITION WATCH"
                memory["reignition_started"] = now
                memory["reignition_expires"] = now + V155["watch_seconds"]
                memory["reignition_source_event_id"] = old_event_id
                memory["reignition_reason"] = "FAILED_HIGH_QUALITY_IGNITION"
            else:
                memory["status"] = "FAILED_PERSISTENCE"
                memory["stage"] = "WATCH"
            event_record = {
                "event": "RESOLVED",
                "event_id": old_event_id,
                "symbol": symbol,
                "ts": now,
                "outcome": "FAILED_PERSISTENCE_REIGNITION_WATCH" if high_quality else "FAILED_PERSISTENCE",
                "stage": "RE-IGNITION WATCH" if high_quality else "WATCH",
                "persistence_seconds": round(age, 1),
                "failures": current_failures,
                "max_price_60s": p60,
                "v154_score": score,
                "v155_reignition_armed": high_quality,
                "v155_reignition_expires": memory.get("reignition_expires", 0),
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

    if memory.get("status") == "PRE_IGNITION_WATCH":
        if btc_off or now >= float(memory.get("pre_watch_expires", 0) or 0):
            memory["status"] = "FAILED_PERSISTENCE"
            memory["stage"] = "WATCH"
            memory["pre_watch_expired"] = True
            memory["last_update"] = now
    if memory.get("status") == "REIGNITION_WATCH":
        if btc_off or now >= float(memory.get("reignition_expires", 0) or 0):
            memory["status"] = "FAILED_PERSISTENCE"
            memory["stage"] = "WATCH"
            memory["reignition_expired"] = True
            memory["last_update"] = now

    status = memory.get("status", status)
    confirmed = status == "PERSISTENCE_CONFIRMED" and not extension
    early_stage = status == "PENDING" and early and not extension

    if confirmed:
        stage = "CONFIRMED IGNITION"
    elif early_stage:
        stage = "REIGNITION EARLY IGNITION" if memory.get("v155_reignition") else "EARLY IGNITION"
    elif memory.get("status") == "PRE_IGNITION_WATCH":
        stage = "PRE-IGNITION WATCH"
    elif memory.get("status") == "REIGNITION_WATCH":
        stage = "RE-IGNITION WATCH"
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
        "v155_reignition_watch": bool(memory.get("status") == "REIGNITION_WATCH"),
        "v155_reignition_active": bool(reignition_active),
        "v155_reignition_source_event_id": memory.get("reignition_source_event_id", ""),
        "v155_reignition_expires": memory.get("reignition_expires", 0),
        "v155_reignition_armed": bool(memory.get("reignition_reason") == "FAILED_HIGH_QUALITY_IGNITION"),
        "v155_reignition_trigger": bool(event_record and event_record.get("event") == "TRIGGER" and memory.get("v155_reignition")),
        "v155_pre_ignition_watch": bool(memory.get("status") == "PRE_IGNITION_WATCH"),
        "v155_pre_ignition_active": bool(pre_watch),
        "v155_pre_ignition_expires": memory.get("pre_watch_expires", 0),
        "v155_pre_ignition_event": bool(event_record and event_record.get("event") == "PRE_WATCH"),

    }
    return out, event_record
