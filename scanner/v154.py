"""V15.6 production early-pump state machine.

V15.6 builds on the V15.4 state machine and promotes the evidence-based
microstructure signatures, adaptive capture, fast-ignition path, and
forward-looking learner into the production signal layer.

V15.6 preserves the staged architecture while adding an independent early-pump signal layer:
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

V156_VERSION = "15.6"

V154 = {
    "price_10s_min": float(os.getenv("V154_PRICE_10S_MIN", "0.15")),
    "price_60s_min": float(os.getenv("V154_PRICE_60S_MIN", "0.30")),
    "volume_ratio_min": float(os.getenv("V154_VOLUME_MIN", "1.80")),
    "trade_accel_min": float(os.getenv("V154_ACCEL_MIN", "1.80")),
    "buy_pressure_min": float(os.getenv("V154_BUY_MIN", "0.62")),
    "rs5_min": float(os.getenv("V154_RS5_MIN", "0.00")),
    "v15_min": float(os.getenv("V154_V15_MIN", "65")),
    "opportunity_min": float(os.getenv("V154_OPPORTUNITY_MIN", "60")),
    "confirmation_min": float(os.getenv("V154_CONFIRMATION_MIN", "60")),
    "accumulation_min": float(os.getenv("V154_ACCUMULATION_MIN", "60")),
    "tv_bull_tf_min": int(os.getenv("V154_TV_BULL_TF_MIN", "3")),
    "persistence_seconds": float(os.getenv("V154_PERSISTENCE_SECONDS", "30")),
    "persistence_min_seconds": float(os.getenv("V154_PERSISTENCE_MIN_SECONDS", "15")),
    "persistence_volume_min": float(os.getenv("V154_PERSISTENCE_VOLUME_MIN", "1.25")),
    "persistence_accel_min": float(os.getenv("V154_PERSISTENCE_ACCEL_MIN", "1.50")),
    "persistence_buy_min": float(os.getenv("V154_PERSISTENCE_BUY_MIN", "0.58")),
    "persistence_price_60s_min": float(os.getenv("V154_PERSISTENCE_PRICE_60S_MIN", "0.00")),
    "persistence_rs5_min": float(os.getenv("V154_PERSISTENCE_RS5_MIN", "0.00")),
    "persistence_v15_min": float(os.getenv("V154_PERSISTENCE_V15_MIN", "65")),
    "persistence_tv_bull_tf_min": int(os.getenv("V154_PERSISTENCE_TV_BULL_TF_MIN", "3")),
    "persistence_fail_count": int(os.getenv("V154_PERSISTENCE_FAIL_COUNT", "2")),
    "alert_cooldown": float(os.getenv("V154_ALERT_COOLDOWN", "180")),
    "exhaustion_price_60s": float(os.getenv("V154_EXHAUSTION_PRICE_60S", "1.50")),
    "exhaustion_accel_drop": float(os.getenv("V154_EXHAUSTION_ACCEL_DROP", "0.50")),
}

V156_ADAPTIVE = {
    "enabled": os.getenv("V156_ADAPTIVE_CAPTURE_ENABLED", "1") == "1",
    "capture_interval": float(os.getenv("V156_CAPTURE_INTERVAL", "10")),
    "capture_window": float(os.getenv("V156_CAPTURE_WINDOW", "120")),
    "min_p60": float(os.getenv("V156_CAPTURE_MIN_P60", "0.50")),
    "min_volume": float(os.getenv("V156_CAPTURE_MIN_VOLUME", "1.50")),
    "min_buy": float(os.getenv("V156_CAPTURE_MIN_BUY", "0.70")),
    "min_accel": float(os.getenv("V156_CAPTURE_MIN_ACCEL", "1.50")),
    "min_v15": float(os.getenv("V156_CAPTURE_MIN_V15", "55")),
    "min_tv_bull_tf": int(os.getenv("V156_CAPTURE_MIN_TV_BULL_TF", "3")),
    "min_bridge": float(os.getenv("V156_CAPTURE_MIN_BRIDGE", "55")),
    "fast_p60": float(os.getenv("V156_FAST_P60", "0.25")),
    "fast_volume": float(os.getenv("V156_FAST_VOLUME", "2.00")),
    "fast_accel": float(os.getenv("V156_FAST_ACCEL", "2.00")),
    "fast_buy": float(os.getenv("V156_FAST_BUY", "0.60")),
    "fast_v15": float(os.getenv("V156_FAST_V15", "70")),
    "fast_opp": float(os.getenv("V156_FAST_OPP", "65")),
    "fast_conf": float(os.getenv("V156_FAST_CONF", "65")),
    "fast_acc": float(os.getenv("V156_FAST_ACC", "60")),
    "fast_score_min": float(os.getenv("V156_FAST_SCORE_MIN", "85")),
    "fast_cooldown": float(os.getenv("V156_FAST_COOLDOWN", "180")),
}

V156_SIGNATURES={"enabled":os.getenv("V156_SIGNATURES_ENABLED","1")=="1","cooldown":float(os.getenv("V156_SIGNATURE_COOLDOWN","240")),"pv_p60":float(os.getenv("V156_PV_P60","0.20")),"pv_volume":float(os.getenv("V156_PV_VOLUME","1.50")),"pv_accel":float(os.getenv("V156_PV_ACCEL","1.50")),"pv_buy":float(os.getenv("V156_PV_BUY","0.55")),"pv_v15":float(os.getenv("V156_PV_V15","55")),"of_p60":float(os.getenv("V156_OF_P60","0.20")),"of_buy":float(os.getenv("V156_OF_BUY","0.60")),"of_book":float(os.getenv("V156_OF_BOOK","0.10")),"of_accel":float(os.getenv("V156_OF_ACCEL","1.75")),"of_v15":float(os.getenv("V156_OF_V15","55")),"ta_p60":float(os.getenv("V156_TA_P60","0.20")),"ta_accel":float(os.getenv("V156_TA_ACCEL","3.00")),"ta_volume":float(os.getenv("V156_TA_VOLUME","0.75")),"ta_buy":float(os.getenv("V156_TA_BUY","0.45")),"ta_v15":float(os.getenv("V156_TA_V15","55"))}

def _early_pump_signatures(row,memory,now,btc_off=False,extension=False):
    if not V156_SIGNATURES["enabled"] or btc_off or extension:return {"price_volume":False,"order_flow":False,"trade_accel":False,"score":0,"alert":False,"signals":""}
    p60=_f(row,"price_60s");p10=_f(row,"price_10s");vol=_f(row,"volume_ratio");accel=_f(row,"trade_accel");buy=_f(row,"buy_pressure");v15=_f(row,"v15_score");book=_f(row,"book_imbalance");rs5=_f(row,"relative_strength_5m")
    pv=p60>=V156_SIGNATURES["pv_p60"] and vol>=V156_SIGNATURES["pv_volume"] and accel>=V156_SIGNATURES["pv_accel"] and buy>=V156_SIGNATURES["pv_buy"] and v15>=V156_SIGNATURES["pv_v15"]
    of=p60>=V156_SIGNATURES["of_p60"] and buy>=V156_SIGNATURES["of_buy"] and book>=V156_SIGNATURES["of_book"] and accel>=V156_SIGNATURES["of_accel"] and v15>=V156_SIGNATURES["of_v15"] and rs5>=0
    ta=p60>=V156_SIGNATURES["ta_p60"] and accel>=V156_SIGNATURES["ta_accel"] and vol>=V156_SIGNATURES["ta_volume"] and buy>=V156_SIGNATURES["ta_buy"] and v15>=V156_SIGNATURES["ta_v15"] and p10>0
    score=(35 if pv else 0)+(35 if of else 0)+(30 if ta else 0);signals="|".join(x for x,ok in [("PRICE_VOLUME",pv),("ORDER_FLOW",of),("TRADE_ACCEL",ta)] if ok)
    last=float(memory.get("v156_signature_last_alert",0) or 0);alert=bool(signals) and now-last>=V156_SIGNATURES["cooldown"]
    if alert:memory["v156_signature_last_alert"]=now
    return {"price_volume":pv,"order_flow":of,"trade_accel":ta,"score":score,"alert":alert,"signals":signals}

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

    # V15.6 evidence-based early-pump structure.
    # Accumulation OR a live re-ignition bridge can qualify the setup; requiring
    # both was too restrictive for early discovery.
    structure_metrics = (
        v15 >= V154["v15_min"]
        and opp >= V154["opportunity_min"]
        and conf >= V154["confirmation_min"]
        and not btc_off
    )
    structure_support = (
        acc >= V154["accumulation_min"]
        or (bridge >= 30 and bridge_trigger)
    )

    # Composite Pump Entry Score (0-100). This is intentionally independent
    # of the legacy V15 score so microstructure can identify an early move.
    def _band(v, bands):
        for threshold, points in bands:
            if v >= threshold:
                return points
        return 0

    price_points = _band(p10, [(0.50,20),(0.30,15),(0.15,10),(0.0,5)])
    volume_points = _band(vol, [(4.0,15),(2.5,14),(1.8,11),(1.5,7),(1.0,4)])
    accel_points = _band(accel, [(5.0,20),(3.0,18),(2.0,15),(1.5,10),(1.0,5)])
    buy_points = _band(buy, [(0.85,15),(0.70,13),(0.62,10),(0.55,7),(0.45,4)])
    v15_points = _band(v15, [(80,10),(70,9),(65,7),(60,5),(50,3)])
    acc_points = _band(acc, [(80,5),(70,4),(60,3),(40,2)])
    bridge_points = 5 if bridge >= 60 and bridge_trigger else 4 if bridge >= 40 else 3 if bridge >= 30 and bridge_trigger else 1 if bridge >= 20 else 0
    tv_points = 5 if tvtf >= 4 else 4 if tvtf >= 3 else 2 if tvtf >= 2 else 0
    rs_points = 5 if rs5 >= 0.30 else 3 if rs5 >= 0.15 else 1 if rs5 > 0 else 0
    pump_entry_score = max(0, min(100, round(
        price_points + volume_points + accel_points + buy_points + v15_points +
        acc_points + bridge_points + tv_points + rs_points
    )))

    hard_veto = (
        btc_off
        or p10 <= 0
        or p60 <= 0
        or buy < 0.45
        or rs5 < 0
        or (vol < 1.20 and accel < 1.50)
    )
    spread_penalty = spread > 50
    structure = structure_metrics and structure_support and not hard_veto
    # Stage 3 confirmation keeps the 3-TF gate and requires the early-pump
    # score to have actually earned its way above the 72/100 threshold.
    confirmation = (
        structure
        and tvtf >= V154["tv_bull_tf_min"]
        and pump_entry_score >= 72
        and not spread_penalty
    )

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
    if hard_veto:
        if btc_off: failures.append("BTC_RISK_OFF")
        if p10 <= 0: failures.append("PRICE_10S_NONPOSITIVE")
        if p60 <= 0: failures.append("PRICE_60S_NONPOSITIVE")
        if buy < 0.45: failures.append("BUY_PRESSURE_HARD_VETO")
        if rs5 < 0: failures.append("RS5_NEGATIVE_HARD_VETO")
    if spread_penalty:
        failures.append("WIDE_SPREAD")

    # EARLY_PUMP_BUY = microstructure + V15 structure + either accumulation
    # or a triggered re-ignition bridge, with a 72/100 composite gate.
    early = (
        participation
        and price
        and structure
        and pump_entry_score >= 72
        and not extension
        and not hard_veto
        and not spread_penalty
    )
    pre_watch = _pre_ignition_setup(row, btc_off, early, extension)
    score = pump_entry_score

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
        "v154_pump_entry_score": pump_entry_score,
        "v154_early_pump_buy": bool(early),
        "v154_hard_veto": bool(hard_veto),
        "v154_spread_penalty": bool(spread_penalty),
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

    # V15.6 adaptive high-frequency retention. The live worker already receives
    # 1-second microstructure updates; this flag tells it when to retain a
    # 10-second evidence stream around PRE/REIGNITION/near-ignition candidates.
    adaptive_reason = []
    adaptive_existing = (
        V156_ADAPTIVE["enabled"]
        and not btc_off
        and now < float(memory.get("v156_adaptive_until", 0) or 0)
    )
    if V156_ADAPTIVE["enabled"] and not btc_off:
        if pre_watch:
            adaptive_reason.append("PRE_IGNITION_WATCH")
        if reignition_active:
            adaptive_reason.append("REIGNITION_WATCH")
        if bridge_trigger and bridge >= V156_ADAPTIVE["min_bridge"]:
            adaptive_reason.append("REIGNITION_BRIDGE")
        if (
            p60 >= V156_ADAPTIVE["min_p60"]
            and (
                vol >= V156_ADAPTIVE["min_volume"]
                or buy >= V156_ADAPTIVE["min_buy"]
                or accel >= V156_ADAPTIVE["min_accel"]
            )
            and (
                v15 >= V156_ADAPTIVE["min_v15"]
                or tvtf >= V156_ADAPTIVE["min_tv_bull_tf"]
            )
        ):
            adaptive_reason.append("NEAR_IGNITION")
        if early_stage:
            adaptive_reason.append("EARLY_IGNITION")
        if confirmed:
            adaptive_reason.append("CONFIRMED_IGNITION")
        if adaptive_existing:
            adaptive_reason.append("ACTIVE_CAPTURE_WINDOW")

    signature = _early_pump_signatures(row,memory,now,btc_off,extension)

    # V15.6 FAST-IGNITION is an early-warning lane. It does not alter the
    # V15.4/V15.5 confirmation or BUY gates.
    fast_score = 0
    fast_score += 20 if p60 >= V156_ADAPTIVE["fast_p60"] else 0
    fast_score += 20 if vol >= V156_ADAPTIVE["fast_volume"] and accel >= V156_ADAPTIVE["fast_accel"] and buy >= V156_ADAPTIVE["fast_buy"] else 0
    fast_score += 20 if v15 >= V156_ADAPTIVE["fast_v15"] else 0
    fast_score += 15 if opp >= V156_ADAPTIVE["fast_opp"] else 0
    fast_score += 15 if acc >= V156_ADAPTIVE["fast_acc"] else 0
    fast_score += 10 if conf >= V156_ADAPTIVE["fast_conf"] else 0
    fast_ignition = (
        V156_ADAPTIVE["enabled"] and not btc_off and not extension
        and p60 >= V156_ADAPTIVE["fast_p60"]
        and vol >= V156_ADAPTIVE["fast_volume"]
        and accel >= V156_ADAPTIVE["fast_accel"]
        and buy >= V156_ADAPTIVE["fast_buy"]
        and fast_score >= V156_ADAPTIVE["fast_score_min"]
    )
    fast_last = float(memory.get("v156_fast_last_alert", 0) or 0)
    fast_alert = fast_ignition and (now - fast_last >= V156_ADAPTIVE["fast_cooldown"])
    if fast_alert:
        memory["v156_fast_last_alert"] = now

    # Explicit V15.6 BUY SIGNAL: stricter entry-quality layer on top of
    # early ignition. This is a scanner signal, not a guaranteed outcome.
    buy_signal = bool(
        # Explicitly require the V15.6 state machine to be in EARLY IGNITION.
        # This prevents a strong standalone metric cluster from becoming a BUY.
        early_stage
        and not btc_off
        and not extension
        and not hard_veto
        and not spread_penalty
        and participation
        and price
        and structure
        and pump_entry_score >= 82
        and p60 >= 0.35
        and vol >= 2.00
        and accel >= 2.00
        and buy >= 0.65
        and rs5 >= 0.05
        and v15 >= 70
        and (tvtf >= 3 or acc >= 70 or (bridge_trigger and bridge >= 60))
    )
    buy_score = min(100, int(round(
        pump_entry_score
        + (5 if tvtf >= 4 else 3 if tvtf >= 3 else 0)
        + (3 if buy >= 0.75 else 0)
        + (2 if accel >= 3.0 else 0)
    )))
    buy_last = float(memory.get("v156_buy_last_alert", 0) or 0)
    buy_alert = buy_signal and (now - buy_last >= V156_ADAPTIVE["fast_cooldown"])
    if buy_alert:
        memory["v156_buy_last_alert"] = now

    adaptive_capture = bool(adaptive_reason)
    adaptive_status = "ACTIVE" if adaptive_capture else "OFF"
    adaptive_until = 0.0
    if adaptive_capture:
        adaptive_until = now + V156_ADAPTIVE["capture_window"]
        memory["v156_adaptive_until"] = adaptive_until


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
        "v155_stage": (
            "CONFIRMED IGNITION" if confirmed
            else "PERSISTENCE" if memory.get("status") == "PENDING"
            else "PRE-IGNITION WATCH" if memory.get("status") == "PRE_IGNITION_WATCH"
            else "RE-IGNITION WATCH" if memory.get("status") == "REIGNITION_WATCH"
            else "EARLY IGNITION" if early_stage
            else "WATCH"
        ),
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
        "v156_adaptive_capture": adaptive_capture,
        "v156_adaptive_status": adaptive_status,
        "v156_adaptive_reason": "|".join(adaptive_reason),
        "v156_adaptive_capture_interval": V156_ADAPTIVE["capture_interval"],
        "v156_adaptive_capture_window": V156_ADAPTIVE["capture_window"],
        "v156_adaptive_until": adaptive_until,
        "v156_signature_score": int(signature["score"]),
        "v156_signature_alert": bool(signature["alert"] and not early),
        "v156_signature_price_volume": bool(signature["price_volume"]),
        "v156_signature_order_flow": bool(signature["order_flow"]),
        "v156_signature_trade_accel": bool(signature["trade_accel"]),
        "v156_signature_signals": signature["signals"],
        "v156_fast_score": int(fast_score),
        "v156_fast_ignition": bool(fast_ignition),
        "v156_fast_alert": bool(fast_alert),
        "v156_fast_reason": "|".join([
            x for x, ok in [
                ("P60", p60 >= V156_ADAPTIVE["fast_p60"]),
                ("FLOW", vol >= V156_ADAPTIVE["fast_volume"] and accel >= V156_ADAPTIVE["fast_accel"] and buy >= V156_ADAPTIVE["fast_buy"]),
                ("V15", v15 >= V156_ADAPTIVE["fast_v15"]),
                ("OPP", opp >= V156_ADAPTIVE["fast_opp"]),
                ("ACC", acc >= V156_ADAPTIVE["fast_acc"]),
                ("CONF", conf >= V156_ADAPTIVE["fast_conf"]),
            ] if ok
        ]),
        "v156_buy_signal": bool(buy_signal),
        "v156_buy_alert": bool(buy_alert),
        "v156_buy_score": int(buy_score),
        "v156_buy_reason": "|".join([
            x for x, ok in [
                ("EARLY_IGNITION", early_stage),
                ("P60", p60 >= 0.35),
                ("VOL", vol >= 2.00),
                ("ACCEL", accel >= 2.00),
                ("BUY", buy >= 0.65),
                ("RS5", rs5 >= 0.05),
                ("V15", v15 >= 70),
                ("TV", tvtf >= 3),
                ("ACCUM", acc >= 70),
                ("BRIDGE", bridge_trigger and bridge >= 60),
            ] if ok
        ]),

    }
    return out, event_record
