"""V15.6 production early-pump architecture.

Stages:
WATCH -> PRE-IGNITION WATCH -> EARLY IGNITION -> PERSISTENCE -> CONFIRMED IGNITION

Revised sweet spot is calibrated for early discovery rather than requiring the
older V15.4/V15.5 gates to fire simultaneously. TV alignment is confirmation,
not an early hard gate; re-ignition bridge is an enhancer only.
"""
import os, time, uuid

V156_VERSION = "15.6"

CFG = {
    "watch_p60": float(os.getenv("V156_WATCH_P60", "0.10")),
    "watch_volume": float(os.getenv("V156_WATCH_VOLUME", "1.50")),
    "watch_accel": float(os.getenv("V156_WATCH_ACCEL", "1.25")),
    "watch_buy": float(os.getenv("V156_WATCH_BUY", "0.55")),
    "early_p10": float(os.getenv("V156_EARLY_P10", "0.20")),
    "early_p60": float(os.getenv("V156_EARLY_P60", "0.35")),
    "early_volume": float(os.getenv("V156_EARLY_VOLUME", "2.00")),
    "early_accel": float(os.getenv("V156_EARLY_ACCEL", "2.00")),
    "early_buy": float(os.getenv("V156_EARLY_BUY", "0.65")),
    "early_rs5": float(os.getenv("V156_EARLY_RS5", "0.00")),
    "early_v15": float(os.getenv("V156_EARLY_V15", "70")),
    "early_opp": float(os.getenv("V156_EARLY_OPP", "65")),
    "early_conf": float(os.getenv("V156_EARLY_CONF", "70")),
    "early_acc": float(os.getenv("V156_EARLY_ACC", "65")),
    "persist_min": float(os.getenv("V156_PERSIST_MIN_SECONDS", "10")),
    "persist_max": float(os.getenv("V156_PERSIST_MAX_SECONDS", "20")),
    "persist_p10": float(os.getenv("V156_PERSIST_P10", "0.00")),
    "persist_p60": float(os.getenv("V156_PERSIST_P60", "0.00")),
    "persist_volume": float(os.getenv("V156_PERSIST_VOLUME", "1.25")),
    "persist_accel": float(os.getenv("V156_PERSIST_ACCEL", "1.50")),
    "persist_buy": float(os.getenv("V156_PERSIST_BUY", "0.58")),
    "persist_v15": float(os.getenv("V156_PERSIST_V15", "65")),
    "persist_rs5": float(os.getenv("V156_PERSIST_RS5", "0.00")),
    "exhaustion_max": float(os.getenv("V156_EXHAUSTION_MAX", "35")),
    "wide_spread": float(os.getenv("V156_WIDE_SPREAD_BPS", "50")),
    "cooldown": float(os.getenv("V156_COOLDOWN", "180")),
}

def _f(row, k, d=0.0):
    try: return float(row.get(k, d) or d)
    except (TypeError, ValueError): return d

def _i(row, k, d=0):
    try: return int(row.get(k, d) or d)
    except (TypeError, ValueError): return d

def _sweet_score(p10,p60,vol,accel,buy,v15,opp,conf,acc,rs5,bridge,tvtf):
    pts = 0
    pts += 15 if p10 >= .30 else 10 if p10 >= .20 else 5 if p10 > 0 else 0
    pts += 15 if p60 >= .50 else 12 if p60 >= .35 else 7 if p60 >= .20 else 0
    pts += 15 if vol >= 3.0 else 12 if vol >= 2.0 else 7 if vol >= 1.5 else 0
    pts += 15 if accel >= 3.0 else 12 if accel >= 2.0 else 7 if accel >= 1.5 else 0
    pts += 12 if buy >= .75 else 10 if buy >= .65 else 6 if buy >= .58 else 0
    pts += 10 if v15 >= 80 else 8 if v15 >= 70 else 5 if v15 >= 65 else 0
    pts += 8 if opp >= 75 else 6 if opp >= 65 else 0
    pts += 8 if conf >= 80 else 6 if conf >= 70 else 0
    pts += 5 if acc >= 75 else 4 if acc >= 65 else 0
    pts += 4 if rs5 >= .10 else 2 if rs5 >= 0 else 0
    pts += 3 if tvtf >= 3 else 2 if tvtf >= 2 else 0
    if bridge >= 60: pts += 2
    return min(100, pts)

def evaluate(row, memory, now=None):
    now = time.time() if now is None else float(now)
    symbol = str(row.get("symbol", "")).upper()
    p10,p60 = _f(row,"price_10s"),_f(row,"price_60s")
    vol,accel,buy = _f(row,"volume_ratio"),_f(row,"trade_accel"),_f(row,"buy_pressure")
    rs5 = _f(row,"relative_strength_5m")
    v15,opp,conf,acc = (_f(row,k) for k in ("v15_score","v15_opportunity_score","v15_confirmation_score","accumulation_score"))
    tvtf = _i(row,"tv_bullish_timeframes")
    bridge = _f(row,"v15_reignition_bridge_score")
    bridge_trigger = bool(row.get("v15_reignition_bridge_trigger",False))
    exhaustion = _f(row,"exhaustion_score")
    spread = _f(row,"spread_bps")
    btc_off = bool(row.get("v15_btc_risk_off",False))

    hard_veto = btc_off or p10 <= 0 or p60 <= 0 or buy < .50 or rs5 < 0 or spread > CFG["wide_spread"]
    exhaustion_veto = exhaustion > CFG["exhaustion_max"] and exhaustion > 0
    hard_veto = hard_veto or exhaustion_veto

    watch = (not btc_off and not exhaustion_veto and p60 >= CFG["watch_p60"] and
             vol >= CFG["watch_volume"] and accel >= CFG["watch_accel"] and buy >= CFG["watch_buy"])

    structure = (v15 >= CFG["early_v15"] and opp >= CFG["early_opp"] and
                 conf >= CFG["early_conf"] and (acc >= CFG["early_acc"] or (bridge_trigger and bridge >= 60)))
    early = (not hard_veto and p10 >= CFG["early_p10"] and p60 >= CFG["early_p60"] and
             vol >= CFG["early_volume"] and accel >= CFG["early_accel"] and buy >= CFG["early_buy"] and
             rs5 >= CFG["early_rs5"] and structure)

    sweet = _sweet_score(p10,p60,vol,accel,buy,v15,opp,conf,acc,rs5,bridge,tvtf)
    status = str(memory.get("status","IDLE"))
    trigger_ts = float(memory.get("trigger_ts",0) or 0)
    age = max(0.0, now-trigger_ts) if status == "PENDING" else 0.0
    event = None

    if early and status in ("IDLE","FAILED","FAILED_PERSISTENCE","COMPLETED","CANCELLED"):
        if now-float(memory.get("last_alert",0) or 0) >= CFG["cooldown"]:
            eid = uuid.uuid4().hex[:12]
            memory.clear()
            memory.update({"status":"PENDING","stage":"EARLY IGNITION","event_id":eid,
                "trigger_ts":now,"last_alert":now,"trigger_price":_f(row,"price"),
                "trigger_p10":p10,"trigger_p60":p60,"trigger_volume":vol,"trigger_accel":accel,
                "trigger_buy":buy,"trigger_rs5":rs5,"trigger_v15":v15,"trigger_sweet_score":sweet})
            event={"event":"TRIGGER","version":V156_VERSION,"event_id":eid,"symbol":symbol,"ts":now,
                   "stage":"EARLY IGNITION","alert":True,"sweet_score":sweet,"price":_f(row,"price"),
                   "p10":p10,"p60":p60,"volume":vol,"accel":accel,"buy":buy,"rs5":rs5,"v15":v15}
            status="PENDING"; age=0

    if status == "PENDING":
        failures=[]
        if p10 <= CFG["persist_p10"]: failures.append("PRICE_10S_LOST")
        if p60 <= CFG["persist_p60"]: failures.append("PRICE_60S_LOST")
        if vol < CFG["persist_volume"]: failures.append("VOLUME_COLLAPSE")
        if accel < CFG["persist_accel"]: failures.append("ACCEL_COLLAPSE")
        if buy < CFG["persist_buy"]: failures.append("BUY_PRESSURE_COLLAPSE")
        if rs5 < CFG["persist_rs5"]: failures.append("RS5_LOST")
        if v15 < CFG["persist_v15"]: failures.append("V15_CONFIRMATION_LOST")
        if btc_off: failures.append("BTC_RISK_OFF")
        if exhaustion_veto: failures.append("EXHAUSTION_VETO")
        memory["failures"]=failures; memory["last_update"]=now
        hard_fail=any(x in failures for x in ("PRICE_10S_LOST","PRICE_60S_LOST","BUY_PRESSURE_COLLAPSE","V15_CONFIRMATION_LOST","BTC_RISK_OFF","EXHAUSTION_VETO"))
        if hard_fail or (age >= CFG["persist_min"] and len(failures) >= 2):
            memory["status"]="FAILED_PERSISTENCE"; memory["stage"]="WATCH"
            event={"event":"RESOLVED","version":V156_VERSION,"event_id":memory.get("event_id",""),"symbol":symbol,"ts":now,
                   "outcome":"FAILED_PERSISTENCE","persistence_seconds":round(age,1),"failures":failures,"sweet_score":sweet}
        elif age >= CFG["persist_min"] and age <= CFG["persist_max"] and not failures:
            memory["status"]="CONFIRMED_IGNITION"; memory["stage"]="CONFIRMED IGNITION"; memory["confirmed_ts"]=now
            event={"event":"RESOLVED","version":V156_VERSION,"event_id":memory.get("event_id",""),"symbol":symbol,"ts":now,
                   "outcome":"CONFIRMED_IGNITION","persistence_seconds":round(age,1),"failures":[],"sweet_score":sweet}
        elif age > CFG["persist_max"]:
            memory["status"]="FAILED_PERSISTENCE"; memory["stage"]="WATCH"
            event={"event":"RESOLVED","version":V156_VERSION,"event_id":memory.get("event_id",""),"symbol":symbol,"ts":now,
                   "outcome":"PERSISTENCE_TIMEOUT","persistence_seconds":round(age,1),"failures":failures,"sweet_score":sweet}

    if memory.get("status") == "CONFIRMED_IGNITION" and now-float(memory.get("confirmed_ts",now)) >= CFG["cooldown"]:
        memory["status"]="COMPLETED"; memory["stage"]="WATCH"

    status=str(memory.get("status","IDLE"))
    if status == "PENDING": stage="PERSISTENCE"
    elif status == "CONFIRMED_IGNITION": stage="CONFIRMED IGNITION"
    elif watch: stage="PRE-IGNITION WATCH"
    else: stage="WATCH"

    buy_signal = status == "CONFIRMED_IGNITION"
    return {
        "v156_version":V156_VERSION,"v156_watch":watch,"v156_early_ignition":early,
        "v156_stage":stage,"v156_status":status,"v156_sweet_score":sweet,
        "v156_persistence_seconds":round(age,1),"v156_persistence_failures":"|".join(memory.get("failures",[])),
        "v156_buy_signal":buy_signal,"v156_alert":bool(event and event.get("event")=="TRIGGER"),
        "v156_event_id":memory.get("event_id",""),"v156_trigger_p10":_f(memory,"trigger_p10"),
        "v156_trigger_p60":_f(memory,"trigger_p60"),"v156_trigger_volume":_f(memory,"trigger_volume"),
        "v156_trigger_accel":_f(memory,"trigger_accel"),"v156_trigger_buy":_f(memory,"trigger_buy"),
        "v156_trigger_v15":_f(memory,"trigger_v15"),"v156_bridge_bonus":5 if bridge_trigger and bridge>=60 else 3 if bridge>=60 else 0,
        "v156_fast_score":sweet,"v156_fast_ignition":early,"v156_fast_alert":bool(event and event.get("event")=="TRIGGER"),
        "v156_fast_reason":"SWEET_SPOT" if early else "WATCH" if watch else "",
        "v156_signature_score":sweet,"v156_signature_alert":False,"v156_signature_signals":"SWEET_SPOT" if early else "",
        "v156_buy_alert":buy_signal,"v156_buy_score":sweet,
        "v156_buy_reason":"CONFIRMED_IGNITION" if buy_signal else "|".join([
            x for x,ok in [("P10",p10>=.20),("P60",p60>=.35),("VOL",vol>=2.0),("ACCEL",accel>=2.0),
                           ("BUY",buy>=.65),("V15",v15>=70),("OPP",opp>=65),("CONF",conf>=70),
                           ("ACCUM",acc>=65),("TV",tvtf>=3)] if ok])
    }, event
