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
    "early_p10": float(os.getenv("V156_EARLY_P10", "0.15")),
    "early_p60": float(os.getenv("V156_EARLY_P60", "0.30")),
    "early_volume": float(os.getenv("V156_EARLY_VOLUME", "2.00")),
    "early_accel": float(os.getenv("V156_EARLY_ACCEL", "1.75")),
    "early_buy": float(os.getenv("V156_EARLY_BUY", "0.65")),
    "early_rs5": float(os.getenv("V156_EARLY_RS5", "0.00")),
    "early_v15": float(os.getenv("V156_EARLY_V15", "65")),
    "early_opp": float(os.getenv("V156_EARLY_OPP", "60")),
    "early_conf": float(os.getenv("V156_EARLY_CONF", "65")),
    "early_acc": float(os.getenv("V156_EARLY_ACC", "65")),
    # Path B: delayed-price-confirmation / re-ignition route.
    # Stricter price/structure confirmation permits buy pressure >=60%.
    "path_b_p10": float(os.getenv("V156_PATH_B_P10", "0.30")),
    "path_b_p60": float(os.getenv("V156_PATH_B_P60", "0.45")),
    "path_b_volume": float(os.getenv("V156_PATH_B_VOLUME", "2.25")),
    "path_b_accel": float(os.getenv("V156_PATH_B_ACCEL", "2.50")),
    "path_b_buy": float(os.getenv("V156_PATH_B_BUY", "0.60")),
    "path_b_v15": float(os.getenv("V156_PATH_B_V15", "75")),
    "path_b_opp": float(os.getenv("V156_PATH_B_OPP", "65")),
    "path_b_conf": float(os.getenv("V156_PATH_B_CONF", "75")),
    "path_b_acc": float(os.getenv("V156_PATH_B_ACC", "70")),
    "path_b_rs5": float(os.getenv("V156_PATH_B_RS5", "0.10")),
    "path_b_tv_tf": int(os.getenv("V156_PATH_B_TV_TF", "4")),
    "path_b_bridge": float(os.getenv("V156_PATH_B_BRIDGE", "15")),
    "path_b_exhaustion": float(os.getenv("V156_PATH_B_EXHAUSTION", "30")),
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
    "persist_max_soft_failures": int(os.getenv("V156_PERSIST_MAX_SOFT_FAILURES", "1")),
    "persist_min_enhancers": int(os.getenv("V156_PERSIST_MIN_ENHANCERS", "2")),
    # V15.8 Tier-1 fast ignition: price/trade-flow can lead rolling volume.
    "v158_tier1_enabled": os.getenv("V158_TIER1_ENABLED", "1") == "1",
    "v158_tier1_p10": float(os.getenv("V158_TIER1_P10", "0.10")),
    "v158_tier1_accel": float(os.getenv("V158_TIER1_ACCEL", "3.50")),
    "v158_tier1_buy": float(os.getenv("V158_TIER1_BUY", "0.65")),
    "v158_tier1_rs5": float(os.getenv("V158_TIER1_RS5", "-0.10")),
    "v158_tier1_support": float(os.getenv("V158_TIER1_SUPPORT", "55")),
    "v158_tier1_spread": float(os.getenv("V158_TIER1_SPREAD", "35")),
    "v158_tier1_volume_confirm": float(os.getenv("V158_TIER1_VOLUME_CONFIRM", "1.25")),
    "v158_tier1_min_confirm": int(os.getenv("V158_TIER1_MIN_CONFIRM", "2")),
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
    v158_pre = _f(row,"v158_pre_ignition_score")
    v158_liq = _f(row,"v158_liquidity_state_score")
    v158_part = _f(row,"v158_participation_score")
    v158_dir = _f(row,"v158_directional_score")
    xvenue = _f(row,"v158_cross_venue_confidence")

    hard_veto = btc_off or p10 <= 0 or p60 <= 0 or buy < .50 or rs5 < 0 or spread > CFG["wide_spread"]
    exhaustion_veto = exhaustion > CFG["exhaustion_max"] and exhaustion > 0
    hard_veto = hard_veto or exhaustion_veto

    watch_core = (not btc_off and not exhaustion_veto and p60 >= CFG["watch_p60"] and
                  vol >= CFG["watch_volume"] and accel >= CFG["watch_accel"] and buy >= CFG["watch_buy"])
    watch_confirmation = sum(bool(x) for x in [
        v15 >= 55, opp >= 55, conf >= 60, tvtf >= 3, (bridge_trigger and bridge >= 15)
    ])
    watch = watch_core and watch_confirmation >= 2

    # Structural confirmation is deliberately combination-based. This allows
    # genuine reset/re-ignition sequences to trigger earlier without lowering
    # the core price/flow requirements.
    confirmation_flags = [
        v15 >= CFG["early_v15"],
        opp >= CFG["early_opp"],
        conf >= CFG["early_conf"],
        tvtf >= 3,
        (bridge_trigger and bridge >= 15),
    ]
    confirmation_count = sum(bool(x) for x in confirmation_flags)
    structure = (
        acc >= CFG["early_acc"] and
        confirmation_count >= 2
    )
    path_a = (not hard_veto and
              p10 >= CFG["early_p10"] and p60 >= CFG["early_p60"] and
              vol >= CFG["early_volume"] and accel >= CFG["early_accel"] and
              buy >= CFG["early_buy"] and rs5 >= CFG["early_rs5"] and structure)

    # Path B is an alternative to Path A, not a relaxation of it. It is designed
    # for strong price-confirmed re-ignition where buy pressure is slightly below
    # the normal 65% gate. Every Path B condition is required.
    path_b = (not hard_veto and
              p10 >= CFG["path_b_p10"] and p60 >= CFG["path_b_p60"] and
              vol >= CFG["path_b_volume"] and accel >= CFG["path_b_accel"] and
              buy >= CFG["path_b_buy"] and v15 >= CFG["path_b_v15"] and
              opp >= CFG["path_b_opp"] and conf >= CFG["path_b_conf"] and
              acc >= CFG["path_b_acc"] and rs5 >= CFG["path_b_rs5"] and
              tvtf >= CFG["path_b_tv_tf"] and
              bridge_trigger and bridge >= CFG["path_b_bridge"] and
              exhaustion <= CFG["path_b_exhaustion"])

    v158_support = max(v158_pre, v158_liq, v158_part, v158_dir)
    tier1_fast = (CFG["v158_tier1_enabled"] and not hard_veto and p10 >= CFG["v158_tier1_p10"] and accel >= CFG["v158_tier1_accel"] and buy >= CFG["v158_tier1_buy"] and rs5 >= CFG["v158_tier1_rs5"] and spread <= CFG["v158_tier1_spread"] and v158_support >= CFG["v158_tier1_support"])
    early = path_a or path_b or tier1_fast

    # Explicit price/participation divergence veto: a positive price move
    # without fresh participation/acceleration is not an early pump.
    participation_divergence = (
        p60 > 0 and vol < 1.25 and accel < 1.50
    )
    if participation_divergence:
        path_a = False
        path_b = False
        early = False

    v158_enhancers = sum(bool(x) for x in [v158_pre >= 55, v158_liq >= 60, v158_part >= 60, v158_dir >= 60, xvenue >= 60])
    enhancer_bonus = min(8, v158_enhancers * 2)
    sweet = min(100, _sweet_score(p10,p60,vol,accel,buy,v15,opp,conf,acc,rs5,bridge,tvtf) + enhancer_bonus)
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
                "trigger_buy":buy,"trigger_rs5":rs5,"trigger_v15":v15,"trigger_sweet_score":sweet,
                "trigger_path":"B" if path_b else "A"})
            event={"event":"TRIGGER","version":V156_VERSION,"event_id":eid,"symbol":symbol,"ts":now,
                   "stage":"EARLY IGNITION","alert":True,"path":"B" if path_b else "A","sweet_score":sweet,"price":_f(row,"price"),
                   "p10":p10,"p60":p60,"volume":vol,"accel":accel,"buy":buy,"rs5":rs5,"v15":v15}
            status="PENDING"; age=0

    tier1b_volume_ok = False
    tier1b_support_count = 0
    if status == "PENDING":
        tier1b_volume_ok = vol >= CFG["v158_tier1_volume_confirm"]
        tier1b_support_count = sum(bool(x) for x in [v158_pre >= CFG["v158_tier1_support"], v158_liq >= 60, v158_part >= 60, v158_dir >= 60, xvenue >= 60])
        failures=[]
        if p10 <= CFG["persist_p10"]: failures.append("PRICE_10S_LOST")
        if p60 <= CFG["persist_p60"]: failures.append("PRICE_60S_LOST")
        if vol < CFG["persist_volume"] and not (tier1_fast and accel >= CFG["v158_tier1_accel"] and buy >= CFG["v158_tier1_buy"]): failures.append("VOLUME_COLLAPSE")
        if accel < CFG["persist_accel"]: failures.append("ACCEL_COLLAPSE")
        if buy < CFG["persist_buy"]: failures.append("BUY_PRESSURE_COLLAPSE")
        if rs5 < CFG["persist_rs5"]: failures.append("RS5_LOST")
        if v15 < CFG["persist_v15"]: failures.append("V15_CONFIRMATION_LOST")
        if btc_off: failures.append("BTC_RISK_OFF")
        if exhaustion_veto: failures.append("EXHAUSTION_VETO")
        memory["failures"]=failures; memory["last_update"]=now
        hard_fail=any(x in failures for x in ("PRICE_10S_LOST","PRICE_60S_LOST","BUY_PRESSURE_COLLAPSE","V15_CONFIRMATION_LOST","BTC_RISK_OFF","EXHAUSTION_VETO"))
        soft_failures=[x for x in failures if x in ("VOLUME_COLLAPSE","ACCEL_COLLAPSE","RS5_LOST")]
        persistence_strength=sum(bool(x) for x in [
            p10 >= _f(memory,"trigger_p10") if _f(memory,"trigger_p10") > 0 else p10 > 0,
            p60 >= _f(memory,"trigger_p60") if _f(memory,"trigger_p60") > 0 else p60 > 0,
            buy >= max(_f(memory,"trigger_buy")*.90, CFG["persist_buy"]),
            accel >= max(_f(memory,"trigger_accel")*.90, CFG["persist_accel"]),
            v158_pre >= 55, v158_liq >= 60, v158_part >= 60, v158_dir >= 60
        ])
        memory["persistence_strength"]=persistence_strength; memory["v158_enhancers"]=v158_enhancers
        tier1b_ready = tier1b_volume_ok
        confirmable=(age >= CFG["persist_min"] and age <= CFG["persist_max"] and not hard_fail and len(soft_failures) <= CFG["persist_max_soft_failures"] and tier1b_ready and (not soft_failures or (persistence_strength >= 3 and v158_enhancers >= CFG["persist_min_enhancers"])))
        if hard_fail or (age >= CFG["persist_min"] and len(failures) >= 2 and not confirmable):
            memory["status"]="FAILED_PERSISTENCE"; memory["stage"]="WATCH"
            event={"event":"RESOLVED","version":V156_VERSION,"event_id":memory.get("event_id",""),"symbol":symbol,"ts":now,
                   "outcome":"FAILED_PERSISTENCE","persistence_seconds":round(age,1),"failures":failures,"sweet_score":sweet,
                   "persistence_strength":persistence_strength,"v158_enhancers":v158_enhancers}
        elif confirmable:
            memory["status"]="CONFIRMED_IGNITION"; memory["stage"]="CONFIRMED IGNITION"; memory["confirmed_ts"]=now
            event={"event":"RESOLVED","version":V156_VERSION,"event_id":memory.get("event_id",""),"symbol":symbol,"ts":now,
                   "outcome":"CONFIRMED_IGNITION","persistence_seconds":round(age,1),"failures":failures,"sweet_score":sweet,
                   "persistence_strength":persistence_strength,"v158_enhancers":v158_enhancers}
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
        "v156_path_a":path_a,"v156_path_b":path_b,"v156_path_v158_tier1":tier1_fast,
        "v156_path":("V158-TIER1" if tier1_fast else "B" if path_b else "A" if path_a else ""),
        "v158_tier1":bool(tier1_fast),
        "v158_tier1b_volume_ok":bool(status == "PENDING" and vol >= CFG["v158_tier1_volume_confirm"]),
        "v158_tier1b_support_count":int(tier1b_support_count if status == "PENDING" else 0),
        "v158_tier1c_confirmed":bool(status == "CONFIRMED_IGNITION" and (vol >= CFG["v158_tier1_volume_confirm"] or v158_enhancers >= CFG["persist_min_enhancers"])),
        "v156_stage":stage,"v156_status":status,"v156_sweet_score":sweet,
        "v156_persistence_seconds":round(age,1),"v156_persistence_failures":"|".join(memory.get("failures",[])),
        "v156_persistence_strength":int(memory.get("persistence_strength",0) or 0),"v156_v158_enhancers":int(v158_enhancers),"v156_enhancer_bonus":int(enhancer_bonus),
        "v156_buy_signal":buy_signal,"v156_alert":bool(event and event.get("event")=="TRIGGER"),
        "v156_event_id":memory.get("event_id",""),"v156_trigger_p10":_f(memory,"trigger_p10"),
        "v156_trigger_p60":_f(memory,"trigger_p60"),"v156_trigger_volume":_f(memory,"trigger_volume"),
        "v156_trigger_accel":_f(memory,"trigger_accel"),"v156_trigger_buy":_f(memory,"trigger_buy"),
        "v156_trigger_v15":_f(memory,"trigger_v15"),"v156_trigger_path":str(memory.get("trigger_path","")),"v156_bridge_bonus":5 if bridge_trigger and bridge>=15 else 3 if bridge>=15 else 0,
        "v156_fast_score":sweet,"v156_fast_ignition":early,"v156_fast_alert":bool(event and event.get("event")=="TRIGGER"),
        "v156_fast_reason":"V158_TIER1_FAST_IGNITION" if tier1_fast else "SWEET_SPOT" if early else "WATCH" if watch else "",
        "v156_signature_score":sweet,"v156_signature_alert":False,"v156_signature_signals":"SWEET_SPOT" if early else "",
        "v156_buy_alert":buy_signal,"v156_buy_score":sweet,
        "v156_path_b_reason":"PATH_B_STRONG_PRICE_STRUCTURAL_CONFIRMATION" if path_b else "",
        "v156_buy_reason":"CONFIRMED_IGNITION" if buy_signal else "|".join([
            x for x,ok in [("P10",p10>=.15),("P60",p60>=.30),("VOL",vol>=2.0),("ACCEL",accel>=1.75),
                           ("BUY",buy>=.65),("V15",v15>=65),("OPP",opp>=60),("CONF",conf>=65),
                           ("ACCUM",acc>=65),("TV",tvtf>=3),("BRIDGE",bridge_trigger and bridge>=15)] if ok])
    }, event
