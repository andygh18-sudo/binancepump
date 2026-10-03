import json, os, time
from pathlib import Path

V157_PATH=Path(os.getenv("V157_LEARNING_PATH","data/v157_observations.jsonl"))

def _safe_float(v, default=0.0):
    try:
        x=float(v)
        return x if x==x and abs(x)<1e12 else default
    except Exception:
        return default

def _stats(state, symbol, sec):
    cut=time.time()-sec
    trades=[z for z in state[symbol]["trades"] if z[0]>=cut]
    if not trades:
        return 0,0,0,0
    notional=sum(z[2] for z in trades)
    buy_notional=sum(z[2] for z in trades if z[3])
    return len(trades),notional,buy_notional/notional if notional else 0,(trades[-1][1]/trades[0][1]-1)*100 if len(trades)>1 else 0

def _accel(v10,v60):
    return v10/max(v60/6.0,1.0)

def _micro_snapshot(symbol, row, state, books):
    try:
        _,v10,cvd_buy10,p10micro=_stats(state,symbol,10)
        _,v30,cvd_buy30,p30micro=_stats(state,symbol,30)
        _,v60,cvd_buy60,p60micro=_stats(state,symbol,60)
        n10=_stats(state,symbol,10)[0]
        cvd10=(2.0*cvd_buy10-v10)/max(v10,1.0)
        cvd30=(2.0*cvd_buy30-v30)/max(v30,1.0)
        cvd60=(2.0*cvd_buy60-v60)/max(v60,1.0)
        buy=_safe_float(row.get("buy_pressure"))
        buy_slope=buy-(cvd_buy30/max(v30,1.0) if v30 else 0.50)
        accel=_safe_float(row.get("trade_accel"))
        accel30=_accel(v30,v60)
        accel_slope=accel-accel30
        vol10_rate=v10/max(v60/6.0,1.0)
        vol30_rate=v30/max(v60/2.0,1.0)
        volume_accel=max(vol10_rate,0)-max(vol30_rate,0)
        cvd_impulse=cvd10-cvd30
        ob=books[symbol].metrics(20)
        return {
            "trades_10s":n10,"flow_10s":_safe_float(v10),"flow_30s":_safe_float(v30),"flow_60s":_safe_float(v60),
            "price_10s_micro":_safe_float(p10micro),"price_30s_micro":_safe_float(p30micro),"price_60s_micro":_safe_float(p60micro),
            "buy_pressure_slope":_safe_float(buy_slope),"trade_accel_slope":_safe_float(accel_slope),
            "volume_10s_rate":_safe_float(vol10_rate),"volume_30s_rate":_safe_float(vol30_rate),
            "volume_accel":_safe_float(volume_accel),"cvd_10s":_safe_float(cvd10),
            "cvd_30s":_safe_float(cvd30),"cvd_60s":_safe_float(cvd60),"cvd_impulse":_safe_float(cvd_impulse),
            "spread_bps":_safe_float(ob.get("spread_bps")),"book_imbalance":_safe_float(ob.get("imbalance")),
            "book_ready":bool(ob.get("ready",False))
        }
    except Exception as exc:
        return {"microstructure_error":str(exc)[:160]}

def persist_v157_observations(rows, fast_candidates, state, books, alerted_symbol="", ts=None):
    """
    Persist a clean V15.7-only learning stream.
    Stores active/near-miss observations plus every qualifying Fastest-Pump
    candidate. This deliberately excludes legacy V15/V15.4/V15.6 learning data.
    """
    stamp=time.time() if ts is None else float(ts)
    candidate_by_symbol={str(r.get("symbol","")).upper():r for r in (fast_candidates or []) if isinstance(r,dict)}
    records=[]
    for row in rows or []:
        if not isinstance(row,dict): continue
        symbol=str(row.get("symbol","")).upper()
        if not symbol: continue
        fast=candidate_by_symbol.get(symbol)
        activity=(
            _safe_float(row.get("price_1m"))>=0.05 or
            _safe_float(row.get("price_5m"))>=0.30 or
            _safe_float(row.get("volume_ratio"))>=1.25 or
            _safe_float(row.get("trade_accel"))>=1.25 or
            _safe_float(row.get("buy_pressure"))>=0.57 or
            _safe_float(row.get("v15_score"))>=60
        )
        if fast is None and not activity:
            continue
        micro=_micro_snapshot(symbol,row,state,books)
        rec={
            "event":"OBS","v157_version":"15.7","ts":stamp,"symbol":symbol,
            "price":_safe_float(row.get("price")),"price_10s":_safe_float(row.get("price_10s")),
            "price_1m":_safe_float(row.get("price_1m")),"price_3m":_safe_float(row.get("price_3m")),
            "price_5m":_safe_float(row.get("price_5m")),"price_10m":_safe_float(row.get("price_10m")),
            "price_15m":_safe_float(row.get("price_15m")),"price_60m":_safe_float(row.get("price_60m_change")),
            "volume_ratio":_safe_float(row.get("volume_ratio")),"trade_accel":_safe_float(row.get("trade_accel")),
            "buy_pressure":_safe_float(row.get("buy_pressure")),"rs5":_safe_float(row.get("v15_relative_strength_5m")),
            "v15_score":_safe_float(row.get("v15_score")),"accumulation_score":_safe_float(row.get("accumulation_score")),
            "exhaustion_score":_safe_float(row.get("exhaustion_score")),
            "btc_risk_off":bool(row.get("v15_btc_risk_off",False)),
            "fast_pump_qualifies":bool(fast is not None),
            "fast_pump_alert":bool(symbol==str(alerted_symbol or "").upper()),
            "fast_pump_score":_safe_float(fast.get("_fast_pump_score")) if fast else 0.0,
            "fast_pump_pre_score":_safe_float(fast.get("_fast_pump_pre_score")) if fast else 0.0,
            "fast_pump_stage":str(fast.get("_fast_pump_stage","")) if fast else "NOT_QUALIFIED",
            "fast_dynamic_exhaustion":_safe_float(fast.get("_fast_dynamic_exhaustion")) if fast else _safe_float(row.get("exhaustion_score")),
            "early_momentum_score":_safe_float(row.get("early_momentum_score")),
            "early_momentum_stage":str(row.get("early_momentum_stage","MONITOR")),
            "early_momentum_quality":bool(row.get("early_momentum_quality",False)),
            "early_momentum_trades_10s":int(_safe_float(row.get("early_momentum_trades_10s"))),
            "early_momentum_trade_rate":_safe_float(row.get("early_momentum_trade_rate")),
            "early_momentum_accel_slope":_safe_float(row.get("early_momentum_accel_slope")),
            "early_momentum_buy_ratio":_safe_float(row.get("early_momentum_buy_ratio")),
            "early_momentum_cvd_10s":_safe_float(row.get("early_momentum_cvd_10s")),
            "early_momentum_cvd_30s":_safe_float(row.get("early_momentum_cvd_30s")),
            "early_momentum_cvd_60s":_safe_float(row.get("early_momentum_cvd_60s")),
            "early_momentum_cvd_impulse":_safe_float(row.get("early_momentum_cvd_impulse")),
            "early_momentum_spread_bps":_safe_float(row.get("early_momentum_spread_bps")),
            "early_momentum_book_ready":bool(row.get("early_momentum_book_ready",False)),
            "v158_discovery_score":_safe_float(row.get("v158_discovery_score")),
            "v158_discovery_velocity_pct_s":_safe_float(row.get("v158_discovery_velocity_pct_s")),
            "v158_discovery_trade_anomaly":_safe_float(row.get("v158_discovery_trade_anomaly")),
            "v158_dynamic_promoted":bool(row.get("v158_dynamic_promoted",False)),
            "v158_score":_safe_float(row.get("v158_score")),
            "v158_whale_score":_safe_float(row.get("v158_whale_score")),
            "v158_large_trade_count":int(_safe_float(row.get("v158_large_trade_count"))),
            "v158_large_buy_notional":_safe_float(row.get("v158_large_buy_notional")),
            "v158_large_sell_notional":_safe_float(row.get("v158_large_sell_notional")),
            "v158_large_trade_imbalance":_safe_float(row.get("v158_large_trade_imbalance")),
            "v158_median_trade_notional":_safe_float(row.get("v158_median_trade_notional")),
            "v158_p95_trade_notional":_safe_float(row.get("v158_p95_trade_notional")),
            "v158_liquidity_score":_safe_float(row.get("v158_liquidity_score")),
            "v158_ask_depth_change":_safe_float(row.get("v158_ask_depth_change")),
            "v158_bid_depth_change":_safe_float(row.get("v158_bid_depth_change")),
            "v158_ask_consumption":_safe_float(row.get("v158_ask_consumption")),
            "v158_bid_consumption":_safe_float(row.get("v158_bid_consumption")),
            "v158_absorption_score":_safe_float(row.get("v158_absorption_score")),
            "v158_liquidity_breakout_score":_safe_float(row.get("v158_liquidity_breakout_score")),
            "v158_data_quality":_safe_float(row.get("v158_data_quality")),
            "v158_adaptive_score":_safe_float(row.get("v158_adaptive_score")),
            "adaptive_trade_z":_safe_float(row.get("adaptive_trade_z")),
            "adaptive_volume_z":_safe_float(row.get("adaptive_volume_z")),
            "adaptive_trade_size_z":_safe_float(row.get("adaptive_trade_size_z")),
            "adaptive_cvd_z":_safe_float(row.get("adaptive_cvd_z")),
            "adaptive_intensity_z":_safe_float(row.get("adaptive_intensity_z")),
            "adaptive_price_impact_z":_safe_float(row.get("adaptive_price_impact_z")),
            "adaptive_regime_change":_safe_float(row.get("adaptive_regime_change")),
            "v158_book_vacuum_score":_safe_float(row.get("v158_book_vacuum_score")),
            "v158_ask_consumption_z":_safe_float(row.get("v158_ask_consumption_z")),
            "v158_price_impact_efficiency":_safe_float(row.get("v158_price_impact_efficiency")),
            "v158_xvenue_ret_pct":_safe_float(row.get("v158_xvenue_ret_pct")),
            "v158_xvenue_confirmed":bool(row.get("v158_xvenue_confirmed",False)),
            "v158_trade_age_s":_safe_float(row.get("v158_trade_age_s"),999.0),
            "v158_book_age_s":_safe_float(row.get("v158_book_age_s"),999.0),
        }
        rec.update(micro)
        records.append(rec)
    if not records:
        return 0
    V157_PATH.parent.mkdir(parents=True,exist_ok=True)
    with V157_PATH.open("a",encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec,separators=(",",":"),allow_nan=False)+"\n")
    return len(records)
