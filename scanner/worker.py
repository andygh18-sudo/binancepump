"""V15 worker bootstrap.

Loads the last clean V15.2 worker source and injects the local V15.4 engine.
This keeps the production worker source recoverable while V15.4 remains
experimental and parallel to V15.
"""
import urllib.request

SOURCE_URL = "https://raw.githubusercontent.com/andygh18-sudo/binancepump/cb0276aeb5786b4b8d8cf8fbaf5ff4d29e656b6c/scanner/worker.py"

def _load():
    req=urllib.request.Request(SOURCE_URL,headers={"User-Agent":"Binance-Pump-Scanner"})
    with urllib.request.urlopen(req,timeout=20) as r:
        return r.read().decode("utf-8")

src=_load()
src=src.replace(
    "from .history_store import append_scan_history, append_microstructure_history",
    "from .history_store import append_scan_history, append_microstructure_history\nfrom .v154 import evaluate as v154_evaluate"
)
src=src.replace("await telegram(", "await legacy_telegram(")
src=src.replace(
    "async def telegram(msg):",
    '''def apply_v154(rows):
    events=[]
    for r in rows:
        s=str(r.get("symbol","")).upper()
        if not s: continue
        result,event_record=v154_evaluate(r,state[s].setdefault("v154",{})); r.update(result)
        if event_record: events.append(event_record)
    return events

def persist_v154_events(events):
    if not events:return
    os.makedirs("data",exist_ok=True)
    with open("data/v154_events.jsonl","a",encoding="utf-8") as f:
        for event in events:f.write(json.dumps(event,separators=(",",":"))+"\\n")

def persist_adaptive_capture(rows):
    now=time.time();os.makedirs("data",exist_ok=True);path="data/adaptive_microstructure_history.jsonl";interval=float(os.getenv("V156_CAPTURE_INTERVAL","10"));records=[]
    for r in rows:
        if not isinstance(r,dict) or not r.get("symbol") or not r.get("v156_adaptive_capture"):continue
        mem=state[str(r["symbol"]).upper()];last=float(mem.get("v156_adaptive_last_capture",0) or 0)
        if now-last<interval:continue
        mem["v156_adaptive_last_capture"]=now
        fields=["symbol","price","price_10s","price_60s","price_1m","volume_ratio","trade_accel","buy_pressure","book_imbalance","spread_bps","v15_score","v15_opportunity_score","v15_confirmation_score","accumulation_score","v15_ignition_score","v15_ignition_stage","v15_reignition_bridge_score","v15_reignition_bridge_stage","v15_reignition_bridge_trigger","v15_relative_strength_5m","v15_relative_strength_15m","v15_btc_risk_off","tv_confirmation","tv_bullish_timeframes","pump_momentum_score","exhaustion_score","exhaustion_state","v154_score","v154_stage","v154_persistence_status","v156_adaptive_reason","v156_signature_score","v156_signature_signals","v156_signature_price_volume","v156_signature_order_flow","v156_signature_trade_accel"]
        rec={k:r.get(k) for k in fields if k in r};rec["ts"]=now;rec["capture_interval_seconds"]=interval;rec["capture_window_seconds"]=float(os.getenv("V156_CAPTURE_WINDOW","120"));records.append(rec)
    if records:
        with open(path,"a",encoding="utf-8") as f:
            for rec in records:f.write(json.dumps(rec,separators=(",",":"))+"\\n")

async def send_v156_main_alerts(rows):
    candidates=[]
    for r in rows:
        if not isinstance(r,dict): continue
        s=str(r.get("symbol"," ")).upper().strip()
        if not s: continue
        confirmed=bool(r.get("v154_high_confidence"))
        early=bool(r.get("v154_alert"))
        fast=bool(r.get("v156_fast_alert"))
        signature=bool(r.get("v156_signature_alert"))
        if not (confirmed or early or fast or signature): continue
        mem=state[s].setdefault("v154",{})
        if confirmed:
            level="CONFIRMED"
        elif early:
            level="EARLY"
        elif fast:
            level="FAST"
        else:
            level="SIGNATURE"
        if mem.get("v156_main_alert_sent")==level: continue
        mem["v156_main_alert_sent"]=level
        candidates.append((level,r))
    priority={"SIGNATURE":1,"FAST":2,"EARLY":3,"CONFIRMED":4}
    for level,r in sorted(candidates,key=lambda x:(priority.get(x[0],0),float(x[1].get("v156_fast_score",0) or 0),float(x[1].get("v156_signature_score",0) or 0),float(x[1].get("v154_pump_entry_score",0) or 0)),reverse=True)[:5]:
        tag={"CONFIRMED":"CONFIRMED PUMP BUY","EARLY":"EARLY PUMP BUY","FAST":"V15.6 FAST EARLY","SIGNATURE":"V15.6 SIGNATURE"}[level]
        await telegram("V15.6 | %s | %s | Price %.10g | Score %.0f | Fast %.0f | Sig %.0f | P10 %+.2f%% | P60 %+.2f%% | Vol %.2fx | Accel %.2fx | Buy %.2f | V15 %.0f | Opp %.0f | Conf %.0f | Accum %.0f | Bridge %.0f | TV %dTF | RS5 %+.2f%% | BTC %s | Exhaust %.0f | Stage %s | Persistence %s" % (tag,r.get("symbol","?"),float(r.get("price",0) or 0),float(r.get("v154_pump_entry_score",r.get("v154_score",0)) or 0),float(r.get("v156_fast_score",0) or 0),float(r.get("v156_signature_score",0) or 0),float(r.get("price_10s",0) or 0),float(r.get("price_60s",0) or 0),float(r.get("volume_ratio",0) or 0),float(r.get("trade_accel",0) or 0),float(r.get("buy_pressure",0) or 0),float(r.get("v15_score",0) or 0),float(r.get("v15_opportunity_score",0) or 0),float(r.get("v15_confirmation_score",0) or 0),float(r.get("accumulation_score",0) or 0),float(r.get("v15_reignition_bridge_score",0) or 0),int(r.get("tv_bullish_timeframes",0) or 0),float(r.get("relative_strength_5m",0) or 0),"RISK-OFF" if r.get("v15_btc_risk_off") else "OK",float(r.get("exhaustion_score",0) or 0),r.get("v154_stage","WATCH"),r.get("v154_persistence_status","IDLE")))

async def telegram(msg):
    await v156_telegram(msg)

async def legacy_telegram(msg):
    # V15.3/legacy Telegram alerts are permanently disabled.
    if os.getenv("V153_ALERTS_ENABLED", "0") != "1":
        return

async def v156_telegram(msg):'''
)
src=src.replace(
    'rows=[r for s in symbols if (r:=score(s))];rows.sort(key=lambda z:z["score"],reverse=True)',
    '''rows=[r for s in symbols if (r:=score(s))]
            v154_events=apply_v154(rows)
            persist_v154_events(v154_events)
            persist_adaptive_capture(rows)
            await send_v156_main_alerts(rows)
            rows.sort(key=lambda z:z["score"],reverse=True)'''
)
exec(compile(src,"scanner/worker.py","exec"),globals(),globals())
