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
src=src.replace(
    "def persist_adaptive_capture(rows):
    """Retain 10-second microstructure evidence around adaptive candidates."""
    now=time.time()
    os.makedirs("data",exist_ok=True)
    path="data/adaptive_microstructure_history.jsonl"
    interval=float(os.getenv("V156_CAPTURE_INTERVAL","10"))
    records=[]
    for r in rows:
        if not isinstance(r,dict) or not r.get("symbol") or not r.get("v156_adaptive_capture"):
            continue
        s=str(r["symbol"]).upper()
        mem=state[s]
        last=float(mem.get("v156_adaptive_last_capture",0) or 0)
        if now-last < interval:
            continue
        mem["v156_adaptive_last_capture"]=now
        fields=["symbol","price","price_10s","price_60s","price_1m","volume_ratio","trade_accel","buy_pressure","book_imbalance","spread_bps","v15_score","v15_opportunity_score","v15_confirmation_score","accumulation_score","v15_ignition_score","v15_ignition_stage","v15_reignition_bridge_score","v15_reignition_bridge_stage","v15_reignition_bridge_trigger","v15_relative_strength_5m","v15_relative_strength_15m","v15_btc_risk_off","tv_confirmation","tv_bullish_timeframes","pump_momentum_score","exhaustion_score","exhaustion_state","v154_score","v154_stage","v154_persistence_status","v156_adaptive_reason"]
        rec={k:r.get(k) for k in fields if k in r}
        rec["ts"]=now
        rec["capture_interval_seconds"]=interval
        rec["capture_window_seconds"]=float(os.getenv("V156_CAPTURE_WINDOW","120"))
        records.append(rec)
    if records:
        with open(path,"a",encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec,separators=(",",":"))+"\n")

async def send_v156_fast_alerts(rows):
    candidates=[r for r in rows if isinstance(r,dict) and r.get("v156_fast_alert")]
    candidates.sort(key=lambda r:(float(r.get("v156_fast_score",0) or 0),float(r.get("v15_score",0) or 0)),reverse=True)
    for r in candidates[:3]:
        await telegram("⚡ V15.6 FAST IGNITION | {symbol}\nPrice: {price} | Fast score: {fs}/100\nP60: {p60:.2f}% | Vol: {vol:.2f}x | Accel: {acc:.2f}x | Buy: {buy:.2f}\nV15: {v15:.0f} | Opp: {opp:.0f} | Conf: {conf:.0f} | Acc: {accum:.0f}\nTV bullish TF: {tv} | Reason: {reason}\nEarly-warning only; not the confirmed BUY gate.".format(
            symbol=r.get("symbol","?"),price=r.get("price","?"),fs=r.get("v156_fast_score",0),
            p60=float(r.get("price_60s",0) or 0),vol=float(r.get("volume_ratio",0) or 0),
            acc=float(r.get("trade_accel",0) or 0),buy=float(r.get("buy_pressure",0) or 0),
            v15=float(r.get("v15_score",0) or 0),opp=float(r.get("v15_opportunity_score",0) or 0),
            conf=float(r.get("v15_confirmation_score",0) or 0),accum=float(r.get("accumulation_score",0) or 0),
            tv=r.get("tv_bullish_timeframes",0),reason=r.get("v156_fast_reason","")))
async def telegram(msg):",
    '''def apply_v154(rows):
    events=[]
    for r in rows:
        s=str(r.get("symbol","")).upper()
        if not s:
            continue
        result,event_record=v154_evaluate(r,state[s].setdefault("v154",{}))
        r.update(result)
        if event_record:
            events.append(event_record)
    return events

def persist_v154_events(events):
    if not events:
        return
    os.makedirs("data",exist_ok=True)
    with open("data/v154_events.jsonl","a",encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event,separators=(",",":"))+"\\n")

def persist_adaptive_capture(rows):
    now=time.time()
    os.makedirs("data",exist_ok=True)
    path="data/adaptive_microstructure_history.jsonl"
    interval=float(os.getenv("V156_CAPTURE_INTERVAL","10"))
    records=[]
    for r in rows:
        if not isinstance(r,dict) or not r.get("symbol") or not r.get("v156_adaptive_capture"):
            continue
        s=str(r["symbol"]).upper()
        mem=state[s]
        last=float(mem.get("v156_adaptive_last_capture",0) or 0)
        if now-last < interval:
            continue
        mem["v156_adaptive_last_capture"]=now
        fields=["symbol","price","price_10s","price_60s","price_1m","volume_ratio","trade_accel","buy_pressure","book_imbalance","spread_bps","v15_score","v15_opportunity_score","v15_confirmation_score","accumulation_score","v15_ignition_score","v15_ignition_stage","v15_reignition_bridge_score","v15_reignition_bridge_stage","v15_reignition_bridge_trigger","v15_relative_strength_5m","v15_relative_strength_15m","v15_btc_risk_off","tv_confirmation","tv_bullish_timeframes","pump_momentum_score","exhaustion_score","exhaustion_state","v154_score","v154_stage","v154_persistence_status","v156_adaptive_reason"]
        rec={k:r.get(k) for k in fields if k in r}
        rec["ts"]=now
        rec["capture_interval_seconds"]=interval
        rec["capture_window_seconds"]=float(os.getenv("V156_CAPTURE_WINDOW","120"))
        records.append(rec)
    if records:
        with open(path,"a",encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec,separators=(",",":"))+"\\n")

async def telegram(msg):'''
)
src=src.replace(
    'rows=[r for s in symbols if (r:=score(s))]\\n            v154_events=apply_v154(rows)\\n            persist_v154_events(v154_events)\\n            persist_adaptive_capture(rows)\\n            rows.sort(key=lambda z:z["score"],reverse=True)',
    'rows=[r for s in symbols if (r:=score(s))]\\n            v154_events=apply_v154(rows)\\n            persist_v154_events(v154_events)\\n            persist_adaptive_capture(rows)\\n            await send_v156_fast_alerts(rows)\\n            rows.sort(key=lambda z:z["score"],reverse=True)'
)
exec(compile(src,"scanner/worker.py","exec"),globals(),globals())
