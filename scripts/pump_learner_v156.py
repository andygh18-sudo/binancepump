#!/usr/bin/env python3
"""V15.6 Pump Learner: measure three early-pump signatures against future pump outcomes.

Labels are strictly forward-looking: a trigger is a success when price reaches
PUMP_TARGET_PCT within LABEL_HORIZON_SECONDS after the trigger sample.
No future data is used in the trigger itself.
"""
import json, os, glob
from collections import defaultdict
from pathlib import Path

ROOT=Path("data")
HISTORY=ROOT/"adaptive_microstructure_history.jsonl"
OUT=ROOT/"v156_signature_learning.json"
TARGET=float(os.getenv("V156_LEARNER_TARGET_PCT","3.0"))
HORIZON=int(os.getenv("V156_LEARNER_HORIZON_SECONDS","600"))
MIN_SAMPLES=int(os.getenv("V156_LEARNER_MIN_SAMPLES","10"))
NAMES=("PRICE_VOLUME","ORDER_FLOW","TRADE_ACCEL")

def load():
    files=glob.glob(str(ROOT/"adaptive_microstructure_history.jsonl"))
    files += glob.glob(str(ROOT/"microstructure"/"*.jsonl"))
    rows=[]
    for fn in files:
        try:
            with open(fn,encoding="utf-8") as f:
                for line in f:
                    try:
                        r=json.loads(line)
                        if r.get("ts") is not None and r.get("symbol"):
                            rows.append(r)
                    except Exception:
                        pass
        except FileNotFoundError:
            pass
    rows.sort(key=lambda r:(str(r.get("symbol")),float(r.get("ts",0))))
    return rows

def price(r):
    try:return float(r.get("price"))
    except:return None

def signature_flags(r):
    raw=str(r.get("v156_signature_signals",""))
    return {n: (bool(r.get("v156_signature_"+n.lower(),False)) or n in raw) for n in NAMES}

def main():
    rows=load()
    by=defaultdict(list)
    for r in rows: by[str(r["symbol"]).upper()].append(r)
    stats={n:{"samples":0,"wins":0,"losses":0,"unknown":0,"precision_pct":None,"avg_lead_seconds":None,"median_lead_seconds":None} for n in NAMES}
    leads=defaultdict(list)
    triggers=defaultdict(list)

    for symbol, seq in by.items():
        for i,r in enumerate(seq):
            p0=price(r)
            if p0 is None or p0<=0: continue
            flags=signature_flags(r)
            if not any(flags.values()): continue
            t0=float(r["ts"])
            future=[]
            for q in seq[i+1:]:
                tq=float(q.get("ts",0))
                if tq<=t0: continue
                if tq-t0>HORIZON: break
                pq=price(q)
                if pq is not None: future.append((tq,pq))
            for n,hit in flags.items():
                if not hit: continue
                s=stats[n];s["samples"]+=1;triggers[n].append((symbol,t0))
                if not future:
                    s["unknown"]+=1;continue
                target=p0*(1+TARGET/100.0)
                winner=next(((t,p) for t,p in future if p>=target),None)
                if winner:
                    s["wins"]+=1
                    leads[n].append(winner[0]-t0)
                else:s["losses"]+=1

    for n,s in stats.items():
        known=s["wins"]+s["losses"]
        if known:s["precision_pct"]=round(100*s["wins"]/known,2)
        if leads[n]:
            a=sorted(leads[n]);s["avg_lead_seconds"]=round(sum(a)/len(a),1);s["median_lead_seconds"]=round(a[len(a)//2],1)
        s["learning_status"]="READY" if s["samples"]>=MIN_SAMPLES else "COLLECTING"

    result={
        "version":"V15.6",
        "target_pct":TARGET,
        "horizon_seconds":HORIZON,
        "minimum_samples_for_tuning":MIN_SAMPLES,
        "total_samples":len(rows),
        "signatures":stats,
        "note":"Metrics are descriptive learner statistics; thresholds should not be promoted automatically from small samples."
    }
    ROOT.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(result,indent=2),encoding="utf-8")
    print(json.dumps(result,indent=2))

if __name__=="__main__": main()
