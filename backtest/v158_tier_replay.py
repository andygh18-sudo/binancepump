#!/usr/bin/env python3
"""V15.8 Tier-1 recovery/participation/directional historical proxy replay."""
from __future__ import annotations
import argparse, json, math
from collections import defaultdict
from pathlib import Path
from v158_replay import ReplayState, load, f, ts, pct, gates, outcomes, summary

HORIZONS=(60,300,600,1800)

def tier_scores(m,a):
    tz=f(a.get("adaptive_trade_z"))
    vz=f(a.get("adaptive_volume_z"))
    buy=f(m.get("buy_pressure"))
    accel=f(m.get("trade_accel"))
    regime=f(a.get("adaptive_regime_change"))
    buy_slope=f(m.get("buy_slope"))
    p10=f(m.get("price_10s"))
    p60=f(m.get("price_60s"))
    cvd_z=f(a.get("adaptive_cvd_z"))
    impact=f(a.get("adaptive_price_impact_z"))

    tier1 = regime >= 55 and f(a.get("adaptive_intensity_z")) >= 1.5 and f(a.get("adaptive_trade_size_z")) >= 1.0
    participation = tz >= 2.0 and vz >= 2.0 and buy >= .60 and accel >= 1.50
    confirmations = sum([
        buy_slope >= .025,
        p10 >= .10,
        p60 > 0,
        cvd_z >= .50,
        impact < 2.50,
    ])
    directional = participation and confirmations == 5

    part_score = min(100, max(0,
        max(0,min(tz,6))/6*25 +
        max(0,min(vz,6))/6*25 +
        max(0,min((buy-.50)/.20,1))*20 +
        max(0,min((accel-1)/2,1))*20 +
        max(0,min(regime/100,1))*10
    ))
    dir_score = min(100, max(0,
        max(0,min(buy_slope/.08,1))*25 +
        max(0,min(p10/.75,1))*20 +
        max(0,min(p60/.75,1))*15 +
        max(0,min(cvd_z/2,1))*20 +
        max(0,min((2.5-impact)/2.5,1))*10 +
        max(0,min(regime/100,1))*5 +
        max(0,min((buy-.55)/.15,1))*2.5 +
        max(0,min((accel-1.25)/2,1))*2.5
    ))
    tier = "TIER_1C_DIRECTIONAL_IGNITION" if directional else (
        "TIER_1B_PARTICIPATION_IGNITION" if participation else (
        "TIER_1_STRONG_EARLY_MOMENTUM" if tier1 else "NONE"))
    return {
        "tier":tier,
        "tier1":tier1,
        "participation":participation,
        "directional":directional,
        "tier1_score":round((regime/100*50 + f(a.get("adaptive_intensity_z"))/3*30 + f(a.get("adaptive_trade_size_z"))/2*20),2),
        "participation_score":round(part_score,2),
        "directional_score":round(dir_score,2),
        "adaptive_trade_z":tz,
        "adaptive_volume_z":vz,
        "buy_slope":buy_slope,
        "price_10s":p10,
        "price_60s":p60,
        "adaptive_cvd_z":cvd_z,
        "adaptive_price_impact_z":impact,
        "participation_confirmations":confirmations,
    }

def run(path, allow_missing):
    E=sorted(load(path), key=lambda x:ts(x.get("ts",x.get("time"))))
    states=defaultdict(ReplayState); prices=defaultdict(list); signals=[]
    for e in E:
        s=str(e.get("symbol") or e.get("s") or "").upper()
        typ=str(e.get("event") or e.get("type") or "").lower()
        if not s: continue
        n=ts(e.get("ts",e.get("time"))); st=states[s]; st.prune(n)
        if typ in ("trade","aggtrade","agg_trade"):
            p=f(e.get("price",e.get("p"))); q=f(e.get("qty",e.get("q")))
            maker=str(e.get("is_buyer_maker",e.get("m",""))).lower() in ("1","true","yes")
            st.t.append((n,p,p*q,not maker)); st.p.append((n,p)); prices[s].append({"ts":n,"price":p})
        elif typ in ("depth","book","orderbook"):
            def L(v):
                if isinstance(v,str):
                    try: v=json.loads(v)
                    except: return {}
                return {f(a[0]):f(a[1]) for a in (v or []) if isinstance(a,(list,tuple)) and len(a)>1 and f(a[0])>0}
            st.d={"bids":L(e.get("bids")),"asks":L(e.get("asks"))}
        else: continue
        if typ not in ("trade","aggtrade","agg_trade"): continue
        b=int(n//5)
        if st.bucket==b: continue
        st.bucket=b
        m=st.micro(n); a=st.adaptive(m)
        exh=e.get("exhaustion_score")
        exh=f(exh) if exh not in (None,"") else None
        ok,_=gates(m,exh,not allow_missing)
        if not ok: continue
        t=tier_scores(m,a)
        if t["tier"]=="NONE": continue
        r={"symbol":s,"ts":n,"price":st.p[-1][1],"exhaustion_score":exh}
        r.update(m); r.update(a); r.update(t)
        signals.append(r)
    by=defaultdict(list)
    for r in signals: by[r["symbol"]].append(r)
    for r in signals:
        r.update(outcomes(prices[r["symbol"]],r,HORIZONS))
    groups={}
    for tier in ("TIER_1_STRONG_EARLY_MOMENTUM","TIER_1B_PARTICIPATION_IGNITION","TIER_1C_DIRECTIONAL_IGNITION"):
        q=[r for r in signals if r["tier"]==tier]
        groups[tier]=summary(q)
    groups["ALL_TIERS"]=summary(signals)
    return {"engine":"V15.8 Tier-1 Historical Proxy","summary":groups,"signals":signals}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--input",required=True); ap.add_argument("--output",default="data/v158_proxy_results/tier1.json")
    ap.add_argument("--allow-missing-exhaustion",action="store_true")
    a=ap.parse_args()
    r=run(a.input,a.allow_missing_exhaustion)
    o=Path(a.output); o.parent.mkdir(parents=True,exist_ok=True); o.write_text(json.dumps(r,indent=2,allow_nan=False))
    print(json.dumps(r["summary"],indent=2))

if __name__=="__main__": main()
