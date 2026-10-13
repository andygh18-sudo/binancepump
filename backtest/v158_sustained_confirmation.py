#!/usr/bin/env python3
"""Historical proxy for V15.8 sustained-pump confirmation using Binance aggTrades.

Reconstructs minute observations from historical trades, arms episodes when a
trade/price proxy resembles a V15.8 fast candidate, then calls the production
pump_persistence.update_monitor() state machine unchanged. This is not an exact
production replay: historical exhaustion, historical L2, and BTC regime inputs
are not available in aggTrades.
"""
from __future__ import annotations
import argparse, json, math
from collections import defaultdict, deque
from pathlib import Path
from scanner.pump_persistence import load_state, update_monitor

def number(v, d=0.0):
    try:
        x=float(v)
        return x if math.isfinite(x) else d
    except (TypeError, ValueError):
        return d

def load_trades(root):
    by=defaultdict(list)
    for path in sorted(Path(root).glob("*.jsonl")):
        with path.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip(): continue
                try: r=json.loads(line)
                except ValueError: continue
                ts=number(r.get("ts"))
                if ts > 1e11: ts/=1000
                p=number(r.get("price")); q=number(r.get("qty"))
                if ts and p>0 and q>0:
                    by[str(r.get("symbol","")).upper()].append((ts,p,q,not bool(r.get("is_buyer_maker"))))
    for s in by: by[s].sort(key=lambda x:x[0])
    return by

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--input",required=True)
    ap.add_argument("--output",required=True)
    ap.add_argument("--window-minutes",type=int,default=30)
    a=ap.parse_args()
    trades=load_trades(a.input)
    if not trades: raise SystemExit("No usable aggTrades found")
    start=min(v[0] for xs in trades.values() for v in xs)
    end=max(v[-1][0] for xs in trades.values() for v in xs)
    # Minute observations emulate the persistence worker's repeated snapshots.
    # Rolling trade metrics are computed strictly from trades available at that timestamp.
    idx={s:0 for s in trades}; hist={s:deque() for s in trades}; last_price={}
    previous_prices={s:deque(maxlen=7) for s in trades}
    state={"schema_version":1,"updated":0.0,"active":{},"completed":[]}
    confirmed=[]; outcomes=[]; observations=0
    t=(int(start//60)+1)*60
    while t<=end:
        rows=[]; candidates=[]
        for s,xs in trades.items():
            i=idx[s]
            while i<len(xs) and xs[i][0]<=t:
                hist[s].append(xs[i]); i+=1
            idx[s]=i
            h=hist[s]
            while h and h[0][0]<t-360: h.popleft()
            if not h: continue
            p=h[-1][1]; last_price[s]=p
            h10=[z for z in h if z[0]>=t-10]
            h60=[z for z in h if z[0]>=t-60]
            if not h10 or not h60: continue
            total10=sum(z[1]*z[2] for z in h10)
            buy10=sum(z[1]*z[2] for z in h10 if z[3])
            total60=sum(z[1]*z[2] for z in h60)
            buy60=sum(z[1]*z[2] for z in h60 if z[3])
            pressure=buy10/total10 if total10 else .5
            pressure60=buy60/total60 if total60 else .5
            accel=len(h10)/max(len(h60)/6,1)
            volume_ratio=total10/max(total60/6,1e-12)
            old1=next((z[1] for z in reversed(h) if z[0]<=t-60),p)
            old5=next((z[1] for z in reversed(h) if z[0]<=t-300),p)
            price1=(p/old1-1)*100 if old1 else 0
            price5=(p/old5-1)*100 if old5 else 0
            cvd=(2*buy10/total10-1) if total10 else 0
            row={"symbol":s,"price":p,"_fast_pump_score":0,
                 "buy_pressure":pressure,"volume_ratio":volume_ratio,
                 "trade_accel":accel,"_fast_cvd":cvd,
                 "_fast_dynamic_exhaustion":0}
            rows.append(row)
            # Conservative aggTrade-only arming proxy; no historical exhaustion/L2.
            if (len(h10)>=3 and pressure>=.57 and accel>=1.25
                and (cvd>=.08 or pressure-pressure60>=.025)
                and price1>=.05 and price5>=.30
                and (p/next((z[1] for z in h if z[0]>=t-10),p)-1)*100>=-.75
                and (p/old1-1)*100>=-1.0):
                row["_fast_pump_score"]=75
                candidates.append(row)
        alerts=update_monitor(rows,candidates,state,t,path=Path("/tmp/v158_episode_backtest_state.json"))
        observations+=len(rows)
        for al in alerts:
            if al.get("type")=="CONFIRMED":
                confirmed.append({"symbol":al["symbol"],"confirmed_ts":t,
                    "entry_price":al["entry_price"],"confirm_price":al["price"],
                    "confirm_return_pct":round(al["return_pct"],4),
                    "checks_passed":al["checks_passed"]})
            elif al.get("type")=="OUTCOME":
                outcomes.append(al)
        t+=60
    # Only score confirmed episodes with a full forward horizon in the downloaded data.
    metrics=[]
    for c in confirmed:
        s=c["symbol"]; xs=trades[s]; ct=c["confirmed_ts"]; cp=c["confirm_price"]
        future=[(ts,p) for ts,p,_,_ in xs if ts>ct]
        rec=dict(c)
        for sec in (300,600,1800):
            vals=[p for ts,p in future if ts<=ct+sec]
            rec[f"mfe_{sec//60}m_pct"]=round(max(((p/cp-1)*100 for p in vals),default=0),4)
            rec[f"mae_{sec//60}m_pct"]=round(min(((p/cp-1)*100 for p in vals),default=0),4)
            rec[f"end_{sec//60}m_pct"]=round(((vals[-1]/cp-1)*100),4) if vals else None
            rec[f"full_{sec//60}m_window"]=bool(future and future[-1][0]>=ct+sec)
        metrics.append(rec)
    complete=[m for m in metrics if m["full_30m_window"]]
    def avg(key, arr):
        vals=[number(x.get(key),float("nan")) for x in arr]
        vals=[v for v in vals if math.isfinite(v)]
        return round(sum(vals)/len(vals),4) if vals else None
    full_outcomes=[x for x in state["completed"] if x.get("outcome")!="STALE_SYMBOL"]
    summary={
      "engine":"V15.8 production persistence monitor + aggTrade-only candidate proxy",
      "window_utc":{"start":start,"end":end},
      "symbols":len(trades),"trade_count":sum(map(len,trades.values())),
      "minute_observations":observations,
      "episodes_completed":len(state["completed"]),
      "outcome_counts":{k:sum(1 for x in state["completed"] if x.get("outcome")==k)
                        for k in ("SUSTAINED_PUMP","FAILED_BURST","NO_CLEAR_PUMP","STALE_SYMBOL")},
      "confirmed_count":len(confirmed),
      "confirmed_with_full_30m_forward_window":len(complete),
      "confirmation_rate_of_completed_nonstale_episodes":round(len(confirmed)/max(1,len(full_outcomes)),4),
      "post_confirmation_avg":{"mfe_5m_pct":avg("mfe_5m_pct",complete),
        "mfe_10m_pct":avg("mfe_10m_pct",complete),"mfe_30m_pct":avg("mfe_30m_pct",complete),
        "mae_5m_pct":avg("mae_5m_pct",complete),"mae_10m_pct":avg("mae_10m_pct",complete),
        "mae_30m_pct":avg("mae_30m_pct",complete),"end_30m_pct":avg("end_30m_pct",complete)},
      "warning":"Proxy only. Candidate arming uses aggTrades-derived gates; historical dynamic exhaustion, L2 depth, BTC regime, cross-sectional routing, and live worker timing are unavailable. MFE/MAE are not profitability or execution estimates."
    }
    out={"summary":summary,"confirmed_episodes":metrics,"completed_episodes":full_outcomes}
    Path(a.output).parent.mkdir(parents=True,exist_ok=True)
    Path(a.output).write_text(json.dumps(out,indent=2),encoding="utf-8")
    print(json.dumps(summary,indent=2))
    print("POST_CONFIRMATION_EPISODES",json.dumps(metrics,indent=2))
    print("COMPLETED_OUTCOMES",json.dumps(full_outcomes,indent=2))

if __name__=="__main__": main()
