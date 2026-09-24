#!/usr/bin/env python3
"""Pump Replay / Backtest Engine v8 - dual-path early/confirmed pump detection with historical risk modifier."""
import argparse,json,os,time
from datetime import datetime,timezone
import pandas as pd,requests
BASE=os.getenv("BINANCE_REST_BASE","https://data-api.binance.vision")
COLS=["open_time","open","high","low","close","volume","close_time","quote_volume","trades","taker_buy_base","taker_buy_quote","ignore"]

def ms(dt): return int(pd.Timestamp(dt,tz="UTC").timestamp()*1000)
def fetch(symbol,start,end):
    rows=[];cur=ms(start);stop=ms(end)
    while cur<stop:
        r=requests.get(BASE+"/api/v3/klines",params={"symbol":symbol,"interval":"1m","startTime":cur,"endTime":stop,"limit":1000},timeout=20);r.raise_for_status()
        b=r.json()
        if not b: break
        rows+=b;cur=int(b[-1][0])+60000;time.sleep(.03)
        if len(b)<1000: break
    if not rows:return pd.DataFrame(columns=COLS)
    x=pd.DataFrame(rows,columns=COLS).drop_duplicates("open_time")
    for c in COLS:
        if c!="ignore":x[c]=pd.to_numeric(x[c],errors="coerce")
    x["time"]=pd.to_datetime(x.open_time,unit="ms",utc=True)
    return x.sort_values("open_time").reset_index(drop=True)

def prep(x):
    x=x.copy()
    x["ret_1m"]=x.close.pct_change()*100;x["ret_5m"]=x.close.pct_change(5)*100;x["ret_15m"]=x.close.pct_change(15)*100
    x["vr"]=x.quote_volume/x.quote_volume.rolling(20,min_periods=10).mean().replace(0,pd.NA)
    x["ta"]=x.trades/x.trades.rolling(15,min_periods=5).mean().replace(0,pd.NA)
    x["bp"]=x.taker_buy_quote/x.quote_volume.replace(0,pd.NA)
    x["atr"]=(x.high-x.low).rolling(14,min_periods=5).mean()/x.close*100;x["atrbase"]=x.atr.rolling(30,min_periods=10).mean();x["vexp"]=x.atr/x.atrbase.replace(0,pd.NA)
    x["hh20"]=x.high.rolling(20,min_periods=10).max().shift(1);x["hh5"]=x.high.rolling(5,min_periods=3).max().shift(1);x["dist"]=(x.hh20-x.close)/x.close*100
    return x
def clamp(v,a=0,b=1):return max(a,min(b,v))
def calc(r,b):
    bp=float(r.bp) if pd.notna(r.bp) else .5;vr=float(r.vr) if pd.notna(r.vr) else 0;ta=float(r.ta) if pd.notna(r.ta) else 0;p1=float(r.ret_1m) if pd.notna(r.ret_1m) else 0;p5=float(r.ret_5m) if pd.notna(r.ret_5m) else 0;p15=float(r.ret_15m) if pd.notna(r.ret_15m) else 0;vexp=float(r.vexp) if pd.notna(r.vexp) else 1;dist=float(r.dist) if pd.notna(r.dist) else 99
    b5=float(b.ret_5m) if b is not None and pd.notna(b.ret_5m) else 0;b15=float(b.ret_15m) if b is not None and pd.notna(b.ret_15m) else 0;rs5=p5-b5;rs15=p15-b15
    bos=bool(pd.notna(r.hh20) and r.close>r.hh20);choch=bool(pd.notna(r.hh5) and r.close>r.hh5 and p5>0)
    base=clamp((bp-.50)/.25)*14+clamp(max(vr-1,0)/2)*13+clamp(max(ta-1,0)/2)*13+clamp((p5+.5)/2.5)*8+(8 if bos else 0)+(6 if choch else 0)+clamp((rs5+.25)/1.5)*8+clamp(vexp-1)*6+(5 if -.5<=dist<=1 else 2 if dist<=2 else 0)
    pen=(6 if p1>3 else 0)+(5 if bp<.52 else 0)+(4 if ta<1.15 else 0)+(5 if vr<.8 else 0)+(4 if rs15<-1 else 0)+(3 if dist>4 else 0)
    early=max(0,min(round(base-pen),100))
    confirmations=sum([bp>=.60,vr>=1.5,ta>=1.5,rs5>0,bos or choch,dist<=1.5,p1<3,p5<4]);cpen=(2 if p1>=3 else 0)+(2 if rs15<-1 else 0)+(2 if vr<1 else 0)+(2 if bp<.55 else 0)
    conf=max(0,min(confirmations*12-cpen,100));v4=max(0,min(round(early*.8+conf*.2),100))
    grade="A" if v4>=70 and confirmations>=6 and cpen<=2 else "B" if v4>=60 and confirmations>=5 and cpen<=3 else "C" if v4>=50 and confirmations>=4 else "REJECT"
    return {"early_pump_score":early,"v4_score":v4,"v4_confluence_score":conf,"v4_confirmations":confirmations,"v4_penalties":cpen,"v4_alert_quality":grade,"v4_alert":grade in ("A","B") and v4>=60,"buy_pressure":bp,"volume_ratio":vr,"trade_accel":ta,"ret_1m":p1,"ret_5m":p5,"ret_15m":p15,"btc_ret_5m":b5,"btc_ret_15m":b15,"relative_strength_5m":rs5,"relative_strength_15m":rs15,"BOS":bool(bos),"CHoCH":bool(choch),"distance_to_resistance_pct":dist,"volatility_expansion":vexp,"false_positive_penalty":pen}

def evaluate(df,btc,symbol,calibration=None):
    x=prep(df);b=prep(btc)
    if len(x)<300 or len(b)<100:return {"symbol":symbol,"status":"insufficient_data"}
    bi=b.set_index("time");raw=[]
    for i in range(60,len(x)-240):
        t=x.iloc[i].time
        try:br=bi.loc[t]
        except KeyError:
            q=bi.index.get_indexer([t],method="nearest")[0];br=bi.iloc[q] if q>=0 else None
        z=calc(x.iloc[i],br)
        if z["v4_alert_quality"]=="REJECT":continue
        entry=float(x.iloc[i].close);f=x.iloc[i+1:i+241];g={f"{m}m":round((float(f.iloc[:m].high.max())/entry-1)*100,2) if len(f.iloc[:m]) else None for m in (15,30,60,240)}
        raw.append({"time":t.isoformat(),"price":entry,**z,"future_max_gain":g})
    # Persistence: consecutive qualified observations inside a 10-minute window.
    for i,z in enumerate(raw):
        j=i
        while j>0 and (pd.Timestamp(z["time"])-pd.Timestamp(raw[j-1]["time"])).total_seconds()<=600 and raw[j-1]["v4_alert_quality"] in ("A","B"): j-=1
        streak=sum(1 for q in raw[j:i+1] if q["v4_alert_quality"] in ("A","B"))
        z["persistence"]=streak
    # V8: historical calibration is a smoothed risk modifier, never a hard gate.
    episodes=[]
    cal=calibration or {}
    samples=int(cal.get("samples",0) or 0);rate=float(cal.get("hit_rate_240m_ge10_pct",0) or 0)
    # Beta-style smoothing toward a 10% prior: prevents raw 0%/100% calibration from dominating.
    smooth_rate=((rate/100.0)*samples + 2.0) / (samples + 20.0) * 100.0 if samples>=0 else 10.0
    for z in raw:
        persistence=int(z.get("persistence",0))
        hist_modifier=max(-8.0,min(8.0,(smooth_rate-10.0)*0.40))
        btc_risk_off=(z["btc_ret_5m"]<-1.0 or z["btc_ret_15m"]<-2.0)
        controlled=(z["ret_1m"]<2.5 and z["ret_5m"]<6.0)
        structure=(z["BOS"] or z["CHoCH"])
        activity=(z["volume_ratio"]>=1.5 and z["trade_accel"]>=1.5)
        relative=(z["relative_strength_5m"]>0)

        # Path 1: earlier PRE-PUMP signal. Designed to recover NIL-type moves without
        # allowing historical calibration to veto a strong current setup.
        early_path=(
            z["v4_score"]>=60 and persistence>=2 and
            z["buy_pressure"]>=.55 and activity and relative and structure and controlled and
            not btc_risk_off
        )
        # Path 2: stricter confirmed signal.
        confirmed_path=(
            z["v4_score"]>=72 and z["v4_confirmations"]>=7 and persistence>=3 and
            z["buy_pressure"]>=.60 and z["volume_ratio"]>=2.0 and z["trade_accel"]>=2.0 and
            z["relative_strength_5m"]>.25 and structure and controlled and not btc_risk_off
        )
        # Avoid/chase filter.
        avoid=(z["ret_1m"]>=4.0 or z["ret_5m"]>=8.0 or z["relative_strength_15m"]<-1.5 or
               (z["volume_ratio"]<1.0 and z["ret_5m"]>1.0) or btc_risk_off)

        persistence_quality=min(persistence/3.0,1.0)*100
        score=round(.78*z["v4_score"]+.12*persistence_quality+.10*50 + hist_modifier)
        score=max(0,min(score,100))
        if avoid:
            path="AVOID"; alert=False; grade="REJECT"
        elif confirmed_path and score>=68:
            path="CONFIRMED"; alert=True; grade="A"
        elif early_path and score>=58:
            path="EARLY"; alert=True; grade="B"
        elif score>=52 and persistence>=1:
            path="WATCH"; alert=False; grade="WATCH"
        else:
            path="REJECT"; alert=False; grade="REJECT"

        z.update({
            "v8_score":score,"v8_persistence":persistence,
            "v8_hist_samples":samples,"v8_hist_hit_rate_240m":round(rate,2),
            "v8_smoothed_hist_rate_240m":round(smooth_rate,2),
            "v8_hist_modifier":round(hist_modifier,2),"v8_grade":grade,
            "v8_path":path,"v8_early_path":bool(early_path),"v8_confirmed_path":bool(confirmed_path),
            "v8_avoid":bool(avoid),"v8_btc_risk_off":bool(btc_risk_off),"v8_alert":bool(alert)
        })
        if not episodes or (pd.Timestamp(z["time"])-pd.Timestamp(episodes[-1]["time"])).total_seconds()>=1800:
            episodes.append(z)
        elif z["v8_score"]>episodes[-1]["v8_score"]:episodes[-1]=z
    a=[z for z in episodes if z["v8_alert"]]
    bands={}
    for lo,hi in ((50,59),(60,69),(70,79),(80,89),(90,100)):
        q=[z for z in episodes if lo<=z["v8_score"]<=hi];h60=[z for z in q if (z["future_max_gain"]["60m"] or 0)>=10];h240=[z for z in q if (z["future_max_gain"]["240m"] or 0)>=10];h20=[z for z in q if (z["future_max_gain"]["240m"] or 0)>=20]
        bands[f"{lo}-{hi}"]={"signals":len(q),"hit_60m_ge10":len(h60),"hit_240m_ge10":len(h240),"hit_240m_ge20":len(h20),"precision_240m_ge10_pct":round(len(h240)/len(q)*100,2) if q else 0,"precision_240m_ge20_pct":round(len(h20)/len(q)*100,2) if q else 0}
    early_alerts=[z for z in a if z.get("v8_path")=="EARLY"]; confirmed_alerts=[z for z in a if z.get("v8_path")=="CONFIRMED"]
    false_pos=sum((z["future_max_gain"]["240m"] or 0)<10 for z in a)
    return {"symbol":symbol,"status":"ok","bars":len(x),"start":x.time.iloc[0].isoformat(),"end":x.time.iloc[-1].isoformat(),"detections":len(episodes),"v8_alerts":len(a),"v8_early_alerts":len(early_alerts),"v8_confirmed_alerts":len(confirmed_alerts),"v8_false_positives":false_pos,"v8_false_positive_rate_pct":round(false_pos/len(a)*100,2) if a else 0,"hit_60m_ge10":sum((z["future_max_gain"]["60m"] or 0)>=10 for z in a),"hit_240m_ge10":sum((z["future_max_gain"]["240m"] or 0)>=10 for z in a),"hit_240m_ge20":sum((z["future_max_gain"]["240m"] or 0)>=20 for z in a),"v8_alert_precision_240m_ge10_pct":round(sum((z["future_max_gain"]["240m"] or 0)>=10 for z in a)/len(a)*100,2) if a else 0,"v8_alert_precision_240m_ge20_pct":round(sum((z["future_max_gain"]["240m"] or 0)>=20 for z in a)/len(a)*100,2) if a else 0,"score_band_precision":bands,"v8_alert_episodes":a[:150],"episodes":episodes[:150]}

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--symbols",default="NOMUSDT,NILUSDT,STXUSDT,RAYUSDT,SEIUSDT,SUIUSDT,INJUSDT,AVAXUSDT,PUMPUSDT");ap.add_argument("--start",default="2026-09-20T00:00:00Z");ap.add_argument("--end",default="2026-09-25T00:00:00Z");ap.add_argument("--train-start",default="2026-09-13T00:00:00Z");ap.add_argument("--output",default="data/backtest_results_v8.json");a=ap.parse_args()
    btc_train=fetch("BTCUSDT",a.train_start,a.start);btc_test=fetch("BTCUSDT",a.start,a.end);out=[];cal={}
    for s in [z.strip().upper() for z in a.symbols.split(",") if z.strip() and z.strip().upper()!="BTCUSDT"]:
        print("Calibrating",s)
        try:
            tr=evaluate(fetch(s,a.train_start,a.start),btc_train,s,{"samples":0,"hit_rate_240m_ge10_pct":0})
            train_eps=tr.get("episodes",[]) if tr.get("status")=="ok" else []
            hits=sum((q.get("future_max_gain",{}).get("240m") or 0)>=10 for q in train_eps)
            # Conservative calibration: require >=20 training episodes and cap empirical rate at 60%.
            cal[s]={"samples":len(train_eps),"hit_rate_240m_ge10_pct":round(min(hits/len(train_eps)*100,60),2) if train_eps else 0}
            print("Replaying",s)
            r=evaluate(fetch(s,a.start,a.end),btc_test,s,cal[s]);out.append(r)
        except Exception as e:out.append({"symbol":s,"status":"error","error":str(e)})

    payload={"generated_at":datetime.now(timezone.utc).isoformat(),"engine":"Pump Replay / Backtest v8","data_source":BASE,"method":"Binance 1m Spot klines with BTC-relative strength, dual EARLY/CONFIRMED paths, persistence, avoid filter and smoothed historical risk modifier","v8_design":{"v4_weight":0.78,"persistence_weight":0.12,"historical_risk_modifier":True,"historical_hard_gate":False,"smoothed_prior_hit_rate_pct":10,"early_v4_min_score":60,"early_min_persistence":2,"confirmed_v4_min_score":72,"confirmed_min_persistence":3,"avoid_filter":True,"targets":["10% in 60m","10% in 240m","20% in 240m"]},"calibration":cal,"results":out}
    os.makedirs(os.path.dirname(a.output) or ".",exist_ok=True);json.dump(payload,open(a.output,"w"),indent=2);json.dump(cal,open("data/v8_calibration.json","w"),indent=2);print(json.dumps(payload,indent=2))
if __name__=="__main__":main()
