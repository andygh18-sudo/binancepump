#!/usr/bin/env python3
"""
Pump Replay / Backtest Engine v3

Historical proxy for the live Binance pump scanner. 1m Spot klines cannot
reproduce live 5-second order-book data, so order-book components are proxied.
v3 adds:
- BTC relative strength
- volatility expansion
- multi-factor structure confirmation
- false-positive penalties
- score-band precision report
- 60m/240m pump hit rates
"""
import argparse, json, os, time
from datetime import datetime, timezone
import pandas as pd
import requests

BASE=os.getenv("BINANCE_REST_BASE","https://data-api.binance.vision")
OUT="data/backtest_results.json"
COLS=["open_time","open","high","low","close","volume","close_time","quote_volume","trades","taker_buy_base","taker_buy_quote","ignore"]

def ms(dt):
    return int(pd.Timestamp(dt,tz="UTC").timestamp()*1000)

def fetch_klines(symbol,start,end):
    rows=[]; cursor=ms(start); stop=ms(end)
    while cursor<stop:
        r=requests.get(BASE+"/api/v3/klines",params={"symbol":symbol,"interval":"1m","startTime":cursor,"endTime":stop,"limit":1000},timeout=20)
        r.raise_for_status()
        batch=r.json()
        if not batch: break
        rows.extend(batch)
        nxt=int(batch[-1][0])+60000
        if nxt<=cursor: break
        cursor=nxt
        time.sleep(.06)
        if len(batch)<1000: break
    if not rows: return pd.DataFrame(columns=COLS)
    x=pd.DataFrame(rows,columns=COLS).drop_duplicates("open_time")
    for c in COLS:
        if c!="ignore": x[c]=pd.to_numeric(x[c],errors="coerce")
    x["time"]=pd.to_datetime(x["open_time"],unit="ms",utc=True)
    return x.sort_values("open_time").reset_index(drop=True)

def prepare(df):
    x=df.copy()
    x["ret_1m"]=x.close.pct_change()*100
    x["ret_5m"]=x.close.pct_change(5)*100
    x["ret_15m"]=x.close.pct_change(15)*100
    x["vol_ratio"]=x.quote_volume/x.quote_volume.rolling(20,min_periods=10).mean().replace(0,pd.NA)
    x["vol_ratio_5"]=x.quote_volume/x.quote_volume.rolling(5,min_periods=3).mean().replace(0,pd.NA)
    x["trade_accel"]=x.trades/x.trades.rolling(15,min_periods=5).mean().replace(0,pd.NA)
    x["buy_pressure"]=x.taker_buy_quote/x.quote_volume.replace(0,pd.NA)
    x["atr_pct"]=(x.high-x.low).rolling(14,min_periods=5).mean()/x.close*100
    x["atr_baseline"]=x.atr_pct.rolling(30,min_periods=10).mean()
    x["volatility_expansion"]=x.atr_pct/x.atr_baseline.replace(0,pd.NA)
    x["compression_ratio"]=x.atr_pct/x.atr_baseline.replace(0,pd.NA)
    x["hh20"]=x.high.rolling(20,min_periods=10).max().shift(1)
    x["ll20"]=x.low.rolling(20,min_periods=10).min().shift(1)
    x["hh5"]=x.high.rolling(5,min_periods=3).max().shift(1)
    x["ll5"]=x.low.rolling(5,min_periods=3).min().shift(1)
    x["dist_res"]=((x.hh20-x.close)/x.close*100)
    return x

def clamp(v,a=0,b=1): return max(a,min(b,v))

def row_score(row, btc_row):
    bp=float(row.buy_pressure) if pd.notna(row.buy_pressure) else .5
    vr=float(row.vol_ratio) if pd.notna(row.vol_ratio) else 0
    vr5=float(row.vol_ratio_5) if pd.notna(row.vol_ratio_5) else 0
    ta=float(row.trade_accel) if pd.notna(row.trade_accel) else 0
    p1=float(row.ret_1m) if pd.notna(row.ret_1m) else 0
    p5=float(row.ret_5m) if pd.notna(row.ret_5m) else 0
    p15=float(row.ret_15m) if pd.notna(row.ret_15m) else 0
    vol_exp=float(row.volatility_expansion) if pd.notna(row.volatility_expansion) else 1
    dist=float(row.dist_res) if pd.notna(row.dist_res) else 99
    hh5=float(row.hh5) if pd.notna(row.hh5) else float(row.close)
    hh20=float(row.hh20) if pd.notna(row.hh20) else float(row.close)

    btc5=float(btc_row.ret_5m) if btc_row is not None and pd.notna(btc_row.ret_5m) else 0
    btc15=float(btc_row.ret_15m) if btc_row is not None and pd.notna(btc_row.ret_15m) else 0
    rs5=p5-btc5
    rs15=p15-btc15

    buy=clamp((bp-.50)/.25)*14
    vol=clamp(max(vr-1,0)/2.0)*13
    trade=clamp(max(ta-1,0)/2.0)*13
    pressure=clamp((p5+.5)/2.5)*8
    compression=clamp((1.0-float(row.compression_ratio) if pd.notna(row.compression_ratio) else 0)/.5)*7
    bos=8 if float(row.close)>hh20 else 0
    choch=6 if float(row.close)>hh5 and p5>0 else 0
    rs=clamp((rs5+.25)/1.5)*8
    vol_exp_score=clamp((vol_exp-1)/1.0)*6
    resistance=5 if -0.5<=dist<=1.0 else 2 if dist<=2 else 0

    false_positive=0
    if p1>3.0: false_positive+=6
    if bp<.52: false_positive+=5
    if ta<1.15: false_positive+=4
    if vr<0.8: false_positive+=5
    if rs15<-1.0: false_positive+=4
    if dist>4.0: false_positive+=3

    early=max(0,min(round(buy+vol+trade+pressure+compression+bos+choch+rs+vol_exp_score+resistance-false_positive),100))
    quality=(early>=50 and bp>=.55 and ta>=1.25 and p1<4 and vr>=.8 and rs5>-1.0)

    if early>=80 and quality: stage="EARLY PUMP"
    elif early>=65 and quality: stage="PRE-PUMP"
    elif early>=50 and quality: stage="BUILDING"
    else: stage="MONITOR"

    return {
        "early_pump_score":early,"early_pump_stage":stage,"early_pump_quality":bool(quality),
        "accumulation_score":early,"accumulation_stage":"ACCUMULATION ALERT" if early>=70 and quality else "ACCUMULATION WATCH" if early>=50 and quality else "MONITOR",
        "accumulation_quality":bool(quality),"buy_pressure":bp,"volume_ratio":vr,
        "trade_accel":ta,"ret_1m":p1,"ret_5m":p5,"ret_15m":p15,
        "btc_ret_5m":btc5,"btc_ret_15m":btc15,"relative_strength_5m":rs5,"relative_strength_15m":rs15,
        "compression_ratio":float(row.compression_ratio) if pd.notna(row.compression_ratio) else None,
        "volatility_expansion":vol_exp,"distance_to_resistance_pct":dist,
        "BOS":bool(float(row.close)>hh20),"CHoCH":bool(float(row.close)>hh5 and p5>0),
        "false_positive_penalty":false_positive
    }

def evaluate(df,btc,symbol):
    x=prepare(df); b=prepare(btc)
    if len(x)<100 or len(b)<100: return {"symbol":symbol,"status":"insufficient_data"}
    bidx=b.set_index("time")
    signals=[]
    for i in range(60,len(x)-240):
        t=x.iloc[i].time
        try: br=bidx.loc[t]
        except KeyError:
            pos=bidx.index.get_indexer([t],method="nearest")[0]
            br=bidx.iloc[pos] if pos>=0 else None
        s=row_score(x.iloc[i],br)
        if not s["early_pump_quality"] or s["early_pump_score"]<50: continue
        entry=float(x.iloc[i].close); future=x.iloc[i+1:i+241]
        gains={}
        for m in (15,30,60,240):
            f=future.iloc[:m]
            gains[f"{m}m"]=round((float(f.high.max())/entry-1)*100,2) if len(f) else None
        signals.append({"time":t.isoformat(),"price":entry,**s,"future_max_gain":gains})

    episodes=[]
    for s in signals:
        if not episodes or (pd.Timestamp(s["time"])-pd.Timestamp(episodes[-1]["time"])).total_seconds()>=1800:
            episodes.append(s)
        elif s["early_pump_score"]>episodes[-1]["early_pump_score"]:
            episodes[-1]=s

    pumps=[]
    for i in range(60,len(x)-240):
        entry=float(x.iloc[i].close)
        g60=(float(x.iloc[i+1:i+61].high.max())/entry-1)*100
        g240=(float(x.iloc[i+1:i+241].high.max())/entry-1)*100
        if max(g60,g240)>=10 and (not pumps or (x.iloc[i].time-pd.Timestamp(pumps[-1]["time"])).total_seconds()>=1800):
            pumps.append({"time":x.iloc[i].time.isoformat(),"price":entry,"gain_60m":round(g60,2),"gain_240m":round(g240,2)})

    bands={}
    for lo,hi in ((50,59),(60,69),(70,79),(80,89),(90,100)):
        ss=[z for z in episodes if lo<=z["early_pump_score"]<=hi]
        h60=[z for z in ss if (z["future_max_gain"]["60m"] or 0)>=10]
        h240=[z for z in ss if (z["future_max_gain"]["240m"] or 0)>=10]
        bands[f"{lo}-{hi}"]={"signals":len(ss),"hit_60m_ge10":len(h60),"hit_240m_ge10":len(h240),
            "precision_240m_pct":round(len(h240)/len(ss)*100,2) if ss else 0}

    hits60=[s for s in episodes if (s["future_max_gain"]["60m"] or 0)>=10]
    hits240=[s for s in episodes if (s["future_max_gain"]["240m"] or 0)>=10]
    return {"symbol":symbol,"status":"ok","bars":len(x),"start":x.time.iloc[0].isoformat(),"end":x.time.iloc[-1].isoformat(),
            "early_pump_episodes":episodes[:150],"accumulation_episodes":episodes[:150],"pump_episodes":pumps[:150],
            "detections":len(episodes),"pump_episodes_count":len(pumps),"hit_60m_ge10":len(hits60),"hit_240m_ge10":len(hits240),
            "hit_rate_240m_ge10_pct":round(len(hits240)/len(episodes)*100,2) if episodes else 0,
            "score_band_precision":bands}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--symbols",default="NOMUSDT,NILUSDT,STXUSDT,RAYUSDT,SEIUSDT,SUIUSDT,INJUSDT,AVAXUSDT,PUMPUSDT")
    ap.add_argument("--start",default="2026-09-20T00:00:00Z")
    ap.add_argument("--end",default="2026-09-25T00:00:00Z")
    ap.add_argument("--output",default=OUT)
    a=ap.parse_args()
    btc=fetch_klines("BTCUSDT",a.start,a.end)
    results=[]
    for symbol in [s.strip().upper() for s in a.symbols.split(",") if s.strip() and s.strip().upper()!="BTCUSDT"]:
        print(f"Replaying {symbol} {a.start} -> {a.end}")
        try: results.append(evaluate(fetch_klines(symbol,a.start,a.end),btc,symbol))
        except Exception as e: results.append({"symbol":symbol,"status":"error","error":str(e)})
    payload={"generated_at":datetime.now(timezone.utc).isoformat(),"engine":"Pump Replay / Backtest v3","data_source":BASE,
        "method":"Binance 1m Spot klines; BTC relative strength and structure are historical proxies for live scanner logic",
        "score_design":{"buy_pressure":14,"volume":13,"trade_acceleration":13,"pressure":8,"compression":7,"BOS":8,"CHoCH":6,"relative_strength":8,"volatility_expansion":6,"resistance":5,"false_positive_penalty":"up to 27"},
        "results":results}
    os.makedirs(os.path.dirname(a.output) or ".",exist_ok=True)
    with open(a.output,"w") as f: json.dump(payload,f,indent=2)
    print(json.dumps(payload,indent=2))

if __name__=="__main__": main()
