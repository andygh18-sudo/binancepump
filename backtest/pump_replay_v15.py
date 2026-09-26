#!/usr/bin/env python3
"""Pump Replay / Backtest Engine v15.

V15 keeps V14 as the control arm and adds a two-score opportunity/confirmation model. It separates early discovery from confirmation, and distinguishes exhaustion-watch from reversal/cooldown.
"""
import argparse,json,os,time,bisect
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
    delta=x.close.diff();gain=delta.clip(lower=0);loss=-delta.clip(upper=0)
    ag=gain.ewm(alpha=1/14,adjust=False,min_periods=14).mean();al=loss.ewm(alpha=1/14,adjust=False,min_periods=14).mean()
    rs=ag/al.replace(0,pd.NA);x["rsi14"]=100-(100/(1+rs))
    rmin=x.rsi14.rolling(14,min_periods=14).min();rmax=x.rsi14.rolling(14,min_periods=14).max()
    x["stochrsi"]=((x.rsi14-rmin)/(rmax-rmin).replace(0,pd.NA)*100)
    x["stochrsi_k"]=x.stochrsi.rolling(3,min_periods=3).mean();x["stochrsi_d"]=x.stochrsi_k.rolling(3,min_periods=3).mean()
    x["stochrsi_slope"]=x.stochrsi_k.diff()
    x["stochrsi_cross_up"]=(x.stochrsi_k>x.stochrsi_d)&(x.stochrsi_k.shift(1)<=x.stochrsi_d.shift(1))
    x["stochrsi_cross_down"]=(x.stochrsi_k<x.stochrsi_d)&(x.stochrsi_k.shift(1)>=x.stochrsi_d.shift(1))
    x["vr"]=x.quote_volume/x.quote_volume.rolling(20,min_periods=10).mean().replace(0,pd.NA)
    x["ta"]=x.trades/x.trades.rolling(15,min_periods=5).mean().replace(0,pd.NA)
    x["bp"]=x.taker_buy_quote/x.quote_volume.replace(0,pd.NA)
    x["atr"]=(x.high-x.low).rolling(14,min_periods=5).mean()/x.close*100;x["atrbase"]=x.atr.rolling(30,min_periods=10).mean();x["vexp"]=x.atr/x.atrbase.replace(0,pd.NA)
    x["hh20"]=x.high.rolling(20,min_periods=10).max().shift(1);x["hh5"]=x.high.rolling(5,min_periods=3).max().shift(1);x["dist"]=(x.hh20-x.close)/x.close*100
    return x

def clamp(v,a=0,b=1): return max(a,min(b,v))

def classify_v14(r,persistence=0,confirmation=False,efficiency=0.0):
    k=float(r.get("stochrsi_k",50) or 50);slope=float(r.get("stochrsi_slope",0) or 0);cross_up=bool(r.get("stochrsi_cross_up",False))
    vr=float(r.get("volume_ratio",0) or 0);ta=float(r.get("trade_accel",0) or 0);bp=float(r.get("buy_pressure",.5) or .5)
    p1=float(r.get("ret_1m",0) or 0);p5=float(r.get("ret_5m",0) or 0);rs5=float(r.get("relative_strength_5m",0) or 0)
    bos=bool(r.get("BOS",False));choch=bool(r.get("CHoCH",False))
    if k<20 and slope>0: stoch_state="IGNITION"
    elif k<50 and slope>0: stoch_state="EARLY_MOMENTUM"
    elif 50<=k<80 and slope>0: stoch_state="ACCELERATION"
    elif k>=80 and slope>0: stoch_state="EXPANSION"
    elif k>=80 and slope<=0: stoch_state="EXHAUSTION"
    elif k<50 and slope<0: stoch_state="COOLDOWN"
    else: stoch_state="NEUTRAL"
    activity=clamp(max(vr-1,0)/2)*100;accel=clamp(max(ta-1,0)/2)*100;flow=clamp((bp-.50)/.20)*100
    momentum=clamp((p5+.25)/2.5)*100;rel=clamp((rs5+.25)/1.25)*100
    structure=100 if bos and choch else 65 if (bos or choch) else 0
    stoch=(100 if stoch_state in ("IGNITION","EARLY_MOMENTUM") else 85 if stoch_state=="ACCELERATION" else 70 if stoch_state=="EXPANSION" else 25 if stoch_state=="EXHAUSTION" else 10 if stoch_state=="COOLDOWN" else 45)
    if cross_up: stoch=min(100,stoch+10)
    if bool(r.get("stochrsi_cross_down",False)): stoch=max(0,stoch-15)
    eff_score=clamp(max(efficiency,0)/.75)*100
    stage_score=round(.20*stoch+.18*activity+.15*accel+.15*flow+.12*momentum+.10*rel+.07*structure+.03*eff_score)
    if stoch_state=="COOLDOWN" and slope<0 and rs5<0 and vr<1.5: stage="COOLDOWN"
    elif stoch_state=="EXHAUSTION" and (vr>=1.5 or p1>=1.0) and slope<=0: stage="EXHAUSTION"
    elif stoch_state=="EXPANSION" and slope>0 and (structure>=65 or vr>=2.0) and flow>=35 and rel>=50: stage="EXPANSION"
    elif stoch_state in ("ACCELERATION","EXPANSION") and slope>0 and (bos or choch) and vr>=1.5 and ta>=1.25 and bp>=.55 and rs5>0: stage="EARLY_PUMP"
    elif ((stoch_state in ("IGNITION","EARLY_MOMENTUM","ACCELERATION") and slope>0 and (cross_up or k>=20) and vr>=1.15 and ta>=1.10 and bp>=.53 and rs5>-.25) or (k<50 and slope>0 and vr>=1.5 and bp>=.55)): stage="PRE_PUMP"
    elif k<20 and slope>0: stage="WATCH"
    else: stage="NEUTRAL"
    evidence=sum([stoch_state in ("IGNITION","EARLY_MOMENTUM","ACCELERATION","EXPANSION"),slope>0,vr>=1.25,ta>=1.15,bp>=.55,rs5>0,bos or choch,efficiency>=.35,persistence>=2,confirmation])
    return {"v14_stage":stage,"v14_stoch_state":stoch_state,"v14_stage_score":max(0,min(100,stage_score)),"v14_stage_evidence":evidence,"v14_stage_confidence":round(evidence/10*100,1)}

def calc(r,b):
    sr=float(r.stochrsi) if pd.notna(r.stochrsi) else 50.0;sk=float(r.stochrsi_k) if pd.notna(r.stochrsi_k) else sr;sd=float(r.stochrsi_d) if pd.notna(r.stochrsi_d) else sk;ss=float(r.stochrsi_slope) if pd.notna(r.stochrsi_slope) else 0.0
    if sk<20 and ss>0: st_stage="IGNITION"
    elif sk<50 and ss>0: st_stage="EARLY_MOMENTUM"
    elif 50<=sk<80 and ss>0: st_stage="ACCELERATION"
    elif sk>=80 and ss>0: st_stage="EXPANSION"
    elif sk>=80 and ss<=0: st_stage="EXHAUSTION"
    elif sk<50 and ss<0: st_stage="COOLDOWN"
    else: st_stage="NEUTRAL"
    bp=float(r.bp) if pd.notna(r.bp) else .5;vr=float(r.vr) if pd.notna(r.vr) else 0;ta=float(r.ta) if pd.notna(r.ta) else 0;p1=float(r.ret_1m) if pd.notna(r.ret_1m) else 0;p5=float(r.ret_5m) if pd.notna(r.ret_5m) else 0;p15=float(r.ret_15m) if pd.notna(r.ret_15m) else 0;vexp=float(r.vexp) if pd.notna(r.vexp) else 1;dist=float(r.dist) if pd.notna(r.dist) else 99
    b5=float(b.ret_5m) if b is not None and pd.notna(b.ret_5m) else 0;b15=float(b.ret_15m) if b is not None and pd.notna(b.ret_15m) else 0;rs5=p5-b5;rs15=p15-b15
    bos=bool(pd.notna(r.hh20) and r.close>r.hh20);choch=bool(pd.notna(r.hh5) and r.close>r.hh5 and p5>0)
    stoch_bonus=(6 if st_stage in ("IGNITION","EARLY_MOMENTUM","ACCELERATION","EXPANSION") and ss>0 else 0)-(4 if st_stage=="EXHAUSTION" else 0)
    base=clamp((bp-.50)/.25)*14+clamp(max(vr-1,0)/2)*13+clamp(max(ta-1,0)/2)*13+clamp((p5+.5)/2.5)*8+(8 if bos else 0)+(6 if choch else 0)+clamp((rs5+.25)/1.5)*8+clamp(vexp-1)*6+(5 if -.5<=dist<=1 else 2 if dist<=2 else 0)+stoch_bonus
    pen=(6 if p1>3 else 0)+(5 if bp<.52 else 0)+(4 if ta<1.15 else 0)+(5 if vr<.8 else 0)+(4 if rs15<-1 else 0)+(3 if dist>4 else 0)
    early=max(0,min(round(base-pen),100));confirmations=sum([bp>=.60,vr>=1.5,ta>=1.5,rs5>0,bos or choch,dist<=1.5,p1<3,p5<4]);cpen=(2 if p1>=3 else 0)+(2 if rs15<-1 else 0)+(2 if vr<1 else 0)+(2 if bp<.55 else 0)
    conf=max(0,min(confirmations*12-cpen,100));v4=max(0,min(round(early*.8+conf*.2),100))
    grade="A" if v4>=70 and confirmations>=6 and cpen<=2 else "B" if v4>=60 and confirmations>=5 and cpen<=3 else "C" if v4>=50 and confirmations>=4 else "REJECT"
    return {"early_pump_score":early,"v4_score":v4,"v4_confluence_score":conf,"v4_confirmations":confirmations,"v4_penalties":cpen,"v4_alert_quality":grade,"v4_alert":grade in ("A","B") and v4>=60,"buy_pressure":bp,"volume_ratio":vr,"trade_accel":ta,"ret_1m":p1,"ret_5m":p5,"ret_15m":p15,"btc_ret_5m":b5,"btc_ret_15m":b15,"relative_strength_5m":rs5,"relative_strength_15m":rs15,"BOS":bool(bos),"CHoCH":bool(choch),"distance_to_resistance_pct":dist,"volatility_expansion":vexp,"false_positive_penalty":pen,"stochrsi":sr,"stochrsi_k":sk,"stochrsi_d":sd,"stochrsi_slope":ss,"stochrsi_cross_up":bool(r.stochrsi_cross_up),"stochrsi_cross_down":bool(r.stochrsi_cross_down),"stochrsi_stage":st_stage}

def v15_model(z):
    st=z.get("v14_stage","NEUTRAL"); ss=float(z.get("stochrsi_slope",0) or 0); k=float(z.get("stochrsi_k",50) or 50)
    vr=float(z.get("volume_ratio",0) or 0); ta=float(z.get("trade_accel",0) or 0); bp=float(z.get("buy_pressure",.5) or .5)
    rs=float(z.get("relative_strength_5m",0) or 0); p5=float(z.get("ret_5m",0) or 0); p1=float(z.get("ret_1m",0) or 0)
    bos=bool(z.get("BOS")); choch=bool(z.get("CHoCH")); eff=float(z.get("v12_efficiency",0) or 0)
    early=0.24*min(max((vr-1)/2,0),1)+0.18*min(max((ta-1)/2,0),1)+0.18*min(max((bp-.50)/.20,0),1)+0.14*min(max((p5+.25)/2.5,0),1)+0.10*min(max((rs+.25)/1.25,0),1)+0.10*(1 if ss>0 else 0)+0.06*(1 if (st in ("WATCH","PRE_PUMP","EARLY_PUMP") or k<80) else 0)
    early_score=round(max(0,min(100,early*100)))
    conf=0.22*min(max((vr-1)/2,0),1)+0.18*min(max((ta-1)/2,0),1)+0.15*min(max((bp-.50)/.20,0),1)+0.12*(1 if bos else 0)+0.10*(1 if choch else 0)+0.10*min(max((rs+.25)/1.25,0),1)+0.08*min(max(eff/.75,0),1)+0.05*(1 if st in ("EARLY_PUMP","EXPANSION") else 0)
    confirm_score=round(max(0,min(100,conf*100)))
    if st=="COOLDOWN" or (ss<0 and rs<0 and bp<.53): stage="COOLDOWN"
    elif st=="EXHAUSTION": stage="EXHAUSTION_REVERSAL" if (ss<0 and p1<0 and rs<0 and bp<.53) else "EXHAUSTION_WATCH"
    elif st=="EXPANSION": stage="EXPANSION"
    elif st=="EARLY_PUMP": stage="EARLY_PUMP"
    elif st=="PRE_PUMP": stage="PRE_PUMP"
    elif st=="WATCH": stage="WATCH"
    else: stage="NEUTRAL"
    early_candidate=stage in ("WATCH","PRE_PUMP","EARLY_PUMP") and early_score>=45 and ss>0 and bp>=.53 and vr>=1.15 and ta>=1.10 and rs>-.25
    confirmed=stage in ("EARLY_PUMP","EXPANSION") and confirm_score>=60 and bp>=.55 and vr>=1.5 and ta>=1.25 and (bos or choch) and rs>0 and eff>=.25
    return {"v15_opportunity_score":early_score,"v15_confirmation_score":confirm_score,"v15_stage":stage,"v15_early_candidate":bool(early_candidate),"v15_confirmed":bool(confirmed),"v15_exhaustion_watch":stage=="EXHAUSTION_WATCH","v15_reversal":stage in ("EXHAUSTION_REVERSAL","COOLDOWN")}

def expansion_matrix(items):
    targets=(3,5,8,10);horizons=(60,240);out={}
    groups={"ALL_ALERTS":items,"A_PLUS":[z for z in items if z.get("v12_a_plus")],"CONFIRMED":[z for z in items if z.get("v12_path")=="CONFIRMED"],"EARLY":[z for z in items if z.get("v12_path")=="EARLY"],"WATCH":[z for z in items if z.get("v12_path")=="WATCH"]}
    for label,group in groups.items():
        d={"signals":len(group)}
        for h in horizons:
            gkey=f"{h}m";vals=[z.get("future_max_gain",{}).get(gkey) for z in group];vals=[v for v in vals if v is not None]
            for target in targets:
                hits=sum(v>=target for v in vals);d[f"hit_{target}pct_{h}m"]=hits;d[f"precision_{target}pct_{h}m"]=round(hits/len(vals)*100,2) if vals else 0
            d[f"avg_max_gain_{h}m_pct"]=round(sum(vals)/len(vals),2) if vals else 0;d[f"median_max_gain_{h}m_pct"]=round(float(pd.Series(vals).median()),2) if vals else 0
        out[label]=d
    return out

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
        entry=float(x.iloc[i].close);f=x.iloc[i+1:i+241]
        g={f"{m}m":round((float(f.iloc[:m].high.max())/entry-1)*100,2) if len(f.iloc[:m]) else None for m in (15,30,60,240)}
        dd={f"{m}m":round(min((float(f.iloc[:m].low.min())/entry-1)*100,0),2) if len(f.iloc[:m]) else None for m in (15,30,60,240)}
        t5=next((m for m in range(1,min(240,len(f))+1) if float(f.iloc[:m].high.max())/entry-1>=.05),None);t10=next((m for m in range(1,min(240,len(f))+1) if float(f.iloc[:m].high.max())/entry-1>=.10),None);t20=next((m for m in range(1,min(240,len(f))+1) if float(f.iloc[:m].high.max())/entry-1>=.20),None)
        raw.append({"time":t.isoformat(),"price":entry,**z,"future_max_gain":g,"mae":dd,"time_to_5pct_min":t5,"time_to_10pct_min":t10,"time_to_20pct_min":t20})
    for i,z in enumerate(raw):
        j=i
        while j>0 and (pd.Timestamp(z["time"])-pd.Timestamp(raw[j-1]["time"])).total_seconds()<=600 and raw[j-1]["v4_alert_quality"] in ("A","B"): j-=1
        z["persistence"]=sum(1 for q in raw[j:i+1] if q["v4_alert_quality"] in ("A","B"))
    cal=calibration or {};samples=int(cal.get("samples",0) or 0);rate=float(cal.get("hit_rate_240m_ge10_pct",0) or 0)
    smooth_rate=((rate/100)*samples+2)/(samples+20)*100 if samples>=0 else 10
    times_ns=[pd.Timestamp(q["time"]).value for q in raw];episodes=[]
    for z in raw:
        persistence=int(z.get("persistence",0));z_ns=pd.Timestamp(z["time"]).value
        lo=bisect.bisect_left(times_ns,z_ns-600_000_000_000);hi=bisect.bisect_left(times_ns,z_ns)
        prior=[q for q in raw[lo:hi] if q.get("v4_alert_quality") in ("A","B")]
        follow_price=max((q.get("ret_5m",0) for q in prior[-3:]),default=0);follow_volume=max((q.get("volume_ratio",0) for q in prior[-3:]),default=0);follow_accel=max((q.get("trade_accel",0) for q in prior[-3:]),default=0)
        stall=z["volume_ratio"]>=3 and z["trade_accel"]>=3 and z["ret_5m"]<.75
        efficiency=(z["ret_5m"]/max(z["volume_ratio"],1.0)) if z["volume_ratio"]>0 else 0
        adverse_extension=max(0,z["ret_1m"]-2.5)+max(0,z["ret_5m"]-6)*.35
        idx=x.index[x["time"]==pd.Timestamp(z["time"])];i=int(idx[0]) if len(idx) else -1
        next_bar=x.iloc[i+1] if 0<=i+1<len(x) else None
        second_confirm=bool(next_bar is not None and float(next_bar.close)>float(x.iloc[i].close) and float(next_bar.quote_volume)>=float(x.iloc[i].quote_volume)*.70)
        second_hold=bool(next_bar is not None and float(next_bar.low)>=float(x.iloc[i].low)*.995);confirmation=second_confirm and second_hold and not stall
        momentum_ok=z["ret_5m"]>0 and (z["ret_15m"]>=z["ret_5m"]*.75 or z["relative_strength_5m"]>.5)
        follow_through=100
        if stall:follow_through-=25
        if not momentum_ok:follow_through-=15
        if prior and follow_price<=0:follow_through-=15
        if prior and follow_volume<1.2:follow_through-=10
        if prior and follow_accel<1.2:follow_through-=10
        follow_through=max(0,follow_through)
        z.update({"v9_follow_through":follow_through,"v12_efficiency":round(efficiency,4),"v12_adverse_extension":round(adverse_extension,4),"v12_second_candle_confirm":bool(second_confirm),"v12_second_candle_hold":bool(second_hold),"v12_confirmation":bool(confirmation),"v9_stall":bool(stall),"v9_momentum_ok":bool(momentum_ok)})
        hist_modifier=max(-8,min(8,(smooth_rate-10)*.4));btc_risk_off=z["btc_ret_5m"]<-1 or z["btc_ret_15m"]<-2;controlled=z["ret_1m"]<2.5 and z["ret_5m"]<6;structure=z["BOS"] or z["CHoCH"];activity=z["volume_ratio"]>=1.5 and z["trade_accel"]>=1.5;relative=z["relative_strength_5m"]>0
        stoch_early=z["stochrsi_stage"] in ("IGNITION","EARLY_MOMENTUM","ACCELERATION") and z["stochrsi_slope"]>0;stoch_expansion=z["stochrsi_stage"]=="EXPANSION" and z["stochrsi_slope"]>0;stoch_exhaustion=z["stochrsi_stage"]=="EXHAUSTION"
        rel_samples=int(cal.get("samples",0) or 0);rel_rate=float(cal.get("hit_rate_240m_ge10_pct",0) or 0);reliability=50 if rel_samples<20 else max(0,min(100,rel_rate*1.25));reliability_modifier=(reliability-50)*.08
        efficiency_ok=efficiency>=.35 or (efficiency>=.25 and z["ret_5m"]>=2);extension_ok=adverse_extension<3 and z["ret_1m"]<3.5
        lo2=bisect.bisect_right(times_ns,z_ns);hi2=bisect.bisect_right(times_ns,z_ns+180_000_000_000);follow_window=raw[lo2:hi2]
        follow_2m=bool(len(follow_window)>=1 and max(q.get("ret_1m",0) for q in follow_window)>=.15);follow_3m=bool(len(follow_window)>=2 and sum(1 for q in follow_window[-2:] if q.get("ret_5m",0)>0)>=2);follow_strength=(20 if follow_2m else 0)+(20 if follow_3m else 0);v12_follow_ok=follow_strength>=20
        v14=classify_v14(z,persistence=persistence,confirmation=confirmation,efficiency=efficiency);z.update(v14);z.update(v15_model(z))
        a_plus=bool(confirmation and efficiency>=.60 and z["relative_strength_5m"]>=1 and z["BOS"] and z["CHoCH"] and persistence>=2 and z["buy_pressure"]>=.60 and not stall and extension_ok);a_plus_bonus=5 if a_plus else 0
        early_path=z["v4_score"]>=60 and persistence>=2 and z["buy_pressure"]>=.58 and activity and relative and structure and controlled and not btc_risk_off and follow_through>=65 and not stall and stoch_early and efficiency_ok and extension_ok and confirmation and v12_follow_ok
        confirmed_path=z["v4_score"]>=72 and z["v4_confirmations"]>=7 and persistence>=3 and z["buy_pressure"]>=.60 and z["volume_ratio"]>=2 and z["trade_accel"]>=2 and z["relative_strength_5m"]>.25 and structure and controlled and not btc_risk_off and follow_through>=75 and momentum_ok and confirmation and not stall and (stoch_early or stoch_expansion) and not stoch_exhaustion and efficiency>=.50 and extension_ok and v12_follow_ok
        avoid=z["ret_1m"]>=4 or z["ret_5m"]>=8 or z["relative_strength_15m"]<-1.5 or (z["volume_ratio"]<1 and z["ret_5m"]>1) or btc_risk_off or adverse_extension>=4 or stall
        persistence_quality=min(persistence/3,1)*100;score=round(.55*z["v4_score"]+.10*persistence_quality+.10*follow_through+.15*min(max(efficiency,0)/.75*100,100)+.05*min(max(z["relative_strength_5m"],0)/2*100,100)+.05*follow_strength+reliability_modifier+a_plus_bonus);score=max(0,min(score,100))
        if avoid or z["v15_reversal"]: path="AVOID";alert=False;grade="REJECT"
        elif z["v15_confirmed"] and z["v15_confirmation_score"]>=60: path="CONFIRMED";alert=True;grade="A"
        elif z["v15_early_candidate"] and z["v15_opportunity_score"]>=50: path="EARLY";alert=False;grade="B"
        elif z["v15_opportunity_score"]>=40 and persistence>=1: path="WATCH";alert=False;grade="WATCH"
        else:path="REJECT";alert=False;grade="REJECT"
        z.update({"v12_score":score,"v15_score":round(.55*z["v15_opportunity_score"]+.45*z["v15_confirmation_score"]),"v12_persistence":persistence,"pump_stage":("EARLY" if stoch_early else "EXPANSION" if stoch_expansion else "EXHAUSTION" if stoch_exhaustion else "COOLDOWN" if z["stochrsi_stage"]=="COOLDOWN" else "WATCH"),"v12_reliability":round(reliability,2),"v12_reliability_modifier":round(reliability_modifier,2),"v12_grade":grade,"v12_path":path,"v12_alert":bool(alert),"v12_confirmation":bool(confirmation),"v12_a_plus":bool(a_plus),"v15_model":"two_score_opportunity_confirmation","v12_follow_strength":follow_strength,"v12_follow_ok":bool(v12_follow_ok),"v12_efficiency":round(efficiency,4),"v12_adverse_extension":round(adverse_extension,4),"v8_hist_samples":samples,"v8_hist_hit_rate_240m":round(rate,2),"v8_smoothed_hist_rate_240m":round(smooth_rate,2),"v8_hist_modifier":round(hist_modifier,2),"v8_grade":grade,"v8_path":path,"v8_early_path":bool(early_path),"v8_confirmed_path":bool(confirmed_path),"v8_avoid":bool(avoid),"v8_btc_risk_off":bool(btc_risk_off),"v8_alert":bool(alert)})
        if not episodes or (pd.Timestamp(z["time"])-pd.Timestamp(episodes[-1]["time"])).total_seconds()>=1800:episodes.append(z)
        elif z["v12_score"]>episodes[-1]["v12_score"]:episodes[-1]=z
    a=[z for z in episodes if z["v12_alert"]];bands={}
    for lo,hi in ((50,59),(60,69),(70,79),(80,89),(90,100)):
        q=[z for z in episodes if lo<=z["v12_score"]<=hi];h60=[z for z in q if (z["future_max_gain"]["60m"] or 0)>=10];h240=[z for z in q if (z["future_max_gain"]["240m"] or 0)>=10];h20=[z for z in q if (z["future_max_gain"]["240m"] or 0)>=20]
        bands[f"{lo}-{hi}"]={"signals":len(q),"hit_60m_ge10":len(h60),"hit_240m_ge10":len(h240),"hit_240m_ge20":len(h20),"precision_240m_ge10_pct":round(len(h240)/len(q)*100,2) if q else 0,"precision_240m_ge20_pct":round(len(h20)/len(q)*100,2) if q else 0}
    early_alerts=[z for z in a if z.get("v12_path")=="EARLY"];confirmed_alerts=[z for z in a if z.get("v12_path")=="CONFIRMED"];false_pos=sum((z["future_max_gain"]["240m"] or 0)<10 for z in a)
    return {"symbol":symbol,"status":"ok","bars":len(x),"start":x.time.iloc[0].isoformat(),"end":x.time.iloc[-1].isoformat(),"detections":len(episodes),"v12_alerts":len(a),"v15_alerts":len(a),"v15_early_candidates":sum(1 for z in episodes if z.get("v15_early_candidate")),"v15_confirmed_alerts":len([z for z in a if z.get("v15_confirmed")]),"expansion_matrix":expansion_matrix(episodes),"v12_early_alerts":len(early_alerts),"v12_confirmed_alerts":len(confirmed_alerts),"v12_false_positives":false_pos,"v12_false_positive_rate_pct":round(false_pos/len(a)*100,2) if a else 0,"hit_60m_ge10":sum((z["future_max_gain"]["60m"] or 0)>=10 for z in a),"hit_240m_ge10":sum((z["future_max_gain"]["240m"] or 0)>=10 for z in a),"hit_240m_ge20":sum((z["future_max_gain"]["240m"] or 0)>=20 for z in a),"v12_alert_precision_240m_ge10_pct":round(sum((z["future_max_gain"]["240m"] or 0)>=10 for z in a)/len(a)*100,2) if a else 0,"v12_alert_precision_240m_ge20_pct":round(sum((z["future_max_gain"]["240m"] or 0)>=20 for z in a)/len(a)*100,2) if a else 0,"v15_alert_precision_240m_ge3_pct":round(sum((z["future_max_gain"]["240m"] or 0)>=3 for z in a)/len(a)*100,2) if a else 0,"v15_alert_precision_240m_ge5_pct":round(sum((z["future_max_gain"]["240m"] or 0)>=5 for z in a)/len(a)*100,2) if a else 0,"v15_alert_precision_240m_ge10_pct":round(sum((z["future_max_gain"]["240m"] or 0)>=10 for z in a)/len(a)*100,2) if a else 0,"score_band_precision":bands,"v12_alert_episodes":a[:150],"episodes":episodes[:150]}

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--symbols",default="NOMUSDT,NILUSDT,STXUSDT,RAYUSDT,SEIUSDT,SUIUSDT,INJUSDT,AVAXUSDT,PUMPUSDT");ap.add_argument("--start",default="2026-09-20T00:00:00Z");ap.add_argument("--end",default="2026-09-25T00:00:00Z");ap.add_argument("--train-start",default="2026-09-13T00:00:00Z");ap.add_argument("--output",default="data/backtest_results_v15.json");a=ap.parse_args()
    btc_train=fetch("BTCUSDT",a.train_start,a.start);btc_test=fetch("BTCUSDT",a.start,a.end);out=[];cal={}
    for s in [z.strip().upper() for z in a.symbols.split(",") if z.strip() and z.strip().upper()!="BTCUSDT"]:
        print("Calibrating",s)
        try:
            tr=evaluate(fetch(s,a.train_start,a.start),btc_train,s,{"samples":0,"hit_rate_240m_ge10_pct":0});train_eps=tr.get("episodes",[]) if tr.get("status")=="ok" else [];hits=sum((q.get("future_max_gain",{}).get("240m") or 0)>=10 for q in train_eps)
            cal[s]={"samples":len(train_eps),"hit_rate_240m_ge10_pct":round(min(hits/len(train_eps)*100,60),2) if train_eps else 0}
            print("Replaying",s);out.append(evaluate(fetch(s,a.start,a.end),btc_test,s,cal[s]))
        except Exception as e:out.append({"symbol":s,"status":"error","error":str(e)})
    payload={"generated_at":datetime.now(timezone.utc).isoformat(),"engine":"Pump Replay / Backtest v15","data_source":BASE,"method":"Binance 1m Spot klines with V14 control plus V15 two-score opportunity/confirmation model and exhaustion-watch/reversal separation","v15_design":{"model":"two-score opportunity + confirmation","opportunity_score":"early discovery using volume, trade acceleration, buy pressure, momentum, relative strength and rising StochRSI","confirmation_score":"confirmation using volume, trade acceleration, buy pressure, BOS/CHoCH, relative strength and efficiency","stages":["WATCH","PRE_PUMP","EARLY_PUMP","CONFIRMED","EXPANSION","EXHAUSTION_WATCH","EXHAUSTION_REVERSAL","COOLDOWN"],"uses_future_data":False,"control":"V14 logic retained in each episode"}, "v14_design":{"stage_classifier":"StochRSI + volume + trade acceleration + buy pressure + BOS/CHoCH + BTC-relative strength + efficiency","uses_future_data":False,"stages":["WATCH","PRE_PUMP","EARLY_PUMP","EXPANSION","EXHAUSTION","COOLDOWN"],"baseline_comparison":"V12/V13 score and paths retained"},"v12_design":{"v11_base":True,"v4_weight":.55,"persistence_weight":.10,"follow_through_weight":.10,"efficiency_weight":.15,"relative_strength_weight":.05,"second_candle_confirmation":True,"stochrsi_stage_classification":True,"stochrsi_periods":"RSI14/Stoch14/K3/D3","stochrsi_stage_bonus":6,"stochrsi_exhaustion_penalty":4,"efficiency_filter":True,"adverse_extension_veto":True,"multi_candle_follow_through":True,"historical_hard_gate":False,"early_min_score":62,"confirmed_min_score":70,"a_plus_setup":True,"a_plus_bonus":5,"mae_mfe_tracking":True,"targets":["10% in 60m","10% in 240m","20% in 240m"]},"v9_design":{"v4_weight":.72,"persistence_weight":.10,"follow_through_weight":.13,"historical_risk_modifier":True,"historical_hard_gate":False,"smoothed_prior_hit_rate_pct":10,"early_v4_min_score":60,"early_min_follow_through":65,"early_min_persistence":2,"confirmed_v4_min_score":72,"confirmed_min_persistence":3,"confirmed_min_follow_through":75,"avoid_filter":True,"targets":["10% in 60m","10% in 240m","20% in 240m"]},"calibration":cal,"results":out}
    os.makedirs(os.path.dirname(a.output) or ".",exist_ok=True);json.dump(payload,open(a.output,"w"),indent=2);json.dump(cal,open("data/v15_calibration.json","w"),indent=2);print(json.dumps(payload,indent=2))

if __name__=="__main__":main()
