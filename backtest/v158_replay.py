#!/usr/bin/env python3
"""Deterministic V15.8 replay/backtest engine.

Event input (JSONL/CSV):
  ts,event,symbol,price,qty,is_buyer_maker,bids,asks,exhaustion_score
Trade events are required for signal reconstruction. Depth events are optional but
must be historical snapshots/updates supplied by the caller; the engine never
invents historical L2 data.

Snapshot mode accepts data/v157_observations.jsonl and evaluates recorded
Fastest-Pump signals against later recorded prices.
"""
from __future__ import annotations
import argparse,csv,json,math,statistics
from collections import defaultdict,deque
from pathlib import Path

GATES={"trades_10s":3,"buy_pressure":.57,"trade_accel":1.25,
       "cvd":.08,"buy_slope":.025,"price_1m":.05,"price_5m":.30,
       "price_10s_floor":-.75,"price_60s_floor":-1.0,"exhaustion_max":65.0}

def f(v,d=0.0):
    try:
        x=float(v); return x if math.isfinite(x) else d
    except: return d
def ts(v):
    x=f(v); return x/1e6 if x>1e14 else x/1e3 if x>1e11 else x
def pct(a,b): return (b/a-1)*100 if a and b else 0.0
def z(v,h):
    a=[f(x) for x in h if math.isfinite(f(x))]
    if len(a)<8:return 0.0
    m=statistics.median(a); mad=statistics.median(abs(x-m) for x in a)
    return max(-8,min(8,(v-m)/max(1.4826*mad,abs(m)*.05,1e-9)))

class S:
    def __init__(self):
        self.t=deque(maxlen=20000); self.p=deque(maxlen=20000); self.d=None
        self.ah=deque(maxlen=60); self.rh=deque(maxlen=60)
        self.abs={"pa":0,"pb":0,"ap":False,"bp":False,"af":0,"bf":0}
        self.bucket=None
    def prune(self,n):
        while self.t and self.t[0][0]<n-3600:self.t.popleft()
        while self.p and self.p[0][0]<n-3600:self.p.popleft()
    def stat(self,n,sec):
        r=[x for x in self.t if x[0]>=n-sec];v=sum(x[2] for x in r)
        b=sum(x[2] for x in r if x[3])
        return len(r),v,b/v if v else .5,pct(r[0][1],r[-1][1]) if len(r)>1 else 0
    def oldmove(self,n,sec):
        old=next((p for t,p in reversed(self.p) if t<=n-sec),0)
        return pct(old,self.p[-1][1]) if old and self.p else 0
    def micro(self,n):
        n10,v10,b10,p10=self.stat(n,10);_,v30,b30,p30=self.stat(n,30);_,v60,b60,p60=self.stat(n,60)
        a=v10/max(v60/6,1);cvd=2*b10-1 if v10 else 0
        return {"trades_10s":n10,"flow_10s":v10,"buy_pressure":b10,"trade_accel":a,
                "cvd_10s":cvd,"buy_slope":b10-(b30 if v30 else .5),
                "price_10s":p10,"price_30s":p30,"price_60s":p60,
                "price_1m":self.oldmove(n,60),"price_5m":self.oldmove(n,300),
                "price_10m":self.oldmove(n,600),"accel_slope":a-v30/max(v60/2,1)}
    def adaptive(self,m):
        r=m["trades_10s"]/10;v=m["flow_10s"]/10;a=m["flow_10s"]/max(m["trades_10s"],1);i=abs(m["price_10s"])/max(m["trades_10s"],1)
        h=list(self.ah);o={"adaptive_trade_z":z(r,[x[0] for x in h]),
          "adaptive_volume_z":z(v,[x[1] for x in h]),"adaptive_trade_size_z":z(a,[x[2] for x in h]),
          "adaptive_cvd_z":z(m["cvd_10s"],[x[3] for x in h]),"adaptive_price_impact_z":z(i,[x[4] for x in h])}
        o["adaptive_intensity_z"]=max(-8,min(8,o["adaptive_volume_z"]-o["adaptive_trade_z"]))
        o["adaptive_regime_change"]=min(100,max(0,max(o["adaptive_trade_z"],0)/3*35+max(o["adaptive_volume_z"],0)/3*25+max(o["adaptive_cvd_z"],0)/3*20+max(o["adaptive_intensity_z"],0)/3*10))
        self.ah.append((r,v,a,m["cvd_10s"],i));return o
    def book(self,m,n):
        if not self.d:return {"book_ready":False}
        B=self.d.get("bids",{});A=self.d.get("asks",{})
        if not B or not A:return {"book_ready":False}
        bids=sorted(B.items(),reverse=True);asks=sorted(A.items());mid=(bids[0][0]+asks[0][0])/2
        def cap(L,side,band):
            return sum(p*q for p,q in L if 0<=((p-mid)/mid*1e4 if side=="a" else (mid-p)/mid*1e4)<=band)
        a10=cap(asks,"a",10);b10=cap(bids,"b",10);target=max(1000,m["flow_10s"]*.25)
        def cost(L):
            rem=target;spent=qty=0;touch=L[0][0]
            for p,q in L:
                take=min(q,rem/p);spent+=take*p;qty+=take;rem-=take*p
                if rem<=1e-9:break
            return abs((spent/qty)/touch-1)*1e4 if qty and rem<=target*.01 else 0
        ac,bc=cost(asks),cost(bids);asym=(b10-a10)/max(a10+b10,1e-9)
        sweep=min(100,max(0,max(0,1-target/max(a10,1))*.45*100+min(ac/8,1)*20+max(0,asym)*15))
        q=self.abs;ad=(1-a10/max(q["pa"],1e-9)) if q["pa"] else 0;bd=(1-b10/max(q["pb"],1e-9)) if q["pb"] else 0
        aa=ad>=.1 and m["buy_pressure"]>=.55;bb=bd>=.1 and m["buy_pressure"]<=.45
        if aa:q["ap"]=True;q["af"]=min(q.get("af",a10) or a10,a10);q["at"]=n
        if bb:q["bp"]=True;q["bf"]=min(q.get("bf",b10) or b10,b10);q["bt"]=n
        if q.get("ap") and a10>=q["af"]*1.12:q["ap"]=False
        if q.get("bp") and b10>=q["bf"]*1.12:q["bp"]=False
        self.rh.append({"a":a10,"b":b10,"aa":int(aa),"bb":int(bb)})
        h=list(self.rh);aae=sum(x["aa"] for x in h);bbe=sum(x["bb"] for x in h)
        ar=sum(1 for i in range(1,len(h)) if h[i]["aa"] and h[i]["a"]>=max(h[i-1]["a"],1e-9)*1.12)
        br=sum(1 for i in range(1,len(h)) if h[i]["bb"] and h[i]["b"]>=max(h[i-1]["b"],1e-9)*1.12)
        persistence=min(100,(ar/max(aae,1))*.45*100+min(ar/3,1)*25+min(br/2,1)*10)
        label="SELLER_ABSORPTION" if aae>=2 and ar>=1 and m["buy_pressure"]>=.58 and m["price_10s"]<.1 else "BULLISH_REPLENISHMENT" if persistence>=60 and (ar or br) else "ABSORPTION_BUILDING" if aae or bbe else "NEUTRAL"
        q["pa"],q["pb"]=a10,b10
        return {"book_ready":True,"v158_sweep_score":sweep,"v158_ask_capacity_10bps":a10,"v158_bid_capacity_10bps":b10,
                "v158_ask_sweep_cost_bps":ac,"v158_bid_sweep_cost_bps":bc,"v158_liquidity_asymmetry":asym,
                "v158_absorption_persistence_score":persistence,"v158_ask_replenishment_events":ar,
                "v158_bid_replenishment_events":br,"v158_absorption_state":label}

def gates(m,exh,strict=True):
    r=[]
    if m["trades_10s"]<3:r+=["trades_10s"]
    if m["buy_pressure"]<.57:r+=["buy_pressure"]
    if m["trade_accel"]<1.25:r+=["trade_accel"]
    if m["cvd_10s"]<.08 and m["buy_slope"]<.025:r+=["cvd_or_buy_slope"]
    if m["price_1m"]<.05 or m["price_5m"]<.30:r+=["price_confirmation"]
    if m["price_10s"]<-.75 or m["price_60s"]<-1:r+=["price_floor"]
    if exh is None and strict:r+=["exhaustion_missing"]
    elif exh is not None and exh>=65:r+=["exhaustion"]
    return not r,r

def load(path):
    p=Path(path);fs=[p] if p.is_file() else sorted(p.glob("*.jsonl"))+sorted(p.glob("*.csv"))
    for x in fs:
        if x.suffix==".jsonl":
            for l in x.read_text().splitlines():
                if l.strip():yield json.loads(l)
        else:
            with x.open(encoding="utf-8",newline="") as h:yield from csv.DictReader(h)

def outcomes(rows,signal,h):
    p0=f(signal["price"]);t0=f(signal["ts"]);future=[(f(x["ts"]),f(x["price"])) for x in rows if f(x["ts"])>t0 and f(x["price"])>0]
    o={}
    for sec in h:
        q=[p for t,p in future if t<=t0+sec];o[f"mfe_{sec}s"]=max([pct(p0,p) for p in q] or [None]);o[f"mae_{sec}s"]=min([pct(p0,p) for p in q] or [None])
    for target in (2,5,10):o[f"time_to_{target}pct_s"]=next((t-t0 for t,p in future if p>=p0*(1+target/100)),None)
    return o

def snapshot(path,h):
    by=defaultdict(list)
    for r in load(path):
        s=str(r.get("symbol","")).upper()
        if s and f(r.get("price")):by[s].append(r)
    out=[]
    for s,rows in by.items():
        rows.sort(key=lambda x:f(x.get("ts")))
        for i,r in enumerate(rows):
            if r.get("fast_pump_qualifies"):
                q={"symbol":s,"ts":f(r.get("ts")),"price":f(r.get("price")),"mode":"recorded_signal"}
                q.update(outcomes(rows,q,h));out.append(q)
    return out

def events(path,h,strict,profile):
    E=sorted(load(path),key=lambda x:ts(x.get("ts",x.get("time"))));S=defaultdict(S);prices=defaultdict(list);sig=[]
    for e in E:
        s=str(e.get("symbol") or e.get("s") or "").upper();typ=str(e.get("event") or e.get("type") or "").lower()
        if not s:continue
        n=ts(e.get("ts",e.get("time")));x=S[s];x.prune(n)
        if typ in ("trade","aggtrade","agg_trade"):
            p=f(e.get("price",e.get("p")));q=f(e.get("qty",e.get("q")));maker=str(e.get("is_buyer_maker",e.get("m",""))).lower() in ("1","true","yes")
            x.t.append((n,p,p*q,not maker));x.p.append((n,p));prices[s].append({"ts":n,"price":p})
        elif typ in ("depth","book","orderbook"):
            def L(v):
                if isinstance(v,str):
                    try:v=json.loads(v)
                    except:return {}
                return {f(a[0]):f(a[1]) for a in (v or []) if isinstance(a,(list,tuple)) and len(a)>1 and f(a[0])>0}
            x.d={"bids":L(e.get("bids")),"asks":L(e.get("asks"))}
        else:continue
        if typ not in ("trade","aggtrade","agg_trade"):continue
        b=int(n//5)
        if x.bucket==b:continue
        x.bucket=b;m=x.micro(n);a=x.adaptive(m);bk=x.book(m,n)
        exh=e.get("exhaustion_score");exh=f(exh) if exh not in (None,"") else None
        ok,why=gates(m,exh,strict)
        if ok:
            if profile=="v157":a={};bk={}
            elif profile=="adaptive":bk={}
            elif profile=="sweep":bk={k:v for k,v in bk.items() if "absorption" not in k and "replenishment" not in k}
            elif profile=="replenishment":bk={k:v for k,v in bk.items() if "sweep" not in k and "capacity" not in k and "asymmetry" not in k}
            r={"symbol":s,"ts":n,"price":x.p[-1][1],"profile":profile,"exhaustion_score":exh,"gate_failures":why};r.update(m);r.update(a);r.update(bk);sig.append(r)
    for r in sig:
        rows=prices[r["symbol"]];r.update(outcomes(rows,r,h))
    return sig

def summary(s):
    if not s:return {"signals":0}
    def av(k):
        q=[f(x.get(k),float("nan")) for x in s];q=[x for x in q if math.isfinite(x)];return sum(q)/len(q) if q else None
    def rt(k,t):
        q=[x.get(k) for x in s if x.get(k) is not None];return sum(f(x)>=t for x in q)/len(q) if q else None
    return {"signals":len(s),"avg_mfe_60s":av("mfe_60s"),"avg_mfe_300s":av("mfe_300s"),"avg_mfe_600s":av("mfe_600s"),
            "avg_mae_300s":av("mae_300s"),"hit_2pct_300s":rt("mfe_300s",2),"hit_5pct_600s":rt("mfe_600s",5),
            "hit_10pct_1800s":rt("mfe_1800s",10),"avg_time_to_5pct_s":av("time_to_5pct_s")}

def main():
    p=argparse.ArgumentParser();p.add_argument("--mode",choices=("events","snapshot"),default="events")
    p.add_argument("--input",required=True);p.add_argument("--output",default="data/v158_backtest_results.json")
    p.add_argument("--profile",choices=("v157","adaptive","sweep","replenishment","full"),default="full")
    p.add_argument("--horizons",default="30,60,180,300,600,1800");p.add_argument("--allow-missing-exhaustion",action="store_true")
    a=p.parse_args();h=[int(x) for x in a.horizons.split(",") if x]
    s=snapshot(a.input,h) if a.mode=="snapshot" else events(a.input,h,not a.allow_missing_exhaustion,a.profile)
    r={"engine":"V15.8 Replay/Backtest Engine","version":"1.0","mode":a.mode,"profile":a.profile,"gate_contract":GATES,"summary":summary(s),"signals":s}
    o=Path(a.output);o.parent.mkdir(parents=True,exist_ok=True);o.write_text(json.dumps(r,indent=2,allow_nan=False))
    print(json.dumps(r["summary"],indent=2));print(o)

if __name__=="__main__":main()
