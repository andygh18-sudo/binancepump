#!/usr/bin/env python3
"""Fast multi-profile V15.8 historical proxy replay.

Runs all five profiles in one pass through the historical event stream.
It preserves the V15.7 hard-gate contract and V15.8 advisory scoring,
while using rolling trade windows and indexed outcome calculations.
"""
from __future__ import annotations
import argparse, bisect, json, math, statistics
from collections import defaultdict, deque
from pathlib import Path

from v158_replay import (
    ReplayState, load, f, ts, pct, z, gates, summary
)

PROFILES = ("v157", "adaptive", "sweep", "replenishment", "full")
HORIZONS = (30, 60, 180, 300, 600, 1800)

class FastState(ReplayState):
    def __init__(self):
        super().__init__()
        self.w10=deque(); self.w30=deque(); self.w60=deque(); self.w300=deque()
        self.s10=[0,0,0]; self.s30=[0,0,0]; self.s60=[0,0,0]; self.s300=[0,0,0]
        self.snap=deque(maxlen=150)

    @staticmethod
    def _add(q, sums, item):
        t,p,notional,isbuy=item
        q.append(item)
        sums[0]+=1; sums[1]+=notional; sums[2]+=notional if isbuy else 0

    @staticmethod
    def _trim(q, sums, now, sec):
        cut=now-sec
        while q and q[0][0] < cut:
            t,p,n,b=q.popleft()
            sums[0]-=1; sums[1]-=n; sums[2]-=n if b else 0

    def add_trade(self, item):
        self._add(self.w10,self.s10,item)
        self._add(self.w30,self.s30,item)
        self._add(self.w60,self.s60,item)
        self._add(self.w300,self.s300,item)
        self.t.append(item)
        self.p.append((item[0],item[1]))

    def prune_fast(self,n):
        self._trim(self.w10,self.s10,n,10)
        self._trim(self.w30,self.s30,n,30)
        self._trim(self.w60,self.s60,n,60)
        self._trim(self.w300,self.s300,n,300)
        while self.p and self.p[0][0] < n-650:
            self.p.popleft()
        while self.t and self.t[0][0] < n-650:
            self.t.popleft()

    def stat_fast(self,q,sums):
        count,vol,buy=sums
        if not count:
            return 0,0,.5,0
        return count,vol,buy/vol if vol else .5,pct(q[0][1],q[-1][1]) if len(q)>1 else 0

    def micro_fast(self,n):
        n10,v10,b10,p10=self.stat_fast(self.w10,self.s10)
        _,v30,b30,p30=self.stat_fast(self.w30,self.s30)
        _,v60,b60,p60=self.stat_fast(self.w60,self.s60)
        _,v300,b300,p300=self.stat_fast(self.w300,self.s300)
        # Preserve the existing engine's formulas.
        a=v10/max(v60/6,1)
        cvd=2*b10-1 if v10 else 0
        total300=v300
        # t is now at most 650s, so this is bounded; use snapshots below for
        # longer price lookbacks.
        self.snap.append((n,self.p[-1][1] if self.p else 0))
        def oldmove(sec):
            target=n-sec
            for t,p in reversed(self.snap):
                if t<=target:
                    return pct(p,self.snap[-1][1]) if p else 0
            return 0
        return {
            "trades_10s":n10,"flow_10s":v10,"buy_pressure":b10,
            "trade_accel":a,
            "volume_ratio":v60/max(total300/5,1),
            "cvd_10s":cvd,"buy_slope":b10-(b30 if v30 else .5),
            "price_10s":p10,"price_30s":p30,"price_60s":p60,
            "price_1m":oldmove(60),"price_5m":oldmove(300),
            "price_10m":oldmove(600),
            "accel_slope":a-v30/max(v60/2,1)
        }

def score_profile(m,a,bk,exh,profile):
    flow_score=min(max((m["buy_pressure"]-.50)/.20,0),1)*18
    buy_slope_score=min(max((m["buy_slope"]-.01)/.10,0),1)*14
    accel_score=min(max((m["trade_accel"]-1.0)/1.5,0),1)*18
    accel_slope_score=min(max((m["accel_slope"]+.05)/.75,0),1)*10
    volume_accel_score=min(max((m["trade_accel"]-1.0)/2.5,0),1)*12
    cvd_score=min(max((m["cvd_10s"]+.05)/.55,0),1)*10
    micro_price_score=min(max((m["price_10s"]+.10)/1.50,0),1)*4
    price_score=min(max((m["price_1m"]+.05)/1.50,0),1)*2
    structure_score=0
    if bk.get("book_ready"):
        bids=bk.get("v158_bid_capacity_10bps",0); asks=bk.get("v158_ask_capacity_10bps",0)
        structure_score += 3 if bids>=asks else 0
        structure_score += 2 if bk.get("v158_ask_sweep_cost_bps",999)<=12 else 0
    adaptive_bonus=0
    if profile in ("adaptive","sweep","replenishment","full"):
        if a.get("adaptive_regime_change",0)>=55: adaptive_bonus+=2
        if a.get("adaptive_intensity_z",0)>=1.5 and a.get("adaptive_trade_size_z",0)>=1.0: adaptive_bonus+=1
        if a.get("adaptive_price_impact_z",0)>=1.5: adaptive_bonus+=1
    sweep_bonus=0
    if profile in ("sweep","full"):
        ss=bk.get("v158_sweep_score",0)
        sweep_bonus=3 if ss>=70 else 2 if ss>=55 else 1 if ss>=40 else 0
        if bk.get("v158_ask_capacity_10bps",0)>0 and bk.get("v158_bid_capacity_10bps",0)>0 and bk.get("v158_ask_capacity_10bps",0)<bk.get("v158_bid_capacity_10bps",0)*.65:
            sweep_bonus+=1
    repl_bonus=0
    if profile in ("replenishment","full"):
        ps=bk.get("v158_absorption_persistence_score",0)
        st=bk.get("v158_absorption_state","")
        resp=bk.get("v158_absorption_price_response",0)
        repl_bonus=3 if st=="BULLISH_REPLENISHMENT" and ps>=60 else 1.5 if ps>=45 and resp>0 else 0
        if st=="SELLER_ABSORPTION": repl_bonus-=2
    dynamic_exhaustion=(exh or 0)+max(0,m["price_1m"]-1.50)*3+max(0,m["price_5m"]-4.0)*1.5
    exhaustion_penalty=max(0,dynamic_exhaustion-20)*.65
    return max(0,min(round(
        flow_score+buy_slope_score+accel_score+accel_slope_score+
        volume_accel_score+cvd_score+micro_price_score+price_score+
        structure_score+adaptive_bonus+sweep_bonus+repl_bonus-exhaustion_penalty
    ),100)), adaptive_bonus, sweep_bonus, repl_bonus

def indexed_outcomes(price_rows, signals):
    """Compute future MFE/MAE with binary-searchable 5-second price snapshots."""
    by=defaultdict(list)
    for s,t,p in price_rows:
        by[s].append((t,p))
    for s in by:
        by[s].sort()
    # Convert to coarse 5-second maxima/minima. Signal decisions also occur
    # every 5 seconds, so this retains the relevant decision-time path.
    coarse={}
    for s,rows in by.items():
        buckets=[]
        cur=None; hi=lo=None; last=None
        for t,p in rows:
            b=int(t//5)
            if cur is None or b!=cur:
                if cur is not None: buckets.append((cur*5,hi,lo,last))
                cur=b;hi=p;lo=p;last=p
            else:
                hi=max(hi,p);lo=min(lo,p);last=p
        if cur is not None:buckets.append((cur*5,hi,lo,last))
        coarse[s]=buckets
    for sig in signals:
        rows=coarse[sig["symbol"]]
        times=[x[0] for x in rows]
        i=bisect.bisect_right(times,sig["ts"])
        p0=sig["price"]
        for sec in HORIZONS:
            j=bisect.bisect_right(times,sig["ts"]+sec)
            q=rows[i:j]
            if q:
                sig[f"mfe_{sec}s"]=max(pct(p0,x[1]) for x in q)
                sig[f"mae_{sec}s"]=min(pct(p0,x[2]) for x in q)
            else:
                sig[f"mfe_{sec}s"]=None;sig[f"mae_{sec}s"]=None
        target=p0*1.05
        tt=None
        for k in range(i,bisect.bisect_right(times,sig["ts"]+1800)):
            if rows[k][1]>=target:
                tt=max(0,rows[k][0]-sig["ts"]);break
        sig["time_to_5pct_s"]=tt
        sig["time_to_2pct_s"]=None
        sig["time_to_10pct_s"]=None

def run(input_path, allow_missing):
    events=sorted(load(input_path),key=lambda x:ts(x.get("ts",x.get("time"))))
    states=defaultdict(FastState)
    prices=defaultdict(list)
    signals={p:[] for p in PROFILES}
    for e in events:
        s=str(e.get("symbol") or e.get("s") or "").upper()
        typ=str(e.get("event") or e.get("type") or "").lower()
        if not s: continue
        n=ts(e.get("ts",e.get("time"))); st=states[s]; st.prune_fast(n)
        if typ in ("trade","aggtrade","agg_trade"):
            p=f(e.get("price",e.get("p")));q=f(e.get("qty",e.get("q")))
            maker=str(e.get("is_buyer_maker",e.get("m",""))).lower() in ("1","true","yes")
            item=(n,p,p*q,not maker)
            st.add_trade(item); prices[s].append((n,p))
        elif typ in ("depth","book","orderbook"):
            def L(v):
                if isinstance(v,str):
                    try:v=json.loads(v)
                    except:return {}
                return {f(a[0]):f(a[1]) for a in (v or []) if isinstance(a,(list,tuple)) and len(a)>1 and f(a[0])>0}
            st.d={"bids":L(e.get("bids")),"asks":L(e.get("asks"))}
        else: continue
        if typ not in ("trade","aggtrade","agg_trade"): continue
        b=int(n//5)
        if st.bucket==b: continue
        st.bucket=b
        m=st.micro_fast(n); a=st.adaptive(m); bk=st.book(m,n)
        exh=e.get("exhaustion_score")
        exh=f(exh) if exh not in (None,"") else None
        ok,_=gates(m,exh,not allow_missing)
        if not ok: continue
        base_score,*_=score_profile(m,a,bk,exh,"v157")
        if base_score<72: continue
        for profile in PROFILES:
            score,ab,sb,rb=score_profile(m,a,bk,exh,profile)
            if score<72: continue
            r={"symbol":s,"ts":n,"price":st.p[-1][1],"profile":profile,
               "exhaustion_score":exh,"fast_score":score,
               "adaptive_bonus":ab,"sweep_bonus":sb,"replenishment_bonus":rb}
            r.update(m);r.update(a);r.update(bk)
            signals[profile].append(r)
    # Build the coarse price index once and reuse it for all profiles.
    indexed_by=defaultdict(list)
    for s,rows in prices.items():
        cur=None; hi=lo=last=None
        for t,p in rows:
            b=int(t//5)
            if cur is None or b!=cur:
                if cur is not None: indexed_by[s].append((cur*5,hi,lo,last))
                cur=b; hi=p; lo=p; last=p
            else:
                hi=max(hi,p); lo=min(lo,p); last=p
        if cur is not None: indexed_by[s].append((cur*5,hi,lo,last))
    for p in PROFILES:
        for sig in signals[p]:
            rows=indexed_by[sig["symbol"]]; times=[x[0] for x in rows]
            i=bisect.bisect_right(times,sig["ts"]); p0=sig["price"]
            for sec in HORIZONS:
                j=bisect.bisect_right(times,sig["ts"]+sec); q=rows[i:j]
                if q:
                    sig[f"mfe_{sec}s"]=max(pct(p0,x[1]) for x in q)
                    sig[f"mae_{sec}s"]=min(pct(p0,x[2]) for x in q)
                else:
                    sig[f"mfe_{sec}s"]=None; sig[f"mae_{sec}s"]=None
            target=p0*1.05; tt=None
            for k in range(i,bisect.bisect_right(times,sig["ts"]+1800)):
                if rows[k][1]>=target:
                    tt=max(0,rows[k][0]-sig["ts"]); break
            sig["time_to_5pct_s"]=tt
            sig["time_to_2pct_s"]=None; sig["time_to_10pct_s"]=None
    return signals

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--input",required=True)
    ap.add_argument("--output-dir",default="data/v158_proxy_results")
    ap.add_argument("--allow-missing-exhaustion",action="store_true")
    a=ap.parse_args()
    sig=run(a.input,a.allow_missing_exhaustion)
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    comparison={}
    for p in PROFILES:
        r={"engine":"V15.8 Fast Multi-Profile Historical Proxy",
           "version":"1.1","mode":"events","profile":p,
           "warning":"Not an exact production replay: historical exhaustion, L2 depth, TradingView/BTC regime, cross-sectional routing and dynamic discovery are unavailable.",
           "summary":summary(sig[p]),"signals":sig[p]}
        (out/f"{p}.json").write_text(json.dumps(r,indent=2,allow_nan=False))
        comparison[p]=r["summary"]
    (out/"comparison.json").write_text(json.dumps({
        "engine":"V15.8 fast historical aggTrade proxy",
        "profiles":comparison,
        "warning":"Proxy only; historical L2/exhaustion/TradingView/BTC/cross-sectional routing/dynamic discovery unavailable."
    },indent=2))
    print(json.dumps(comparison,indent=2))

if __name__=="__main__":
    main()
