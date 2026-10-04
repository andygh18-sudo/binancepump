import math, statistics, time
from collections import deque


def _robust_z(value, history):
    vals=[float(x) for x in history if x is not None and math.isfinite(float(x))]
    if len(vals)<8:return 0.0
    med=statistics.median(vals)
    mad=statistics.median([abs(x-med) for x in vals])
    scale=max(1.4826*mad,abs(med)*0.05,1e-9)
    return max(-8.0,min(8.0,(float(value)-med)/scale))

def _clip01(x): return max(0.0,min(1.0,float(x)))

def adaptive_micro_features(state,books,symbol):
    """Per-symbol adaptive anomaly features; advisory only, never a BUY gate."""
    x=state[symbol];now=time.time();trades=[z for z in x.get("trades",[]) if z[0]>=now-10]
    n=len(trades);flow=sum(float(z[2]) for z in trades);buy=sum(float(z[2]) for z in trades if z[3])
    buy_ratio=buy/flow if flow else 0.5;trade_rate=n/10.0;volume_rate=flow/10.0;avg_trade=flow/n if n else 0.0
    cvd=(2.0*buy_ratio-1.0) if flow else 0.0
    px10=((trades[-1][1]/trades[0][1])-1.0)*100.0 if len(trades)>1 and trades[0][1] else 0.0
    px_per_trade=abs(px10)/max(n,1)
    hist=x.setdefault("v158_adaptive_history",deque(maxlen=60));prior=list(hist)
    tz=_robust_z(trade_rate,[p.get("trade_rate",0) for p in prior]);vz=_robust_z(volume_rate,[p.get("volume_rate",0) for p in prior])
    sz=_robust_z(avg_trade,[p.get("avg_trade",0) for p in prior]);cz=_robust_z(cvd,[p.get("cvd",0) for p in prior]);piz=_robust_z(px_per_trade,[p.get("px_per_trade",0) for p in prior])
    buy_delta=buy_ratio-(statistics.median([p.get("buy_ratio",0.5) for p in prior]) if prior else 0.5)
    intensity=max(-8.0,min(8.0,vz-tz)); participation=math.sqrt(max(tz,0.0)**2+max(vz,0.0)**2); synchronized=min(8.0,participation/math.sqrt(2.0));positive=sum(1 for p in prior[-6:] if p.get("trade_z",0)>1.0 or p.get("volume_z",0)>1.0)
    regime=max(0.0,min(100.0,_clip01(max(tz,0)/3)*35+_clip01(max(vz,0)/3)*25+_clip01(max(cz,0)/3)*20+_clip01(max(intensity,0)/3)*10+_clip01(positive/4)*10))
    hist.append({"ts":now,"trade_rate":trade_rate,"volume_rate":volume_rate,"avg_trade":avg_trade,"cvd":cvd,"buy_ratio":buy_ratio,"px_per_trade":px_per_trade,"trade_z":tz,"volume_z":vz})
    return {"adaptive_trade_rate":round(trade_rate,4),"adaptive_volume_rate":round(volume_rate,4),"adaptive_avg_trade_size":round(avg_trade,4),"adaptive_buy_ratio":round(buy_ratio,4),"adaptive_buy_delta":round(buy_delta,4),"adaptive_trade_z":round(tz,3),"adaptive_volume_z":round(vz,3),"adaptive_participation_z":round(participation,3),"adaptive_synchronized_flow_z":round(synchronized,3),"adaptive_trade_size_z":round(sz,3),"adaptive_cvd_z":round(cz,3),"adaptive_intensity_z":round(intensity,3),"adaptive_price_impact_z":round(piz,3),"adaptive_regime_change":round(regime,1),"adaptive_baseline_samples":len(prior)}

def depth_sweep_features(state,books,symbol):
    """Measure executable liquidity cost/capacity across near-price ask/bid bands.
    Uses the existing local L2 book; advisory only and intentionally cheap.
    """
    b=books.get(symbol)
    if not b or not getattr(b,"ready",False):
        return {"v158_sweep_score":0.0,"v158_ask_capacity_5bps":0.0,"v158_ask_capacity_10bps":0.0,
                "v158_ask_capacity_25bps":0.0,"v158_bid_capacity_5bps":0.0,"v158_bid_capacity_10bps":0.0,
                "v158_bid_capacity_25bps":0.0,"v158_ask_sweep_cost_bps":0.0,"v158_bid_sweep_cost_bps":0.0,
                "v158_ask_liquidity_gap_bps":0.0,"v158_bid_liquidity_gap_bps":0.0,"v158_liquidity_asymmetry":0.0}
    try:
        asks=sorted((float(p),float(q)) for p,q in b.asks.items() if float(q)>0)
        bids=sorted(((float(p),float(q)) for p,q in b.bids.items() if float(q)>0),reverse=True)
        if not asks or not bids:return {"v158_sweep_score":0.0}
        best_ask=asks[0][0];best_bid=bids[0][0];mid=(best_ask+best_bid)/2.0
        if mid<=0:return {"v158_sweep_score":0.0}
        def bands(levels,side):
            out={5:0.0,10:0.0,25:0.0};gap=0.0;prev=0.0
            for price,qty in levels:
                dist=((price-mid)/mid*10000.0) if side=="ask" else ((mid-price)/mid*10000.0)
                if dist<0:continue
                notional=price*qty
                for band in out:
                    if dist<=band:out[band]+=notional
                if gap==0.0 and dist>2.0 and prev>0 and dist-prev>5.0:gap=dist-prev
                prev=dist
            return out,gap
        ask,gap_a=bands(asks,"ask");bid,gap_b=bands(bids,"bid")
        # Cost-to-trade for a small normalized order: walk the book until the
        # reference notional is filled, then report VWAP slippage from the touch.
        hist=state[symbol].get("trades",[]);now=time.time();recent=[float(z[2]) for z in hist if z[0]>=now-10 and float(z[2])>0]
        target=max(1000.0,(sum(recent)/max(len(recent),1))*max(len(recent),1)*0.25)
        def sweep_cost(levels,target,side):
            remaining=target;spent=0.0;qty=0.0
            touch=levels[0][0]
            for price,size in levels:
                take=min(size,remaining/price)
                if take<=0:break
                spent+=take*price;qty+=take;remaining-=take*price
                if remaining<=1e-9:break
            if qty<=0 or remaining>target*0.01:return 0.0
            vwap=spent/qty
            return abs(vwap/touch-1.0)*10000.0
        ask_cost=sweep_cost(asks,target,"ask");bid_cost=sweep_cost(bids,target,"bid")
        ask10=ask.get(10,0.0);bid10=bid.get(10,0.0);ask25=ask.get(25,0.0);bid25=bid.get(25,0.0)
        asym=(bid10-ask10)/max(bid10+ask10,1e-9)
        thin=max(0.0,min(1.0,(target/max(ask10,1.0)-1.0)/4.0))
        gap_score=max(0.0,min(gap_a/25.0,1.0))
        cost_score=max(0.0,min(ask_cost/8.0,1.0))
        sweep_score=max(0.0,min(100.0,thin*45.0+gap_score*20.0+cost_score*20.0+max(0.0,asym)*15.0))
        return {"v158_sweep_score":round(sweep_score,1),"v158_ask_capacity_5bps":round(ask.get(5,0.0),2),
                "v158_ask_capacity_10bps":round(ask10,2),"v158_ask_capacity_25bps":round(ask25,2),
                "v158_bid_capacity_5bps":round(bid.get(5,0.0),2),"v158_bid_capacity_10bps":round(bid10,2),
                "v158_bid_capacity_25bps":round(bid25,2),"v158_ask_sweep_cost_bps":round(ask_cost,3),
                "v158_bid_sweep_cost_bps":round(bid_cost,3),"v158_ask_liquidity_gap_bps":round(gap_a,3),
                "v158_bid_liquidity_gap_bps":round(gap_b,3),"v158_liquidity_asymmetry":round(asym,4),
                "v158_sweep_target_notional":round(target,2)}
    except Exception:
        return {"v158_sweep_score":0.0}

def replenishment_absorption_features(state,books,symbol):
    """Detect repeated aggressive-flow absorption followed by L2 replenishment.
    Existing aggTrade + depth@100ms only; advisory, not a hard gate.
    """
    b=books.get(symbol); x=state[symbol]
    empty={"v158_absorption_persistence_score":0.0,"v158_ask_replenishment_ratio":0.0,"v158_bid_replenishment_ratio":0.0,"v158_ask_replenishment_events":0,"v158_bid_replenishment_events":0,"v158_ask_absorption_events":0,"v158_bid_absorption_events":0,"v158_absorption_price_response":0.0,"v158_absorption_state":"NO_BOOK"}
    if not b or not getattr(b,"ready",False): return empty
    try:
        asks=sorted((float(p),float(q)) for p,q in b.asks.items() if float(q)>0); bids=sorted(((float(p),float(q)) for p,q in b.bids.items() if float(q)>0),reverse=True)
        if not asks or not bids:return empty
        mid=(asks[0][0]+bids[0][0])/2.0
        def near(levels,side):
            total=0.0
            for price,qty in levels:
                dist=((price-mid)/mid*10000.0) if side=="ask" else ((mid-price)/mid*10000.0)
                if 0<=dist<=10: total+=price*qty
            return total
        ask10,bid10=near(asks,"ask"),near(bids,"bid"); now=time.time()
        trades=[z for z in x.get("trades",[]) if z[0]>=now-10]; flow=sum(float(z[2]) for z in trades); buy_flow=sum(float(z[2]) for z in trades if z[3]); buy_ratio=buy_flow/flow if flow else 0.5
        p10=((trades[-1][1]/trades[0][1])-1.0)*100.0 if len(trades)>1 and trades[0][1] else 0.0
        st=x.setdefault("v158_absorption_state",{"prev_ask":0.0,"prev_bid":0.0,"ask_pending":False,"bid_pending":False,"ask_floor":0.0,"bid_floor":0.0,"ask_pending_ts":0.0,"bid_pending_ts":0.0}); hist=x.setdefault("v158_absorption_history",deque(maxlen=60))
        prev_ask=float(st.get("prev_ask",0) or 0); prev_bid=float(st.get("prev_bid",0) or 0); ask_drop=(1-ask10/max(prev_ask,1e-9)) if prev_ask>0 else 0.0; bid_drop=(1-bid10/max(prev_bid,1e-9)) if prev_bid>0 else 0.0
        ask_abs=ask_drop>=0.10 and buy_ratio>=0.55 and flow>0; bid_abs=bid_drop>=0.10 and buy_ratio<=0.45 and flow>0
        if ask_abs:
            if not st.get("ask_pending"): st["ask_floor"]=ask10
            else: st["ask_floor"]=min(float(st.get("ask_floor",ask10) or ask10),ask10)
            st["ask_pending"]=True; st["ask_pending_ts"]=now
        elif st.get("ask_pending") and ask10>=float(st.get("ask_floor",ask10) or ask10)*1.12: st["ask_pending"]=False
        if bid_abs:
            if not st.get("bid_pending"): st["bid_floor"]=bid10
            else: st["bid_floor"]=min(float(st.get("bid_floor",bid10) or bid10),bid10)
            st["bid_pending"]=True; st["bid_pending_ts"]=now
        elif st.get("bid_pending") and bid10>=float(st.get("bid_floor",bid10) or bid10)*1.12: st["bid_pending"]=False
        hist.append({"ts":now,"ask":ask10,"bid":bid10,"ask_abs":int(ask_abs),"bid_abs":int(bid_abs),"buy_ratio":buy_ratio,"p10":p10})
        if st.get("ask_pending") and now-float(st.get("ask_pending_ts",now))>30: st["ask_pending"]=False
        if st.get("bid_pending") and now-float(st.get("bid_pending_ts",now))>30: st["bid_pending"]=False
        recent=list(hist); ask_abs_events=sum(int(h.get("ask_abs",0)) for h in recent); bid_abs_events=sum(int(h.get("bid_abs",0)) for h in recent)
        ask_repl=sum(1 for i in range(1,len(recent)) if recent[i].get("ask_abs") and recent[i].get("ask",0)>=max(recent[i-1].get("ask",0),1e-9)*1.12); bid_repl=sum(1 for i in range(1,len(recent)) if recent[i].get("bid_abs") and recent[i].get("bid",0)>=max(recent[i-1].get("bid",0),1e-9)*1.12)
        ask_ratio=min(1.0,ask_repl/max(ask_abs_events,1)); bid_ratio=min(1.0,bid_repl/max(bid_abs_events,1)); persistence=min(100.0,ask_ratio*45.0+min(ask_repl/3.0,1.0)*25.0+min(bid_repl/2.0,1.0)*10.0)
        response_score=min(20.0,max(0.0,(max(-1.0,min(2.0,p10))+0.25)/2.25*20.0)); directional=min(1.0,max(0.0,(buy_ratio-0.50)/0.25)); score=persistence*0.55+response_score*0.25+directional*20.0
        seller_wall=ask_abs_events>=2 and ask_repl>=1 and buy_ratio>=0.58 and p10<0.10; bid_support=bid_repl>=1 and buy_ratio<=0.50
        if seller_wall: label="SELLER_ABSORPTION"
        elif score>=60 and (ask_repl>=1 or bid_support): label="BULLISH_REPLENISHMENT"
        elif ask_abs_events or bid_abs_events: label="ABSORPTION_BUILDING"
        else: label="NEUTRAL"
        st["prev_ask"]=ask10; st["prev_bid"]=bid10
        return {"v158_absorption_persistence_score":round(max(0,min(100,score)),1),"v158_ask_replenishment_ratio":round(ask_ratio,3),"v158_bid_replenishment_ratio":round(bid_ratio,3),"v158_ask_replenishment_events":ask_repl,"v158_bid_replenishment_events":bid_repl,"v158_ask_absorption_events":ask_abs_events,"v158_bid_absorption_events":bid_abs_events,"v158_absorption_price_response":round(p10,4),"v158_absorption_state":label,"v158_ask_absorption_pending":bool(st.get("ask_pending",False)),"v158_bid_absorption_pending":bool(st.get("bid_pending",False))}
    except Exception: return empty

def adaptive_book_features(state,books,symbol):
    """Detect unusual ask-liquidity depletion/book vacuum cheaply from existing depth data."""
    b=books.get(symbol)
    if not b or not getattr(b,"ready",False):return {"v158_book_vacuum_score":0.0,"v158_ask_consumption_z":0.0,"v158_price_impact_efficiency":0.0}
    m=b.metrics(20);now=time.time();x=state[symbol];ask=max(float(m.get("ask_depth",0) or 0),0.0);bid=max(float(m.get("bid_depth",0) or 0),0.0)
    prev=x.get("v158_book_prev");ask_cons=0.0
    if prev:ask_cons=max(0.0,(1.0-ask/max(float(prev.get("ask",ask)),1e-9))*100.0)
    hist=x.setdefault("v158_book_history",deque(maxlen=60));prior=list(hist);az=_robust_z(ask_cons,[p.get("ask_consumption",0) for p in prior]);spread=float(m.get("spread_bps",0) or 0)
    hist.append({"ts":now,"ask_consumption":ask_cons});x["v158_book_prev"]={"ts":now,"ask":ask,"bid":bid}
    recent=[z for z in x.get("trades",[]) if z[0]>=now-10];flow=sum(float(z[2]) for z in recent);p10=((recent[-1][1]/recent[0][1])-1.0)*100.0 if len(recent)>1 and recent[0][1] else 0.0
    impact=abs(p10)/max(flow/100000.0,1e-6) if flow else 0.0
    vacuum=max(0.0,min(100.0,max(az,0)/3.0*70.0+max(0.0,ask_cons)*1.5))
    return {"v158_book_vacuum_score":round(vacuum,1),"v158_ask_consumption_z":round(az,3),"v158_price_impact_efficiency":round(impact,6),"v158_ask_consumption":round(ask_cons,2),"v158_spread_bps":round(spread,2)}

class MarketDiscovery:
    """Lightweight all-market spot discovery using Binance !ticker@arr data."""
    def __init__(self, max_promoted=20, min_quote_volume=10000, ttl=180,
                 min_score=55):
        self.max_promoted=int(max_promoted)
        self.min_quote_volume=float(min_quote_volume)
        self.ttl=float(ttl)
        self.min_score=float(min_score)
        self.items={}
        self.promoted={}
        self.events=0
        self.cross_section_refresh=0
        self.last_cross_section_ts=0.0

    def update(self, t):
        try:
            s=str(t.get("s","")).upper()
            if not s or not s.endswith("USDT"): return None
            price=float(t.get("c",0) or 0)
            if price<=0: return None
            now=float(t.get("E",0) or 0)/1000.0 or time.time()
            quote=float(t.get("q",0) or 0)
            pct=float(t.get("P",0) or 0)
            high=float(t.get("h",0) or 0)
            low=float(t.get("l",0) or 0)
            trades=float(t.get("n",0) or 0)
            old=self.items.get(s)
            velocity=0.0; trade_rate=0.0; trade_anomaly=1.0
            volume_rate=0.0
            if old:
                dt=max(now-old["ts"],0.2)
                velocity=(price/old["price"]-1.0)*100.0/dt
                trade_rate=max(0.0,trades-old["trades"])/dt
                volume_rate=max(0.0,quote-old["quote"])/dt
                hist=old["trade_rates"]
                if trade_rate>0 and len(hist)>=5:
                    med=max(statistics.median(hist),0.01)
                    trade_anomaly=trade_rate/med
                hist.append(trade_rate)
                if len(hist)>30: hist.popleft()
            else:
                trade_rate=0.0
            range_pos=(price-low)/max(high-low,price*1e-9) if high>low else 0.5
            liquidity=min(max(math.log10(max(quote,10000)/10000.0)*8.0,0),24)
            vel_score=min(max(velocity/0.08,0),1)*28
            trade_score=min(max((trade_anomaly-1.0)/3.0,0),1)*24
            pct_score=min(max((pct-0.5)/5.0,0),1)*14
            range_score=min(max((range_pos-0.55)/0.45,0),1)*10
            score=round(min(100,vel_score+trade_score+pct_score+range_score+liquidity))
            item={"ts":now,"price":price,"quote":quote,"trades":trades,
                  "trade_rates":old["trade_rates"] if old else deque(maxlen=30),
                  "velocity_pct_s":velocity,"trade_rate":trade_rate,
                  "trade_anomaly":trade_anomaly,"volume_rate":volume_rate,
                  "score":score,"pct24h":pct,"quote_volume":quote}
            self.items[s]=item
            self.events+=1
            if now-self.last_cross_section_ts>=2.0:
                self._refresh_cross_section(now)
                self.last_cross_section_ts=now
            item=self.items[s]
            eligible=(quote>=self.min_quote_volume and
                      (score>=self.min_score or
                       (float(item.get("cross_section_percentile",0.0))>=99.0 and
                        int(item.get("cross_section_rank",999999))<=max(20,self.max_promoted))) and
                      (velocity>=0.025 or trade_anomaly>=1.45 or pct>=2.0))
            if eligible:
                self.promoted[s]=max(now+self.ttl,self.promoted.get(s,0))
            self._trim(now)
            if eligible:
                return {"symbol":s,**item,"promotion_score":float(item.get("cross_section_route_score",score) or score)}
        except Exception:
            return None
        return None

    def _refresh_cross_section(self, now):
        """Rank discovery anomalies across the live Binance universe.
        Routing-only: downstream V15.7/V15.8 scores and hard gates are unchanged.
        """
        items=[(s,v) for s,v in self.items.items()
               if float(v.get("quote_volume",0) or 0)>=self.min_quote_volume]
        if len(items)<10:
            for s,v in items:
                v["cross_section_rank"]=len(items)
                v["cross_section_percentile"]=0.0
                v["cross_section_route_score"]=float(v.get("score",0) or 0)
            return
        def key(v):
            return (
                0.45*min(max(float(v.get("score",0) or 0)/100.0,0),1) +
                0.20*min(max(float(v.get("trade_anomaly",1) or 1)/4.0,0),1) +
                0.20*min(max(float(v.get("velocity_pct_s",0) or 0)/0.10,0),1) +
                0.15*min(max(float(v.get("volume_rate",0) or 0)/(max(float(v.get("quote_volume",0) or 1),1)*0.02),0),1)
            )
        ranked=sorted(items,key=lambda sv:key(sv[1]),reverse=True)
        n=len(ranked)
        for rank,(s,v) in enumerate(ranked,1):
            pct=1.0-(rank-1)/max(n-1,1)
            v["cross_section_rank"]=rank
            v["cross_section_percentile"]=round(pct*100.0,2)
            v["cross_section_route_score"]=round(0.75*float(v.get("score",0) or 0)+0.25*pct*100.0,2)
        self.cross_section_refresh+=1

    def _trim(self, now):
        active=[(s,t) for s,t in self.promoted.items() if t>now]
        active.sort(key=lambda x:self.items.get(x[0],{}).get("score",0),reverse=True)
        keep=dict(active[:self.max_promoted])
        self.promoted=keep

    def active(self, now=None):
        now=time.time() if now is None else now
        self._trim(now)
        return set(self.promoted)

def trade_features(state, symbol, seconds=60):
    trades=[x for x in state[symbol]["trades"] if x[0]>=time.time()-seconds]
    if not trades:
        return {"whale_score":0.0,"large_trade_count":0,"large_buy_notional":0.0,
                "large_sell_notional":0.0,"large_trade_notional":0.0,
                "large_trade_imbalance":0.0,"median_trade_notional":0.0,
                "p95_trade_notional":0.0}
    vals=sorted(float(x[2]) for x in trades if float(x[2])>0)
    med=statistics.median(vals) if vals else 0.0
    p95=vals[min(len(vals)-1,max(0,int(len(vals)*0.95)-1))] if vals else 0.0
    threshold=max(p95,med*5.0,0.0)
    large=[x for x in trades if float(x[2])>=threshold and threshold>0]
    lb=sum(float(x[2]) for x in large if x[3])
    ls=sum(float(x[2]) for x in large if not x[3])
    total=lb+ls
    imbalance=(lb-ls)/total if total else 0.0
    count=len(large)
    size_score=min(max((p95/max(med,1e-9)-1.0)/9.0,0),1)*45
    count_score=min(count/5.0,1)*25
    flow_score=(imbalance+1.0)/2.0*30
    return {"whale_score":round(min(100,size_score+count_score+flow_score),1),
            "large_trade_count":count,"large_buy_notional":lb,
            "large_sell_notional":ls,"large_trade_notional":total,
            "large_trade_imbalance":imbalance,"median_trade_notional":med,
            "p95_trade_notional":p95}

def liquidity_features(state, books, symbol):
    b=books.get(symbol)
    if not b: return {"liquidity_score":0.0,"ask_depth_change":0.0,"bid_depth_change":0.0,
                      "ask_consumption":0.0,"bid_consumption":0.0,
                      "absorption_score":0.0,"liquidity_breakout_score":0.0}
    m=b.metrics(20)
    if not m.get("ready"):
        return {"liquidity_score":0.0,"ask_depth_change":0.0,"bid_depth_change":0.0,
                "ask_consumption":0.0,"bid_consumption":0.0,
                "absorption_score":0.0,"liquidity_breakout_score":0.0}
    x=state[symbol]
    now=time.time()
    prev=x.get("v158_liquidity")
    bd=float(m.get("bid_depth",0) or 0); ad=float(m.get("ask_depth",0) or 0)
    if not prev:
        x["v158_liquidity"]={"ts":now,"bid":bd,"ask":ad}
        return {"liquidity_score":0.0,"ask_depth_change":0.0,"bid_depth_change":0.0,
                "ask_consumption":0.0,"bid_consumption":0.0,
                "absorption_score":0.0,"liquidity_breakout_score":0.0}
    dt=max(now-float(prev.get("ts",now)),0.5)
    ask_change=(ad/max(float(prev.get("ask",ad)),1e-9)-1)*100
    bid_change=(bd/max(float(prev.get("bid",bd)),1e-9)-1)*100
    ask_cons=max(0.0,-ask_change); bid_cons=max(0.0,-bid_change)
    x["v158_liquidity"]={"ts":now,"bid":bd,"ask":ad}
    breakout=min(100.0,ask_cons*3.0 + max(0.0,bid_change)*1.0)
    absorption=min(100.0,max(0.0,ask_change)*2.0 + max(0.0,bid_change)*0.5)
    liquidity=min(100.0,breakout*0.7+absorption*0.3)
    return {"liquidity_score":round(liquidity,1),
            "ask_depth_change":round(ask_change,2),
            "bid_depth_change":round(bid_change,2),
            "ask_consumption":round(ask_cons,2),
            "bid_consumption":round(bid_cons,2),
            "absorption_score":round(absorption,1),
            "liquidity_breakout_score":round(breakout,1)}

def data_quality(state, books, symbol):
    x=state[symbol]
    now=time.time()
    last_trade=float(x.get("last_trade_event",0) or 0)
    trade_age=max(0,now-last_trade) if last_trade else 999
    b=books.get(symbol)
    ready=bool(b and b.ready)
    book_age=max(0,now-float(x.get("last_book_event",0) or 0)) if x and x.get("last_book_event") else 999
    score=0.0
    score+=45 if trade_age<=3 else 30 if trade_age<=8 else 10 if trade_age<=15 else 0
    score+=35 if ready and book_age<=3 else 20 if ready and book_age<=8 else 0
    score+=20 if x.get("candle") else 0
    return {"data_quality_score":round(score,1),"trade_age_s":round(trade_age,1),
            "book_age_s":round(book_age,1),"book_ready":ready}
