import asyncio

class LocalOrderBook:
    def __init__(self,symbol,rest_base,limit=1000):
        self.symbol=symbol; self.rest_base=rest_base; self.limit=limit
        self.bids={}; self.asks={}; self.last_update_id=0
        self.buffer=[]; self.ready=False; self.gaps=0; self.lock=asyncio.Lock()

    def buffer_event(self,e):
        if self.ready:
            return self.apply(e)
        self.buffer.append(e)
        if len(self.buffer)>8000:self.buffer=self.buffer[-8000:]
        return True

    async def resync(self,session):
        async with self.lock:
            self.ready=False
            try:
                async with session.get(f"{self.rest_base}/api/v3/depth",
                    params={"symbol":self.symbol,"limit":self.limit},timeout=12) as r:
                    r.raise_for_status(); snap=await r.json()
                self.bids={float(p):float(q) for p,q in snap["bids"] if float(q)>0}
                self.asks={float(p):float(q) for p,q in snap["asks"] if float(q)>0}
                self.last_update_id=int(snap["lastUpdateId"])
                buf=self.buffer; self.buffer=[]
                start=None
                for i,e in enumerate(buf):
                    if int(e["u"])<=self.last_update_id: continue
                    if int(e["U"])<=self.last_update_id+1<=int(e["u"]):
                        start=i; break
                if start is None:
                    self.buffer=buf[-4000:]
                    return False
                for e in buf[start:]:
                    if int(e["u"])<=self.last_update_id: continue
                    if int(e["U"])>self.last_update_id+1:
                        self.gaps+=1; self.ready=False
                        self.buffer=buf[buf.index(e):]
                        return False
                    self.apply(e)
                self.ready=True
                return True
            except Exception:
                return False

    def apply(self,e):
        U,u=int(e["U"]),int(e["u"])
        if u<=self.last_update_id:return True
        if U>self.last_update_id+1:
            self.ready=False;self.gaps+=1;return False
        for p,q in e.get("b",[]):
            p=float(p);q=float(q)
            if q==0:self.bids.pop(p,None)
            else:self.bids[p]=q
        for p,q in e.get("a",[]):
            p=float(p);q=float(q)
            if q==0:self.asks.pop(p,None)
            else:self.asks[p]=q
        self.last_update_id=u
        return True

    def metrics(self,levels=20):
        if not self.ready or not self.bids or not self.asks:
            return {"imbalance":0,"spread_bps":0,"bid_depth":0,"ask_depth":0,"ready":False}
        bids=sorted(self.bids.items(),reverse=True)[:levels]
        asks=sorted(self.asks.items())[:levels]
        bd=sum(p*q for p,q in bids);ad=sum(p*q for p,q in asks)
        imb=(bd-ad)/(bd+ad) if bd+ad else 0
        mid=(bids[0][0]+asks[0][0])/2
        spread=(asks[0][0]-bids[0][0])/mid*10000 if mid else 0
        return {"imbalance":imb,"spread_bps":spread,"bid_depth":bd,"ask_depth":ad,"ready":True}
