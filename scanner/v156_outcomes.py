"""V15.6 forward outcome engine.
Creates one durable episode per genuine EARLY IGNITION signal and resolves
1m/3m/5m/10m/30m/60m checkpoints from future observations only.
"""
import json, os, time, uuid
from pathlib import Path

PATH=Path(os.getenv("V156_OUTCOME_PATH","data/v156_outcomes.jsonl"))
CHECKPOINTS=(60,180,300,600,1800,3600)
OBS_INTERVAL=float(os.getenv("V156_OUTCOME_OBS_INTERVAL","10"))
MAX_OPEN=int(os.getenv("V156_OUTCOME_MAX_OPEN","500"))

class OutcomeEngine:
    def __init__(self,path=PATH):
        self.path=Path(path); self.path.parent.mkdir(parents=True,exist_ok=True)
        self.events={}; self._load()

    def _load(self):
        try:
            with self.path.open(encoding="utf-8") as f:
                for line in f:
                    try:r=json.loads(line)
                    except Exception:continue
                    eid=r.get("event_id")
                    if not eid:continue
                    typ=r.get("event")
                    if typ=="SIGNAL": self.events[eid]=r
                    elif eid in self.events and typ=="OBS":
                        e=self.events[eid]
                        e["last_obs_ts"]=r.get("ts",e.get("last_obs_ts")); e["last_price"]=r.get("price",e.get("last_price"))
                        e["mfe_pct"]=max(float(e.get("mfe_pct",0) or 0),float(r.get("return_pct",0) or 0))
                        e["mae_pct"]=min(float(e.get("mae_pct",0) or 0),float(r.get("return_pct",0) or 0))
                    elif eid in self.events and typ=="CHECKPOINT":
                        self.events[eid].setdefault("checkpoints",{})[str(r.get("checkpoint"))]=r
                    elif eid in self.events and typ=="RESOLVED":
                        self.events[eid].update(r); self.events[eid]["resolved"]=True
        except FileNotFoundError:pass

    def _append(self,r):
        with self.path.open("a",encoding="utf-8") as f:f.write(json.dumps(r,separators=(",",":"),allow_nan=False)+"\n")

    @staticmethod
    def _price(row):
        try:
            p=float(row.get("price")); return p if p>0 else None
        except Exception:return None

    def register_signal(self,event,row,now=None):
        if not event or event.get("event")!="TRIGGER":return None
        eid=str(event.get("event_id") or uuid.uuid4().hex[:12])
        if eid in self.events:return eid
        p=self._price(row)
        if p is None:return None
        ts=float(event.get("ts",now or time.time()))
        rec={"event":"SIGNAL","event_id":eid,"symbol":str(event.get("symbol") or row.get("symbol") or "").upper(),
             "signal_ts":ts,"signal_price":p,"version":"V15.6","stage":"EARLY IGNITION",
             "signal_score":event.get("v154_pump_entry_score",event.get("v154_score")),
             "v15_score":event.get("v15_score"),"volume_ratio":event.get("volume_ratio"),
             "trade_accel":event.get("trade_accel"),"buy_pressure":event.get("buy_pressure"),
             "price_10s":event.get("price_10s"),"price_60s":event.get("price_60s"),
             "rs5":event.get("rs5"),"tv_bullish_timeframes":event.get("tv_bullish_timeframes"),
             "fast_score":row.get("v156_fast_score"),"signature_score":row.get("v156_signature_score"),
             "signature_signals":row.get("v156_signature_signals"),"mfe_pct":0.0,"mae_pct":0.0,
             "last_obs_ts":ts,"last_price":p,"checkpoints":{},"resolved":False}
        self.events[eid]=rec; self._append(rec); return eid

    def observe(self,rows,now=None):
        now=float(time.time() if now is None else now)
        by={str(r.get("symbol","")).upper():r for r in rows if isinstance(r,dict) and r.get("symbol")}
        writes=0
        for eid,e in list(self.events.items()):
            if e.get("resolved"):continue
            if len([x for x in self.events.values() if not x.get("resolved")])>MAX_OPEN:continue
            row=by.get(str(e.get("symbol","")).upper())
            if not row:continue
            p=self._price(row)
            if p is None:continue
            t0=float(e.get("signal_ts",0) or 0); age=now-t0
            if age<0:continue
            base=float(e.get("signal_price",p) or p); ret=(p/base-1.0)*100.0
            e["last_obs_ts"]=now;e["last_price"]=p
            e["mfe_pct"]=max(float(e.get("mfe_pct",0) or 0),ret)
            e["mae_pct"]=min(float(e.get("mae_pct",0) or 0),ret)
            pending={}
            for cp in CHECKPOINTS:
                key=str(cp)
                if key in e.get("checkpoints",{}):continue
                if age>=cp:
                    pending[key]={"checkpoint_seconds":cp,"actual_ts":now,"lag_seconds":round(age-cp,2),
                                  "price":p,"return_pct":round(ret,4),"hit_1pct":ret>=1,"hit_2pct":ret>=2,
                                  "hit_3pct":ret>=3,"hit_5pct":ret>=5,"hit_10pct":ret>=10}
            if pending:
                e.setdefault("checkpoints",{}).update(pending)
                for key,val in pending.items():
                    self._append({"event":"CHECKPOINT","event_id":eid,"symbol":e["symbol"],"ts":now,"checkpoint":int(key),**val});writes+=1
            last=float(e.get("last_persist_ts",0) or 0)
            if now-last>=OBS_INTERVAL:
                self._append({"event":"OBS","event_id":eid,"symbol":e["symbol"],"ts":now,"price":p,"return_pct":round(ret,4)});e["last_persist_ts"]=now;writes+=1
            if age>=CHECKPOINTS[-1]:
                e["resolved"]=True;e["resolved_ts"]=now;e["final_return_pct"]=round(ret,4)
                e["mfe_pct"]=round(float(e.get("mfe_pct",0)),4);e["mae_pct"]=round(float(e.get("mae_pct",0)),4)
                c10=e.get("checkpoints",{}).get("600",{})
                e["confirmed_3pct_10m"]=bool(c10.get("hit_3pct",False))
                self._append({"event":"RESOLVED","event_id":eid,"symbol":e["symbol"],"ts":now,"resolved":True,
                              "final_return_pct":e["final_return_pct"],"mfe_pct":e["mfe_pct"],"mae_pct":e["mae_pct"],
                              "confirmed_3pct_10m":e["confirmed_3pct_10m"]});writes+=1
        return writes

    def summary(self):
        events=list(self.events.values());resolved=[e for e in events if e.get("resolved")]
        def cp_stats(sec):
            vals=[e.get("checkpoints",{}).get(str(sec)) for e in events]
            vals=[v for v in vals if v]
            if not vals:return {"samples":0}
            return {"samples":len(vals),
                    "avg_return_pct":round(sum(float(v.get("return_pct",0)) for v in vals)/len(vals),4),
                    "hit_1pct":sum(bool(v.get("hit_1pct")) for v in vals),"hit_2pct":sum(bool(v.get("hit_2pct")) for v in vals),
                    "hit_3pct":sum(bool(v.get("hit_3pct")) for v in vals),"hit_5pct":sum(bool(v.get("hit_5pct")) for v in vals),
                    "hit_10pct":sum(bool(v.get("hit_10pct")) for v in vals)}
        return {"version":"V15.6","signals":len(events),"resolved":len(resolved),"open":len(events)-len(resolved),
                "checkpoints":{"1m":cp_stats(60),"3m":cp_stats(180),"5m":cp_stats(300),"10m":cp_stats(600),
                               "30m":cp_stats(1800),"60m":cp_stats(3600)},
                "resolved_mfe_avg_pct":round(sum(float(e.get("mfe_pct",0)) for e in resolved)/len(resolved),4) if resolved else None,
                "resolved_mae_avg_pct":round(sum(float(e.get("mae_pct",0)) for e in resolved)/len(resolved),4) if resolved else None}

    def write_summary(self):
        self.path.with_name("v156_outcome_summary.json").write_text(json.dumps(self.summary(),indent=2),encoding="utf-8")
