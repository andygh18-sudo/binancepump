import json, os, time

def _f(x, k, d=0.0):
    try:
        v=float(x.get(k,d) or d)
        return v if v == v and abs(v) != float("inf") else d
    except (TypeError,ValueError):
        return d

def _score(r):
    base=max(0,min(100,_f(r,"_sustained_pump_score",_f(r,"sustained_pump_score"))))
    follow=max(0,min(100,50+_f(r,"price_5m")*5+_f(r,"price_10m")*2+_f(r,"price_15m")))
    buy=max(0,min(100,(_f(r,"buy_pressure")-.45)*200))
    accel=max(0,min(100,(_f(r,"trade_accel")-.5)*35))
    vol=max(0,min(100,(_f(r,"volume_ratio")-.5)*30))
    cvd=max(0,min(100,50+_f(r,"cvd")*50))
    penalty=max(0,min(30,(_f(r,"exhaustion_score")-35)*.6))
    return round(max(0,min(100,base*.4+follow*.2+buy*.15+accel*.08+vol*.07+cvd*.1-penalty)),2)

def update_global_top2(candidates, observed_symbols=None, now=None, path=None):
    now=now or time.time()
    path=path or os.getenv("SUSTAINED_PUMP_LEADERBOARD_FILE","data/v158_sustained_pump_top2.json")
    ttl=float(os.getenv("SUSTAINED_PUMP_POOL_TTL_SECONDS","1800"))
    margin=float(os.getenv("SUSTAINED_PUMP_REPLACEMENT_MARGIN","5"))
    step=float(os.getenv("SUSTAINED_PUMP_ALERT_SCORE_STEP","5"))
    os.makedirs(os.path.dirname(path) or ".",exist_ok=True)
    try:
        with open(path,encoding="utf-8") as f: old=json.load(f)
        pool=old.get("pool",{}) if isinstance(old,dict) else {}
        prior=set(str(s).upper() for s in old.get("top2",[])) if isinstance(old,dict) else set()
    except (OSError,ValueError,TypeError):
        pool={}; prior=set()
    if not isinstance(pool,dict): pool={}
    eligible={str(r.get("symbol","")).upper() for r in candidates}
    observed={str(s).upper() for s in (observed_symbols or [])}
    for sym,item in list(pool.items()):
        if now-_f(item,"last_seen")>ttl or (observed and sym.upper() in observed and sym.upper() not in eligible):
            pool.pop(sym,None)
    for r in candidates:
        sym=str(r.get("symbol","")).upper()
        if not sym: continue
        item=dict(r); item["symbol"]=sym; item["last_seen"]=now
        item["global_rank_score"]=_score(item)
        prev=pool.get(sym,{})
        item["last_alert_score"]=_f(prev,"last_alert_score")
        item["last_alert_at"]=_f(prev,"last_alert_at")
        pool[sym]=item
    ranked=sorted(pool.values(),key=lambda r:(_f(r,"global_rank_score"),_f(r,"_sustained_pump_score"),_f(r,"price_15m")),reverse=True)
    leaders=ranked[:2]; weakest=min((_f(pool.get(s,{}),"global_rank_score") for s in prior if s in pool),default=0)
    alerts=[]
    for r in leaders:
        sym=r["symbol"]; score=_f(r,"global_rank_score")
        new=sym not in prior and (len(prior)<2 or score>=weakest+margin)
        improved=now-_f(r,"last_alert_at")>=300 and score>=_f(r,"last_alert_score")+step
        if new or improved:
            alerts.append(r); r["last_alert_score"]=score; r["last_alert_at"]=now
    payload={"updated":now,"selector":"V15.8_GLOBAL_SUSTAINED_PUMP_TOP2","pool_ttl_seconds":ttl,
      "top2":[r["symbol"] for r in leaders],"pool":pool,
      "leaderboard":[{k:r.get(k,0) for k in ("symbol","global_rank_score","_sustained_pump_score","price_5m","price_10m","price_15m","buy_pressure","volume_ratio","trade_accel","exhaustion_score")} for r in ranked[:20]]}
    tmp=path+".tmp"
    with open(tmp,"w",encoding="utf-8") as f: json.dump(payload,f,indent=2)
    os.replace(tmp,path)
    return alerts,ranked
