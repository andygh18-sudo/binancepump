import json, os, time
from pathlib import Path

V156_STATE_PATH=Path(os.getenv("V156_POSTBUY_STATE_PATH","data/v157_observations.jsonl"))
V156_WINDOW=float(os.getenv("V156_POSTBUY_WINDOW_SECONDS","3600"))
V156_GRACE=float(os.getenv("V156_POSTBUY_GRACE_SECONDS","30"))
V156_CONFIRM=int(os.getenv("V156_POSTBUY_CONFIRM_OBS","3"))
V156_GAP=float(os.getenv("V156_POSTBUY_OBSERVATION_GAP_SECONDS","60"))
V156_MIN_BUY=float(os.getenv("V156_POSTBUY_MIN_BUY","0.55"))
V156_MIN_ACCEL=float(os.getenv("V156_POSTBUY_MIN_ACCEL","1.25"))
V156_MIN_VOLUME=float(os.getenv("V156_POSTBUY_MIN_VOLUME","1.00"))
V156_MAX_RS5=float(os.getenv("V156_POSTBUY_MAX_RS5","-0.10"))
V156_MAX_EXHAUSTION=float(os.getenv("V156_POSTBUY_MAX_EXHAUSTION","70"))
V156_MAX_DRAWDOWN=float(os.getenv("V156_POSTBUY_MAX_DRAWDOWN","-0.80"))
V156_STATE_VERSION=2

class V156DeteriorationMonitor:
    """Persistent post-BUY deterioration monitor.

    Episodes exist only after the real V15.6 BUY Telegram has been sent.
    Confirmation votes are time-separated and require independent breakdown
    families so normal short-term consolidation does not qualify.
    """

    def __init__(self,path=None):
        self.path=Path(path or V156_STATE_PATH)
        self.episodes={}
        self._load()

    def _load(self):
        try:
            if self.path.suffix == ".jsonl":
                with self.path.open("r",encoding="utf-8") as fh:
                    for line in fh:
                        try:
                            rec=json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if rec.get("event")=="V156_POSTBUY_STATE":
                            if isinstance(rec.get("episode"),dict) and rec.get("symbol"):
                                self.episodes[str(rec["symbol"]).upper()]=rec["episode"]
                            elif isinstance(rec.get("episodes"),dict):
                                self.episodes.update(rec["episodes"])
                return
            payload=json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(payload,dict) and payload.get("version")==V156_STATE_VERSION and isinstance(payload.get("episodes"),dict):
                self.episodes=payload["episodes"]
        except (FileNotFoundError,OSError,TypeError):
            self.episodes={}

    def _save(self,symbol=None):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        if self.path.suffix == ".jsonl":
            if symbol is None or symbol not in self.episodes:
                return
            payload={"event":"V156_POSTBUY_STATE","v156_state_version":V156_STATE_VERSION,"ts":time.time(),"symbol":symbol,"episode":self.episodes[symbol]}
            with self.path.open("a",encoding="utf-8") as fh:
                fh.write(json.dumps(payload,separators=(",",":"),allow_nan=False)+"\\n")
            return
        tmp=self.path.with_suffix(self.path.suffix+".tmp")
        tmp.write_text(json.dumps({"version":V156_STATE_VERSION,"updated":time.time(),"episodes":self.episodes},separators=(",",":"),allow_nan=False),encoding="utf-8")
        tmp.replace(self.path)

    @staticmethod
    def _f(obj,key,default=0.0):
        try:
            v=float(obj.get(key,default) or default)
            return v if v==v else default
        except (TypeError,ValueError,AttributeError):
            return default

    def start_episode(self,symbol,entry_price,buy_score,ts=None):
        now=time.time() if ts is None else float(ts)
        symbol=str(symbol).upper()
        entry=float(entry_price or 0)
        score=float(buy_score or 0)
        episode_id=f"{symbol}:{int(now)}:{int(round(score))}"
        self.episodes[symbol]={
            "episode_id":episode_id,"active":True,"started_at":now,
            "ready_at":now+V156_GRACE,"window_until":now+V156_WINDOW,
            "entry_price":entry,"entry_buy_score":score,"baseline_price":entry,
            "observations":0,"bad_streak":0,"last_observation_at":0.0,
            "prev_rsi":{},"state":"BUY_GRACE","alert_sent":False,"last_alert_at":0.0
        }
        self._save(symbol)
        return episode_id

    def _finish(self,symbol,ep,state,now):
        ep["active"]=False
        ep["state"]=state
        ep["closed_at"]=now
        self._save(symbol)

    def observe(self,symbol,row,ts=None):
        now=time.time() if ts is None else float(ts)
        symbol=str(symbol).upper()
        ep=self.episodes.get(symbol)
        if not ep or not ep.get("active"):
            return None
        if now>float(ep.get("window_until",0) or 0):
            self._finish(symbol,ep,"CLOSED",now)
            return None
        if now<float(ep.get("ready_at",0) or 0):
            ep["state"]="BUY_GRACE"
            return None

        last_obs=float(ep.get("last_observation_at",0) or 0)
        if last_obs and now-last_obs<V156_GAP:
            return None

        price=self._f(row,"price")
        base=self._f(ep,"baseline_price")
        drawdown=((price/base)-1.0)*100 if price>0 and base>0 else 0.0
        p10=self._f(row,"price_10s");p60=self._f(row,"price_60s")
        buy=self._f(row,"buy_pressure");accel=self._f(row,"trade_accel");vol=self._f(row,"volume_ratio")
        rs5=self._f(row,"v15_relative_strength_5m");ex=self._f(row,"exhaustion_score")
        pre=self._f(row,"v158_pre_ignition_score");liq=self._f(row,"v158_liquidity_state_score")
        part=self._f(row,"v158_participation_score");directional=self._f(row,"v158_directional_score")

        rsi={"5m":self._f(row,"tv_5m_rsi"),"30m":self._f(row,"tv_30m_rsi"),
             "1h":self._f(row,"tv_1h_rsi"),"4h":self._f(row,"tv_4h_rsi")}
        prev=ep.get("prev_rsi",{}) or {}
        falling={tf:(prev.get(tf) is not None and value>0 and value<float(prev.get(tf))-0.5)
                 for tf,value in rsi.items()}
        bearish=[
            rsi["5m"]>0 and rsi["5m"]<45 and falling["5m"],
            rsi["30m"]>0 and rsi["30m"]<45 and falling["30m"],
            rsi["1h"]>0 and rsi["1h"]<50 and falling["1h"],
            rsi["4h"]>0 and rsi["4h"]<50 and falling["4h"],
        ]
        rsi_bear_count=sum(bool(x) for x in bearish)
        rsi_confirmed=(rsi_bear_count>=2 or
                       (rsi["1h"]>0 and rsi["4h"]>0 and rsi["1h"]<50 and rsi["4h"]<50 and
                        (falling["1h"] or falling["4h"])))

        price_break=((p10<=-0.10 and p60<=-0.05) or drawdown<=V156_MAX_DRAWDOWN)
        flow_break=(buy<V156_MIN_BUY and accel<V156_MIN_ACCEL and vol<V156_MIN_VOLUME)
        structural_break=((pre<35 and liq<40) or (part<45 and directional<45))
        momentum_break=(rs5<V156_MAX_RS5 or ex>=V156_MAX_EXHAUSTION)
        core_count=sum((price_break,flow_break,structural_break))
        family_count=core_count+int(momentum_break)

        # Require two of the three core breakdown families plus a third,
        # independent confirmation family. This is deliberately stronger than
        # counting correlated symptoms as separate votes.
        confirmed_observation=(
            core_count>=2 and family_count>=3 and
            (rsi_confirmed or momentum_break or drawdown<=V156_MAX_DRAWDOWN)
        )

        ep["last_observation_at"]=now
        ep["observations"]=int(ep.get("observations",0))+1
        ep["prev_rsi"]=rsi
        if ep["observations"]==1:
            ep["baseline_price"]=price if price>0 else ep.get("baseline_price",0)
            ep["bad_streak"]=0
            ep["state"]="MONITORING"
            self._save(symbol)
            return None

        if confirmed_observation:
            ep["bad_streak"]=int(ep.get("bad_streak",0))+1
            ep["state"]="DETERIORATION_WATCH"
        else:
            ep["bad_streak"]=0
            ep["state"]="MONITORING"

        result={
            "alert":False,"episode_id":ep["episode_id"],"bad_streak":ep["bad_streak"],
            "episode_age":now-float(ep.get("started_at",now)),
            "price_10s":p10,"price_60s":p60,"buy_pressure":buy,"trade_accel":accel,
            "volume_ratio":vol,"rs5":rs5,"exhaustion":ex,"drawdown":drawdown,
            "rsi5":rsi["5m"],"rsi30":rsi["30m"],"rsi1h":rsi["1h"],"rsi4h":rsi["4h"],
            "rsi_bear_count":rsi_bear_count,"family_count":family_count,"core_family_count":core_count,
        }
        if ep["bad_streak"]>=V156_CONFIRM and not ep.get("alert_sent",False):
            ep["state"]="CONFIRMED_DETERIORATION";ep["alert_sent"]=True;ep["last_alert_at"]=now
            result["alert"]=True
            self._save(symbol)
            return result
        self._save(symbol)
        return result
