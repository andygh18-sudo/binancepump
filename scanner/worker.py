import asyncio,aiohttp,json,os,time
from collections import defaultdict,deque
from dotenv import load_dotenv
from .orderbook import LocalOrderBook

load_dotenv()
WS=os.getenv("BINANCE_WS_BASE","wss://data-stream.binance.vision/stream")
REST=os.getenv("BINANCE_REST_BASE","https://data-api.binance.vision")
MAX=int(os.getenv("MAX_SYMBOLS","120"))
MINVOL=float(os.getenv("MIN_QUOTE_VOLUME","100000"))
DISCOVERY_MINVOL=float(os.getenv("DISCOVERY_MIN_QUOTE_VOLUME","50000"))
MOMENTUM_SYMBOLS=int(os.getenv("MOMENTUM_SYMBOLS","40"))
LIQUID_SYMBOLS=int(os.getenv("LIQUID_SYMBOLS","80"))
RUN_SECONDS=int(os.getenv("RUN_SECONDS","250"));INTERVAL=float(os.getenv("DECISION_INTERVAL","5"))
COOLDOWN=float(os.getenv("ALERT_COOLDOWN","60"));LIMIT=int(os.getenv("ORDERBOOK_LIMIT","1000"))
TOP_ALERTS=int(os.getenv("TOP_ALERTS","5"));MIN_ALERT_SCORE=int(os.getenv("MIN_ALERT_SCORE","38"));ACCUM_ALERT_SCORE=int(os.getenv("ACCUM_ALERT_SCORE","60"));V4_ALERT_SCORE=int(os.getenv("V4_ALERT_SCORE","60"));V5_ALERT_SCORE=int(os.getenv("V5_ALERT_SCORE","65"));V5_MIN_PERSISTENCE=int(os.getenv("V5_MIN_PERSISTENCE","2"));V5_MIN_HIST_SAMPLES=int(os.getenv("V5_MIN_HIST_SAMPLES","5"));V5_MIN_HIST_RATE=float(os.getenv("V5_MIN_HIST_RATE","8"));V6_ALERT_SCORE=int(os.getenv("V6_ALERT_SCORE","65"));V6_MIN_PERSISTENCE=int(os.getenv("V6_MIN_PERSISTENCE","2"));V6_MIN_HIST_SAMPLES=int(os.getenv("V6_MIN_HIST_SAMPLES","20"));V6_MIN_HIST_RATE=float(os.getenv("V6_MIN_HIST_RATE","8"));V7_ALERT_SCORE=int(os.getenv("V7_ALERT_SCORE","65"));V7_MIN_PERSISTENCE=int(os.getenv("V7_MIN_PERSISTENCE","2"));V7_MIN_HIST_SAMPLES=int(os.getenv("V7_MIN_HIST_SAMPLES","20"));V7_MIN_HIST_RATE=float(os.getenv("V7_MIN_HIST_RATE","8"));V8_ALERT_SCORE=int(os.getenv("V8_ALERT_SCORE","58"));V8_MIN_PERSISTENCE=int(os.getenv("V8_MIN_PERSISTENCE","2"));V9_ALERT_SCORE=int(os.getenv("V9_ALERT_SCORE","58"));V9_MIN_PERSISTENCE=int(os.getenv("V9_MIN_PERSISTENCE","2"));V10_ALERT_SCORE=int(os.getenv("V10_ALERT_SCORE","60"));V10_MIN_PERSISTENCE=int(os.getenv("V10_MIN_PERSISTENCE","2"));V11_ALERT_SCORE=int(os.getenv("V11_ALERT_SCORE","65"));V11_CONFIRMED_SCORE=int(os.getenv("V11_CONFIRMED_SCORE","72"));V11_MIN_PERSISTENCE=int(os.getenv("V11_MIN_PERSISTENCE","2"));V12_ALERT_SCORE=int(os.getenv("V12_ALERT_SCORE","62"));V12_CONFIRMED_SCORE=int(os.getenv("V12_CONFIRMED_SCORE","70"));V12_MIN_PERSISTENCE=int(os.getenv("V12_MIN_PERSISTENCE","2"));EXHAUSTION_ALERT_SCORE=int(os.getenv("EXHAUSTION_ALERT_SCORE","72"));EXHAUSTION_MIN_EXTENSION=float(os.getenv("EXHAUSTION_MIN_EXTENSION","2.5"));EXHAUSTION_COOLDOWN=float(os.getenv("EXHAUSTION_COOLDOWN","120"))
symbols=[];books={};state=defaultdict(lambda:{"trades":deque(maxlen=12000),"price":None,"candle":None,"last_alert":0,"last_alert_rank":None,"last_accum_alert":0,"last_accum_score":0.0,"v5_streak":0,"v5_last_bucket":-1,"v5_last_score":0.0,"v6_streak":0,"v6_last_bucket":-1,"v6_last_score":0.0,"v7_streak":0,"v7_last_bucket":-1,"v7_last_score":0.0,"v8_streak":0,"v8_last_bucket":-1,"v8_last_score":0.0,"v10_streak":0,"v10_last_bucket":-1,"v10_last_score":0.0,"v12_streak":0,"v12_last_bucket":-1,"v12_last_score":0.0,"last_exhaustion_alert":0,"last_exhaustion_score":0.0,"last_exhaustion_state":"","last_ignition_alert":0,"last_ignition_score":0.0,"last_ignition_stage":""})

async def get_json(s,url,params=None):
    async with s.get(url,params=params,timeout=12) as r:
        r.raise_for_status();return await r.json()

async def discover(s):
    """
    Build a wider pump-detection universe instead of selecting only the
    highest 24h-volume symbols.

    The intensive WebSocket/order-book scan is the union of:
      1) liquidity leaders (stable, high-volume markets), and
      2) momentum leaders (large current 24h price moves with enough liquidity).

    This lets a lower-ranked coin enter the monitored universe when it starts
    moving rapidly, rather than waiting until its 24h volume rank catches up.
    """
    info=await get_json(s,REST+"/api/v3/exchangeInfo")
    stable_bases={"USDT","USDC","FDUSD","TUSD","USDP","DAI","BUSD","PYUSD","USDD","EUR","GBP","TRY","BRL","ARS","AUD","RUB","UAH","PLN","RON","ZAR","NGN","JPY"}
    trad={x["symbol"] for x in info["symbols"] if x["status"]=="TRADING" and x["quoteAsset"]=="USDT" and x.get("baseAsset") not in stable_bases}
    ticks=await get_json(s,REST+"/api/v3/ticker/24hr")

    rows=[
        x for x in ticks
        if x["symbol"] in trad
        and float(x.get("quoteVolume",0)) >= DISCOVERY_MINVOL
    ]

    # Keep a strong liquidity core.
    liquidity=sorted(
        rows,
        key=lambda x: float(x.get("quoteVolume",0)),
        reverse=True
    )[:LIQUID_SYMBOLS]

    # Add markets showing unusually strong 24h momentum. Use quote volume as
    # a secondary key so extremely illiquid percentage moves do not dominate.
    momentum=sorted(
        rows,
        key=lambda x: (
            float(x.get("priceChangePercent",0)),
            float(x.get("quoteVolume",0))
        ),
        reverse=True
    )[:MOMENTUM_SYMBOLS]

    selected={}
    for row in liquidity + momentum:
        selected[row["symbol"]]=row

    # Respect the overall WebSocket budget while guaranteeing the liquidity
    # core remains represented.
    out=[x["symbol"] for x in liquidity]
    for x in momentum:
        if x["symbol"] not in out and len(out)<MAX:
            out.append(x["symbol"])

    if "BTCUSDT" not in out and "BTCUSDT" in trad:
        if len(out)>=MAX:
            out[-1]="BTCUSDT"
        else:
            out.append("BTCUSDT")

    return out[:MAX]

def event(stream,d):
    s=d.get("s","")
    if not s:return
    x=state[s];now=time.time()
    if stream.endswith("@aggTrade"):
        p=float(d["p"]);n=p*float(d["q"]);buy=not bool(d.get("m",False));x["trades"].append((now,p,n,buy));x["price"]=p
    elif stream.endswith("@bookTicker"):x["price"]=float(d["a"])
    elif "@kline_1m" in stream:
        k=d["k"];new_c={"open":float(k["o"]),"high":float(k["h"]),"low":float(k["l"]),"close":float(k["c"])};x["candle"]=new_c;x["price"]=float(k["c"]);
        if k.get("x"):x["prev_candle"]=new_c
    elif stream.endswith("@depth@100ms") and s in books:books[s].buffer_event(d)

def stats(s,sec):
    cut=time.time()-sec;r=[z for z in state[s]["trades"] if z[0]>=cut]
    if not r:return 0,0,0,0
    n=sum(z[2] for z in r);b=sum(z[2] for z in r if z[3])
    return len(r),n,b/n if n else 0,(r[-1][1]/r[0][1]-1)*100 if len(r)>1 else 0


def acceleration_ratio(v10,v60):
    return v10/max(v60/6,1)

def accumulation(s):
    x=state[s];c=x["candle"]
    if not x["price"] or not c or s not in books:return None
    n10,v10,b10,p10=stats(s,10);n60,v60,b60,p60=stats(s,60);_,v300,_,_=stats(s,300)
    ob=books[s].metrics(20);imb=ob["imbalance"]
    buy_component=max(0,min((b10-.50)/.25,1))*30
    trade_component=max(0,min((acceleration_ratio(v10,v60)-1)/2.0,1))*25
    volume_ratio=max(v60/max(v300/5,1),0)
    volume_component=max(0,min(volume_ratio/2.0,1))*20
    book_component=max(0,min((imb+.20)/.60,1))*15
    price_component=max(0,min((p10+0.5)/3.0,1))*10
    activity_bonus=5 if n10>=4 and n60>=12 else 0
    raw=buy_component+trade_component+volume_component+book_component+price_component+activity_bonus
    extension_penalty=max(0,min((p10-2.0)*8,20))
    acc=max(0,min(round(raw-extension_penalty),100))
    quality=(acc>=50 and n10>=4 and n60>=12 and b10>=.55 and acceleration_ratio(v10,v60)>=1.25 and p10<4)
    stage="ACCUMULATION ALERT" if acc>=70 and quality else "ACCUMULATION WATCH" if acc>=50 and quality else "MONITOR"
    return {"accumulation_score":acc,"accumulation_stage":stage,"accumulation_quality":quality,"accum_buy_pressure":b10,"accum_trade_accel":acceleration_ratio(v10,v60),"accum_volume_ratio":volume_ratio,"accum_book_imbalance":imb,"accum_price_10s":p10,"accum_trades_10s":n10}
def early_pump_score(s, ac):
    x=state[s];c=x["candle"]
    if not c or not ac:return None
    _,v10,b10,p10=stats(s,10);_,v60,b60,p60=stats(s,60);_,v300,_,_=stats(s,300)
    vr=v60/max(v300/5,1); accel=acceleration_ratio(v10,v60)
    p1=(x["price"]/c["open"]-1)*100 if c["open"] else 0
    ob=books[s].metrics(20);imb=ob["imbalance"]

    btc5=0.0; btc15=0.0
    if s!="BTCUSDT" and state.get("BTCUSDT",{}).get("price"):
        _,_,_,btc5=stats("BTCUSDT",5)
        _,_,_,btc15=stats("BTCUSDT",15)
    rs5=p10-btc5
    rs15=p60-btc15

    buy=max(0,min((b10-.50)/.25,1))*14
    vol=max(0,min((vr-1)/2,1))*13
    trade=max(0,min((accel-1)/2,1))*13
    pressure=max(0,min((p10+.5)/2.5,1))*8
    compression=7 if abs(p60)<2 and vr<1.5 else 0
    structure=8 if p10>0 and p60>0 else 4 if p10>=-0.25 else 0
    book=max(0,min((imb+.20)/.60,1))*8
    activity=5 if stats(s,10)[0]>=4 and stats(s,60)[0]>=12 else 0
    relative=max(0,min((rs5+.25)/1.5,1))*7
    volatility=5 if abs(p10)>0.35 and vr>1.3 else 0
    resistance=5 if -0.5 <= p1 <= 1.5 else 1

    penalty=0
    if p1>3: penalty+=6
    if b10<.52: penalty+=5
    if accel<1.15: penalty+=4
    if vr<.8: penalty+=5
    if rs15 < -1: penalty+=4

    score=max(0,min(round(buy+vol+trade+pressure+compression+structure+book+activity+relative+volatility+resistance-penalty),100))
    quality=score>=50 and b10>=.55 and accel>=1.25 and p10<4 and rs5>-1
    stage="EARLY PUMP" if score>=80 and quality else "PRE-PUMP" if score>=65 and quality else "BUILDING" if score>=50 and quality else "MONITOR"
    return {"early_pump_score":score,"early_pump_stage":stage,"early_pump_quality":quality,
            "relative_strength_5m":rs5,"relative_strength_15m":rs15,"btc_ret_5m":btc5,"btc_ret_15m":btc15,
            "false_positive_penalty":penalty}

def v4_confluence(s, eps, ac):
    if not eps or not ac:return None
    _,v10,b10,p10=stats(s,10);_,v60,_,p60=stats(s,60);_,v300,_,_=stats(s,300)
    vr=v60/max(v300/5,1);acc=acceleration_ratio(v10,v60)
    rs5=eps.get("relative_strength_5m",0);rs15=eps.get("relative_strength_15m",0)
    ob=books[s].metrics(20);imb=ob["imbalance"]
    confirmations=sum([b10>=.60,vr>=1.5,acc>=1.5,rs5>0,p10>0 and p60>0,imb>=0,p10<3,p60<4])
    penalties=0
    if p10>=3: penalties+=2
    if rs15<-1: penalties+=2
    if vr<1: penalties+=2
    if b10<.55: penalties+=2
    conf=max(0,min(confirmations*12-penalties,100))
    v4=max(0,min(round(eps["early_pump_score"]*.80+conf*.20),100))
    if v4>=70 and confirmations>=6 and penalties<=2: grade="A"
    elif v4>=60 and confirmations>=5 and penalties<=3: grade="B"
    elif v4>=50 and confirmations>=4: grade="C"
    else: grade="REJECT"
    return {"v4_score":v4,"v4_confluence_score":conf,"v4_confirmations":confirmations,"v4_penalties":penalties,"v4_alert_quality":grade,"v4_alert":grade in ("A","B") and v4>=V4_ALERT_SCORE}

def load_v5_calibration():
    try:
        with open("data/v5_calibration.json") as f:return json.load(f)
    except Exception:return {}
V5_CALIBRATION=load_v5_calibration()
def load_v6_calibration():
    try:
        with open("data/v6_calibration.json") as f:return json.load(f)
    except Exception:return {}
V6_CALIBRATION=load_v6_calibration()

def v6_signal(s,v4):
    if not v4:return None
    x=state[s];bucket=int(time.time()/max(INTERVAL,1));prev=int(x.get("v6_last_bucket",-1))
    _,v10,b10,p10=stats(s,10);_,v60,_,p60=stats(s,60);_,v300,_,_=stats(s,300)
    vr=v60/max(v300/5,1);acc=acceleration_ratio(v10,v60)
    rs5=float(v4.get("relative_strength_5m",0) or 0)
    cal=V6_CALIBRATION.get(s,{}) if isinstance(V6_CALIBRATION,dict) else {}
    samples=int(cal.get("samples",0) or 0);rate=float(cal.get("hit_rate_240m_ge10_pct",0) or 0)
    qualifying=v4.get("v4_alert_quality") in ("A","B") and v4.get("v4_score",0)>=60
    if qualifying:
        if bucket==prev+1:x["v6_streak"]=int(x.get("v6_streak",0))+1
        elif bucket!=prev:x["v6_streak"]=1
    else:
        if bucket!=prev:x["v6_streak"]=0
    x["v6_last_bucket"]=bucket;streak=int(x.get("v6_streak",0))
    hist_quality=min(rate/25.0,1.0)*100 if samples>=V6_MIN_HIST_SAMPLES else 50.0
    persistence_quality=min(streak/3.0,1.0)*100
    score=round(.70*v4.get("v4_score",0)+.15*persistence_quality+.15*hist_quality)
    early_exception=(v4.get("v4_alert_quality") in ("A","B") and v4.get("v4_score",0)>=68 and v4.get("v4_confirmations",0)>=7 and b10>=.60 and vr>=2 and acc>=2 and rs5>0 and p60>0 and p10<2.5)
    if samples>=V6_MIN_HIST_SAMPLES and rate>=V6_MIN_HIST_RATE and streak>=3 and score>=72 and v4.get("v4_alert_quality")=="A":grade="A"
    elif samples>=V6_MIN_HIST_SAMPLES and rate>=V6_MIN_HIST_RATE and streak>=2 and score>=65 and v4.get("v4_alert_quality") in ("A","B"):grade="B"
    elif early_exception and score>=60:grade="EARLY"
    elif score>=55 and streak>=1:grade="WATCH"
    else:grade="REJECT"
    alert=(grade=="EARLY" and score>=60) or (grade in ("A","B") and score>=V6_ALERT_SCORE and streak>=V6_MIN_PERSISTENCE and samples>=V6_MIN_HIST_SAMPLES and rate>=V6_MIN_HIST_RATE)
    return {"v6_score":max(0,min(score,100)),"v6_persistence":streak,"v6_hist_samples":samples,"v6_hist_hit_rate_240m":rate,"v6_grade":grade,"v6_early_exception":early_exception,"v6_alert":alert}

def load_v7_calibration():
    try:
        with open("data/v7_calibration.json") as f:return json.load(f)
    except Exception:return {}
V7_CALIBRATION=load_v7_calibration()

def v7_signal(s,v4):
    if not v4:return None
    x=state[s];bucket=int(time.time()/max(INTERVAL,1));prev=int(x.get("v7_last_bucket",-1))
    _,v10,b10,p10=stats(s,10);_,v60,_,p60=stats(s,60);_,v300,_,_=stats(s,300)
    vr=v60/max(v300/5,1);acc=acceleration_ratio(v10,v60)
    rs5=float(v4.get("relative_strength_5m",0) or 0)
    cal=V7_CALIBRATION.get(s,{}) if isinstance(V7_CALIBRATION,dict) else {}
    samples=int(cal.get("samples",0) or 0);rate=float(cal.get("hit_rate_240m_ge10_pct",0) or 0)
    qualifying=v4.get("v4_alert_quality") in ("A","B") and v4.get("v4_score",0)>=60
    if qualifying:
        if bucket==prev+1:x["v7_streak"]=int(x.get("v7_streak",0))+1
        elif bucket!=prev:x["v7_streak"]=1
    else:
        if bucket!=prev:x["v7_streak"]=0
    x["v7_last_bucket"]=bucket;streak=int(x.get("v7_streak",0))
    hist_quality=min(rate/25.0,1.0)*100 if samples>=V7_MIN_HIST_SAMPLES else 50.0
    persistence_quality=min(streak/3.0,1.0)*100
    score=round(.75*v4.get("v4_score",0)+.10*persistence_quality+.15*hist_quality)
    early_exception=(v4.get("v4_alert_quality") in ("A","B") and v4.get("v4_score",0)>=72 and v4.get("v4_confirmations",0)>=7 and streak>=2 and b10>=.60 and vr>=2 and acc>=2 and rs5>.50 and p60>0 and p10<2.5)
    if samples>=V7_MIN_HIST_SAMPLES and rate>=V7_MIN_HIST_RATE and streak>=3 and score>=72 and v4.get("v4_alert_quality")=="A":grade="A"
    elif samples>=V7_MIN_HIST_SAMPLES and rate>=V7_MIN_HIST_RATE and streak>=2 and score>=65 and v4.get("v4_alert_quality") in ("A","B"):grade="B"
    elif early_exception and score>=68:grade="EARLY"
    elif score>=58 and streak>=1:grade="WATCH"
    else:grade="REJECT"
    alert=(grade=="EARLY" and score>=68 and streak>=2) or (grade in ("A","B") and score>=V7_ALERT_SCORE and streak>=V7_MIN_PERSISTENCE and samples>=V7_MIN_HIST_SAMPLES and rate>=V7_MIN_HIST_RATE)
    reason="EARLY_STRONG_CONFLUENCE" if grade=="EARLY" else "CALIBRATED_PERSISTENT" if grade in ("A","B") else "WATCH" if grade=="WATCH" else "REJECT"
    return {"v7_score":max(0,min(score,100)),"v7_persistence":streak,"v7_hist_samples":samples,"v7_hist_hit_rate_240m":rate,"v7_grade":grade,"v7_early_exception":early_exception,"v7_alert":alert,"v7_reason":reason}

def load_v8_calibration():
    try:
        with open("data/v8_calibration.json") as f:return json.load(f)
    except Exception:return {}
V8_CALIBRATION=load_v8_calibration()

def v8_signal(s,v4,eps):
    if not v4 or not eps:return None
    x=state[s];bucket=int(time.time()/max(INTERVAL,1));prev=int(x.get("v8_last_bucket",-1))
    _,v10,b10,p10=stats(s,10);_,v60,_,p60=stats(s,60);_,v300,_,_=stats(s,300)
    vr=v60/max(v300/5,1);acc=acceleration_ratio(v10,v60)
    rs5=float(eps.get("relative_strength_5m",0) or 0);rs15=float(eps.get("relative_strength_15m",0) or 0)
    btc5=float(eps.get("btc_ret_5m",0) or 0);btc15=float(eps.get("btc_ret_15m",0) or 0)
    qualifying=v4.get("v4_score",0)>=55 and v4.get("v4_alert_quality") in ("A","B","C")
    if qualifying:
        if bucket==prev+1:x["v8_streak"]=int(x.get("v8_streak",0))+1
        elif bucket!=prev:x["v8_streak"]=1
    elif bucket!=prev:x["v8_streak"]=0
    x["v8_last_bucket"]=bucket;streak=int(x.get("v8_streak",0))

    cal=V8_CALIBRATION.get(s,{}) if isinstance(V8_CALIBRATION,dict) else {}
    samples=int(cal.get("samples",0) or 0);rate=float(cal.get("hit_rate_240m_ge10_pct",0) or 0)
    smooth=((rate/100.0)*samples+2.0)/(samples+20.0)*100.0
    hist_modifier=max(-8.0,min(8.0,(smooth-10.0)*.40))
    btc_risk=(btc5<-1.0 or btc15<-2.0)
    structure=(p10>0 and p60>0)
    controlled=(p10<2.5 and p60<6.0)
    activity=(vr>=1.5 and acc>=1.5)
    early=(v4.get("v4_score",0)>=60 and streak>=V8_MIN_PERSISTENCE and b10>=.55 and activity and rs5>0 and structure and controlled and not btc_risk)
    confirmed=(v4.get("v4_score",0)>=72 and v4.get("v4_confirmations",0)>=7 and streak>=3 and b10>=.60 and vr>=2 and acc>=2 and rs5>.25 and structure and controlled and not btc_risk)
    avoid=(p10>=4 or p60>=8 or rs15<-1.5 or (vr<1 and p60>1) or btc_risk)
    persist=min(streak/3.0,1.0)*100
    score=max(0,min(round(.78*v4.get("v4_score",0)+.12*persist+5+hist_modifier),100))
    if avoid:path="AVOID";grade="REJECT";alert=False
    elif confirmed and score>=68:path="CONFIRMED";grade="A";alert=True
    elif early and score>=V8_ALERT_SCORE:path="EARLY";grade="B";alert=True
    elif score>=52 and streak>=1:path="WATCH";grade="WATCH";alert=False
    else:path="REJECT";grade="REJECT";alert=False
    return {"v8_score":score,"v8_persistence":streak,"v8_hist_samples":samples,"v8_hist_hit_rate_240m":rate,
            "v8_smoothed_hist_rate_240m":round(smooth,2),"v8_hist_modifier":round(hist_modifier,2),
            "v8_grade":grade,"v8_path":path,"v8_early_path":early,"v8_confirmed_path":confirmed,
            "v8_avoid":avoid,"v8_btc_risk_off":btc_risk,"v8_alert":alert}

def v9_signal(s,v4,eps):
    if not v4 or not eps:return None
    x=state[s];bucket=int(time.time()/max(INTERVAL,1));prev=int(x.get("v9_last_bucket",-1))
    _,v10,b10,p10=stats(s,10);_,v60,_,p60=stats(s,60);_,v300,_,_=stats(s,300)
    vr=v60/max(v300/5,1);acc=acceleration_ratio(v10,v60)
    rs5=float(eps.get("relative_strength_5m",0) or 0);rs15=float(eps.get("relative_strength_15m",0) or 0)
    btc5=float(eps.get("btc_ret_5m",0) or 0);btc15=float(eps.get("btc_ret_15m",0) or 0)
    qualifying=v4.get("v4_score",0)>=55 and v4.get("v4_alert_quality") in ("A","B","C")
    if qualifying:
        if bucket==prev+1:x["v9_streak"]=int(x.get("v9_streak",0))+1
        elif bucket!=prev:x["v9_streak"]=1
    elif bucket!=prev:x["v9_streak"]=0
    x["v9_last_bucket"]=bucket;streak=int(x.get("v9_streak",0))
    momentum_ok=(p60>0 and (p60>=p10*0.75 or rs5>0.5))
    stall=(vr>=3 and acc>=3 and p60<0.75)
    follow=100-(25 if stall else 0)-(15 if not momentum_ok else 0)-(10 if vr<1.2 else 0)-(10 if acc<1.2 else 0)
    follow=max(0,follow)
    btc_risk=(btc5<-1 or btc15<-2)
    early=(v4.get("v4_score",0)>=60 and streak>=V9_MIN_PERSISTENCE and b10>=.55 and vr>=1.5 and acc>=1.5 and rs5>0 and momentum_ok and p10<2.5 and not btc_risk and follow>=65 and not stall)
    confirmed=(v4.get("v4_score",0)>=72 and v4.get("v4_confirmations",0)>=7 and streak>=3 and b10>=.60 and vr>=2 and acc>=2 and rs5>.25 and momentum_ok and p10<2.5 and not btc_risk and follow>=75)
    avoid=(p10>=4 or p60>=8 or rs15<-1.5 or (vr<1 and p60>1) or btc_risk)
    score=max(0,min(round(.72*v4.get("v4_score",0)+.10*min(streak/3,1)*100+.13*follow+.05),100))
    if avoid:path="AVOID";grade="REJECT";alert=False
    elif confirmed and score>=68:path="CONFIRMED";grade="A";alert=True
    elif early and score>=V9_ALERT_SCORE:path="EARLY";grade="B";alert=True
    elif score>=52 and streak>=1:path="WATCH";grade="WATCH";alert=False
    else:path="REJECT";grade="REJECT";alert=False
    return {"v9_score":score,"v9_persistence":streak,"v9_follow_through":follow,"v9_stall":stall,"v9_momentum_ok":momentum_ok,"v9_grade":grade,"v9_path":path,"v9_avoid":avoid,"v9_btc_risk_off":btc_risk,"v9_alert":alert}

def load_v10_calibration():
    try:
        with open("data/v10_calibration.json") as f:return json.load(f)
    except Exception:return {}
V10_CALIBRATION=load_v10_calibration()

def v10_signal(s,v4,eps):
    if not v4 or not eps:return None
    x=state[s];bucket=int(time.time()/max(INTERVAL,1));prev=int(x.get("v10_last_bucket",-1))
    _,v10,b10,p10=stats(s,10);_,v60,_,p60=stats(s,60);_,v300,_,_=stats(s,300)
    vr=v60/max(v300/5,1);acc=acceleration_ratio(v10,v60)
    rs5=float(eps.get("relative_strength_5m",0) or 0);rs15=float(eps.get("relative_strength_15m",0) or 0)
    btc5=float(eps.get("btc_ret_5m",0) or 0);btc15=float(eps.get("btc_ret_15m",0) or 0)
    qualifying=v4.get("v4_score",0)>=55 and v4.get("v4_alert_quality") in ("A","B","C")
    if qualifying:
        if bucket==prev+1:x["v10_streak"]=int(x.get("v10_streak",0))+1
        elif bucket!=prev:x["v10_streak"]=1
    elif bucket!=prev:x["v10_streak"]=0
    x["v10_last_bucket"]=bucket;streak=int(x.get("v10_streak",0))
    cal=V10_CALIBRATION.get(s,{}) if isinstance(V10_CALIBRATION,dict) else {}
    samples=int(cal.get("samples",0) or 0);rate=float(cal.get("hit_rate_240m_ge10_pct",0) or 0)
    reliability=50.0 if samples<20 else max(0.0,min(100.0,rate*1.25))
    rel_mod=(reliability-50.0)*0.08
    prev_c=x.get("prev_candle");cur_c=x.get("candle")
    second_confirm=bool(prev_c and cur_c and x.get("price") and x["price"]>prev_c["close"] and prev_c["close"]>=prev_c["open"])
    efficiency=(p60/max(vr,1.0)) if vr>0 else 0.0
    extension=max(0.0,p10-2.5)+max(0.0,p60-6.0)*0.35
    momentum_ok=(p60>0 and (p60>=p10*0.75 or rs5>0.5))
    stall=(vr>=3 and acc>=3 and p60<0.75)
    btc_risk=(btc5<-1 or btc15<-2)
    controlled=(p10<2.5 and p60<6)
    activity=(vr>=1.5 and acc>=1.5)
    efficiency_ok=(efficiency>=0.20 or p60>=2)
    extension_ok=(extension<3 and p10<3.5)
    early=(v4.get("v4_score",0)>=60 and streak>=V10_MIN_PERSISTENCE and b10>=.55 and activity and rs5>0 and momentum_ok and
           controlled and not btc_risk and second_confirm and efficiency_ok and extension_ok and not stall)
    confirmed=(v4.get("v4_score",0)>=72 and v4.get("v4_confirmations",0)>=7 and streak>=3 and b10>=.60 and vr>=2 and acc>=2 and
               rs5>.25 and momentum_ok and controlled and not btc_risk and second_confirm and efficiency_ok and extension_ok and not stall)
    avoid=(p10>=4 or p60>=8 or rs15<-1.5 or (vr<1 and p60>1) or btc_risk or extension>=4 or stall)
    persist=min(streak/3,1)*100
    score=max(0,min(round(.68*v4.get("v4_score",0)+.10*persist+.12*(100 if momentum_ok else 50)+5+rel_mod),100))
    if avoid:path="AVOID";grade="REJECT";alert=False
    elif confirmed and score>=68:path="CONFIRMED";grade="A";alert=True
    elif early and score>=V10_ALERT_SCORE:path="EARLY";grade="B";alert=True
    elif score>=52 and streak>=1:path="WATCH";grade="WATCH";alert=False
    else:path="REJECT";grade="REJECT";alert=False
    return {"v10_score":score,"v10_persistence":streak,"v10_reliability":round(reliability,2),"v10_reliability_modifier":round(rel_mod,2),
            "v10_second_candle_confirm":second_confirm,"v10_efficiency":round(efficiency,4),"v10_adverse_extension":round(extension,4),
            "v10_stall":stall,"v10_grade":grade,"v10_path":path,"v10_avoid":avoid,"v10_alert":alert}

def load_v11_calibration():
    try:
        with open("data/v11_calibration.json") as f:return json.load(f)
    except Exception:return {}
V11_CALIBRATION=load_v11_calibration()

def v11_signal(s,v4,eps):
    if not v4 or not eps:return None
    x=state[s];bucket=int(time.time()/max(INTERVAL,1));prev=int(x.get("v11_last_bucket",-1))
    _,v10,b10,p10=stats(s,10);_,v60,_,p60=stats(s,60);_,v300,_,_=stats(s,300)
    vr=v60/max(v300/5,1);acc=acceleration_ratio(v10,v60)
    rs5=float(eps.get("relative_strength_5m",0) or 0);rs15=float(eps.get("relative_strength_15m",0) or 0)
    btc5=float(eps.get("btc_ret_5m",0) or 0);btc15=float(eps.get("btc_ret_15m",0) or 0)
    qualifying=v4.get("v4_score",0)>=55 and v4.get("v4_alert_quality") in ("A","B","C")
    if qualifying:
        if bucket==prev+1:x["v11_streak"]=int(x.get("v11_streak",0))+1
        elif bucket!=prev:x["v11_streak"]=1
    elif bucket!=prev:x["v11_streak"]=0
    x["v11_last_bucket"]=bucket;streak=int(x.get("v11_streak",0))
    prev_c=x.get("prev_candle");cur_c=x.get("candle")
    second_confirm=bool(prev_c and cur_c and cur_c["close"]>prev_c["close"] and prev_c["close"]>=prev_c["open"])
    second_hold=bool(prev_c and cur_c and cur_c["low"]>=prev_c["low"]*0.995)
    confirmation=second_confirm and second_hold
    efficiency=(p60/max(vr,1.0)) if vr>0 else 0.0
    extension=max(0.0,p10-2.5)+max(0.0,p60-6.0)*0.35
    stall=(vr>=3 and acc>=3 and p60<0.75)
    momentum_ok=(p60>0 and (p60>=p10*0.75 or rs5>0.5))
    btc_risk=(btc5<-1 or btc15<-2)
    controlled=(p10<2.5 and p60<6)
    activity=(vr>=1.5 and acc>=1.5)
    efficiency_ok=(efficiency>=0.35 or (efficiency>=0.25 and p60>=2))
    extension_ok=(extension<3 and p10<3.5)
    follow_ok=(streak>=2 and momentum_ok and confirmation)
    cal=V11_CALIBRATION.get(s,{}) if isinstance(V11_CALIBRATION,dict) else {}
    samples=int(cal.get("samples",0) or 0);rate=float(cal.get("hit_rate_240m_ge10_pct",0) or 0)
    reliability=50.0 if samples<20 else max(0.0,min(100.0,rate))
    rel_mod=(reliability-50.0)*0.03
    persist=min(streak/3,1)*100
    eff_component=min(max(efficiency,0)/0.75*100,100)
    rs_component=min(max(rs5,0)/2.0*100,100)
    score=max(0,min(round(.55*v4.get("v4_score",0)+.10*persist+.10*(100 if momentum_ok else 50)+.15*eff_component+.05*rs_component+.05*(100 if confirmation else 0)+rel_mod),100))
    avoid=(p10>=4 or p60>=8 or rs15<-1.5 or btc_risk or extension>=4 or stall or efficiency<0.15)
    confirmed=(v4.get("v4_score",0)>=72 and v4.get("v4_confirmations",0)>=7 and streak>=3 and b10>=.60 and vr>=2 and acc>=2 and rs5>.25 and momentum_ok and controlled and not btc_risk and confirmation and efficiency>=.50 and extension_ok and not stall)
    early=(v4.get("v4_score",0)>=60 and streak>=V11_MIN_PERSISTENCE and b10>=.58 and vr>=1.5 and acc>=1.5 and rs5>0 and momentum_ok and controlled and not btc_risk and confirmation and efficiency_ok and extension_ok and follow_ok and not stall)
    if avoid:path="AVOID";grade="REJECT";alert=False
    elif confirmed and score>=V11_CONFIRMED_SCORE:path="CONFIRMED";grade="A";alert=True
    elif early and score>=V11_ALERT_SCORE:path="EARLY";grade="B";alert=True
    elif score>=55 and streak>=1:path="WATCH";grade="WATCH";alert=False
    else:path="REJECT";grade="REJECT";alert=False
    return {"v11_score":score,"v11_persistence":streak,"v11_reliability":round(reliability,2),"v11_reliability_modifier":round(rel_mod,2),"v11_second_candle_confirm":second_confirm,"v11_second_candle_hold":second_hold,"v11_confirmation":confirmation,"v11_efficiency":round(efficiency,4),"v11_adverse_extension":round(extension,4),"v11_follow_ok":follow_ok,"v11_grade":grade,"v11_path":path,"v11_avoid":avoid,"v11_alert":alert}

def v12_signal(s,v4,eps):
    if not v4 or not eps:return None
    x=state[s];bucket=int(time.time()/max(INTERVAL,1));prev=int(x.get("v12_last_bucket",-1))
    _,v10,b10,p10=stats(s,10);_,v60,_,p60=stats(s,60);_,v300,_,_=stats(s,300)
    vr=v60/max(v300/5,1);acc=acceleration_ratio(v10,v60)
    rs5=float(eps.get("relative_strength_5m",0) or 0);rs15=float(eps.get("relative_strength_15m",0) or 0)
    btc5=float(eps.get("btc_ret_5m",0) or 0);btc15=float(eps.get("btc_ret_15m",0) or 0)
    qualifying=v4.get("v4_score",0)>=55 and v4.get("v4_alert_quality") in ("A","B","C")
    if qualifying:
        if bucket==prev+1:x["v12_streak"]=int(x.get("v12_streak",0))+1
        elif bucket!=prev:x["v12_streak"]=1
    elif bucket!=prev:x["v12_streak"]=0
    x["v12_last_bucket"]=bucket;streak=int(x.get("v12_streak",0))
    prev_c=x.get("prev_candle");cur_c=x.get("candle")
    confirmation=bool(prev_c and cur_c and cur_c["close"]>prev_c["close"] and cur_c["low"]>=prev_c["low"]*.995 and prev_c["close"]>=prev_c["open"])
    efficiency=(p60/max(vr,1.0)) if vr>0 else 0.0
    stall=(vr>=3 and acc>=3 and p60<0.75)
    momentum_ok=(p60>0 and (p60>=p10*.75 or rs5>0.5))
    extension=max(0.0,p10-2.5)+max(0.0,p60-6.0)*.35
    btc_risk=(btc5<-1 or btc15<-2)
    controlled=(p10<2.5 and p60<6)
    structure=bool(eps.get("BOS",False) or eps.get("CHoCH",False))
    a_plus=bool(confirmation and efficiency>=.60 and rs5>=1.0 and eps.get("BOS",False) and eps.get("CHoCH",False) and streak>=2 and b10>=.60 and not stall and extension<3)
    persist=min(streak/3,1)*100
    eff_component=min(max(efficiency,0)/.75*100,100)
    rs_component=min(max(rs5,0)/2*100,100)
    score=max(0,min(round(.55*v4.get("v4_score",0)+.10*persist+.10*(100 if momentum_ok else 50)+.15*eff_component+.05*rs_component+.05*(100 if confirmation else 0)+(5 if a_plus else 0)),100))
    avoid=(p10>=4 or p60>=8 or rs15<-1.5 or btc_risk or extension>=4 or stall or efficiency<.15)
    early=(a_plus and score>=V12_ALERT_SCORE) or (v4.get("v4_score",0)>=60 and streak>=V12_MIN_PERSISTENCE and b10>=.58 and vr>=1.5 and acc>=1.5 and rs5>0 and momentum_ok and controlled and not btc_risk and confirmation and efficiency>=.35 and extension<3 and not stall)
    confirmed=((a_plus and score>=V12_CONFIRMED_SCORE) or (v4.get("v4_score",0)>=72 and v4.get("v4_confirmations",0)>=7 and streak>=3 and b10>=.60 and vr>=2 and acc>=2 and rs5>.25 and momentum_ok and controlled and not btc_risk and confirmation and efficiency>=.50 and extension<3 and not stall))
    if avoid:path="AVOID";grade="REJECT";alert=False
    elif confirmed and score>=V12_CONFIRMED_SCORE:path="CONFIRMED";grade="A+" if a_plus else "A";alert=True
    elif early and score>=V12_ALERT_SCORE:path="EARLY";grade="A+" if a_plus else "B";alert=True
    elif score>=55 and streak>=1:path="WATCH";grade="WATCH";alert=False
    else:path="REJECT";grade="REJECT";alert=False
    return {"v12_score":score,"v12_persistence":streak,"v12_efficiency":round(efficiency,4),"v12_a_plus":a_plus,"v12_confirmation":confirmation,"v12_grade":grade,"v12_path":path,"v12_alert":alert,"v12_avoid":avoid}

def v5_signal(s,v4):
    if not v4:return None
    x=state[s];bucket=int(time.time()/max(INTERVAL,1));prev=int(x.get("v5_last_bucket",-1))
    qualifying=v4.get("v4_alert_quality") in ("A","B") and v4.get("v4_score",0)>=60
    if qualifying:
        if bucket==prev+1:x["v5_streak"]=int(x.get("v5_streak",0))+1
        elif bucket!=prev:x["v5_streak"]=1
    else:
        if bucket!=prev:x["v5_streak"]=0
    x["v5_last_bucket"]=bucket
    streak=int(x.get("v5_streak",0))
    cal=V5_CALIBRATION.get(s,{}) if isinstance(V5_CALIBRATION,dict) else {}
    samples=int(cal.get("samples",0) or 0);rate=float(cal.get("hit_rate_240m_ge10_pct",0) or 0)
    hist_quality=min(rate/25.0,1.0)*100 if samples>=V5_MIN_HIST_SAMPLES else 50.0
    persistence_quality=min(streak/3.0,1.0)*100
    v5=round(.70*v4.get("v4_score",0)+.15*persistence_quality+.15*hist_quality)
    if samples>=V5_MIN_HIST_SAMPLES and rate>=V5_MIN_HIST_RATE and streak>=3 and v5>=72 and v4.get("v4_alert_quality")=="A":grade="A"
    elif samples>=V5_MIN_HIST_SAMPLES and rate>=V5_MIN_HIST_RATE and streak>=2 and v5>=65 and v4.get("v4_alert_quality") in ("A","B"):grade="B"
    elif v5>=55 and streak>=1:grade="WATCH"
    else:grade="REJECT"
    return {"v5_score":max(0,min(v5,100)),"v5_persistence":streak,"v5_hist_samples":samples,"v5_hist_hit_rate_240m":rate,"v5_grade":grade,"v5_alert":grade in ("A","B") and v5>=V5_ALERT_SCORE and streak>=V5_MIN_PERSISTENCE and samples>=V5_MIN_HIST_SAMPLES and rate>=V5_MIN_HIST_RATE}

def exhaustion_momentum(s, eps, v12, v15):
    """Detect momentum exhaustion after an extended move.
    This is deliberately a reversal-risk alert, not a short/exit command.
    """
    x=state[s]
    _,v10,b10,p10=stats(s,10)
    _,v60,b60,p60=stats(s,60)
    _,v300,_,_=stats(s,300)
    if not x.get("price") or not x.get("candle"):
        return None

    vr=v60/max(v300/5,1)
    acc=acceleration_ratio(v10,v60)
    rs5=float((eps or {}).get("relative_strength_5m",0) or 0)
    rs15=float((eps or {}).get("relative_strength_15m",0) or 0)
    v15_score=float((v15 or {}).get("v15_score",0) or 0)
    v15_stage=str((v15 or {}).get("v15_stage","") or "")
    efficiency=float((v12 or {}).get("v12_efficiency",0) or 0)
    ob=books[s].metrics(20)
    imb=float(ob.get("imbalance",0) or 0)

    # Current extension: fast price expansion is the first exhaustion ingredient.
    extension=max(0.0,p10)*0.55 + max(0.0,p60)*0.30 + max(0.0,(p10-p60))*0.15

    # Momentum rollover: strong activity but weaker price follow-through.
    activity=min(max((vr-1)/3,0),1)*100
    acceleration=min(max((acc-1)/3,0),1)*100
    follow_through=max(0.0,min((p60/max(p10,0.25))*100,120)) if p10>0 else 0.0
    stall_penalty=30 if (vr>=2.5 and acc>=2.0 and p60<1.0) else 0
    rollover=max(0.0,min(100.0, 100.0-follow_through)) + stall_penalty

    # Buying-pressure deterioration and order-book weakening increase exhaustion risk.
    buy_stress=max(0.0,min((0.58-b10)/0.18,1))*100
    book_stress=max(0.0,min((0.05-imb)/0.35,1))*100

    # Relative-strength fade: still positive, but losing leadership versus the prior window.
    rs_fade=max(0.0,min((rs5-rs15+0.25)/1.25,1))*100 if rs5<rs15 else max(0.0,min((rs15-rs5+0.25)/1.25,1))*100
    if rs5 < rs15:
        rs_fade=100.0
    elif rs5 < 0:
        rs_fade=75.0
    else:
        rs_fade=max(0.0,min((rs15-rs5+0.25)/1.25,1))*100

    # V15 score at very high levels raises the consequence of extension, but does not create exhaustion alone.
    score_pressure=max(0.0,min((v15_score-70)/25,1))*100
    extension_component=max(0.0,min((extension-EXHAUSTION_MIN_EXTENSION)/4.0,1))*100
    efficiency_stress=max(0.0,min((0.45-efficiency)/0.45,1))*100

    raw=(
        extension_component*0.28 +
        rollover*0.22 +
        buy_stress*0.16 +
        book_stress*0.10 +
        rs_fade*0.09 +
        score_pressure*0.08 +
        efficiency_stress*0.07
    )
    # Require an actual extended move plus at least two exhaustion symptoms.
    symptoms=sum([
        extension>=EXHAUSTION_MIN_EXTENSION,
        rollover>=45,
        b10<0.54,
        imb<0.0,
        rs5<rs15,
        (vr>=2.0 and acc>=1.5 and p60<1.0),
    ])
    score=max(0,min(round(raw),100))

    if extension<EXHAUSTION_MIN_EXTENSION:
        state_name="NORMAL"
    elif score>=EXHAUSTION_ALERT_SCORE and symptoms>=3:
        state_name="EXHAUSTION ALERT"
    elif score>=58 and symptoms>=2:
        state_name="EXHAUSTION WATCH"
    else:
        state_name="EXTENDED / MONITOR"

    alert=state_name=="EXHAUSTION ALERT" and v15_stage!="AVOID"
    return {
        "exhaustion_score":score,
        "exhaustion_state":state_name,
        "exhaustion_alert":alert,
        "exhaustion_extension":round(extension,2),
        "exhaustion_rollover":round(rollover,1),
        "exhaustion_buy_stress":round(buy_stress,1),
        "exhaustion_book_stress":round(book_stress,1),
        "exhaustion_rs_fade":round(rs_fade,1),
        "exhaustion_efficiency_stress":round(efficiency_stress,1),
        "exhaustion_symptoms":symptoms,
        "exhaustion_volume_ratio":round(vr,2),
        "exhaustion_trade_accel":round(acc,2),
    }

def v15_early_ignition(s, eps, v12):
    """Fast lane for early pump ignition before aggregate volume catches up."""
    if not eps:return None
    _,v5,b5,p5=stats(s,5);_,v10,b10,p10=stats(s,10);_,v30,b30,p30=stats(s,30);_,v60,b60,p60=stats(s,60);_,v300,_,_=stats(s,300)
    vr=v60/max(v300/5,1)
    accel10=acceleration_ratio(v10,v60)
    accel30=acceleration_ratio(v30,v60)
    accel_slope=accel10-accel30
    buy_slope=b10-b30
    rs5=float(eps.get("relative_strength_5m",0) or 0)
    rs15=float(eps.get("relative_strength_15m",0) or 0)
    btc5=float(eps.get("btc_ret_5m",0) or 0);btc15=float(eps.get("btc_ret_15m",0) or 0)
    btc_risk=(btc5<-1.0 or btc15<-2.0)
    trades10=n10 if (n10:=stats(s,10)[0]) else 0
    pressure_score=min(max((b10-.53)/.18,0),1)*24
    accel_score=min(max((accel10-1.20)/1.00,0),1)*26
    slope_score=min(max(accel_slope/.75,0),1)*14
    rs_score=min(max((rs5-.05)/.75,0),1)*16
    buy_slope_score=min(max(buy_slope/.08,0),1)*8
    activity_score=7 if trades10>=4 else 3 if trades10>=2 else 0
    compression_bonus=5 if abs(p60)<2.0 and p10<3.0 else 0
    volume_bonus=min(max((vr-.50)/1.50,0),1)*5
    penalty=0
    if p10>=3: penalty+=8
    if p60>=5: penalty+=8
    if rs5<-.25: penalty+=8
    if btc_risk: penalty+=25
    score=max(0,min(round(pressure_score+accel_score+slope_score+rs_score+buy_slope_score+activity_score+compression_bonus+volume_bonus-penalty),100))
    signals=sum([
        b10>=.55,
        accel10>=1.50,
        accel_slope>=0.15,
        rs5>=.10,
        buy_slope>=0.01,
        trades10>=4,
        p10<3.0,
    ])
    if btc_risk or p10>=4 or p60>=8:stage="AVOID"
    elif score>=78 and signals>=4:stage="EARLY_IGNITION"
    elif score>=65 and signals>=3:stage="PRE_PUMP_IGNITION"
    elif score>=55 and signals>=3:stage="IGNITION_WATCH"
    else:stage="NORMAL"
    alert=stage in ("EARLY_IGNITION","PRE_PUMP_IGNITION") and score>=65 and signals>=3 and not btc_risk
    return {
        "v15_ignition_score":score,"v15_ignition_stage":stage,"v15_ignition_alert":alert,
        "v15_ignition_signals":signals,"v15_trade_accel_slope":round(accel_slope,2),
        "v15_buy_pressure_slope":round(buy_slope,4),"v15_ignition_volume_ratio":round(vr,2),
        "v15_ignition_accel":round(accel10,2),"v15_ignition_rs5":round(rs5,2),
        "v15_ignition_rs15":round(rs15,2),"v15_ignition_trades_10s":trades10,
    }

def v15_signal(s,v4,eps,v12):
    if not v4 or not eps or not v12:return None
    x=state[s];bucket=int(time.time()/max(INTERVAL,1));prev=int(x.get("v15_last_bucket",-1))
    _,v10,b10,p10=stats(s,10);_,v60,_,p60=stats(s,60);_,v300,_,_=stats(s,300)
    vr=v60/max(v300/5,1);acc=acceleration_ratio(v10,v60)
    rs5=float(eps.get("relative_strength_5m",0) or 0);rs15=float(eps.get("relative_strength_15m",0) or 0)
    btc5=float(eps.get("btc_ret_5m",0) or 0);btc15=float(eps.get("btc_ret_15m",0) or 0)
    efficiency=float(v12.get("v12_efficiency",0) or 0);confirmation=bool(v12.get("v12_confirmation",False));eff=efficiency
    structure=bool(v4.get("v4_confirmations",0)>=6 or confirmation)
    qualifying=v4.get("v4_score",0)>=55 and v4.get("v4_alert_quality") in ("A","B","C")
    if qualifying:
        if bucket==prev+1:x["v15_streak"]=int(x.get("v15_streak",0))+1
        elif bucket!=prev:x["v15_streak"]=1
    elif bucket!=prev:x["v15_streak"]=0
    x["v15_last_bucket"]=bucket;streak=int(x.get("v15_streak",0))
    btc_risk=(btc5<-1.0 or btc15<-2.0)
    early=0.24*min(max((vr-1)/2,0),1)+0.18*min(max((acc-1)/2,0),1)+0.18*min(max((b10-.50)/.20,0),1)+0.14*min(max((p60+.25)/2.5,0),1)+0.10*min(max((rs5+.25)/1.25,0),1)+0.10*(1 if streak>=1 else 0)+0.06*(1 if not btc_risk else 0)
    opportunity=max(0,min(round(early*100),100))
    conf=0.22*min(max((vr-1)/2,0),1)+0.18*min(max((acc-1)/2,0),1)+0.15*min(max((b10-.50)/.20,0),1)+0.12*(1 if structure else 0)+0.10*(1 if confirmation else 0)+0.10*min(max((rs5+.25)/1.25,0),1)+0.08*min(max(eff/.75,0),1)+0.05*(1 if v4.get("v4_alert_quality") in ("A","B") else 0)
    confirmation_score=max(0,min(round(conf*100),100))
    if btc_risk or p10>=4 or p60>=8 or rs15<-1.5:stage="AVOID"
    elif v4.get("v4_alert_quality")=="A" and confirmation_score>=70 and p10<2.5:stage="CONFIRMED"
    elif eps.get("early_pump_stage")=="EARLY PUMP" and opportunity>=60:stage="EARLY_PUMP"
    elif eps.get("early_pump_stage")=="PRE-PUMP" and opportunity>=50:stage="PRE_PUMP"
    elif opportunity>=40:stage="WATCH"
    else:stage="NEUTRAL"
    early_candidate=stage in ("PRE_PUMP","EARLY_PUMP") and opportunity>=50 and b10>=.53 and vr>=1.15 and acc>=1.10 and rs5>-.25 and not btc_risk
    confirmed=stage=="CONFIRMED" and confirmation_score>=60 and b10>=.55 and vr>=1.5 and acc>=1.25 and rs5>0 and efficiency>=.25 and not btc_risk
    return {"v15_opportunity_score":opportunity,"v15_confirmation_score":confirmation_score,"v15_score":max(opportunity,confirmation_score),"v15_stage":stage,"v15_early_candidate":early_candidate,"v15_confirmed":confirmed,"v15_streak":streak,"v15_btc_risk_off":btc_risk,"v15_relative_strength_5m":rs5,"v15_relative_strength_15m":rs15}

def v11_v12_hybrid(v11,v12):
    if not v11 or not v12:return None
    a_plus=bool(v12.get("v12_a_plus",False));confirmation=bool(v12.get("v12_confirmation",False));efficiency=float(v12.get("v12_efficiency",0) or 0)
    v11_score=float(v11.get("v11_score",0) or 0);v12_score=float(v12.get("v12_score",0) or 0)
    score=max(0,min(round(.55*v11_score+.45*v12_score),100))
    confirmed=bool(v11.get("v11_path")=="CONFIRMED" and v11_score>=V11_CONFIRMED_SCORE and confirmation and efficiency>=.35)
    early=bool((v11.get("v11_alert") and v12.get("v12_alert") and confirmation and efficiency>=.35) or a_plus)
    alert=bool(a_plus or confirmed or (early and score>=70))
    if a_plus:path="A_PLUS";grade="A+"
    elif confirmed:path="CONFIRMED";grade="A"
    elif early:path="EARLY";grade="B"
    else:path="WATCH";grade="WATCH"
    return {"hybrid_score":score,"hybrid_path":path,"hybrid_grade":grade,"hybrid_alert":alert,"hybrid_a_plus":a_plus,"hybrid_confirmation":confirmation,"hybrid_efficiency":round(efficiency,4)}

def score(s):
    x=state[s];c=x["candle"]
    if not x["price"] or not c or s not in books:return None
    _,raw_v10,b10,p10=stats(s,10);_,v60,b60,p60=stats(s,60);_,v300,_,_=stats(s,300)
    ac=accumulation(s)
    eps=early_pump_score(s,ac)
    v4=v4_confluence(s,eps,ac)
    v5=v5_signal(s,v4)
    v6=v6_signal(s,v4)
    v7=v7_signal(s,v4)
    v8=v8_signal(s,v4,eps)
    v9=v9_signal(s,v4,eps)
    v10=v10_signal(s,v4,eps)
    v11=v11_signal(s,v4,eps)
    v12=v12_signal(s,v4,eps)
    ignition=v15_early_ignition(s,eps,v12)
    v15=v15_signal(s,v4,eps,v12)
    if not v15:v15={"v15_opportunity_score":0,"v15_confirmation_score":0,"v15_score":0,"v15_stage":"NEUTRAL","v15_early_candidate":False,"v15_confirmed":False,"v15_streak":0,"v15_btc_risk_off":False,"v15_relative_strength_5m":eps.get("relative_strength_5m",0) if eps else 0,"v15_relative_strength_15m":eps.get("relative_strength_15m",0) if eps else 0}
    v15.update(ignition or {})
    exhaustion=exhaustion_momentum(s,eps,v12,v15)
    hybrid=v11_v12_hybrid(v11,v12)
    hs=hybrid.get("hybrid_score",0) if hybrid else 0
    alert_tier="HIGH PRIORITY" if hs>=80 else "EARLY ACTION" if hs>=70 else "PRE-PUMP WATCH" if hs>=62 else "BELOW WATCH"
    base=max(v300/30,1);vr=v60/max(v300/5,1);acc=acceleration_ratio(raw_v10,v60)
    p1=(x["price"]/c["open"]-1)*100 if c["open"] else 0
    ob=books[s].metrics(20);imb=ob["imbalance"]
    raw=min(max(p10,0)*10,20)+min(max(vr-1,0)*14,28)+min(max(acc-1,0)*12,18)
    raw+=max(min((b10-.5)*50,12),-12)+max(min(imb*30,12),-12)
    sc=max(0,min(100,round(raw)))
    stage="CONFIRMED PUMP" if sc>=82 else "BREAKOUT" if sc>=70 else "EARLY MOMENTUM" if sc>=55 else "PRE-PUMP" if sc>=45 else "BUILDING" if sc>=30 else "QUIET"
    early=sc>=45 and p1<4 and b10>=.56 and vr>=1.5 and imb>=-.05
    confirm=sc>=70 and b10>=.60 and vr>=2
    chase=p1>=5 or sc>=92
    entry="EARLY ENTRY" if early and not chase else "CONFIRMATION ENTRY" if confirm and not chase else "CHASE RISK" if chase else "WATCH"
    panic=p1<-3 or (imb<-.30 and b10<.42);dist=imb<-.15 and b10<.48;mom=b10<.50 and b60<.53 and sc<45
    sell="PANIC EXIT" if panic else "DISTRIBUTION" if dist else "MOMENTUM EXIT" if mom else "TAKE PROFIT" if sc<50 and x["price"]<c["open"] else "HOLD"
    return {"hybrid_score":hs,"alert_tier":alert_tier,"hybrid_path":hybrid.get("hybrid_path","") if hybrid else "","hybrid_grade":hybrid.get("hybrid_grade","") if hybrid else "","hybrid_alert":hybrid.get("hybrid_alert",False) if hybrid else False,"hybrid_a_plus":hybrid.get("hybrid_a_plus",False) if hybrid else False,"hybrid_confirmation":hybrid.get("hybrid_confirmation",False) if hybrid else False,"hybrid_efficiency":hybrid.get("hybrid_efficiency",0) if hybrid else 0,"symbol":s,"price":x["price"],"score":sc,"stage":stage,"entry":entry,"sell":sell,"price_1m":p1,"price_10s":p10,"volume_ratio":vr,"trade_accel":acc,"buy_pressure":b10,"book_imbalance":imb,"spread_bps":ob["spread_bps"],"book_ready":ob["ready"],"book_gaps":books[s].gaps,"early_pump_score":eps["early_pump_score"],"early_pump_stage":eps["early_pump_stage"],"early_pump_quality":eps["early_pump_quality"],"relative_strength_5m":eps.get("relative_strength_5m"),"relative_strength_15m":eps.get("relative_strength_15m"),"btc_ret_5m":eps.get("btc_ret_5m"),"btc_ret_15m":eps.get("btc_ret_15m"),"false_positive_penalty":eps.get("false_positive_penalty",0),"accumulation_score":ac["accumulation_score"],"accumulation_stage":ac["accumulation_stage"],"accumulation_quality":ac["accumulation_quality"],"accum_buy_pressure":ac["accum_buy_pressure"],"accum_trade_accel":ac["accum_trade_accel"],"accum_volume_ratio":ac["accum_volume_ratio"],"accum_book_imbalance":ac["accum_book_imbalance"],"accum_price_10s":ac["accum_price_10s"],"accum_trades_10s":ac["accum_trades_10s"],**v4,**v5,**v6,**v7,**v8,**v9,**v10,**v11,**v12,"v15_model":"v15_1_early_ignition","v15_alert":bool(v15 and (v15.get("v15_confirmed") or (v15.get("v15_early_candidate") and v15.get("v15_opportunity_score",0)>=55))),"v15_opportunity_score":v15.get("v15_opportunity_score",0) if v15 else 0,"v15_confirmation_score":v15.get("v15_confirmation_score",0) if v15 else 0,"v15_score":v15.get("v15_score",0) if v15 else 0,"v15_stage":v15.get("v15_stage","") if v15 else "","v15_early_candidate":v15.get("v15_early_candidate",False) if v15 else False,"v15_confirmed":v15.get("v15_confirmed",False) if v15 else False,"v15_streak":v15.get("v15_streak",0) if v15 else 0,"v15_btc_risk_off":v15.get("v15_btc_risk_off",False) if v15 else False,"v15_relative_strength_5m":v15.get("v15_relative_strength_5m",0) if v15 else 0,"v15_relative_strength_15m":v15.get("v15_relative_strength_15m",0) if v15 else 0,**(exhaustion or {}),"updated":time.time()}

async def telegram(msg):
    token=os.getenv("TELEGRAM_BOT_TOKEN");chat=os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(f"https://api.telegram.org/bot{token}/sendMessage",json={"chat_id":chat,"text":msg},timeout=8)
    except Exception:pass

async def resync_books(http):return await asyncio.gather(*(b.resync(http) for b in books.values()),return_exceptions=True)

async def resync_unready_books(http):
    bad=[b for b in books.values() if not b.ready]
    if not bad:return None
    return await asyncio.gather(*(b.resync(http) for b in bad),return_exceptions=True)

async def main():
    global symbols,books
    os.makedirs("data",exist_ok=True);start=time.time();timeout=aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as http:
        symbols=await discover(http);books={s:LocalOrderBook(s,REST,LIMIT) for s in symbols};streams=[]
        for s in symbols:
            q=s.lower();streams += [f"{q}@aggTrade",f"{q}@bookTicker",f"{q}@depth@100ms",f"{q}@kline_1m"]
        url=WS+"?streams="+"/".join(streams)
        async with http.ws_connect(url,heartbeat=20,autoping=True,max_msg_size=16*1024*1024) as ws:
            sync=asyncio.create_task(resync_books(http))
            try:
                while time.time()-start<RUN_SECONDS:
                    try:
                        m=await asyncio.wait_for(ws.receive(),timeout=1)
                        if m.type==aiohttp.WSMsgType.TEXT:
                            z=json.loads(m.data);event(z.get("stream",""),z.get("data",{}))
                    except asyncio.TimeoutError:pass
                    if sync.done() and any(not b.ready for b in books.values()):
                        async def _resync_pending():return await resync_unready_books(http)
                        sync=asyncio.create_task(_resync_pending())
                    if int(time.time()/max(INTERVAL,1)) != int((time.time()-1)/max(INTERVAL,1)):
                        rows=[r for s in symbols if (r:=score(s))]
                        rows.sort(key=lambda z: float(z.get("hybrid_score", 0) or 0), reverse=True)
                        candidates=[r for r in rows if r.get("v15_alert",False) and r.get("v15_score",0)>=55 and r.get("v15_stage") in ("PRE_PUMP","EARLY_PUMP","CONFIRMED")][:TOP_ALERTS]
                        top_symbols={r["symbol"]:i+1 for i,r in enumerate(candidates)}
                        for r in candidates:
                            s=r["symbol"];old=state[s];rank=top_symbols[s]
                            changed=(old.get("last_stage")!=r["stage"] or old.get("last_entry")!=r["entry"] or old.get("last_alert_rank")!=rank)
                            now=time.time()
                            if changed and now-old["last_alert"]>=COOLDOWN:
                                await telegram(f"🚨 {r['alert_tier']} | V15 TOP {rank} | {s} | {r['stage']} | V15 {r['v15_score']}/100 | Opportunity {r['v15_opportunity_score']}/100 | Confirmation {r['v15_confirmation_score']}/100 | V11 {r['v11_score']}/100 | V12 {r['v12_score']}/100\nEntry: {r['entry']}\n1m: {r['price_1m']:.2f}% | 10s: {r['price_10s']:.2f}% | Vol: {r['volume_ratio']:.2f}x\nBuy: {r['buy_pressure']*100:.1f}% | OB: {r['book_imbalance']:+.2f} | Spread: {r['spread_bps']:.2f} bps\nPrice: {r['price']}")
                                old["last_alert"]=now;old["last_alert_rank"]=rank
                            old["last_stage"]=r["stage"];old["last_entry"]=r["entry"]
                        ignition_candidates=[r for r in rows if r.get("v15_ignition_alert") and r.get("v15_ignition_score",0)>=65 and r.get("v15_ignition_signals",0)>=3]
                        ignition_candidates=sorted(ignition_candidates,key=lambda r:(r.get("v15_ignition_score",0),r.get("v15_trade_accel_slope",0)),reverse=True)[:TOP_ALERTS]
                        for r in ignition_candidates:
                            s=r["symbol"];old=state[s];now=time.time()
                            changed=(r.get("v15_ignition_stage")!=old.get("last_ignition_stage") or r.get("v15_ignition_score",0)-float(old.get("last_ignition_score",0))>=5)
                            if changed and now-old["last_ignition_alert"]>=COOLDOWN:
                                msg=(f"🟢 V15.1 EARLY IGNITION | {s} | {r['v15_ignition_stage']} | Ignition {r['v15_ignition_score']}/100 | Signals {r['v15_ignition_signals']}\\n"
                                      f"Trade accel: {r['v15_ignition_accel']:.2f}x | Accel slope: {r['v15_trade_accel_slope']:+.2f}x | Buy: {r['buy_pressure']*100:.1f}% | Buy slope: {r['v15_buy_pressure_slope']:+.3f}\\n"
                                      f"RS 5m: {r['v15_ignition_rs5']:+.2f}% | RS 15m: {r['v15_ignition_rs15']:+.2f}% | Vol: {r['volume_ratio']:.2f}x | 10s trades: {r['v15_ignition_trades_10s']}\\n"
                                      f"Price: {r['price']} | 10s: {r['price_10s']:.2f}%\\n"
                                      "⚠️ Early ignition signal — volume confirmation may still be developing.")
                                await telegram(msg)
                                old["last_ignition_alert"]=now
                            old["last_ignition_score"]=r.get("v15_ignition_score",0);old["last_ignition_stage"]=r.get("v15_ignition_stage","")
                        exhaustion_candidates=[r for r in rows if r.get("exhaustion_alert") and r.get("exhaustion_score",0)>=EXHAUSTION_ALERT_SCORE and r.get("v15_score",0)>=55]
                        exhaustion_candidates=sorted(exhaustion_candidates,key=lambda r:(r.get("exhaustion_score",0),r.get("v15_score",0)),reverse=True)[:TOP_ALERTS]
                        for r in exhaustion_candidates:
                            s=r["symbol"];old=state[s];now=time.time()
                            changed=(r.get("exhaustion_state")!=old.get("last_exhaustion_state") or r.get("exhaustion_score",0)-float(old.get("last_exhaustion_score",0))>=5)
                            if changed and now-old["last_exhaustion_alert"]>=EXHAUSTION_COOLDOWN:
                                await telegram(f"⚠️ EXHAUSTION MOMENTUM | {s} | {r['exhaustion_state']} | Exhaustion {r['exhaustion_score']}/100\\nV15 {r['v15_score']}/100 | Stage: {r['v15_stage']} | Extension: {r['exhaustion_extension']:.2f}% | Rollover: {r['exhaustion_rollover']:.0f}\\nBuy pressure: {r['buy_pressure']*100:.1f}% | Vol: {r['volume_ratio']:.2f}x | Trade accel: {r['trade_accel']:.2f}x\\nBook imbalance: {r['book_imbalance']:+.2f} | RS 5m: {r['v15_relative_strength_5m']:+.2f}% | RS 15m: {r['v15_relative_strength_15m']:+.2f}%\\n⚠️ Momentum is extended and showing deterioration signals; confirmation of reversal is still required.")
                                old["last_exhaustion_alert"]=now
                            old["last_exhaustion_score"]=r.get("exhaustion_score",0);old["last_exhaustion_state"]=r.get("exhaustion_state","")
                        accum_candidates=[r for r in rows if r.get("accumulation_score",0)>=ACCUM_ALERT_SCORE and r.get("accumulation_quality") and r.get("accumulation_stage") in ("ACCUMULATION WATCH","ACCUMULATION ALERT") and r.get("score",0)>=70]
                        accum_candidates=sorted(accum_candidates,key=lambda r:(r.get("accumulation_score",0),r.get("score",0)),reverse=True)[:TOP_ALERTS]
                        for r in accum_candidates:
                            s=r["symbol"];old=state[s];now=time.time();prev=float(old.get("last_accum_score",0))
                            changed=(r["accumulation_score"]-prev>=5 or old.get("last_accum_stage")!=r["accumulation_stage"])
                            if changed and now-old["last_accum_alert"]>=COOLDOWN:
                                await telegram(f"🟣 ACCUMULATION / PRE-PUMP | {s} | {r['accumulation_stage']} | Accum {r['accumulation_score']}/100\nPump score: {r['score']}/100 | 1m: {r['price_1m']:.2f}% | 10s: {r['price_10s']:.2f}%\nBuy pressure: {r['accum_buy_pressure']*100:.1f}% | Trade accel: {r['accum_trade_accel']:.2f}x | Vol ratio: {r['accum_volume_ratio']:.2f}x\nBook imbalance: {r['accum_book_imbalance']:+.2f} | Trades/10s: {r['accum_trades_10s']}\nPrice: {r['price']}\n⚠️ Early signal — confirmation still required.")
                                old["last_accum_alert"]=now
                            old["last_accum_score"]=r["accumulation_score"];old["last_accum_stage"]=r["accumulation_stage"]
                        with open("data/latest.json","w") as f:json.dump({"updated":time.time(),"rows":rows},f,indent=2)
                        history_fields=[
    "symbol","price","score","stage","entry","sell","price_1m","price_10s",
    "volume_ratio","trade_accel","buy_pressure","book_imbalance",
    "early_pump_score","early_pump_stage","relative_strength_5m","relative_strength_15m",
    "v15_score","v15_stage","v15_opportunity_score","v15_confirmation_score","v15_ignition_score","v15_ignition_stage","v15_ignition_alert","v15_ignition_signals","v15_trade_accel_slope","v15_buy_pressure_slope",
    "v12_score","v12_efficiency","hybrid_score","hybrid_path","hybrid_grade",
    "exhaustion_score","exhaustion_state","exhaustion_alert","exhaustion_extension",
    "exhaustion_rollover","exhaustion_symptoms"
]
                        compact_rows=[{key:r.get(key) for key in history_fields if key in r} for r in rows]
                        with open("data/history.jsonl","a") as f:f.write(json.dumps({"ts":time.time(),"rows":compact_rows},separators=(",",":"))+"\n")
                        await asyncio.sleep(1)
            finally:
                if not sync.done():
                    sync.cancel()
                    try:await sync
                    except asyncio.CancelledError:pass
            rows=[r for s in symbols if (r:=score(s))];rows.sort(key=lambda z:z["score"],reverse=True)
            with open("data/latest.json","w") as f:json.dump({"updated":time.time(),"rows":rows},f,indent=2)

# V15 production deployment active
if __name__=="__main__":asyncio.run(main())