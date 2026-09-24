import asyncio,aiohttp,json,os,time
from collections import defaultdict,deque
from dotenv import load_dotenv
from .orderbook import LocalOrderBook

load_dotenv()
WS=os.getenv("BINANCE_WS_BASE","wss://data-stream.binance.vision/stream")
REST=os.getenv("BINANCE_REST_BASE","https://data-api.binance.vision")
MAX=int(os.getenv("MAX_SYMBOLS","40"));MINVOL=float(os.getenv("MIN_QUOTE_VOLUME","1000000"))
RUN_SECONDS=int(os.getenv("RUN_SECONDS","250"));INTERVAL=float(os.getenv("DECISION_INTERVAL","5"))
COOLDOWN=float(os.getenv("ALERT_COOLDOWN","60"));LIMIT=int(os.getenv("ORDERBOOK_LIMIT","1000"))
TOP_ALERTS=int(os.getenv("TOP_ALERTS","5"));MIN_ALERT_SCORE=int(os.getenv("MIN_ALERT_SCORE","38"));ACCUM_ALERT_SCORE=int(os.getenv("ACCUM_ALERT_SCORE","60"));V4_ALERT_SCORE=int(os.getenv("V4_ALERT_SCORE","60"));V5_ALERT_SCORE=int(os.getenv("V5_ALERT_SCORE","65"));V5_MIN_PERSISTENCE=int(os.getenv("V5_MIN_PERSISTENCE","2"));V5_MIN_HIST_SAMPLES=int(os.getenv("V5_MIN_HIST_SAMPLES","5"));V5_MIN_HIST_RATE=float(os.getenv("V5_MIN_HIST_RATE","8"));V6_ALERT_SCORE=int(os.getenv("V6_ALERT_SCORE","65"));V6_MIN_PERSISTENCE=int(os.getenv("V6_MIN_PERSISTENCE","2"));V6_MIN_HIST_SAMPLES=int(os.getenv("V6_MIN_HIST_SAMPLES","20"));V6_MIN_HIST_RATE=float(os.getenv("V6_MIN_HIST_RATE","8"));V7_ALERT_SCORE=int(os.getenv("V7_ALERT_SCORE","65"));V7_MIN_PERSISTENCE=int(os.getenv("V7_MIN_PERSISTENCE","2"));V7_MIN_HIST_SAMPLES=int(os.getenv("V7_MIN_HIST_SAMPLES","20"));V7_MIN_HIST_RATE=float(os.getenv("V7_MIN_HIST_RATE","8"));V8_ALERT_SCORE=int(os.getenv("V8_ALERT_SCORE","58"));V8_MIN_PERSISTENCE=int(os.getenv("V8_MIN_PERSISTENCE","2"));V9_ALERT_SCORE=int(os.getenv("V9_ALERT_SCORE","58"));V9_MIN_PERSISTENCE=int(os.getenv("V9_MIN_PERSISTENCE","2"));V10_ALERT_SCORE=int(os.getenv("V10_ALERT_SCORE","60"));V10_MIN_PERSISTENCE=int(os.getenv("V10_MIN_PERSISTENCE","2"));V11_ALERT_SCORE=int(os.getenv("V11_ALERT_SCORE","65"));V11_CONFIRMED_SCORE=int(os.getenv("V11_CONFIRMED_SCORE","72"));V11_MIN_PERSISTENCE=int(os.getenv("V11_MIN_PERSISTENCE","2"));V12_ALERT_SCORE=int(os.getenv("V12_ALERT_SCORE","62"));V12_CONFIRMED_SCORE=int(os.getenv("V12_CONFIRMED_SCORE","70"));V12_MIN_PERSISTENCE=int(os.getenv("V12_MIN_PERSISTENCE","2"))
symbols=[];books={};state=defaultdict(lambda:{"trades":deque(maxlen=12000),"price":None,"candle":None,"last_alert":0,"last_alert_rank":None,"last_accum_alert":0,"last_accum_score":0.0,"v5_streak":0,"v5_last_bucket":-1,"v5_last_score":0.0,"v6_streak":0,"v6_last_bucket":-1,"v6_last_score":0.0,"v7_streak":0,"v7_last_bucket":-1,"v7_last_score":0.0,"v8_streak":0,"v8_last_bucket":-1,"v8_last_score":0.0,"v9_streak":0,"v9_last_bucket":-1,"v9_last_score":0.0,"v10_streak":0,"v10_last_bucket":-1,"v10_last_score":0.0,"v11_streak":0,"v11_last_bucket":-1,"v11_last_score":0.0,"v12_streak":0,"v12_last_bucket":-1,"v12_last_score":0.0})

async def get_json(s,url,params=None):
    async with s.get(url,params=params,timeout=12) as r:
        r.raise_for_status();return await r.json()

async def discover(s):
    info=await get_json(s,REST+"/api/v3/exchangeInfo")
    stable_bases={"USDT","USDC","FDUSD","TUSD","USDP","DAI","BUSD","PYUSD","USDD","EUR","GBP","TRY","BRL","ARS","AUD","RUB","UAH","PLN","RON","ZAR","NGN","JPY"}
    trad={x["symbol"] for x in info["symbols"] if x["status"]=="TRADING" and x["quoteAsset"]=="USDT" and x.get("baseAsset") not in stable_bases}
    ticks=await get_json(s,REST+"/api/v3/ticker/24hr")
    rows=[x for x in ticks if x["symbol"] in trad and float(x.get("quoteVolume",0))>=MINVOL]
    rows.sort(key=lambda x:float(x.get("quoteVolume",0)),reverse=True)
    out=[x["symbol"] for x in rows[:MAX]]
    if "BTCUSDT" not in out and "BTCUSDT" in trad:out.append("BTCUSDT")
    return out

def event(stream,d):
    s=d.get("s","")
    if not s:return
    x=state[s];now=time.time()
    if stream.endswith("@aggTrade"):
        p=float(d["p"]);n=p*float(d["q"]);buy=not bool(d.get("m",False));x["trades"].append((now,p,n,buy));x["price"]=p
    elif stream.endswith("@bookTicker"):x["price"]=float(d["a"])
    elif "@kline_1m" in stream:
        k=d["k"];new_c={"open":float(k["o"]),"high":float(k["h"]),"close":float(k["c"])};x["candle"]=new_c;x["price"]=float(k["c"])
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
    btc5=0.0;btc15=0.0
    if s!="BTCUSDT" and state.get("BTCUSDT",{}).get("price"):
        _,_,_,btc5=stats("BTCUSDT",5);_,_,_,btc15=stats("BTCUSDT",15)
    rs5=p10-btc5;rs15=p60-btc15
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
    return {"early_pump_score":score,"early_pump_stage":stage,"early_pump_quality":quality,"relative_strength_5m":rs5,"relative_strength_15m":rs15,"btc_ret_5m":btc5,"btc_ret_15m":btc15,"false_positive_penalty":penalty}

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
# [existing v6-v10 signal implementations retained verbatim in repository]
