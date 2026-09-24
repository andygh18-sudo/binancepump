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
TOP_ALERTS=int(os.getenv("TOP_ALERTS","5"));MIN_ALERT_SCORE=int(os.getenv("MIN_ALERT_SCORE","38"));ACCUM_ALERT_SCORE=int(os.getenv("ACCUM_ALERT_SCORE","60"))
symbols=[];books={};state=defaultdict(lambda:{"trades":deque(maxlen=12000),"price":None,"candle":None,"last_alert":0,"last_alert_rank":None,"last_accum_alert":0,"last_accum_score":0.0})

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
        k=d["k"];x["candle"]={"open":float(k["o"]),"high":float(k["h"]),"close":float(k["c"])};x["price"]=float(k["c"])
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

def score(s):
    x=state[s];c=x["candle"]
    if not x["price"] or not c or s not in books:return None
    _,v10,b10,p10=stats(s,10);_,v60,b60,p60=stats(s,60);_,v300,_,_=stats(s,300)
    ac=accumulation(s)
    eps=early_pump_score(s,ac)
    base=max(v300/30,1);vr=v60/max(v300/5,1);acc=v10/max(v60/6,1)
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
    return {"symbol":s,"price":x["price"],"score":sc,"stage":stage,"entry":entry,"sell":sell,"price_1m":p1,"price_10s":p10,"volume_ratio":vr,"trade_accel":acc,"buy_pressure":b10,"book_imbalance":imb,"spread_bps":ob["spread_bps"],"book_ready":ob["ready"],"book_gaps":books[s].gaps,"early_pump_score":eps["early_pump_score"],"early_pump_stage":eps["early_pump_stage"],"early_pump_quality":eps["early_pump_quality"],"relative_strength_5m":eps.get("relative_strength_5m"),"relative_strength_15m":eps.get("relative_strength_15m"),"btc_ret_5m":eps.get("btc_ret_5m"),"btc_ret_15m":eps.get("btc_ret_15m"),"false_positive_penalty":eps.get("false_positive_penalty",0),"accumulation_score":ac["accumulation_score"],"accumulation_stage":ac["accumulation_stage"],"accumulation_quality":ac["accumulation_quality"],"accum_buy_pressure":ac["accum_buy_pressure"],"accum_trade_accel":ac["accum_trade_accel"],"accum_volume_ratio":ac["accum_volume_ratio"],"accum_book_imbalance":ac["accum_book_imbalance"],"accum_price_10s":ac["accum_price_10s"],"accum_trades_10s":ac["accum_trades_10s"],"updated":time.time()}

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
                    if int(time.time()-start)%int(INTERVAL)==0:
                        rows=[r for s in symbols if (r:=score(s))]
                        rows.sort(key=lambda z:z.get("early_pump_score",0),reverse=True)
                        candidates=[r for r in rows if r.get("early_pump_score",0)>=MIN_ALERT_SCORE and r["stage"] in ("BUILDING","PRE-PUMP","EARLY MOMENTUM","BREAKOUT","CONFIRMED PUMP")][:TOP_ALERTS]
                        top_symbols={r["symbol"]:i+1 for i,r in enumerate(candidates)}
                        for r in candidates:
                            s=r["symbol"];old=state[s];rank=top_symbols[s]
                            changed=(old.get("last_stage")!=r["stage"] or old.get("last_entry")!=r["entry"] or old.get("last_alert_rank")!=rank)
                            now=time.time()
                            if changed and now-old["last_alert"]>=COOLDOWN:
                                await telegram(f"⚡ TOP {rank} EARLY PUMP/MOMENTUM | {s} | {r['stage']} | {r['score']}/100\nEntry: {r['entry']}\n1m: {r['price_1m']:.2f}% | 10s: {r['price_10s']:.2f}% | Vol: {r['volume_ratio']:.2f}x\nBuy: {r['buy_pressure']*100:.1f}% | OB: {r['book_imbalance']:+.2f} | Spread: {r['spread_bps']:.2f} bps\nPrice: {r['price']}")
                                old["last_alert"]=now;old["last_alert_rank"]=rank
                            old["last_stage"]=r["stage"];old["last_entry"]=r["entry"]
                        accum_candidates=[r for r in rows if r.get("accumulation_score",0)>=ACCUM_ALERT_SCORE and r.get("accumulation_quality") and r.get("accumulation_stage") in ("ACCUMULATION WATCH","ACCUMULATION ALERT") and r.get("score",0)<70]
                        accum_candidates=sorted(accum_candidates,key=lambda r:(r.get("accumulation_score",0),r.get("score",0)),reverse=True)[:TOP_ALERTS]
                        for r in accum_candidates:
                            s=r["symbol"];old=state[s];now=time.time();prev=float(old.get("last_accum_score",0))
                            changed=(r["accumulation_score"]-prev>=5 or old.get("last_accum_stage")!=r["accumulation_stage"])
                            if changed and now-old["last_accum_alert"]>=COOLDOWN:
                                await telegram(f"🟣 ACCUMULATION / PRE-PUMP | {s} | {r['accumulation_stage']} | Accum {r['accumulation_score']}/100\nPump score: {r['score']}/100 | 1m: {r['price_1m']:.2f}% | 10s: {r['price_10s']:.2f}%\nBuy pressure: {r['accum_buy_pressure']*100:.1f}% | Trade accel: {r['accum_trade_accel']:.2f}x | Vol ratio: {r['accum_volume_ratio']:.2f}x\nBook imbalance: {r['accum_book_imbalance']:+.2f} | Trades/10s: {r['accum_trades_10s']}\nPrice: {r['price']}\n⚠️ Early signal — confirmation still required.")
                                old["last_accum_alert"]=now
                            old["last_accum_score"]=r["accumulation_score"];old["last_accum_stage"]=r["accumulation_stage"]
                        with open("data/latest.json","w") as f:json.dump({"updated":time.time(),"rows":rows},f,indent=2)
                        with open("data/history.jsonl","a") as f:f.write(json.dumps({"ts":time.time(),"rows":rows})+"\n")
                        await asyncio.sleep(1)
            finally:
                if not sync.done():
                    sync.cancel()
                    try:await sync
                    except asyncio.CancelledError:pass
            rows=[r for s in symbols if (r:=score(s))];rows.sort(key=lambda z:z["score"],reverse=True)
            with open("data/latest.json","w") as f:json.dump({"updated":time.time(),"rows":rows},f,indent=2)

if __name__=="__main__":asyncio.run(main())
