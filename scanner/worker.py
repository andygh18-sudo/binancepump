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
symbols=[];books={};state=defaultdict(lambda:{"trades":deque(maxlen=12000),"price":None,"candle":None,"last_alert":0})

async def get_json(s,url,params=None):
    async with s.get(url,params=params,timeout=12) as r:
        r.raise_for_status();return await r.json()

async def discover(s):
    info=await get_json(s,REST+"/api/v3/exchangeInfo")
    # Scan only USDT-quoted crypto assets. Exclude stablecoins and fiat/currency bases.
    stable_bases={
        "USDT","USDC","FDUSD","TUSD","USDP","DAI","BUSD","PYUSD","USDD",
        "EUR","GBP","TRY","BRL","ARS","AUD","RUB","UAH","PLN","RON","ZAR","NGN","JPY"
    }
    trad={
        x["symbol"] for x in info["symbols"]
        if x["status"]=="TRADING"
        and x["quoteAsset"]=="USDT"
        and x.get("baseAsset") not in stable_bases
    }
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
        p=float(d["p"]);n=p*float(d["q"]);buy=not bool(d.get("m",False))
        x["trades"].append((now,p,n,buy));x["price"]=p
    elif stream.endswith("@bookTicker"):x["price"]=float(d["a"])
    elif "@kline_1m" in stream:
        k=d["k"];x["candle"]={"open":float(k["o"]),"high":float(k["h"]),"close":float(k["c"])}
        x["price"]=float(k["c"])
    elif stream.endswith("@depth@100ms") and s in books:
        books[s].buffer_event(d)

def stats(s,sec):
    cut=time.time()-sec;r=[z for z in state[s]["trades"] if z[0]>=cut]
    if not r:return 0,0,0,0
    n=sum(z[2] for z in r);b=sum(z[2] for z in r if z[3])
    return len(r),n,b/n if n else 0,(r[-1][1]/r[0][1]-1)*100 if len(r)>1 else 0

def score(s):
    x=state[s];c=x["candle"]
    if not x["price"] or not c or s not in books:return None
    _,v10,b10,p10=stats(s,10);_,v60,b60,p60=stats(s,60);_,v300,_,_=stats(s,300)
    base=max(v300/30,1);vr=v60/max(base*60,1);acc=v10/max(v60/6,1)
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
    panic=p1<-3 or (imb<-.30 and b10<.42)
    dist=imb<-.15 and b10<.48
    mom=b10<.50 and b60<.53 and sc<45
    sell="PANIC EXIT" if panic else "DISTRIBUTION" if dist else "MOMENTUM EXIT" if mom else "TAKE PROFIT" if sc<50 and x["price"]<c["open"] else "HOLD"
    return {"symbol":s,"price":x["price"],"score":sc,"stage":stage,"entry":entry,"sell":sell,
            "price_1m":p1,"price_10s":p10,"volume_ratio":vr,"trade_accel":acc,
            "buy_pressure":b10,"book_imbalance":imb,"spread_bps":ob["spread_bps"],
            "book_ready":ob["ready"],"book_gaps":books[s].gaps,"updated":time.time()}

async def telegram(msg):
    token=os.getenv("TELEGRAM_BOT_TOKEN");chat=os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(f"https://api.telegram.org/bot{token}/sendMessage",
                         json={"chat_id":chat,"text":msg},timeout=8)
    except Exception:
        pass

async def resync_books(http):
    results=await asyncio.gather(
        *(b.resync(http) for b in books.values()),
        return_exceptions=True,
    )
    return results

async def resync_unready_books(http):
    bad=[b for b in books.values() if not b.ready]
    if not bad:
        return None
    return await asyncio.gather(
        *(b.resync(http) for b in bad),
        return_exceptions=True,
    )

async def main():
    global symbols,books
    os.makedirs("data", exist_ok=True)
    start=time.time()
    timeout=aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as http:
        symbols=await discover(http)
        books={s:LocalOrderBook(s,REST,LIMIT) for s in symbols}
        streams=[]
        for s in symbols:
            q=s.lower()
            streams += [f"{q}@aggTrade",f"{q}@bookTicker",f"{q}@depth@100ms",f"{q}@kline_1m"]
        url=WS+"?streams="+"/".join(streams)

        async with http.ws_connect(
            url,
            heartbeat=20,
            autoping=True,
            max_msg_size=16*1024*1024,
        ) as ws:
            # FIX: gather() returns a Future; create_task() expects a coroutine.
            sync=asyncio.create_task(resync_books(http))

            try:
                while time.time()-start<RUN_SECONDS:
                    try:
                        m=await asyncio.wait_for(ws.receive(),timeout=1)
                        if m.type==aiohttp.WSMsgType.TEXT:
                            z=json.loads(m.data)
                            event(z.get("stream",""),z.get("data",{}))
                    except asyncio.TimeoutError:
                        pass

                    if sync.done() and any(not b.ready for b in books.values()):
                        # FIX: wrap gather() in a coroutine before passing to create_task().
                        async def _resync_pending():
                            return await resync_unready_books(http)
                        sync=asyncio.create_task(_resync_pending())

                    if int(time.time()-start)%int(INTERVAL)==0:
                        rows=[]
                        for s in symbols:
                            r=score(s)
                            if not r:
                                continue
                            old=state[s]
                            alert=(
                                r["stage"]=="PRE-PUMP"
                                or r["entry"] in ("EARLY ENTRY","CONFIRMATION ENTRY")
                                or r["sell"] in ("TAKE PROFIT","MOMENTUM EXIT","DISTRIBUTION","PANIC EXIT")
                            )
                            changed=(
                                old.get("last_stage")!=r["stage"]
                                or old.get("last_entry")!=r["entry"]
                                or old.get("last_sell")!=r["sell"]
                            )
                            now=time.time()
                            if changed and alert and now-old["last_alert"]>=COOLDOWN:
                                await telegram(
                                    f"⚡ {s} | {r['stage']} | {r['score']}/100\\n"
                                    f"Entry: {r['entry']} | Sell: {r['sell']}\\n"
                                    f"1m: {r['price_1m']:.2f}% | 10s: {r['price_10s']:.2f}% | Vol: {r['volume_ratio']:.2f}x\\n"
                                    f"Buy: {r['buy_pressure']*100:.1f}% | OB: {r['book_imbalance']:+.2f} | "
                                    f"Spread: {r['spread_bps']:.2f} bps\\nPrice: {r['price']}"
                                )
                                old["last_alert"]=now
                            old["last_stage"]=r["stage"]
                            old["last_entry"]=r["entry"]
                            old["last_sell"]=r["sell"]
                            rows.append(r)

                        rows.sort(key=lambda z:z["score"],reverse=True)
                        with open("data/latest.json","w") as f:
                            json.dump({"updated":time.time(),"rows":rows},f,indent=2)
                        with open("data/history.jsonl","a") as f:
                            f.write(json.dumps({"ts":time.time(),"rows":rows})+"\\n")
                        await asyncio.sleep(1)

            finally:
                if not sync.done():
                    sync.cancel()
                    try:
                        await sync
                    except asyncio.CancelledError:
                        pass

            rows=[score(s) for s in symbols if score(s)]
            rows.sort(key=lambda z:z["score"],reverse=True)
            with open("data/latest.json","w") as f:
                json.dump({"updated":time.time(),"rows":rows},f,indent=2)

if __name__=="__main__":
    asyncio.run(main())
