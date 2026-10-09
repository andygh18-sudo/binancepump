import asyncio,aiohttp,json,os,time,math
from collections import defaultdict,deque
from dotenv import load_dotenv
from .orderbook import LocalOrderBook
from .tradingview import fetch_tradingview_signals
from .v157_learning import persist_v157_observations
from .v158 import MarketDiscovery,trade_features,liquidity_features,data_quality,adaptive_micro_features,adaptive_book_features,queue_transition_imbalance_features,depth_sweep_features,replenishment_absorption_features,v158_pre_ignition_build_features,v158_liquidity_state_features,v158_temporal_reignition_memory
from .v156 import evaluate as v156_evaluate
from .v156_deterioration import V156DeteriorationMonitor
# V15.6 deterioration redesign validation marker

load_dotenv()
WS=os.getenv("BINANCE_WS_BASE","wss://data-stream.binance.vision/stream")
REST=os.getenv("BINANCE_REST_BASE","https://data-api.binance.vision")
MAX=int(os.getenv("MAX_SYMBOLS","120"))
MINVOL=float(os.getenv("MIN_QUOTE_VOLUME","100000"))
DISCOVERY_MINVOL=float(os.getenv("DISCOVERY_MIN_QUOTE_VOLUME","50000"))
MOMENTUM_SYMBOLS=int(os.getenv("MOMENTUM_SYMBOLS","40"))
LIQUID_SYMBOLS=int(os.getenv("LIQUID_SYMBOLS","80"))
RUN_SECONDS=int(os.getenv("RUN_SECONDS","250"));INTERVAL=float(os.getenv("DECISION_INTERVAL","5"));IGNITION_HISTORY_SAMPLES=int(os.getenv("IGNITION_HISTORY_SAMPLES","60"))
COOLDOWN=float(os.getenv("ALERT_COOLDOWN","60"));LIMIT=int(os.getenv("ORDERBOOK_LIMIT","1000"));HISTORY_SAMPLE_INTERVAL=float(os.getenv("HISTORY_SAMPLE_INTERVAL","300"))
TOP_ALERTS=int(os.getenv("TOP_ALERTS","5"));MIN_ALERT_SCORE=int(os.getenv("MIN_ALERT_SCORE","38"));ACCUM_ALERT_SCORE=int(os.getenv("ACCUM_ALERT_SCORE","60"));V4_ALERT_SCORE=int(os.getenv("V4_ALERT_SCORE","60"));V5_ALERT_SCORE=int(os.getenv("V5_ALERT_SCORE","65"));V5_MIN_PERSISTENCE=int(os.getenv("V5_MIN_PERSISTENCE","2"));V5_MIN_HIST_SAMPLES=int(os.getenv("V5_MIN_HIST_SAMPLES","5"));V5_MIN_HIST_RATE=float(os.getenv("V5_MIN_HIST_RATE","8"));V6_ALERT_SCORE=int(os.getenv("V6_ALERT_SCORE","65"));V6_MIN_PERSISTENCE=int(os.getenv("V6_MIN_PERSISTENCE","2"));V6_MIN_HIST_SAMPLES=int(os.getenv("V6_MIN_HIST_SAMPLES","20"));V6_MIN_HIST_RATE=float(os.getenv("V6_MIN_HIST_RATE","8"));V7_ALERT_SCORE=int(os.getenv("V7_ALERT_SCORE","65"));V7_MIN_PERSISTENCE=int(os.getenv("V7_MIN_PERSISTENCE","2"));V7_MIN_HIST_SAMPLES=int(os.getenv("V7_MIN_HIST_SAMPLES","20"));V7_MIN_HIST_RATE=float(os.getenv("V7_MIN_HIST_RATE","8"));V8_ALERT_SCORE=int(os.getenv("V8_ALERT_SCORE","58"));V8_MIN_PERSISTENCE=int(os.getenv("V8_MIN_PERSISTENCE","2"));V9_ALERT_SCORE=int(os.getenv("V9_ALERT_SCORE","58"));V9_MIN_PERSISTENCE=int(os.getenv("V9_MIN_PERSISTENCE","2"));V10_ALERT_SCORE=int(os.getenv("V10_ALERT_SCORE","60"));V10_MIN_PERSISTENCE=int(os.getenv("V10_MIN_PERSISTENCE","2"));V11_ALERT_SCORE=int(os.getenv("V11_ALERT_SCORE","65"));V11_CONFIRMED_SCORE=int(os.getenv("V11_CONFIRMED_SCORE","72"));V11_MIN_PERSISTENCE=int(os.getenv("V11_MIN_PERSISTENCE","2"));V12_ALERT_SCORE=int(os.getenv("V12_ALERT_SCORE","62"));V12_CONFIRMED_SCORE=int(os.getenv("V12_CONFIRMED_SCORE","70"));V12_MIN_PERSISTENCE=int(os.getenv("V12_MIN_PERSISTENCE","2"));BUY_ALERT_COOLDOWN=float(os.getenv("BUY_ALERT_COOLDOWN","1800"));BUY_ALERT_TOP=int(os.getenv("BUY_ALERT_TOP","3"));BUY_ALERT_MIN_QUALITY=float(os.getenv("BUY_ALERT_MIN_QUALITY","80"));BUY_ALERT_MIN_CONFIRMATION=float(os.getenv("BUY_ALERT_MIN_CONFIRMATION","70"));BUY_ALERT_MIN_OPPORTUNITY=float(os.getenv("BUY_ALERT_MIN_OPPORTUNITY","60"));BUY_ALERT_MIN_TV=float(os.getenv("BUY_ALERT_MIN_TV","70"));BUY_ALERT_MIN_BULL_TF=int(os.getenv("BUY_ALERT_MIN_BULL_TF","4"));BUY_ALERT_MAX_EXHAUSTION=float(os.getenv("BUY_ALERT_MAX_EXHAUSTION","58"));PUMP_MOMENTUM_ALERT_MIN=float(os.getenv("PUMP_MOMENTUM_ALERT_MIN","75"));PUMP_MOMENTUM_ALERT_EXTREME=float(os.getenv("PUMP_MOMENTUM_ALERT_EXTREME","90"));PUMP_MOMENTUM_ALERT_JUMP=float(os.getenv("PUMP_MOMENTUM_ALERT_JUMP","10"));PUMP_MOMENTUM_ALERT_COOLDOWN=float(os.getenv("PUMP_MOMENTUM_ALERT_COOLDOWN","300"));PUMP_MOMENTUM_ALERT_TOP=int(os.getenv("PUMP_MOMENTUM_ALERT_TOP","3"));FAST_PUMP_MIN_1M=float(os.getenv("FAST_PUMP_MIN_1M","0.15"));FAST_PUMP_MIN_5M=float(os.getenv("FAST_PUMP_MIN_5M","0.60"));FAST_PUMP_MIN_VOLUME=float(os.getenv("FAST_PUMP_MIN_VOLUME","2.00"));FAST_PUMP_MIN_ACCEL=float(os.getenv("FAST_PUMP_MIN_ACCEL","1.60"));FAST_PUMP_MIN_BUY=float(os.getenv("FAST_PUMP_MIN_BUY","0.65"));FAST_PUMP_MIN_RS5=float(os.getenv("FAST_PUMP_MIN_RS5","-0.10"));FAST_PUMP_MIN_SCORE=float(os.getenv("FAST_PUMP_MIN_SCORE","80"));FAST_PUMP_MAX_EXHAUSTION=float(os.getenv("FAST_PUMP_MAX_EXHAUSTION","35"));FAST_PUMP_ALERT_COOLDOWN=float(os.getenv("FAST_PUMP_ALERT_COOLDOWN","0"));FAST_PUMP_V15_PREFERENCE=float(os.getenv("FAST_PUMP_V15_PREFERENCE","60"));FAST_PUMP_MIN_ACCUMULATION=float(os.getenv("FAST_PUMP_MIN_ACCUMULATION","60"));FAST_PUMP_MIN_BULL_TF=int(os.getenv("FAST_PUMP_MIN_BULL_TF","2"));FAST_PUMP_LEADER_MARGIN=float(os.getenv("FAST_PUMP_LEADER_MARGIN","5"));FAST_PUMP_TOP_N=int(os.getenv("FAST_PUMP_TOP_N","2"));FAST_PUMP_ROLLING_WINDOW=float(os.getenv("FAST_PUMP_ROLLING_WINDOW","900"));FAST_PUMP_MIN_OBSERVATIONS=int(os.getenv("FAST_PUMP_MIN_OBSERVATIONS","3"));FAST_PUMP_MIN_SPAN_SECONDS=float(os.getenv("FAST_PUMP_MIN_SPAN_SECONDS","20"));FAST_PUMP_REPLACEMENT_MARGIN=float(os.getenv("FAST_PUMP_REPLACEMENT_MARGIN","5"));FAST_PUMP_EPISODE_GAP_SECONDS=float(os.getenv("FAST_PUMP_EPISODE_GAP_SECONDS","20"));FAST_PUMP_EPISODE_ALERT_COOLDOWN=float(os.getenv("FAST_PUMP_EPISODE_ALERT_COOLDOWN","900"));SUSTAINED_PUMP_ENABLED=os.getenv("SUSTAINED_PUMP_ENABLED","1")=="1";SUSTAINED_PUMP_MIN_SCORE=float(os.getenv("SUSTAINED_PUMP_MIN_SCORE","76"));SUSTAINED_PUMP_MIN_1M=float(os.getenv("SUSTAINED_PUMP_MIN_1M","0.10"));SUSTAINED_PUMP_MIN_5M=float(os.getenv("SUSTAINED_PUMP_MIN_5M","0.50"));SUSTAINED_PUMP_MIN_BUY=float(os.getenv("SUSTAINED_PUMP_MIN_BUY","0.53"));SUSTAINED_PUMP_MIN_ACCEL=float(os.getenv("SUSTAINED_PUMP_MIN_ACCEL","1.10"));SUSTAINED_PUMP_MIN_VOLUME=float(os.getenv("SUSTAINED_PUMP_MIN_VOLUME","1.10"));SUSTAINED_PUMP_MIN_CVD=float(os.getenv("SUSTAINED_PUMP_MIN_CVD","0.00"));SUSTAINED_PUMP_MIN_OBS=int(os.getenv("SUSTAINED_PUMP_MIN_OBS","2"));SUSTAINED_PUMP_MAX_EXHAUSTION=float(os.getenv("SUSTAINED_PUMP_MAX_EXHAUSTION","55"));SUSTAINED_PUMP_ALERT_COOLDOWN=float(os.getenv("SUSTAINED_PUMP_ALERT_COOLDOWN","300"));V158_EARLY_MOMENTUM_RECOVERY_ENABLED=os.getenv("V158_EARLY_MOMENTUM_RECOVERY_ENABLED","1")=="1";V158_RECOVERY_MIN_REGIME=float(os.getenv("V158_RECOVERY_MIN_REGIME","55"));V158_RECOVERY_MIN_INTENSITY=float(os.getenv("V158_RECOVERY_MIN_INTENSITY","1.5"));V158_RECOVERY_MIN_TRADE_SIZE=float(os.getenv("V158_RECOVERY_MIN_TRADE_SIZE","1.0"));V158_PARTICIPATION_IGNITION_ENABLED=os.getenv("V158_PARTICIPATION_IGNITION_ENABLED","1")=="1";V158_PARTICIPATION_MIN_TRADE_Z=float(os.getenv("V158_PARTICIPATION_MIN_TRADE_Z","2.0"));V158_PARTICIPATION_MIN_VOLUME_Z=float(os.getenv("V158_PARTICIPATION_MIN_VOLUME_Z","2.0"));V158_PARTICIPATION_MIN_BUY=float(os.getenv("V158_PARTICIPATION_MIN_BUY","0.60"));V158_PARTICIPATION_MIN_ACCEL=float(os.getenv("V158_PARTICIPATION_MIN_ACCEL","1.50"));V158_DIRECTIONAL_ACCELERATION_ENABLED=os.getenv("V158_DIRECTIONAL_ACCELERATION_ENABLED","1")=="1";V158_DIRECTIONAL_MIN_BUY_SLOPE=float(os.getenv("V158_DIRECTIONAL_MIN_BUY_SLOPE","0.025"));V158_DIRECTIONAL_MIN_PRICE_10S=float(os.getenv("V158_DIRECTIONAL_MIN_PRICE_10S","0.10"));V158_DIRECTIONAL_MIN_PRICE_60S=float(os.getenv("V158_DIRECTIONAL_MIN_PRICE_60S","0.00"));V158_DIRECTIONAL_MIN_CVD_Z=float(os.getenv("V158_DIRECTIONAL_MIN_CVD_Z","0.50"));V158_DIRECTIONAL_MAX_PRICE_IMPACT_Z=float(os.getenv("V158_DIRECTIONAL_MAX_PRICE_IMPACT_Z","2.50"));EARLY_MOMENTUM_MIN_SCORE=float(os.getenv("EARLY_MOMENTUM_MIN_SCORE","55"));EARLY_MOMENTUM_MIN_TRADES=int(os.getenv("EARLY_MOMENTUM_MIN_TRADES","3"));EARLY_MOMENTUM_MIN_BUY=float(os.getenv("EARLY_MOMENTUM_MIN_BUY","0.55"));EARLY_MOMENTUM_MIN_ACCEL=float(os.getenv("EARLY_MOMENTUM_MIN_ACCEL","1.15"));EARLY_MOMENTUM_MAX_SPREAD=float(os.getenv("EARLY_MOMENTUM_MAX_SPREAD","15"));FAST_PUMP_LEADERBOARD_FILE=os.getenv("FAST_PUMP_LEADERBOARD_FILE","data/v157_fastest_pump_leaderboard.json");V156_POSTBUY_WINDOW_SECONDS=float(os.getenv("V156_POSTBUY_WINDOW_SECONDS","3600"));V156_POSTBUY_GRACE_SECONDS=float(os.getenv("V156_POSTBUY_GRACE_SECONDS","10"));V156_POSTBUY_CONFIRM_OBS=int(os.getenv("V156_POSTBUY_CONFIRM_OBS","5"));V156_POSTBUY_MIN_BUY=float(os.getenv("V156_POSTBUY_MIN_BUY","0.55"));V156_POSTBUY_MIN_ACCEL=float(os.getenv("V156_POSTBUY_MIN_ACCEL","1.25"));V156_POSTBUY_MIN_VOLUME=float(os.getenv("V156_POSTBUY_MIN_VOLUME","1.00"));V156_POSTBUY_MAX_RS5=float(os.getenv("V156_POSTBUY_MAX_RS5","-0.10"));V156_POSTBUY_MAX_EXHAUSTION=float(os.getenv("V156_POSTBUY_MAX_EXHAUSTION","70"));V156_POSTBUY_CONFIRM_DROP=float(os.getenv("V156_POSTBUY_CONFIRM_DROP","20"));V156_POSTBUY_MAX_DRAWDOWN=float(os.getenv("V156_POSTBUY_MAX_DRAWDOWN","-0.80"));EXHAUSTION_ALERT_SCORE=int(os.getenv("EXHAUSTION_ALERT_SCORE","72"));EXHAUSTION_MIN_EXTENSION=float(os.getenv("EXHAUSTION_MIN_EXTENSION","2.5"));EXHAUSTION_COOLDOWN=float(os.getenv("EXHAUSTION_COOLDOWN","120"));TRADINGVIEW_ENABLED=os.getenv("TRADINGVIEW_ENABLED","1")=="1";TRADINGVIEW_REFRESH_SECONDS=float(os.getenv("TRADINGVIEW_REFRESH_SECONDS","30"))
V158_ENABLED=os.getenv("V158_ENABLED","1")=="1";V158_MAX_DYNAMIC=int(os.getenv("V158_MAX_DYNAMIC","20"));V158_MIN_QUOTE_VOLUME=float(os.getenv("V158_MIN_QUOTE_VOLUME","10000"));V158_PROMOTION_TTL=float(os.getenv("V158_PROMOTION_TTL","180"));V158_PROMOTION_SCORE=float(os.getenv("V158_PROMOTION_SCORE","55"));V158_EMERGENCY_MIN_QUOTE_VOLUME=float(os.getenv("V158_EMERGENCY_MIN_QUOTE_VOLUME","5000"));V158_EMERGENCY_SCORE=float(os.getenv("V158_EMERGENCY_SCORE","35"));V158_EMERGENCY_VELOCITY=float(os.getenv("V158_EMERGENCY_VELOCITY","0.05"));V158_EMERGENCY_TRADE_ANOMALY=float(os.getenv("V158_EMERGENCY_TRADE_ANOMALY","1.60"));V158_EMERGENCY_PCT=float(os.getenv("V158_EMERGENCY_PCT","3.0"));V158_XVENUE_ENABLED=os.getenv("V158_XVENUE_ENABLED","1")=="1";V158_XVENUE_INTERVAL=float(os.getenv("V158_XVENUE_INTERVAL","30"))
symbols=[];books={};tv_cache={};tv_last_refresh=0.0;v158_xvenue_cache={};v158_xvenue_last=0.0
v158_discovery=MarketDiscovery(max_promoted=V158_MAX_DYNAMIC,min_quote_volume=V158_MIN_QUOTE_VOLUME,ttl=V158_PROMOTION_TTL,min_score=V158_PROMOTION_SCORE,emergency_min_quote_volume=V158_EMERGENCY_MIN_QUOTE_VOLUME,emergency_score=V158_EMERGENCY_SCORE,emergency_velocity=V158_EMERGENCY_VELOCITY,emergency_trade_anomaly=V158_EMERGENCY_TRADE_ANOMALY,emergency_pct=V158_EMERGENCY_PCT);v158_dynamic_tasks={};v158_dynamic_until={};v158_core_symbols=set();v158_promoting=set()
v156_deterioration=V156DeteriorationMonitor()
state=defaultdict(lambda:{"trades":deque(maxlen=12000),"price":None,"candle":None,"ignition_window":deque(maxlen=60),"last_alert":0,"last_alert_rank":None,"last_accum_alert":0,"last_accum_score":0.0,"v5_streak":0,"v5_last_bucket":-1,"v5_last_score":0.0,"v6_streak":0,"v6_last_bucket":-1,"v6_last_score":0.0,"v7_streak":0,"v7_last_bucket":-1,"v7_last_score":0.0,"v8_streak":0,"v8_last_bucket":-1,"v8_last_score":0.0,"v10_streak":0,"v10_last_bucket":-1,"v10_last_score":0.0,"v12_streak":0,"v12_last_bucket":-1,"v12_last_score":0.0,"last_exhaustion_alert":0,"last_exhaustion_score":0.0,"last_exhaustion_state":"","last_ignition_alert":0,"last_ignition_score":0.0,"last_ignition_stage":"","last_buy_alert":0,"last_buy_decision":"","last_buy_quality":0.0,"last_pump_momentum_alert":0,"last_pump_momentum_score":0.0,"last_pump_momentum_label":"","last_top5_price_alert":0,"last_top5_price_rank":None,"last_top5_price_score":0.0,"last_fast_pump_alert":0,"last_fast_pump_score":0.0,"last_fast_pump_symbol":"","early_momentum_score":0.0,"early_momentum_stage":"MONITOR","early_momentum_last":0.0,"v156_postbuy_active":False,"v156_postbuy_started":0.0,"v156_postbuy_ready_at":0.0,"v156_postbuy_baseline_price":0.0,"v156_postbuy_baseline_confirmation":0.0,"v156_postbuy_price":0.0,"v156_postbuy_confirmation":0.0,"v156_postbuy_observations":0,"v156_postbuy_bad_streak":0,"v156_postbuy_last_alert":0.0,"v156_postbuy_state":"","v156_postbuy_prev_rsi":{},"reignition_armed_until":0,"reignition_armed_score":0.0,"last_reignition_alert":0,"last_reignition_stage":"","last_reignition_score":0.0,"v158_recovery_active":False,"v158_recovery_score":0.0,"v158_recovery_last":0.0,"v158_participation_active":False,"v158_participation_score":0.0,"v158_participation_last":0.0,"v158_directional_ignition_active":False,"v158_directional_ignition_score":0.0,"v158_directional_ignition_last":0.0,"sustained_pump_streak":0,"sustained_pump_score":0.0,"sustained_pump_last_alert":0.0,"sustained_pump_state":"MONITOR"})

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
        p=float(d["p"]);n=p*float(d["q"]);buy=not bool(d.get("m",False));x["trades"].append((now,p,n,buy));x["price"]=p;x["last_trade_event"]=now
    elif stream.endswith("@bookTicker"):x["price"]=float(d["a"]);x["last_book_event"]=now
    elif "@kline_1m" in stream:
        k=d["k"];new_c={"open":float(k["o"]),"high":float(k["h"]),"low":float(k["l"]),"close":float(k["c"])};x["candle"]=new_c;x["price"]=float(k["c"]);
        if k.get("x"):x["prev_candle"]=new_c
    elif stream.endswith("@depth@100ms") and s in books:books[s].buffer_event(d);x["last_book_event"]=now

def stats(s,sec):
    cut=time.time()-sec;r=[z for z in state[s]["trades"] if z[0]>=cut]
    if not r:return 0,0,0,0
    n=sum(z[2] for z in r);b=sum(z[2] for z in r if z[3])
    return len(r),n,b/n if n else 0,(r[-1][1]/r[0][1]-1)*100 if len(r)>1 else 0


def acceleration_ratio(v10,v60):
    return v10/max(v60/6,1)

def early_momentum(s):
    """Lightweight PRE-FAST momentum layer. No Telegram and no final BUY decision."""
    if s not in books or not state[s].get("trades"): return None
    n10,v10,b10,p10=stats(s,10); n30,v30,b30,p30=stats(s,30); n60,v60,b60,p60=stats(s,60)
    if n10 < EARLY_MOMENTUM_MIN_TRADES: return None
    accel=acceleration_ratio(v10,v60); accel30=acceleration_ratio(v30,v60); accel_slope=accel-accel30
    cvd10=2.0*b10-1.0; cvd30=2.0*b30-1.0; cvd60=2.0*b60-1.0; cvd_impulse=cvd10-cvd30
    ob=books[s].metrics(20); spread=float(ob.get("spread_bps",0) or 0); ready=bool(ob.get("ready",False))
    buy_score=max(0,min((b10-0.50)/0.25,1))*30.0
    trade_score=max(0,min((accel-1.0)/1.5,1))*25.0
    cvd_score=max(0,min((cvd10+0.05)/0.55,1))*20.0
    impulse_score=max(0,min((cvd_impulse+0.01)/0.10,1))*10.0
    spread_score=(max(0,min((EARLY_MOMENTUM_MAX_SPREAD-spread)/EARLY_MOMENTUM_MAX_SPREAD,1))*10.0) if ready else 5.0
    activity_score=5.0 if n10>=6 and n60>=18 else 0.0
    score=round(max(0,min(buy_score+trade_score+cvd_score+impulse_score+spread_score+activity_score,100)))
    quality=(score>=EARLY_MOMENTUM_MIN_SCORE and b10>=EARLY_MOMENTUM_MIN_BUY and accel>=EARLY_MOMENTUM_MIN_ACCEL and (not ready or spread<=EARLY_MOMENTUM_MAX_SPREAD))
    stage="EARLY-MOMENTUM" if quality and score>=70 else "PRE-FAST WATCH" if quality else "MONITOR"
    return {"early_momentum_score":score,"early_momentum_stage":stage,"early_momentum_quality":quality,
            "early_momentum_trades_10s":n10,"early_momentum_trade_rate":accel,"early_momentum_accel_slope":accel_slope,
            "early_momentum_buy_ratio":b10,"early_momentum_cvd_10s":cvd10,"early_momentum_cvd_30s":cvd30,
            "early_momentum_cvd_60s":cvd60,"early_momentum_cvd_impulse":cvd_impulse,"early_momentum_spread_bps":spread,
            "early_momentum_book_ready":ready,"early_momentum_price_10s":p10,"early_momentum_price_60s":p60}

def sustained_pump_signal(r, cvd10, ex, btc_off):
    """Expansion lane for participation-led pumps that do not look explosive in 10s."""
    if not SUSTAINED_PUMP_ENABLED or btc_off:return None
    p1=float(r.get("price_1m",0) or 0); p5=float(r.get("price_5m",0) or 0)
    p10=float(r.get("price_10m",0) or 0); p15=float(r.get("price_15m",0) or 0)
    buy=float(r.get("buy_pressure",0) or 0); accel=float(r.get("trade_accel",0) or 0)
    vol=float(r.get("volume_ratio",0) or 0); rs5=float(r.get("v15_relative_strength_5m",0) or 0)
    part=float(r.get("adaptive_participation_z",0) or 0); tz=float(r.get("adaptive_trade_z",0) or 0); vz=float(r.get("adaptive_volume_z",0) or 0)
    cz=float(r.get("adaptive_cvd_z",0) or 0); sweep=float(r.get("v158_sweep_score",0) or 0); liq=float(r.get("v158_liquidity_score",0) or 0)
    pre=float(r.get("v158_pre_ignition_score",0) or 0)
    if p1<SUSTAINED_PUMP_MIN_1M or p5<SUSTAINED_PUMP_MIN_5M:return None
    if buy<SUSTAINED_PUMP_MIN_BUY or accel<SUSTAINED_PUMP_MIN_ACCEL or vol<SUSTAINED_PUMP_MIN_VOLUME:return None
    if cvd10<SUSTAINED_PUMP_MIN_CVD or p10<=0 or p15<-0.50 or ex>=SUSTAINED_PUMP_MAX_EXHAUSTION:return None
    anomaly=max(part,tz,vz)
    if anomaly<0.75 and not (tz>=1.0 and vz>=1.0):return None
    score=(min(max((p5-.50)/2,0),1)*22 + min(max((p10+.25)/3,0),1)*10 +
           min(max((p15+.25)/5,0),1)*5 + min(max((buy-.50)/.15,0),1)*15 +
           min(max((accel-1)/1,0),1)*12 + min(max((vol-1)/2,0),1)*10 +
           min(max((cvd10+.05)/.35,0),1)*10 + min(max((cz+.5)/2,0),1)*5 +
           min(max((anomaly-.75)/2,0),1)*10 +
           (3 if rs5>0 else 0) + (2 if sweep>=45 or liq>=45 else 0) + (2 if pre>=55 else 0) -
           max(0,ex-35)*.35)
    return {"score":max(0,min(round(score),100)),"price_1m":p1,"price_5m":p5,"price_10m":p10,"price_15m":p15,
            "buy":buy,"accel":accel,"volume":vol,"cvd":cvd10,"participation_z":part,"trade_z":tz,"volume_z":vz,"cvd_z":cz,"exhaustion":ex}
    
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
    # V15.1: use the 60s move as the primary short-term price direction.
    # The 10s move remains a microstructure input, not a hard direction gate.
    pressure=max(0,min((p60+.25)/2.5,1))*8
    compression=7 if abs(p60)<2 and vr<1.5 else 0
    structure=8 if p60>0 else 4 if p60>=-0.25 else 0
    book=max(0,min((imb+.20)/.60,1))*8
    activity=5 if stats(s,10)[0]>=4 and stats(s,60)[0]>=12 else 0
    relative=max(0,min((rs5+.25)/1.5,1))*7
    volatility=5 if abs(p60)>0.75 and vr>1.3 else 0
    resistance=5 if -0.5 <= p1 <= 1.5 else 1

    penalty=0
    if p1>3: penalty+=6
    if b10<.52: penalty+=5
    if accel<1.15: penalty+=4
    if vr<.8: penalty+=5
    if rs15 < -1: penalty+=4

    score=max(0,min(round(buy+vol+trade+pressure+compression+structure+book+activity+relative+volatility+resistance-penalty),100))
    quality=score>=50 and b10>=.55 and accel>=1.25 and p60<8 and rs5>-1
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

def track_v15_ignition_trajectory(s, ignition):
    """Maintain a five-minute rolling trajectory for V15.1 ignition."""
    if not ignition:return None
    now=time.time();window=state[s]["ignition_window"]
    window.append({
        "ts":now,"price":float(state[s].get("price") or 0),
        "score":float(ignition.get("v15_ignition_score",0) or 0),
        "stage":str(ignition.get("v15_ignition_stage","") or ""),
        "accel":float(ignition.get("v15_ignition_accel",0) or 0),
        "buy":float(ignition.get("buy_pressure",0) or 0),
        "rs5":float(ignition.get("v15_ignition_rs5",0) or 0),
        "rs15":float(ignition.get("v15_ignition_rs15",0) or 0),
    })
    cutoff=now-300.0
    while len(window)>1 and window[0]["ts"]<cutoff:window.popleft()
    samples=len(window);first=window[0];last=window[-1]
    elapsed=max(last["ts"]-first["ts"],1.0)
    score_delta=last["score"]-first["score"];accel_delta=last["accel"]-first["accel"]
    buy_delta=last["buy"]-first["buy"];rs5_delta=last["rs5"]-first["rs5"]
    price_change=((last["price"]/first["price"])-1.0)*100.0 if first["price"] else 0.0
    seq=list(window)
    rising=sum(1 for a,b in zip(seq,seq[1:]) if b["score"]>=a["score"]) / max(samples-1,1)
    watch_samples=sum(1 for z in window if z["stage"] in ("IGNITION_WATCH","PRE_PUMP_IGNITION","EARLY_IGNITION"))
    early_samples=sum(1 for z in window if z["stage"]=="EARLY_IGNITION")
    persistence=watch_samples/samples if samples else 0.0
    trend=min(max(score_delta/25.0,0),1)*35
    accel_component=min(max(accel_delta/.75,0),1)*20
    buy_component=min(max(buy_delta/.08,0),1)*15
    rs_component=min(max(rs5_delta/.75,0),1)*10
    persistence_component=min(max(persistence,0),1)*10
    rising_component=min(max((rising-.50)/.50,0),1)*10
    trajectory_score=max(0,min(round(trend+accel_component+buy_component+rs_component+persistence_component+rising_component),100))
    confirmed=(samples>=12 and elapsed>=55 and trajectory_score>=65 and last["score"]>=62 and
               score_delta>=8 and rising>=.60 and last["accel"]>=1.35 and last["buy"]>=.54 and
               last["rs5"]>=.05 and price_change<3.5)
    stage="EARLY_IGNITION_CONFIRMED" if confirmed else "BUILDING_5M" if samples>=6 and persistence>=.50 and last["score"]>=50 else "TRACKING_5M"
    return {
        "v15_ignition_samples_5m":samples,"v15_ignition_window_seconds":round(min(elapsed,300),1),
        "v15_ignition_score_delta_5m":round(score_delta,1),"v15_trade_accel_delta_5m":round(accel_delta,2),
        "v15_buy_pressure_delta_5m":round(buy_delta,4),"v15_ignition_rs5_delta_5m":round(rs5_delta,2),
        "v15_ignition_price_change_5m":round(price_change,2),"v15_ignition_rising_ratio_5m":round(rising,2),
        "v15_ignition_persistence_5m":round(persistence,2),"v15_ignition_early_samples_5m":early_samples,
        "v15_ignition_trajectory_score":trajectory_score,"v15_ignition_trajectory_stage":stage,
        "v15_ignition_trajectory_confirmed":confirmed,
        "v15_ignition_alert":bool(ignition.get("v15_ignition_alert",False) or confirmed),
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
    compression_bonus=5 if abs(p60)<2.0 else 0
    volume_bonus=min(max((vr-.50)/1.50,0),1)*5
    penalty=0
    # 10s is deliberately a soft reversal/exhaustion signal.
    if p10 < -0.75: penalty+=5
    if p10 < -1.25: penalty+=5
    if p60>=5: penalty+=8
    if rs5<-.25: penalty+=8
    if btc_risk: penalty+=25
    score=max(0,min(round(pressure_score+accel_score+slope_score+rs_score+buy_slope_score+activity_score+compression_bonus+volume_bonus-penalty),100))
    # Primary early-pump gates: 60s direction + participation, then independent confirmation.
    confirmations=sum([
        p60>=1.50,
        vr>=2.00,
        accel10>=1.50,
        b10>=.60,
        buy_slope>=.15,
        p5>0,
        rs5>0,
    ])
    signals=sum([
        p60>=1.50,
        vr>=2.00,
        accel10>=1.50,
        b10>=.60,
        accel_slope>=0.15,
        rs5>=0,
    ])
    if btc_risk or p60>=8 or p10 < -1.25:stage="AVOID"
    elif p60>=2.00 and vr>=2.50 and accel10>=1.75 and confirmations>=5 and score>=78:stage="EARLY_IGNITION"
    elif p60>=1.50 and vr>=2.00 and accel10>=1.50 and confirmations>=4 and score>=65:stage="PRE_PUMP_IGNITION"
    elif p60>=0.75 and score>=55 and signals>=3:stage="IGNITION_WATCH"
    else:stage="NORMAL"
    alert=stage in ("EARLY_IGNITION","PRE_PUMP_IGNITION") and score>=65 and not btc_risk
    return {
        "v15_ignition_score":score,"v15_ignition_stage":stage,"v15_ignition_alert":alert,
        "v15_ignition_signals":signals,"v15_ignition_confirmations":confirmations,"v15_trade_accel_slope":round(accel_slope,2),
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
    tv=tv_cache.get(s,{}) or {};tv_score=float(tv.get("tv_score",50) or 50);tv_adj=round((tv_score-50.0)*0.18);confirmation_score=max(0,min(round(conf*100)+tv_adj,100))
    # 60s direction is the main short-term gate; 10s only protects against a sharp reversal.
    if btc_risk or p60>=8 or p10 < -1.25 or rs15<-1.5:stage="AVOID"
    elif v4.get("v4_alert_quality")=="A" and confirmation_score>=70 and p60>=2.5 and vr>=3.0 and acc>=2.0:stage="CONFIRMED"
    elif eps.get("early_pump_stage")=="EARLY PUMP" and opportunity>=60:stage="EARLY_PUMP"
    elif eps.get("early_pump_stage")=="PRE-PUMP" and opportunity>=50:stage="PRE_PUMP"
    elif opportunity>=40:stage="WATCH"
    else:stage="NEUTRAL"
    # Recommended thresholds:
    # MONITOR 60s >= +0.75%; EARLY >= +1.5% + 2x volume + 1.5x accel;
    # STRONG EARLY >= +2% + 2.5x volume + 1.75x accel;
    # CONFIRMED >= +2.5% + 3x volume + 2x accel.
    early_candidate=(stage in ("PRE_PUMP","EARLY_PUMP") and opportunity>=50
                     and p60>=1.50 and vr>=2.00 and acc>=1.50
                     and sum([b10>=.60, p60>=1.50, rs5>0, rs15>0, structure])>=2
                     and not btc_risk)
    confirmed=(stage=="CONFIRMED" and confirmation_score>=60
               and p60>=2.50 and vr>=3.00 and acc>=2.00
               and b10>=.55 and rs5>0 and efficiency>=.25 and not btc_risk)
    return {"v15_opportunity_score":opportunity,"v15_confirmation_score":confirmation_score,"v15_score":max(opportunity,confirmation_score),"v15_tv_adjustment":tv_adj,"v15_tv_score":tv_score,"v15_stage":stage,"v15_early_candidate":early_candidate,"v15_confirmed":confirmed,"v15_streak":streak,"v15_btc_risk_off":btc_risk,"v15_relative_strength_5m":rs5,"v15_relative_strength_15m":rs15}

def buy_setup_quality(v15, tv, exhaustion, v12, volume_ratio, trade_accel, buy_pressure, btc_risk=False):
    """Composite setup-quality layer; does not modify the underlying V15 score."""
    v15=v15 or {}; tv=tv or {}; exhaustion=exhaustion or {}; v12=v12 or {}
    opportunity=float(v15.get("v15_opportunity_score",0) or 0)
    confirmation=float(v15.get("v15_confirmation_score",0) or 0)
    reignition=float(v15.get("v15_reignition_score",0) or 0)
    tv_score=float(tv.get("tv_score",0) or 0)
    bull_tf=float(tv.get("tv_bullish_timeframes",0) or 0)
    rsi_1d=float(tv.get("tv_1d_rsi",0) or 0)
    rsi_1w=float(tv.get("tv_1w_rsi",0) or 0)
    vr=float(volume_ratio or 0); accel=float(trade_accel or 0); buy=float(buy_pressure or 0)
    rs5=float(v15.get("v15_relative_strength_5m",0) or 0)
    rs15=float(v15.get("v15_relative_strength_15m",0) or 0)
    exhaustion_score=float(exhaustion.get("exhaustion_score",0) or 0)
    v12_confirmation=bool(v12.get("v12_confirmation",False))
    v12_eff=float(v12.get("v12_efficiency",0) or 0)

    htf=(min(max((tv_score-50)/35,0),1)*15 +
         min(max((bull_tf-2)/4,0),1)*5 +
         min(max((rsi_1d-50)/30,0),1)*2.5 +
         min(max((rsi_1w-50)/30,0),1)*2.5)
    momentum=opportunity*0.15 + confirmation*0.10
    participation=(min(max((vr-0.75)/2.25,0),1)*5 +
                   min(max((accel-0.75)/2.25,0),1)*5 +
                   min(max((buy-.48)/.22,0),1)*3 +
                   min(max((rs5+.50)/1.50,0),1)*2)
    confirmation_bonus=(5 if v12_confirmation else 0) + min(max(v12_eff/.60,0),1)*3
    quality=htf+momentum+reignition*0.15+participation+confirmation_bonus
    penalty=0.0; reasons=[]
    if btc_risk or bool(v15.get("v15_btc_risk_off",False)):
        penalty+=25; reasons.append("BTC RISK-OFF")
    if v15.get("v15_stage")=="AVOID":
        penalty+=35; reasons.append("V15 AVOID")
    if exhaustion_score>=72:
        penalty+=20; reasons.append("EXHAUSTION ALERT")
    elif exhaustion_score>=58:
        penalty+=10; reasons.append("EXHAUSTION WATCH")
    if rs15 < -1.0:
        penalty+=8; reasons.append("WEAK RS")
    quality=max(0,min(round(quality-penalty),100))
    if v15.get("v15_stage")=="AVOID" or btc_risk:
        label="NO SETUP"
    elif quality>=80:
        label="HIGH-QUALITY SETUP"
    elif quality>=65:
        label="SETUP DEVELOPING"
    elif quality>=50:
        label="WATCH / WAIT"
    elif quality>=35:
        label="WEAK SETUP"
    else:
        label="NO SETUP"
    if not reasons:
        if quality>=65: reasons.append("MULTI-FACTOR CONFLUENCE")
        elif reignition>=60 and tv_score>=70: reasons.append("RE-IGNITION WATCH")
        else: reasons.append("WAIT FOR MOMENTUM")
    return {"buy_setup_quality":quality,"buy_setup_quality_label":label,
            "buy_setup_quality_reasons":" | ".join(reasons[:3])}

def buy_setup_quality(v15, tv, exhaustion, v12, volume_ratio, trade_accel, buy_pressure, btc_risk=False):
    """Composite setup-quality layer; does not modify the underlying V15 score."""
    v15=v15 or {}; tv=tv or {}; exhaustion=exhaustion or {}; v12=v12 or {}
    opportunity=float(v15.get("v15_opportunity_score",0) or 0)
    confirmation=float(v15.get("v15_confirmation_score",0) or 0)
    reignition=float(v15.get("v15_reignition_score",0) or 0)
    tv_score=float(tv.get("tv_score",0) or 0)
    bull_tf=float(tv.get("tv_bullish_timeframes",0) or 0)
    rsi_1d=float(tv.get("tv_1d_rsi",0) or 0)
    rsi_1w=float(tv.get("tv_1w_rsi",0) or 0)
    vr=float(volume_ratio or 0); accel=float(trade_accel or 0); buy=float(buy_pressure or 0)
    rs5=float(v15.get("v15_relative_strength_5m",0) or 0)
    rs15=float(v15.get("v15_relative_strength_15m",0) or 0)
    exhaustion_score=float(exhaustion.get("exhaustion_score",0) or 0)
    v12_confirmation=bool(v12.get("v12_confirmation",False))
    v12_eff=float(v12.get("v12_efficiency",0) or 0)

    htf=(min(max((tv_score-50)/35,0),1)*15 +
         min(max((bull_tf-2)/4,0),1)*5 +
         min(max((rsi_1d-50)/30,0),1)*2.5 +
         min(max((rsi_1w-50)/30,0),1)*2.5)
    momentum=opportunity*0.15 + confirmation*0.10
    participation=(min(max((vr-0.75)/2.25,0),1)*5 +
                   min(max((accel-0.75)/2.25,0),1)*5 +
                   min(max((buy-.48)/.22,0),1)*3 +
                   min(max((rs5+.50)/1.50,0),1)*2)
    confirmation_bonus=(5 if v12_confirmation else 0) + min(max(v12_eff/.60,0),1)*3
    quality=htf+momentum+reignition*0.15+participation+confirmation_bonus
    penalty=0.0; reasons=[]
    if btc_risk or bool(v15.get("v15_btc_risk_off",False)):
        penalty+=25; reasons.append("BTC RISK-OFF")
    if v15.get("v15_stage")=="AVOID":
        penalty+=35; reasons.append("V15 AVOID")
    if exhaustion_score>=72:
        penalty+=20; reasons.append("EXHAUSTION ALERT")
    elif exhaustion_score>=58:
        penalty+=10; reasons.append("EXHAUSTION WATCH")
    if rs15 < -1.0:
        penalty+=8; reasons.append("WEAK RS")
    quality=max(0,min(round(quality-penalty),100))
    if v15.get("v15_stage")=="AVOID" or btc_risk:
        label="NO SETUP"
    elif quality>=80:
        label="HIGH-QUALITY SETUP"
    elif quality>=65:
        label="SETUP DEVELOPING"
    elif quality>=50:
        label="WATCH / WAIT"
    elif quality>=35:
        label="WEAK SETUP"
    else:
        label="NO SETUP"
    if not reasons:
        if quality>=65: reasons.append("MULTI-FACTOR CONFLUENCE")
        elif reignition>=60 and tv_score>=70: reasons.append("RE-IGNITION WATCH")
        else: reasons.append("WAIT FOR MOMENTUM")
    return {"buy_setup_quality":quality,"buy_setup_quality_label":label,
            "buy_setup_quality_reasons":" | ".join(reasons[:3])}


def buy_decision_layer(v15, tv, exhaustion, buy_quality, v12):
    """Separate BUY / WAIT / AVOID decision layer; does not alter V15 or quality."""
    v15=v15 or {}; tv=tv or {}; exhaustion=exhaustion or {}; v12=v12 or {}
    q=float((buy_quality or {}).get("buy_setup_quality",0) or 0)
    conf=float(v15.get("v15_confirmation_score",0) or 0)
    opp=float(v15.get("v15_opportunity_score",0) or 0)
    stage=str(v15.get("v15_stage","") or "")
    tv_score=float(tv.get("tv_score",0) or 0)
    bull_tf=int(tv.get("tv_bullish_timeframes",0) or 0)
    ex=float(exhaustion.get("exhaustion_score",0) or 0)
    btc_off=bool(v15.get("v15_btc_risk_off",False))
    v12_conf=bool(v12.get("v12_confirmation",False))
    reasons=[]

    if btc_off or stage=="AVOID" or ex>=72:
        decision="AVOID"
        if btc_off: reasons.append("BTC RISK-OFF")
        if stage=="AVOID": reasons.append("V15 AVOID")
        if ex>=72: reasons.append("EXHAUSTION ALERT")
    elif q>=80 and conf>=70 and opp>=60 and tv_score>=70 and bull_tf>=4 and ex<58:
        decision="BUY"
        reasons.append("HIGH-QUALITY CONFLUENCE")
    elif q>=70 and conf>=65 and tv_score>=68 and bull_tf>=3 and ex<58 and (v12_conf or opp>=70):
        decision="BUY"
        reasons.append("STRONG SETUP + CONFIRMATION")
    elif q>=65:
        decision="WAIT"
        reasons.append("SETUP DEVELOPING — WAIT FOR CONFIRMATION")
    elif q>=50:
        decision="WAIT"
        reasons.append("WATCH / WAIT")
    else:
        decision="AVOID"
        reasons.append("INSUFFICIENT SETUP QUALITY")

    if decision=="BUY":
        if stage not in ("PRE_PUMP","EARLY_PUMP","CONFIRMED"):
            decision="WAIT"; reasons.append("V15 STAGE NOT READY")
        if ex>=58:
            decision="WAIT"; reasons.append("EXHAUSTION WATCH")
    return {
        "buy_decision":decision,
        "buy_decision_reasons":" | ".join(reasons[:3]),
        "buy_decision_score":q
    }
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

def v158_early_momentum_recovery(r):
    """V15.8 Tier-1 recovery lane; advisory only and independent of Fastest-Pump."""
    if not V158_EARLY_MOMENTUM_RECOVERY_ENABLED or not isinstance(r,dict):
        return {"v158_early_momentum_recovery":False,"v158_early_momentum_recovery_tier":"OFF","v158_early_momentum_recovery_score":0.0}
    regime=float(r.get("adaptive_regime_change",0) or 0)
    intensity=float(r.get("adaptive_intensity_z",0) or 0)
    trade_size=float(r.get("adaptive_trade_size_z",0) or 0)
    qualifies=(regime>=V158_RECOVERY_MIN_REGIME and intensity>=V158_RECOVERY_MIN_INTENSITY and trade_size>=V158_RECOVERY_MIN_TRADE_SIZE)
    score=min(100.0,max(0.0,min(regime/100.0,1.0)*35.0+min(max(intensity,0.0)/4.0,1.0)*35.0+min(max(trade_size,0.0)/4.0,1.0)*30.0))
    return {"v158_early_momentum_recovery":qualifies,"v158_early_momentum_recovery_tier":"TIER_1_STRONG" if qualifies else "NONE","v158_early_momentum_recovery_score":round(score,1),"v158_early_momentum_recovery_regime":round(regime,2),"v158_early_momentum_recovery_intensity":round(intensity,2),"v158_early_momentum_recovery_trade_size":round(trade_size,2)}

def v158_participation_ignition(r):
    """V15.8 Tier-1B synchronized participation ignition; separate from Tier-1."""
    if not V158_PARTICIPATION_IGNITION_ENABLED or not isinstance(r,dict):
        return {"v158_participation_ignition":False,"v158_participation_tier":"OFF","v158_participation_score":0.0}
    tz=float(r.get("adaptive_trade_z",0) or 0); vz=float(r.get("adaptive_volume_z",0) or 0)
    buy=float(r.get("buy_pressure",0) or 0); accel=float(r.get("trade_accel",0) or 0)
    regime=float(r.get("adaptive_regime_change",0) or 0)
    participation=float(r.get("adaptive_participation_z",math.sqrt(max(tz,0.0)**2+max(vz,0.0)**2)) or 0)
    core=(tz>=V158_PARTICIPATION_MIN_TRADE_Z and vz>=V158_PARTICIPATION_MIN_VOLUME_Z and buy>=V158_PARTICIPATION_MIN_BUY and accel>=V158_PARTICIPATION_MIN_ACCEL)
    strong=(tz>=4.0 and vz>=4.0 and buy>=0.65 and accel>=2.0 and float(r.get("adaptive_cvd_z",0) or 0)>=1.0)
    score=min(100.0,max(0.0,min(max(tz,0.0)/6.0,1.0)*25.0+min(max(vz,0.0)/6.0,1.0)*25.0+min(max(buy-0.50,0.0)/0.30,1.0)*20.0+min(max(accel-1.0,0.0)/2.0,1.0)*20.0+min(max(regime,0.0)/100.0,1.0)*10.0))
    return {"v158_participation_ignition":core,"v158_participation_tier":"TIER_1B_STRONG" if strong else ("TIER_1B_CORE" if core else "NONE"),"v158_participation_score":round(score,1),"v158_participation_trade_z":round(tz,2),"v158_participation_volume_z":round(vz,2),"v158_participation_z":round(participation,2),"v158_participation_buy":round(buy,3),"v158_participation_accel":round(accel,2),"v158_participation_regime":round(regime,1)}

def v158_directional_ignition(r):
    """Participation ignition confirmed by directional acceleration; advisory only."""
    if not V158_DIRECTIONAL_ACCELERATION_ENABLED or not isinstance(r,dict):
        return {"v158_directional_ignition":False,"v158_directional_tier":"OFF","v158_directional_score":0.0}
    tz=float(r.get("adaptive_trade_z",0) or 0);vz=float(r.get("adaptive_volume_z",0) or 0)
    participation=(tz>=V158_PARTICIPATION_MIN_TRADE_Z and vz>=V158_PARTICIPATION_MIN_VOLUME_Z and float(r.get("buy_pressure",0) or 0)>=V158_PARTICIPATION_MIN_BUY and float(r.get("trade_accel",0) or 0)>=V158_PARTICIPATION_MIN_ACCEL)
    buy_slope=float(r.get("v158_directional_buy_slope",0) or 0)
    p10=float(r.get("price_10s",0) or 0);p60=float(r.get("price_60s",0) or 0)
    cvd_z=float(r.get("adaptive_cvd_z",0) or 0);impact_z=float(r.get("adaptive_price_impact_z",0) or 0)
    regime=float(r.get("adaptive_regime_change",0) or 0);buy=float(r.get("buy_pressure",0) or 0);accel=float(r.get("trade_accel",0) or 0)
    flow_ok=buy_slope>=V158_DIRECTIONAL_MIN_BUY_SLOPE
    price10_ok=p10>=V158_DIRECTIONAL_MIN_PRICE_10S
    price60_ok=p60>V158_DIRECTIONAL_MIN_PRICE_60S
    cvd_ok=cvd_z>=V158_DIRECTIONAL_MIN_CVD_Z
    impact_ok=impact_z<V158_DIRECTIONAL_MAX_PRICE_IMPACT_Z
    core=participation and flow_ok and price10_ok and price60_ok and cvd_ok and impact_ok
    confirmations=sum([flow_ok,price10_ok,price60_ok,cvd_ok,impact_ok])
    score=(min(max(buy_slope/0.10,0),1)*25+min(max(p10/0.50,0),1)*15+min(max(p60/1.50,0),1)*20+min(max((cvd_z-0.25)/2.0,0),1)*20+min(max((2.5-impact_z)/2.5,0),1)*10+min(max((regime-40)/60,0),1)*5+min(max((buy-0.55)/0.20,0),1)*2.5+min(max((accel-1.25)/2.0,0),1)*2.5)
    score=max(0,min(100,score))
    tier="TIER_1C_DIRECTIONAL_IGNITION" if core else ("DIRECTIONAL_BUILDING" if participation and confirmations>=3 else "NONE")
    return {"v158_directional_ignition":core,"v158_directional_tier":tier,"v158_directional_score":round(score,1),"v158_directional_buy_slope":round(buy_slope,4),"v158_directional_price_10s":round(p10,4),"v158_directional_price_60s":round(p60,4),"v158_directional_cvd_z":round(cvd_z,2),"v158_directional_price_impact_z":round(impact_z,2),"v158_directional_regime":round(regime,1),"v158_directional_confirmations":confirmations,"v158_directional_flow_ok":flow_ok,"v158_directional_price_ok":price10_ok and price60_ok,"v158_directional_cvd_ok":cvd_ok,"v158_directional_impact_ok":impact_ok}
def pump_momentum_score(s, v15, eps):
    """Dedicated short-term pump intensity score; independent of BUY SETUP QUALITY."""
    x=state[s]
    _,v10,b10,p10=stats(s,10)
    _,v60,_,p60=stats(s,60)
    _,v300,_,_=stats(s,300)
    vr=v60/max(v300/5,1)
    accel=acceleration_ratio(v10,v60)
    ob=books[s].metrics(20)
    imb=float(ob.get("imbalance",0) or 0)

    rs5=float((eps or {}).get("relative_strength_5m",0) or 0)
    rs15=float((eps or {}).get("relative_strength_15m",0) or 0)
    v15_score=float((v15 or {}).get("v15_score",0) or 0)
    v15_opp=float((v15 or {}).get("v15_opportunity_score",0) or 0)
    ign_score=float((v15 or {}).get("v15_ignition_score",0) or 0)
    accel_slope=float((v15 or {}).get("v15_trade_accel_slope",0) or 0)
    buy_slope=float((v15 or {}).get("v15_buy_pressure_slope",0) or 0)

    # Price intensity: reward sustained 60s movement while retaining the
    # faster 10s impulse as an early-pump component.
    price_10=max(0,min((p10+0.25)/2.75,1))
    price_60=max(0,min((p60+0.25)/5.0,1))
    price_component=(price_10*8 + price_60*12)

    volume_component=max(0,min((vr-0.75)/2.75,1))*15
    trade_component=max(0,min((accel-0.75)/2.75,1))*15
    buy_component=max(0,min((b10-.45)/.35,1))*10
    book_component=max(0,min((imb+.25)/.75,1))*10
    rs_component=(max(0,min((rs5+.50)/2.0,1))*6 +
                  max(0,min((rs15+.75)/3.0,1))*4)

    trajectory=(max(0,min(v15_score/100,1))*4 +
                max(0,min(v15_opp/100,1))*2 +
                max(0,min(ign_score/100,1))*2 +
                max(0,min((accel_slope+0.25)/1.25,1))*1 +
                max(0,min((buy_slope+0.01)/0.06,1))*1)

    score=max(0,min(round(price_component+volume_component+trade_component+
                         buy_component+book_component+rs_component+trajectory),100))

    if score>=90:
        label="EXTREME"
    elif score>=75:
        label="EXPLOSIVE"
    elif score>=60:
        label="STRONG"
    elif score>=40:
        label="BUILDING"
    else:
        label="NORMAL"

    reasons=[]
    if price_component>=15: reasons.append("PRICE ACCELERATION")
    if volume_component>=10: reasons.append("VOLUME EXPANSION")
    if trade_component>=10: reasons.append("TRADE ACCELERATION")
    if buy_component>=7: reasons.append("BUY PRESSURE")
    if book_component>=7: reasons.append("ORDER BOOK")
    if rs_component>=7: reasons.append("OUTPERFORMING BTC")
    if trajectory>=7: reasons.append("V15 TRAJECTORY")
    if not reasons: reasons.append("MOMENTUM BUILDING")

    return {
        "pump_momentum_score":score,
        "pump_momentum_label":label,
        "pump_momentum_reasons":" | ".join(reasons[:3]),
    }


def v15_reignition_bridge(s, v15, ac, eps, tv):
    """Bridge strong V15 structure back into short-term ignition without changing V15."""
    v15=v15 or {}; ac=ac or {}; eps=eps or {}; tv=tv or {}
    now=time.time(); x=state[s]
    v15_score=float(v15.get("v15_score",0) or 0)
    opportunity=float(v15.get("v15_opportunity_score",0) or 0)
    confirmation=float(v15.get("v15_confirmation_score",0) or 0)
    accumulation=float(ac.get("accumulation_score",0) or 0)
    tv_score=float(tv.get("tv_score",0) or 0)
    bull_tf=int(tv.get("tv_bullish_timeframes",0) or 0)
    btc_off=bool(v15.get("v15_btc_risk_off",False))
    _,v10,b10,p10=stats(s,10); _,v60,_,p60=stats(s,60); _,v300,_,_=stats(s,300)
    vr=v60/max(v300/5,1); accel=acceleration_ratio(v10,v60)
    _,v30,_,_=stats(s,30); accel_slope=accel-acceleration_ratio(v30,v60)
    ignition_score=float(v15.get("v15_ignition_score",0) or 0)

    structural=(v15_score>=75 and confirmation>=70 and opportunity>=70 and
                accumulation>=70 and tv_score>=65 and bull_tf>=4 and not btc_off)
    if structural:
        x["reignition_armed_until"]=now+300.0
        x["reignition_armed_score"]=max(float(x.get("reignition_armed_score",0) or 0),v15_score)

    armed=float(x.get("reignition_armed_until",0) or 0)>=now and not btc_off
    # Re-ignition must include genuine short-term price/buy-pressure confirmation.
    # Trade acceleration is supporting evidence only; it cannot trigger RE-IGNITION by itself.
    trigger=armed and (
        p60>=0.50 or
        (p60>=0.30 and accel>=1.35) or
        (p60>=0.20 and b10>=0.55) or
        (ignition_score>=45 and p60>0)
    )
    if btc_off: stage="OFF"
    elif trigger: stage="REIGNITION"
    elif armed: stage="HIGH-TF IGNITION WATCH"
    else: stage="NORMAL"

    score=(
        min(max((v15_score-60)/35,0),1)*30 +
        min(max((confirmation-60)/30,0),1)*20 +
        min(max((opportunity-60)/30,0),1)*15 +
        min(max((accumulation-60)/30,0),1)*10 +
        min(max((tv_score-60)/30,0),1)*10 +
        min(max((bull_tf-3)/3,0),1)*5 +
        min(max((p60+0.25)/1.25,0),1)*5 +
        min(max((accel-1.0)/1.0,0),1)*5
    )
    if btc_off: score=0
    return {
        "v15_reignition_bridge_score":max(0,min(round(score),100)),
        "v15_reignition_bridge_stage":stage,
        "v15_reignition_bridge_armed":armed,
        "v15_reignition_bridge_trigger":trigger,
        "v15_reignition_bridge_price_60s":round(p60,2),
        "v15_reignition_bridge_accel":round(accel,2),
        "v15_reignition_bridge_volume_ratio":round(vr,2),
        "v15_reignition_bridge_buy_pressure":round(b10,4),
        "v15_reignition_bridge_accel_slope":round(accel_slope,2),
    }


def score(s):
    x=state[s];c=x["candle"]
    if not x["price"] or not c or s not in books:return None
    _,raw_v10,b10,p10=stats(s,10);_,v60,b60,p60=stats(s,60);_,v180,_,p180=stats(s,180);_,v300,_,p300=stats(s,300);_,v600,_,p600=stats(s,600);_,v900,_,p900=stats(s,900);_,v1800,_,p1800=stats(s,1800);_,v3600,_,p3600=stats(s,3600)
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
    if ignition:
        ignition.update(track_v15_ignition_trajectory(s,ignition) or {})
    v15=v15_signal(s,v4,eps,v12)
    if not v15:v15={"v15_opportunity_score":0,"v15_confirmation_score":0,"v15_score":0,"v15_stage":"NEUTRAL","v15_early_candidate":False,"v15_confirmed":False,"v15_streak":0,"v15_btc_risk_off":False,"v15_relative_strength_5m":eps.get("relative_strength_5m",0) if eps else 0,"v15_relative_strength_15m":eps.get("relative_strength_15m",0) if eps else 0}
    v15.update(ignition or {})

    # Separate regime layer: keep V15 as a short-term pump score while
    # identifying strong higher-timeframe trends that have temporarily reset.
    tv=tv_cache.get(s,{}) or {}
    tv_score_now=float(tv.get("tv_score",0) or 0)
    tv_bull_tf=int(tv.get("tv_bullish_timeframes",0) or 0)
    tv_1d_rsi=float(tv.get("tv_1d_rsi",0) or 0)
    tv_1w_rsi=float(tv.get("tv_1w_rsi",0) or 0)
    v15_now=float(v15.get("v15_score",0) or 0)
    regime_points=0
    regime_points += min(max((tv_score_now-60)/20,0),1)*35
    regime_points += min(max((tv_bull_tf-3)/3,0),1)*20
    regime_points += min(max((tv_1d_rsi-55)/25,0),1)*20
    regime_points += min(max((tv_1w_rsi-55)/25,0),1)*15
    regime_points += 10 if v15_now<=30 else 5 if v15_now<=45 else 0
    reignition_score=max(0,min(round(regime_points),100))
    high_tf_bullish=(tv_score_now>=70 and tv_bull_tf>=4 and tv_1d_rsi>=60 and tv_1w_rsi>=60)
    reset_condition=high_tf_bullish and v15_now<=30 and not bool(v15.get("v15_btc_risk_off",False))
    if reset_condition:
        v15_regime="HIGH-TF BULLISH / SHORT-TERM RESET"
    elif high_tf_bullish and v15_now<=45:
        v15_regime="HIGH-TF BULLISH / MOMENTUM COOLING"
    elif high_tf_bullish:
        v15_regime="HIGH-TF BULLISH / ACTIVE"
    elif tv_score_now>=60 and tv_bull_tf>=3:
        v15_regime="MIXED / DEVELOPING"
    else:
        v15_regime="NO HIGH-TF CONFIRMATION"
    v15["v15_regime"]=v15_regime
    v15["v15_reignition_score"]=reignition_score
    v15["v15_reignition_watch"]=reset_condition
    reignition_bridge=v15_reignition_bridge(s,v15,ac,eps,tv)
    exhaustion=exhaustion_momentum(s,eps,v12,v15)
    pump_momentum=pump_momentum_score(s,v15,eps)
    base=max(v300/30,1);vr=v60/max(v300/5,1);acc=acceleration_ratio(raw_v10,v60)
    buy_quality=buy_setup_quality(v15,tv,exhaustion,v12,vr,acc,b10,bool(v15.get("v15_btc_risk_off",False)))
    buy_decision=buy_decision_layer(v15,tv,exhaustion,buy_quality,v12)
    hybrid=v11_v12_hybrid(v11,v12)
    hs=hybrid.get("hybrid_score",0) if hybrid else 0
    alert_tier="HIGH PRIORITY" if hs>=80 else "EARLY ACTION" if hs>=70 else "PRE-PUMP WATCH" if hs>=62 else "BELOW WATCH"
    p1=(x["price"]/c["open"]-1)*100 if c["open"] else 0
    ob=books[s].metrics(20);imb=ob["imbalance"]
    v158_trades=trade_features(state,s,60)
    v158_liq=liquidity_features(state,books,s)
    adaptive=adaptive_micro_features(state,books,s)
    _,v30_dir,b30_dir,_=stats(s,30)
    directional_buy_slope=b10-(b30_dir if v30_dir else 0.50)
    adaptive["directional_buy_slope"]=directional_buy_slope
    adaptive_book=adaptive_book_features(state,books,s)
    queue_transition=queue_transition_imbalance_features(state,books,s)
    sweep=depth_sweep_features(state,books,s)
    replenishment=replenishment_absorption_features(state,books,s)
    pre_ignition=v158_pre_ignition_build_features(state,s,adaptive,queue_transition)
    liquidity_state=v158_liquidity_state_features(state,s,queue_transition,adaptive_book,v158_liq,sweep,replenishment)
    temporal_reignition=v158_temporal_reignition_memory(state,s,v15.get("v15_score",0),ac.get("accumulation_score",0),adaptive,p10,p60,vr,acc,b10,bool(v15.get("v15_btc_risk_off",False)))
    v158_quality=data_quality(state,books,s)
    v158_adaptive_score=min(100.0,max(0.0,35.0*min(max(adaptive.get("adaptive_trade_z",0),0)/3.0,1)+25.0*min(max(adaptive.get("adaptive_volume_z",0),0)/3.0,1)+20.0*min(max(adaptive.get("adaptive_cvd_z",0),0)/3.0,1)+10.0*min(max(adaptive.get("adaptive_intensity_z",0),0)/3.0,1)+10.0*min(max(adaptive.get("adaptive_regime_change",0)/100.0,0),1)))
    v158_score=min(100.0,float(v158_trades.get("whale_score",0))*0.20+float(v158_liq.get("liquidity_breakout_score",0))*0.16+float(v158_liq.get("absorption_score",0))*0.09+v158_adaptive_score*0.12+float(queue_transition.get("v158_queue_transition_score",0))*0.06+float(adaptive_book.get("v158_book_vacuum_score",0))*0.05+float(sweep.get("v158_sweep_score",0))*0.09+float(replenishment.get("v158_absorption_persistence_score",0))*0.09+float(v158_quality.get("data_quality_score",0))*0.07+float(pre_ignition.get("v158_pre_ignition_score",0))*0.03+float(liquidity_state.get("v158_liquidity_state_score",0))*0.03)
    xv=v158_xvenue_cache.get(s,{})
    xr=float(xv.get("ret_pct",0) or 0)
    xconf=100.0 if xv and ((p10>=0 and xr>=0) or (p10<0 and xr<0)) and abs(xr)>=0.02 else 0.0 if xv and ((p10>0.05 and xr<-0.02) or (p10<-0.05 and xr>0.02)) else 50.0
    v156, v156_event = v156_evaluate({"symbol":s,"price":x["price"],"price_10s":p10,"price_60s":p60,"volume_ratio":vr,"trade_accel":acc,"buy_pressure":b10,"relative_strength_5m":eps.get("relative_strength_5m",0),"v15_score":v15.get("v15_score",0),"v15_opportunity_score":v15.get("v15_opportunity_score",0),"v15_confirmation_score":v15.get("v15_confirmation_score",0),"accumulation_score":ac.get("accumulation_score",0),"tv_bullish_timeframes":tv_bull_tf,"v15_reignition_bridge_score":reignition_bridge.get("v15_reignition_bridge_score",0),"v15_reignition_bridge_trigger":reignition_bridge.get("v15_reignition_bridge_trigger",False),"exhaustion_score":(exhaustion or {}).get("exhaustion_score",0),"spread_bps":ob["spread_bps"],"v15_btc_risk_off":v15.get("v15_btc_risk_off",False),"v158_pre_ignition_score":pre_ignition.get("v158_pre_ignition_score",0),"v158_liquidity_state_score":liquidity_state.get("v158_liquidity_state_score",0),"v158_participation_score":v158_participation_ignition({**adaptive,"buy_pressure":b10,"trade_accel":acc}).get("v158_participation_score",0),"v158_directional_score":v158_directional_ignition({**adaptive,"buy_pressure":b10,"trade_accel":acc,"price_10s":p10,"price_60s":p60}).get("v158_directional_score",0),"v158_temporal_score":temporal_reignition.get("v158_temporal_score",0),"v158_temporal_reignition":temporal_reignition.get("v158_temporal_reignition",False),"v158_cross_venue_confidence":xconf},state[s].setdefault("v156",{}),time.time())
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
    return {"hybrid_score":hs,"alert_tier":alert_tier,"hybrid_path":hybrid.get("hybrid_path","") if hybrid else "","hybrid_grade":hybrid.get("hybrid_grade","") if hybrid else "","hybrid_alert":hybrid.get("hybrid_alert",False) if hybrid else False,"hybrid_a_plus":hybrid.get("hybrid_a_plus",False) if hybrid else False,"hybrid_confirmation":hybrid.get("hybrid_confirmation",False) if hybrid else False,"hybrid_efficiency":hybrid.get("hybrid_efficiency",0) if hybrid else 0,"symbol":s,**pump_momentum,"price":x["price"],"score":sc,"stage":stage,"price_3m":p180,"price_5m":p300,"price_10m":p600,"price_15m":p900,"price_30m":p1800,"price_60m_change":p3600,"entry":entry,"sell":sell,"price_1m":p1,"price_60s":p60,"price_10s":p10,"volume_ratio":vr,"trade_accel":acc,"buy_pressure":b10,"book_imbalance":imb,"spread_bps":ob["spread_bps"],"book_ready":ob["ready"],"book_gaps":books[s].gaps,
            "v158_score":round(v158_score,1),"v158_adaptive_score":round(v158_adaptive_score,1),**v158_early_momentum_recovery(adaptive),**v158_participation_ignition({**adaptive,"buy_pressure":b10,"trade_accel":acc}),**v158_directional_ignition({**adaptive,"buy_pressure":b10,"trade_accel":acc,"price_10s":p10,"price_60s":p60}),**adaptive,**adaptive_book,**queue_transition,**pre_ignition,**liquidity_state,**temporal_reignition,**sweep,**replenishment,"v158_whale_score":v158_trades.get("whale_score",0),
            "v158_large_trade_count":v158_trades.get("large_trade_count",0),"v158_large_buy_notional":v158_trades.get("large_buy_notional",0),
            "v158_large_sell_notional":v158_trades.get("large_sell_notional",0),"v158_large_trade_imbalance":v158_trades.get("large_trade_imbalance",0),
            "v158_median_trade_notional":v158_trades.get("median_trade_notional",0),"v158_p95_trade_notional":v158_trades.get("p95_trade_notional",0),
            "v158_liquidity_score":v158_liq.get("liquidity_score",0),"v158_ask_depth_change":v158_liq.get("ask_depth_change",0),
            "v158_bid_depth_change":v158_liq.get("bid_depth_change",0),"v158_ask_consumption":v158_liq.get("ask_consumption",0),
            "v158_bid_consumption":v158_liq.get("bid_consumption",0),"v158_absorption_score":v158_liq.get("absorption_score",0),
            "v158_liquidity_breakout_score":v158_liq.get("liquidity_breakout_score",0),"v158_data_quality":v158_quality.get("data_quality_score",0),
            "v158_trade_age_s":v158_quality.get("trade_age_s",999),"v158_book_age_s":v158_quality.get("book_age_s",999),"early_pump_score":eps["early_pump_score"],"early_pump_stage":eps["early_pump_stage"],"early_pump_quality":eps["early_pump_quality"],"relative_strength_5m":eps.get("relative_strength_5m"),"relative_strength_15m":eps.get("relative_strength_15m"),"btc_ret_5m":eps.get("btc_ret_5m"),"btc_ret_15m":eps.get("btc_ret_15m"),"false_positive_penalty":eps.get("false_positive_penalty",0),"accumulation_score":ac["accumulation_score"],"accumulation_stage":ac["accumulation_stage"],"accumulation_quality":ac["accumulation_quality"],"accum_buy_pressure":ac["accum_buy_pressure"],"accum_trade_accel":ac["accum_trade_accel"],"accum_volume_ratio":ac["accum_volume_ratio"],"accum_book_imbalance":ac["accum_book_imbalance"],"accum_price_10s":ac["accum_price_10s"],"accum_trades_10s":ac["accum_trades_10s"],**v4,**v5,**v6,**v7,**v8,**v9,**v10,**v11,**v12,"v15_model":"v15_1_early_ignition",**v156,"v15_alert":bool(v15 and (v15.get("v15_confirmed") or (v15.get("v15_early_candidate") and v15.get("v15_opportunity_score",0)>=55))),"v15_opportunity_score":v15.get("v15_opportunity_score",0) if v15 else 0,"v15_confirmation_score":v15.get("v15_confirmation_score",0) if v15 else 0,"v15_score":v15.get("v15_score",0) if v15 else 0,"v15_stage":v15.get("v15_stage","") if v15 else "","v15_early_candidate":v15.get("v15_early_candidate",False) if v15 else False,"v15_confirmed":v15.get("v15_confirmed",False) if v15 else False,"v15_streak":v15.get("v15_streak",0) if v15 else 0,"v15_btc_risk_off":v15.get("v15_btc_risk_off",False) if v15 else False,"v15_relative_strength_5m":v15.get("v15_relative_strength_5m",0) if v15 else 0,"v15_relative_strength_15m":v15.get("v15_relative_strength_15m",0) if v15 else 0,"v15_regime":v15.get("v15_regime","NO HIGH-TF CONFIRMATION") if v15 else "NO HIGH-TF CONFIRMATION","v15_reignition_score":v15.get("v15_reignition_score",0) if v15 else 0,"v15_reignition_watch":v15.get("v15_reignition_watch",False) if v15 else False,**reignition_bridge,**buy_quality,**buy_decision,**(exhaustion or {}),**(tv_cache.get(s,{}) or {}),"updated":time.time()}


def v158_fast_pump_phase(r, n10=0, cvd10=0.0):
    """Learning-only pump phase classifier. Never changes Fastest-Pump gates."""
    p10=float(r.get("price_10s",0) or 0); p1=float(r.get("price_1m",0) or 0); p5=float(r.get("price_5m",0) or 0)
    buy=float(r.get("buy_pressure",0) or 0); accel=float(r.get("trade_accel",0) or 0)
    ex=float(r.get("exhaustion_score",0) or 0)
    part=float(r.get("adaptive_participation_z",0) or 0); tz=float(r.get("adaptive_trade_z",0) or 0); vz=float(r.get("adaptive_volume_z",0) or 0)
    if ex>=60 or (p10<0 and buy<0.50 and cvd10<0): return "EXHAUSTION"
    if p10>=0.10 and cvd10>0 and buy>=0.55 and (tz>=1.0 or vz>=1.0):
        return "FAST_PUMP" if accel>=1.60 and p1>=0.15 and p5>=0.60 else "ACCELERATION"
    if p1>=0.10 and p5>=0.40 and (part>=0.75 or tz>=0.75 or vz>=0.75) and cvd10>=0: return "SUSTAINED_EXPANSION"
    if (p10>=0.03 or p1>=0.05 or buy>=0.53) and (n10>=3 or tz>=0.5): return "IGNITION"
    return "WATCH"

def v158_price_lead_signature(r, cvd10=0.0, buy_slope=0.0, vol10_rate=0.0):
    """Learning-only price -> participation -> flow sequence detector."""
    p10=float(r.get("price_10s",0) or 0); p1=float(r.get("price_1m",0) or 0)
    buy=float(r.get("buy_pressure",0) or 0); accel=float(r.get("trade_accel",0) or 0)
    trade_z=float(r.get("adaptive_trade_z",0) or 0); vol_z=float(r.get("adaptive_volume_z",0) or 0)
    price_lead=bool(p10>=0.05 or p1>=0.08)
    participation_confirm=bool(accel>=1.05 or trade_z>=0.50 or vol10_rate>=1.05 or vol_z>=0.50)
    flow_confirm=bool(cvd10>=0.03 and buy>=0.52 and buy_slope>=0.0)
    sequence=int(price_lead)+int(participation_confirm)+int(flow_confirm)
    stage="CONFIRMED_SEQUENCE" if sequence>=3 else "LEADING" if sequence==2 else "EARLY" if sequence==1 else "NONE"
    return {"v158_price_lead_score":sequence*33.3,"v158_price_lead_stage":stage,"v158_price_lead":sequence>=2}

def v158_fast_gate_reasons(r, n10=0, cvd10=0.0, buy_slope=0.0, accel=0.0):
    """Research-only explanation of Fastest-Pump gate failures."""
    reasons=[]
    p1=float(r.get("price_1m",0) or 0); p5=float(r.get("price_5m",0) or 0)
    p10s=float(r.get("price_10s",0) or 0); p60s=float(r.get("price_60s",0) or 0); buy=float(r.get("buy_pressure",0) or 0)
    if n10<3: reasons.append("LOW_TRADES_10S")
    if buy<0.57: reasons.append("LOW_BUY_PRESSURE")
    if accel<1.25: reasons.append("LOW_TRADE_ACCEL")
    if cvd10<0.08 and buy_slope<0.025: reasons.append("WEAK_CVD_OR_BUY_SLOPE")
    if p1<0.05: reasons.append("LOW_1M_PRICE")
    if p5<0.30: reasons.append("LOW_5M_PRICE")
    if p10s<-0.75: reasons.append("NEGATIVE_10S_PRICE")
    if p60s<-1.0: reasons.append("NEGATIVE_60S_PRICE")
    return reasons

def load_fast_pump_leaderboard():
    try:
        with open(FAST_PUMP_LEADERBOARD_FILE) as f:
            d=json.load(f)
        if not isinstance(d,dict): return {"updated":0,"symbols":{},"top":[]}
        d.setdefault("symbols",{});d.setdefault("top",[])
        return d
    except Exception:
        return {"updated":0,"symbols":{},"top":[]}

def _fast_episode_score(samples):
    if not samples:return 0.0
    vals=[float(x.get("score",0) or 0) for x in samples]
    recent=vals[-6:]
    peak=max(vals)
    recent_avg=sum(recent)/len(recent)
    overall=sum(vals)/len(vals)
    persistence=min(len(vals)/12.0,1.0)*100.0
    return max(0.0,min(100.0,
        recent_avg*0.35 + peak*0.25 + overall*0.15 + persistence*0.10 +
        float(samples[-1].get("score",0) or 0)*0.15))

def update_fast_pump_leaderboard(candidates, now):
    """
    Rank live qualifying episodes and alert only fresh episodes among the
    exact current Top-N leaders for this scan. Previous leaders do not retain
    eligibility when they fall outside the current ranking.
    """
    lb=state["__V156_FAST_PUMP__"].setdefault("leaderboard",load_fast_pump_leaderboard())
    symbols_lb=lb.setdefault("symbols",{})
    cutoff=now-FAST_PUMP_ROLLING_WINDOW

    for s,d in list(symbols_lb.items()):
        samples=[x for x in d.get("samples",[]) if float(x.get("ts",0) or 0)>=cutoff]
        if samples:
            d["samples"]=samples
        else:
            symbols_lb.pop(s,None)

    for r in candidates:
        s=str(r.get("symbol","") or "")
        if not s: continue
        d=symbols_lb.setdefault(s,{"samples":[]})
        d["samples"]=[x for x in d.get("samples",[]) if float(x.get("ts",0) or 0)>=cutoff]
        d["samples"].append({
            "ts":now,
            "score":float(r.get("_fast_pump_score",0) or 0),
            "buy_slope":float(r.get("_fast_buy_slope",0) or 0),
            "accel_slope":float(r.get("_fast_accel_slope",0) or 0),
            "volume_accel":float(r.get("_fast_volume_accel",0) or 0),
            "cvd":float(r.get("_fast_cvd",0) or 0),
            "cvd_impulse":float(r.get("_fast_cvd_impulse",0) or 0),
            "spread_bps":float(r.get("_fast_spread_bps",0) or 0),
            "exhaustion":float(r.get("_fast_dynamic_exhaustion",0) or 0),
            "price_1m":float(r.get("price_1m",0) or 0),
            "price_5m":float(r.get("price_5m",0) or 0)
        })
        d["samples"]=d["samples"][-180:]
        d["episode_score"]=_fast_episode_score(d["samples"])
        d["last_seen"]=now

    ranked=[]
    episode_alerts=[]
    for s,d in symbols_lb.items():
        samples=d.get("samples",[])
        if not samples: continue
        last=float(samples[-1].get("ts",0) or 0)
        if now-last>max(15.0,INTERVAL*3): continue
        if len(samples)<FAST_PUMP_MIN_OBSERVATIONS: continue

        # Split the rolling history into the current qualifying episode.
        # A gap means the previous pump episode has ended.
        episode=list(reversed(samples))
        current=[episode[0]]
        for sample in episode[1:]:
            if float(current[-1].get("ts",0) or 0)-float(sample.get("ts",0) or 0)>FAST_PUMP_EPISODE_GAP_SECONDS:
                break
            current.append(sample)
        current=list(reversed(current))
        if len(current)<FAST_PUMP_MIN_OBSERVATIONS: continue

        first_ts=float(current[0].get("ts",last) or last)
        span=last-first_ts
        if span<FAST_PUMP_MIN_SPAN_SECONDS: continue

        episode_score=_fast_episode_score(current)
        ranked.append({"symbol":s,"episode_score":float(episode_score),"samples":len(current)})

        episode_id=str(int(first_ts))
        last_alert_episode=str(d.get("last_alert_episode","") or "")
        last_alert_ts=float(d.get("last_alert_ts",0) or 0)

        # Alert once per fresh episode. The episode is only considered fresh
        # after the normal persistence requirements above have been satisfied.
        if episode_id!=last_alert_episode and now-last_alert_ts>=FAST_PUMP_EPISODE_ALERT_COOLDOWN:
            episode_alerts.append({
                "symbol":s,
                "episode_score":float(episode_score),
                "samples":len(current),
                "_episode_id":episode_id
            })

    ranked.sort(key=lambda x:(x["episode_score"],x["samples"]),reverse=True)
    top=ranked[:FAST_PUMP_TOP_N]

    # Telegram eligibility is the exact current Top-N by episode score for
    # this scan. Previous leaders do not retain eligibility if they fall
    # outside the current ranking.
    selected=list(top[:FAST_PUMP_TOP_N])
    selected_symbols={str(x.get("symbol","")) for x in selected}
    lb["top"]=selected
    lb["updated"]=now

    # Only fresh episodes belonging to the exact current Top-N can reach Telegram.
    episode_alerts.sort(key=lambda x:(x["episode_score"],x["samples"]),reverse=True)
    alerts=[]
    for candidate in episode_alerts:
        s=candidate["symbol"]
        if s not in selected_symbols:
            continue
        d=symbols_lb.get(s,{})
        d["last_alert_episode"]=candidate["_episode_id"]
        d["last_alert_ts"]=now
        alerts.append({k:v for k,v in candidate.items() if k!="_episode_id"})
        if len(alerts)>=FAST_PUMP_TOP_N:
            break

    try:
        os.makedirs(os.path.dirname(FAST_PUMP_LEADERBOARD_FILE) or ".",exist_ok=True)
        with open(FAST_PUMP_LEADERBOARD_FILE,"w") as f:
            json.dump(lb,f,separators=(",",":"))
    except Exception:
        pass
    return selected,alerts

async def telegram(msg):
    token=os.getenv("TELEGRAM_BOT_TOKEN");chat=os.getenv("TELEGRAM_CHAT_ID")
    msg=str(msg)
    # Production Telegram policy:
    # - V15.6 CONFIRMED-IGNITION / BUY Telegram alerts are disabled.
    # - FASTEST-PUMP remains enabled as its own independent alert lane.
    # - Confirmed deterioration alerts remain enabled.
    # - Legacy V15/early alerts remain disabled.
    allowed_confirmed=False
    allowed_fastest=msg.startswith("🚀 V15.6 FASTEST-PUMP")
    allowed_deterioration=msg.startswith("🔴 V15.6 CONFIRMED DETERIORATION")
    if not (allowed_confirmed or allowed_fastest or allowed_deterioration): return
    if not token or not chat:return
    # Normalize all scanner alerts to one Telegram line.
    # This prevents literal \\n / \\ artifacts from reaching Telegram.
    msg=str(msg).replace("\\\\n"," | ").replace("\\r"," | ").replace("\\n"," | ").replace("\\\\","")
    msg=msg.replace("\r"," | ").replace("\n"," | ").replace("\\","")
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(f"https://api.telegram.org/bot{token}/sendMessage",json={"chat_id":chat,"text":msg},timeout=8)
    except Exception:pass


async def refresh_v158_cross_exchange(http, market_symbols):
    """Optional low-cost external confirmation using OKX public spot tickers.
    It is advisory only and never blocks a Binance signal."""
    global v158_xvenue_cache, v158_xvenue_last
    now=time.time()
    if not V158_XVENUE_ENABLED or now-v158_xvenue_last<V158_XVENUE_INTERVAL:return
    try:
        async with http.get("https://www.okx.com/api/v5/market/tickers",params={"instType":"SPOT"},timeout=8) as resp:
            if resp.status!=200:return
            payload=await resp.json()
        data=payload.get("data",[]) if isinstance(payload,dict) else []
        wanted={str(s).upper() for s in market_symbols}
        previous=dict(v158_xvenue_cache)
        current={}
        for item in data:
            inst=str(item.get("instId","")).upper()
            if not inst.endswith("-USDT"):continue
            symbol=inst.replace("-","")
            if symbol not in wanted:continue
            px=float(item.get("last",0) or 0)
            if px<=0:continue
            old=previous.get(symbol,{})
            old_px=float(old.get("price",0) or 0)
            ret=((px/old_px)-1)*100 if old_px else 0.0
            current[symbol]={"price":px,"ret_pct":ret,"ts":now}
        v158_xvenue_cache=current;v158_xvenue_last=now
    except Exception:pass

async def v158_dynamic_stream(http,symbol):
    global symbols
    streams="/".join([f"{symbol.lower()}@aggTrade",f"{symbol.lower()}@bookTicker",f"{symbol.lower()}@depth@100ms",f"{symbol.lower()}@kline_1m"])
    url=WS+"?streams="+streams
    while time.time()<float(v158_dynamic_until.get(symbol,0) or 0):
        try:
            async with http.ws_connect(url,heartbeat=20,autoping=True,max_msg_size=8*1024*1024) as ws:
                while time.time()<float(v158_dynamic_until.get(symbol,0) or 0):
                    try:m=await asyncio.wait_for(ws.receive(),timeout=1)
                    except asyncio.TimeoutError:continue
                    if m.type==aiohttp.WSMsgType.TEXT:
                        z=json.loads(m.data);event(z.get("stream",""),z.get("data",{}))
                    elif m.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):break
        except asyncio.CancelledError:raise
        except Exception:await asyncio.sleep(1)
    v158_dynamic_tasks.pop(symbol,None);v158_dynamic_until.pop(symbol,None)
    if symbol not in v158_core_symbols:
        try:symbols.remove(symbol)
        except ValueError:pass
        books.pop(symbol,None)

async def v158_promote(http,meta):
    global symbols
    symbol=str(meta.get("symbol","") or "").upper()
    if not symbol or symbol in v158_core_symbols or symbol in v158_promoting:return
    v158_promoting.add(symbol)
    until=float(meta.get("ts",time.time()))+V158_PROMOTION_TTL
    v158_dynamic_until[symbol]=max(until,float(v158_dynamic_until.get(symbol,0) or 0))
    if symbol in v158_dynamic_tasks:return
    active=[s for s,t in v158_dynamic_until.items() if t>time.time() and s not in v158_core_symbols]
    if len(active)>=V158_MAX_DYNAMIC:
        weakest=min(active,key=lambda s:v158_discovery.items.get(s,{}).get("score",0))
        if weakest!=symbol and not bool(meta.get("emergency",False)) and float(meta.get("promotion_score",0) or 0)<=float(v158_discovery.items.get(weakest,{}).get("score",0) or 0)+5:return
        old=v158_dynamic_tasks.get(weakest)
        if old:old.cancel()
        v158_dynamic_tasks.pop(weakest,None);v158_dynamic_until.pop(weakest,None)
        if weakest in symbols:
            try:symbols.remove(weakest)
            except ValueError:pass
        books.pop(weakest,None)
    books[symbol]=LocalOrderBook(symbol,REST,LIMIT)
    if symbol not in symbols:symbols.append(symbol)
    try:
        await books[symbol].resync(http)
        v158_dynamic_tasks[symbol]=asyncio.create_task(v158_dynamic_stream(http,symbol))
    finally:
        v158_promoting.discard(symbol)

async def v158_discovery_loop(http):
    if not V158_ENABLED:return
    url=WS+"?streams=!ticker@arr"
    while True:
        try:
            async with http.ws_connect(url,heartbeat=20,autoping=True,max_msg_size=8*1024*1024) as ws:
                while True:
                    m=await asyncio.wait_for(ws.receive(),timeout=5)
                    if m.type!=aiohttp.WSMsgType.TEXT:
                        if m.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):break
                        continue
                    z=json.loads(m.data);payload=z.get("data",z);items=payload if isinstance(payload,list) else [payload]
                    for t in items:
                        if not isinstance(t,dict) or not str(t.get("s","")).upper().endswith("USDT"):continue
                        meta=v158_discovery.update(t)
                        if meta:
                            asyncio.create_task(v158_promote(http,meta))
                            try:
                                with open("data/v158_discovery.jsonl","a",encoding="utf-8") as df:
                                    df.write(json.dumps({"ts":time.time(),"event":"EMERGENCY_PROMOTION" if meta.get("emergency") else "PROMOTION","symbol":meta["symbol"],"score":meta.get("promotion_score",0),"discovery_score":meta.get("score",0),"cross_section_rank":meta.get("cross_section_rank",0),"cross_section_percentile":meta.get("cross_section_percentile",0),"cross_section_route_score":meta.get("cross_section_route_score",0),"velocity_pct_s":meta.get("velocity_pct_s",0),"trade_anomaly":meta.get("trade_anomaly",0),"quote_volume":meta.get("quote_volume",0),"emergency_route":bool(meta.get("emergency",False))},separators=(",",":"))+"\n")
                            except Exception:pass
        except asyncio.CancelledError:raise
        except Exception:await asyncio.sleep(1)

async def resync_books(http):return await asyncio.gather(*(b.resync(http) for b in books.values()),return_exceptions=True)

async def resync_unready_books(http):
    bad=[b for b in books.values() if not b.ready]
    if not bad:return None
    return await asyncio.gather(*(b.resync(http) for b in bad),return_exceptions=True)

async def main():
    global symbols,books,tv_cache,tv_last_refresh,v158_core_symbols
    os.makedirs("data",exist_ok=True);start=time.time();timeout=aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as http:
        symbols=await discover(http);v158_core_symbols=set(symbols);books={s:LocalOrderBook(s,REST,LIMIT) for s in symbols};state["__V156_FAST_PUMP__"]["leaderboard"]=load_fast_pump_leaderboard();streams=[];history_last_write=0.0;micro_history_last_write=0.0
        for s in symbols:
            q=s.lower();streams += [f"{q}@aggTrade",f"{q}@bookTicker",f"{q}@depth@100ms",f"{q}@kline_1m"]
        url=WS+"?streams="+"/".join(streams)
        async with http.ws_connect(url,heartbeat=20,autoping=True,max_msg_size=16*1024*1024) as ws:
            sync=asyncio.create_task(resync_books(http))
            discovery_task=asyncio.create_task(v158_discovery_loop(http)) if V158_ENABLED else None
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
                        if V158_XVENUE_ENABLED and (time.time()-v158_xvenue_last)>=V158_XVENUE_INTERVAL:
                            await refresh_v158_cross_exchange(http,symbols)
                        if TRADINGVIEW_ENABLED and (time.time()-tv_last_refresh)>=TRADINGVIEW_REFRESH_SECONDS:
                            try:
                                tv_cache,tv_last_refresh=await fetch_tradingview_signals(http,symbols)
                            except Exception:
                                tv_cache={};tv_last_refresh=time.time()
                        rows=[r for s in list(symbols) if (r:=score(s))]
                        for rr in rows:
                            md=v158_discovery.items.get(str(rr.get("symbol","")).upper(),{})
                            rr["v158_discovery_score"]=float(md.get("score",0) or 0)
                            rr["v158_discovery_velocity_pct_s"]=float(md.get("velocity_pct_s",0) or 0)
                            rr["v158_discovery_trade_anomaly"]=float(md.get("trade_anomaly",0) or 0)
                            rr["v158_dynamic_promoted"]=str(rr.get("symbol","")).upper() in v158_discovery.active()
                            xv=v158_xvenue_cache.get(str(rr.get("symbol","")).upper(),{})
                            rr["v158_xvenue_price"]=float(xv.get("price",0) or 0)
                            rr["v158_xvenue_ret_pct"]=float(xv.get("ret_pct",0) or 0)
                            bn=float(rr.get("price_10s",0) or 0)
                            xr=float(xv.get("ret_pct",0) or 0)
                            same_direction=bool(xv and ((bn>=0 and xr>=0) or (bn<0 and xr<0)) and abs(xr)>=0.02)
                            divergent=bool(xv and ((bn>0.05 and xr<-0.02) or (bn<-0.05 and xr>0.02)))
                            xconf=100.0 if same_direction else 0.0 if divergent else 50.0
                            rr["v158_xvenue_confirmed"]=same_direction
                            rr["v158_xvenue_divergence"]=divergent
                            rr["v158_xvenue_confidence"]=xconf
                            rr["v158_cross_venue_modifier"]=round((xconf-50.0)*0.04,2)
                        rows.sort(key=lambda z: float(z.get("hybrid_score", 0) or 0), reverse=True)
                        # Dedicated TOP-1 V15.6 FASTEST-PUMP Telegram lane.
                        # V15.7 engine: microstructure-first early-pump detection.
                        # This lane remains completely independent from the normal
                        # V15.6 CONFIRMED-IGNITION / BUY decision layer.
                        #
                        # Design:
                        #   - detect acceleration/flow change before relying on price;
                        #   - score buy-pressure acceleration, trade acceleration-of-acceleration,
                        #     volume acceleration and CVD/aggressive-buying flow;
                        #   - use price movement as confirmation, not the dominant signal;
                        #   - apply a dynamic extension/exhaustion penalty;
                        #   - keep a PRE-PUMP WATCH state internally;
                        #   - retain TOP-1 leader selection and loop-to-loop episode memory.
                        fast_pump_candidates=[]
                        sustained_pump_candidates=[]
                        fast_alerted_symbol=""
                        for r in rows:
                            p1=float(r.get("price_1m",0) or 0)
                            p3=float(r.get("price_3m",0) or 0)
                            p5=float(r.get("price_5m",0) or 0)
                            p10=float(r.get("price_10m",0) or 0)
                            p15=float(r.get("price_15m",0) or 0)
                            p10s=float(r.get("price_10s",0) or 0)
                            p60s=float(r.get("price_60s",0) or 0)
                            vol=float(r.get("volume_ratio",0) or 0)
                            accel=float(r.get("trade_accel",0) or 0)
                            buy=float(r.get("buy_pressure",0) or 0)
                            rs5=float(r.get("v15_relative_strength_5m",0) or 0)
                            ex=float(r.get("exhaustion_score",0) or 0)
                            v15=float(r.get("v15_score",0) or 0)
                            accum=float(r.get("accumulation_score",0) or 0)
                            bull_tf=int(r.get("tv_bullish_timeframes",0) or 0)
                            btc_off=bool(r.get("v15_btc_risk_off",False))
                            em=early_momentum(r["symbol"])
                            adaptive_fast={
                                "adaptive_regime_change":float(r.get("adaptive_regime_change",0) or 0),
                                "adaptive_trade_z":float(r.get("adaptive_trade_z",0) or 0),
                                "adaptive_volume_z":float(r.get("adaptive_volume_z",0) or 0),
                                "adaptive_cvd_z":float(r.get("adaptive_cvd_z",0) or 0),
                                "adaptive_intensity_z":float(r.get("adaptive_intensity_z",0) or 0),
                                "adaptive_trade_size_z":float(r.get("adaptive_trade_size_z",0) or 0),
                                "adaptive_price_impact_z":float(r.get("adaptive_price_impact_z",0) or 0),
                            }
                            r["v158_adaptive_early"]=adaptive_fast.get("adaptive_regime_change",0)>=55
                            if em:
                                r.update(em)
                                state[r["symbol"]]["early_momentum_score"]=em["early_momentum_score"]
                                state[r["symbol"]]["early_momentum_stage"]=em["early_momentum_stage"]
                                state[r["symbol"]]["early_momentum_last"]=time.time()
                            if btc_off or str(r.get("v15_stage","") or "")=="AVOID":
                                continue

                            # Raw live microstructure windows. These are derived from
                            # the existing Binance aggTrade stream; no extra API/feed.
                            _,v10,cvd_buy10,p10micro=stats(r["symbol"],10)
                            _,v30,cvd_buy30,p30micro=stats(r["symbol"],30)
                            _,v60,cvd_buy60,p60micro=stats(r["symbol"],60)
                            n10=stats(r["symbol"],10)[0]
                            cvd10=2.0*cvd_buy10-1.0
                            cvd30=2.0*cvd_buy30-1.0
                            cvd60=2.0*cvd_buy60-1.0
                            buy_slope=buy-(cvd_buy30/max(v30,1.0) if v30 else 0.50)
                            accel30=acceleration_ratio(v30,v60)
                            accel_slope=accel-accel30
                            vol10_rate=v10/max(v60/6.0,1.0)
                            vol30_rate=v30/max(v60/2.0,1.0)
                            volume_accel=max(vol10_rate,0)-max(vol30_rate,0)
                            cvd_impulse=cvd10-cvd30
                            ob=books[r["symbol"]].metrics(20)
                            spread=float(ob.get("spread_bps",0) or 0)
                            imb=float(ob.get("imbalance",0) or 0)
                            phase=v158_fast_pump_phase(r,n10=n10,cvd10=cvd10)
                            lead=v158_price_lead_signature(r,cvd10=cvd10,buy_slope=buy_slope,vol10_rate=vol10_rate)
                            gate_reasons=v158_fast_gate_reasons(r,n10=n10,cvd10=cvd10,buy_slope=buy_slope,accel=accel)
                            r.update({**lead,"v158_fast_pump_phase":phase,"v158_fast_gate_reasons":gate_reasons})

                            if SUSTAINED_PUMP_ENABLED:
                                sp=sustained_pump_signal(r,cvd10,ex,btc_off)
                                st=state[r["symbol"]]
                                if sp:
                                    st["sustained_pump_streak"]=int(st.get("sustained_pump_streak",0))+1
                                    st["sustained_pump_score"]=sp["score"]
                                    st["sustained_pump_state"]="CONFIRMED" if st["sustained_pump_streak"]>=SUSTAINED_PUMP_MIN_OBS and sp["score"]>=SUSTAINED_PUMP_MIN_SCORE else "WATCH"
                                    r["sustained_pump_score"]=sp["score"]; r["sustained_pump_state"]=st["sustained_pump_state"]; r["sustained_pump_streak"]=st["sustained_pump_streak"]; r["sustained_pump_qualifies"]=st["sustained_pump_state"]=="CONFIRMED"
                                    if st["sustained_pump_state"]=="CONFIRMED":
                                        rr_sp=dict(r); rr_sp.update(sp); rr_sp["_sustained_pump_score"]=sp["score"]
                                        sustained_pump_candidates.append(rr_sp)
                                else:
                                    st["sustained_pump_streak"]=0; st["sustained_pump_score"]=0.0; st["sustained_pump_state"]="MONITOR"

                            # PRE-PUMP hard gates: require genuine participation and
                            # improving flow, but deliberately tolerate very small price
                            # movement so the lane can fire before a candle-sized pump.
                            if n10<3:
                                continue
                            if buy<0.57:
                                continue
                            if accel<1.25:
                                continue
                            if cvd10<0.08 and buy_slope<0.025:
                                continue
                            if p1<0.05 or p5<0.30:
                                continue
                            if p10s<-0.75 or p60s<-1.0:
                                continue

                            # Price is now a confirmation component, not the dominant one.
                            flow_score=min(max((buy-0.50)/0.20,0),1)*18.0
                            buy_slope_score=min(max((buy_slope-0.01)/0.10,0),1)*14.0
                            accel_score=min(max((accel-1.0)/1.5,0),1)*18.0
                            accel_slope_score=min(max((accel_slope+0.05)/0.75,0),1)*10.0
                            volume_accel_score=min(max((vol10_rate-1.0)/2.5,0),1)*12.0
                            cvd_score=min(max((cvd10+0.05)/0.55,0),1)*10.0
                            micro_price_score=min(max((p10s+0.10)/1.50,0),1)*4.0
                            price_score=min(max((p1+0.05)/1.50,0),1)*2.0
                            structure_score=(
                                (3.0 if imb>=0 else 0.0)+
                                (2.0 if spread<=12 else 0.0)+
                                (2.0 if rs5>0 else 0.0)+
                                (2.0 if accum>=FAST_PUMP_MIN_ACCUMULATION else 0.0)+
                                (2.0 if v15>=FAST_PUMP_V15_PREFERENCE else 0.0)+
                                (1.0 if bull_tf>=FAST_PUMP_MIN_BULL_TF else 0.0)
                            )

                            # Early-momentum is advisory; it does not bypass the existing final V15.7 gates.
                            if em and em.get("early_momentum_quality"):
                                structure_score += 2.0
                            # Adaptive anomaly is advisory and cannot bypass existing Fastest-Pump gates.
                            if adaptive_fast.get("adaptive_regime_change",0)>=55:
                                structure_score += 2.0
                            if adaptive_fast.get("adaptive_intensity_z",0)>=1.5 and adaptive_fast.get("adaptive_trade_size_z",0)>=1.0:
                                structure_score += 1.0
                            if adaptive_fast.get("adaptive_price_impact_z",0)>=1.5:
                                structure_score += 1.0
                            # Executable depth is advisory: reward thin/cheap-to-sweep asks,
                            # but never bypass the existing Fastest-Pump hard gates.
                            sweep_score=float(r.get("v158_sweep_score",0) or 0)
                            ask_cost=float(r.get("v158_ask_sweep_cost_bps",0) or 0)
                            ask_cap=float(r.get("v158_ask_capacity_10bps",0) or 0)
                            bid_cap=float(r.get("v158_bid_capacity_10bps",0) or 0)
                            if sweep_score>=70:
                                structure_score += 3.0
                            elif sweep_score>=55:
                                structure_score += 2.0
                            elif sweep_score>=40:
                                structure_score += 1.0
                            if ask_cost>0 and bid_cap>0 and ask_cap<bid_cap*0.65:
                                structure_score += 1.0

                            # Replenishment/absorption is advisory and cannot bypass Fastest-Pump hard gates.
                            absorption_score=float(r.get("v158_absorption_persistence_score",0) or 0)
                            absorption_state=str(r.get("v158_absorption_state","") or "")
                            ask_repl=int(r.get("v158_ask_replenishment_events",0) or 0)
                            bid_repl=int(r.get("v158_bid_replenishment_events",0) or 0)
                            absorption_response=float(r.get("v158_absorption_price_response",0) or 0)
                            if absorption_state=="BULLISH_REPLENISHMENT" and absorption_score>=60: structure_score += 3.0
                            elif absorption_score>=45 and absorption_response>0: structure_score += 1.5
                            if absorption_state=="SELLER_ABSORPTION": structure_score -= 2.0

                            # Dynamic exhaustion/extension protection.
                            extension=max(0.0,p1-1.50)*3.0+max(0.0,p5-4.0)*1.5
                            dynamic_exhaustion=ex+extension
                            exhaustion_penalty=max(0.0,dynamic_exhaustion-20.0)*0.65
                            if dynamic_exhaustion>=65:
                                continue

                            fast_score=max(0,min(round(
                                flow_score+buy_slope_score+accel_score+accel_slope_score+
                                volume_accel_score+cvd_score+micro_price_score+price_score+
                                structure_score-exhaustion_penalty
                            ),100))

                            # Internal PRE-PUMP state is useful for learning/debugging,
                            # but only the final Fastest-Pump trigger is sent to Telegram.
                            pre_pump_score=max(0,min(round(
                                flow_score+buy_slope_score+accel_score+accel_slope_score+
                                volume_accel_score+cvd_score+structure_score
                            ),100))
                            pre_pump_stage="PRE-PUMP WATCH" if pre_pump_score<FAST_PUMP_MIN_SCORE else "FAST-PUMP"

                            if fast_score<FAST_PUMP_MIN_SCORE:
                                continue
                            rr=dict(r)
                            rr["_fast_pump_score"]=fast_score
                            rr["_fast_pump_pre_score"]=pre_pump_score
                            rr["_fast_pump_stage"]=pre_pump_stage
                            rr["_fast_buy_slope"]=buy_slope
                            rr["_fast_accel_slope"]=accel_slope
                            rr["_fast_volume_accel"]=volume_accel
                            rr["_fast_cvd"]=cvd10
                            rr["_fast_cvd_impulse"]=cvd_impulse
                            rr["_fast_spread_bps"]=spread
                            rr["_fast_dynamic_exhaustion"]=dynamic_exhaustion
                            rr["_fast_imbalance"]=imb
                            rr["_fast_sweep_score"]=sweep_score
                            rr["_fast_ask_sweep_cost_bps"]=ask_cost
                            rr["_fast_ask_capacity_10bps"]=ask_cap
                            rr["_fast_bid_capacity_10bps"]=bid_cap
                            rr["_fast_absorption_score"]=absorption_score
                            rr["_fast_absorption_state"]=absorption_state
                            rr["_fast_ask_replenishments"]=ask_repl
                            rr["_fast_bid_replenishments"]=bid_repl
                            rr["_fast_absorption_response"]=absorption_response
                            rr["v158_fast_pump_phase"]=phase
                            rr["v158_price_lead_score"]=lead.get("v158_price_lead_score",0)
                            rr["v158_price_lead_stage"]=lead.get("v158_price_lead_stage","NONE")
                            rr["v158_price_lead"]=lead.get("v158_price_lead",False)
                            rr["v158_fast_gate_reasons"]=gate_reasons
                            rr["_fast_adaptive_regime"]=adaptive_fast.get("adaptive_regime_change",0)
                            rr["_fast_trade_z"]=adaptive_fast.get("adaptive_trade_z",0)
                            rr["_fast_volume_z"]=adaptive_fast.get("adaptive_volume_z",0)
                            rr["_fast_cvd_z"]=adaptive_fast.get("adaptive_cvd_z",0)
                            rr["_fast_intensity_z"]=adaptive_fast.get("adaptive_intensity_z",0)
                            rr["_fast_volume_10s_rate"]=vol10_rate
                            fast_pump_candidates.append(rr)

                        # V15.8 Tier-1 Strong Early-Momentum recovery lane.
                        # Advisory only; it never changes V15.7 hard gates and sends no Telegram alert.
                        recovery_candidates=[]
                        if V158_EARLY_MOMENTUM_RECOVERY_ENABLED:
                            for rr in rows:
                                rec=v158_early_momentum_recovery(rr)
                                rr.update(rec)
                                if rec.get("v158_early_momentum_recovery"):
                                    recovery_candidates.append(rr)
                                    sx=str(rr.get("symbol","")).upper()
                                    state[sx]["v158_recovery_active"]=True
                                    state[sx]["v158_recovery_score"]=float(rec.get("v158_early_momentum_recovery_score",0) or 0)
                                    state[sx]["v158_recovery_last"]=time.time()
                        recovery_candidates=sorted(
                            recovery_candidates,
                            key=lambda x:float(x.get("v158_early_momentum_recovery_score",0) or 0),
                            reverse=True
                        )
                        try:
                            with open("data/v158_early_momentum_recovery.json","w") as rf:
                                json.dump({
                                    "updated":time.time(),
                                    "tier":"TIER_1_STRONG",
                                    "thresholds":{
                                        "regime_change_min":V158_RECOVERY_MIN_REGIME,
                                        "intensity_z_min":V158_RECOVERY_MIN_INTENSITY,
                                        "trade_size_z_min":V158_RECOVERY_MIN_TRADE_SIZE
                                    },
                                    "count":len(recovery_candidates),
                                    "candidates":recovery_candidates[:20]
                                },rf,indent=2)
                        except Exception:
                            pass

                        fast_pump_candidates=sorted(
                            fast_pump_candidates,
                            key=lambda r:(
                                r.get("_fast_pump_score",0),
                                r.get("price_1m",0),
                                r.get("price_5m",0),
                                r.get("trade_accel",0),
                                r.get("volume_ratio",0)
                            ),
                            reverse=True
                        )

                        # New sustained-expansion alert lane; Fastest-Pump remains unchanged.
                        if sustained_pump_candidates:
                            sustained_pump_candidates.sort(key=lambda x: float(x.get("_sustained_pump_score",0) or 0), reverse=True)
                            for candidate in sustained_pump_candidates[:2]:
                                s=candidate["symbol"]; st=state[s]; now=time.time()
                                if now-float(st.get("sustained_pump_last_alert",0) or 0)<SUSTAINED_PUMP_ALERT_COOLDOWN: continue
                                st["sustained_pump_last_alert"]=now
                                await telegram(
                                    f"📈 SUSTAINED-PUMP | {s} | Expansion Lane | Score {float(candidate.get('_sustained_pump_score',0) or 0):.0f}/100 | "
                                    f"1m {candidate.get('price_1m',0):+.2f}% | 5m {candidate.get('price_5m',0):+.2f}% | "
                                    f"10m {candidate.get('price_10m',0):+.2f}% | 15m {candidate.get('price_15m',0):+.2f}% | "
                                    f"Volume {candidate.get('volume_ratio',0):.2f}x | Trade accel {candidate.get('trade_accel',0):.2f}x | "
                                    f"Buy {candidate.get('buy_pressure',0)*100:.1f}% | CVD {candidate.get('cvd',0):+.2f} | "
                                    f"Participation Z {candidate.get('participation_z',0):+.2f} | Exhaustion {candidate.get('exhaustion_score',0):.1f}"
                                )
                        # Rolling Top-2 Fastest-Pump Telegram selector.

                        # The underlying V15.7 detector remains unchanged: all qualifying
                        # candidates are retained for learning, while Telegram compares
                        # their recent pump episodes before selecting only the strongest two.
                        fast_ranked,fast_alerts=update_fast_pump_leaderboard(fast_pump_candidates,time.time())
                        fast_alerted_symbol=""
                        for candidate in fast_alerts[:FAST_PUMP_TOP_N]:
                            s=candidate["symbol"]
                            leader_score=float(candidate.get("episode_score",0) or 0)
                            source=next((x for x in fast_pump_candidates if x.get("symbol")==s),None)
                            if not source:continue
                            await telegram(
                                f"🚀 V15.6 FASTEST-PUMP | {s} | V15.7 Engine | Rolling Top-2 | "
                                f"Episode Score {leader_score:.0f}/100 | Current {float(source.get('_fast_pump_score',0) or 0):.0f}/100 | "
                                f"Price: {source.get('price',0)} | "
                                f"1m: {source.get('price_1m',0):+.2f}% | 3m: {source.get('price_3m',0):+.2f}% | "
                                f"5m: {source.get('price_5m',0):+.2f}% | 10m: {source.get('price_10m',0):+.2f}% | "
                                f"15m: {source.get('price_15m',0):+.2f}% | Volume: {source.get('volume_ratio',0):.2f}x | "
                                f"Trade accel: {source.get('trade_accel',0):.2f}x | Accel slope: {source.get('_fast_accel_slope',0):+.2f}x | "
                                f"Buy: {source.get('buy_pressure',0)*100:.1f}% | Buy slope: {source.get('_fast_buy_slope',0)*100:+.1f}pp | "
                                f"CVD: {source.get('_fast_cvd',0):+.2f} | CVD impulse: {source.get('_fast_cvd_impulse',0):+.2f} | "
                                f"10s flow: {source.get('_fast_volume_10s_rate',0):.2f}x | "
                                f"RS5: {source.get('v15_relative_strength_5m',0):+.2f}% | "
                                f"Spread: {source.get('_fast_spread_bps',0):.1f}bps | "
                                f"V15: {source.get('v15_score',0):.0f}/100 | "
                                f"Accum: {source.get('accumulation_score',0):.0f} | "
                                f"Exhaustion: {source.get('_fast_dynamic_exhaustion',0):.0f} | "
                                f"Persistence: {int(candidate.get('samples',0))} obs"
                            )
                            fast_alerted_symbol=s
                        # Preserve all qualifying candidates for V15.7 learning; Telegram
                        # is now restricted to the rolling Top-2 selector above.

                        # V15.6-only Telegram lane.
                        # Telegram sends ONLY the final V15.6 CONFIRMED-IGNITION / BUY signal.
                        # EARLY-IGNITION, FAST-IGNITION, and SIGNATURE are intentionally
                        # excluded here; they remain internal scanner states/signals.
                        v156_candidates=[]
                        for r in rows:
                            if bool(r.get("v15_btc_risk_off",False)):
                                continue
                            if not bool(r.get("v156_buy_alert",False)):
                                continue
                            rr=dict(r)
                            rr["_v156_buy"]=True
                            v156_candidates.append(rr)
                        v156_candidates=sorted(
                            v156_candidates,
                            key=lambda r:(r.get("v156_buy_score",0),
                                          r.get("v156_sweet_score",0)),
                            reverse=True
                        )[:TOP_ALERTS]
                        for r in v156_candidates:
                            s=r["symbol"];old=state[s];now=time.time()
                            mode="CONFIRMED-IGNITION"
                            stage=str(r.get("v156_stage") or "CONFIRMED")
                            alert_score=float(r.get("v156_buy_score",0) or 0)
                            previous=float(old.get("last_v156_telegram_score",0) or 0)
                            previous_mode=str(old.get("last_v156_telegram_mode","") or "")
                            changed=(mode!=previous_mode or alert_score-previous>=5)
                            if changed and now-float(old.get("last_v156_telegram_alert",0) or 0)>=float(os.getenv("V156_TELEGRAM_COOLDOWN","180")):
                                reason=r.get("v156_buy_reason","CONFIRMED_IGNITION_BUY")
                                await telegram(
                                    f"V15.6 {mode} / BUY | {s} | {stage} | Score {alert_score:.0f}/100 | Price: {r.get('price',0)} | "
                                    f"60s: {r.get('price_60s',0):+.2f}% | Vol: {r.get('volume_ratio',0):.2f}x | "
                                    f"Trade accel: {r.get('trade_accel',0):.2f}x | Buy: {r.get('buy_pressure',0)*100:.1f}% | "
                                    f"RS 5m: {r.get('v15_relative_strength_5m',0):+.2f}% | RS 15m: {r.get('v15_relative_strength_15m',0):+.2f}% | "
                                    f"V15: {r.get('v15_score',0):.0f}/100 | BUY SCORE: {r.get('v156_buy_score',0):.0f} | "
                                    f"Drivers: {reason}"
                                )
                                v156_deterioration.start_episode(s,r.get("price",0),alert_score,now)
                                old["last_v156_telegram_alert"]=now
                            old["last_v156_telegram_score"]=alert_score
                            old["last_v156_telegram_mode"]=mode
                        # Legacy V15 TOP alerts are intentionally disabled here.
                        # Early Telegram alerts are governed exclusively by the three-stage V15.1 ignition model below.
                        # V15.1 Telegram uses a single three-stage ignition model:
                        # 1) IGNITION_WATCH      >=55 score, >=3 signals, 60s >=0.75%
                        # 2) PRE_PUMP_IGNITION   >=65 score, >=4 confirmations, 60s >=1.50%, Vol >=2x, Accel >=1.50x
                        # 3) EARLY_IGNITION      >=78 score, >=5 confirmations, 60s >=2.00%, Vol >=2.50x, Accel >=1.75x
                        # AVOID/BTC risk-off never alerts. Alerts are sent on stage entry or meaningful score improvement.
                        def telegram_ignition_stage(r):
                            stage=r.get("v15_ignition_stage","NORMAL")
                            score_i=float(r.get("v15_ignition_score",0) or 0)
                            p60=float(r.get("price_60s",0) or 0)
                            vr_i=float(r.get("volume_ratio",0) or 0)
                            acc_i=float(r.get("v15_ignition_accel",0) or 0)
                            sig=int(r.get("v15_ignition_signals",0) or 0)
                            confirmations=int(r.get("v15_ignition_confirmations",sig) or sig)
                            btc_off=bool(r.get("v15_btc_risk_off",False))
                            if btc_off or stage in ("AVOID","NORMAL"):
                                return None
                            if stage=="EARLY_IGNITION" and score_i>=78 and confirmations>=5 and p60>=2.0 and vr_i>=2.5 and acc_i>=1.75:
                                return "EARLY_IGNITION"
                            if stage=="PRE_PUMP_IGNITION" and score_i>=65 and confirmations>=4 and p60>=1.5 and vr_i>=2.0 and acc_i>=1.5:
                                return "PRE_PUMP_IGNITION"
                            if stage=="IGNITION_WATCH" and score_i>=55 and sig>=3 and p60>=0.75:
                                return "IGNITION_WATCH"
                            return None

                        ignition_candidates=[]
                        for r in rows:
                            stage=telegram_ignition_stage(r)
                            if stage:
                                rr=dict(r);rr["_telegram_ignition_stage"]=stage
                                ignition_candidates.append(rr)
                        ignition_candidates=sorted(
                            ignition_candidates,
                            key=lambda r: (
                                {"EARLY_IGNITION":3,"PRE_PUMP_IGNITION":2,"IGNITION_WATCH":1}.get(r["_telegram_ignition_stage"],0),
                                r.get("v15_ignition_score",0),
                                r.get("v15_trade_accel_slope",0)
                            ),
                            reverse=True
                        )[:TOP_ALERTS]
                        for r in ignition_candidates:
                            s=r["symbol"];old=state[s];now=time.time();stage=r["_telegram_ignition_stage"]
                            changed=(stage!=old.get("last_ignition_stage") or
                                     r.get("v15_ignition_score",0)-float(old.get("last_ignition_score",0))>=5)
                            if changed and now-old["last_ignition_alert"]>=COOLDOWN:
                                icon={"IGNITION_WATCH":"🟡","PRE_PUMP_IGNITION":"🟠","EARLY_IGNITION":"🟢"}[stage]
                                msg=(f"{icon} V15.1 {stage} | {s} | Ignition {r['v15_ignition_score']}/100 | Signals {r['v15_ignition_signals']}\
"
                                      f"60s: {r.get('price_60s',0):+.2f}% | Trade accel: {r['v15_ignition_accel']:.2f}x | Accel slope: {r['v15_trade_accel_slope']:+.2f}x\
"
                                      f"Buy: {r['buy_pressure']*100:.1f}% | Buy slope: {r['v15_buy_pressure_slope']:+.3f} | Vol: {r['volume_ratio']:.2f}x\
"
                                      f"RS 5m: {r['v15_ignition_rs5']:+.2f}% | RS 15m: {r['v15_ignition_rs15']:+.2f}% | 10s trades: {r['v15_ignition_trades_10s']}\
"
                                      f"Price: {r['price']}\
"
                                      "Three-stage V15.1 trajectory alert — confirmation strengthens as the stage advances.")
                                await telegram(msg)
                                old["last_ignition_alert"]=now
                            old["last_ignition_score"]=r.get("v15_ignition_score",0);old["last_ignition_stage"]=stage
                        # V15.3 Re-Ignition Bridge: preserve strong structure while short-term momentum resets,
                        # then alert when momentum returns. This does not alter the V15 score.
                        bridge_candidates=[]
                        for r in rows:
                            stage=str(r.get("v15_reignition_bridge_stage","") or "")
                            bs=float(r.get("v15_reignition_bridge_score",0) or 0)
                            trigger=bool(r.get("v15_reignition_bridge_trigger",False))
                            btc_off=bool(r.get("v15_btc_risk_off",False))
                            if btc_off or stage!="REIGNITION" or not trigger or bs<65:
                                continue
                            rr=dict(r);rr["_bridge_score"]=bs
                            bridge_candidates.append(rr)
                        bridge_candidates=sorted(
                            bridge_candidates,
                            key=lambda r:(r.get("_bridge_score",0),r.get("v15_score",0),r.get("v15_confirmation_score",0)),
                            reverse=True
                        )[:TOP_ALERTS]
                        for r in bridge_candidates:
                            s=r["symbol"];old=state[s];now=time.time()
                            bs=float(r.get("_bridge_score",0) or 0)
                            changed=(old.get("last_reignition_stage")!="REIGNITION" or
                                     bs-float(old.get("last_reignition_score",0) or 0)>=5)
                            if changed and now-float(old.get("last_reignition_alert",0) or 0)>=COOLDOWN:
                                await telegram(
                                    f"🔵 V15.3 RE-IGNITION | {s} | Bridge {bs:.0f}/100 | "
                                    f"V15: {r.get('v15_score',0):.0f}/100 | Opportunity: {r.get('v15_opportunity_score',0):.0f} | "
                                    f"Confirmation: {r.get('v15_confirmation_score',0):.0f} | "
                                    f"Accumulation: {r.get('accumulation_score',0):.0f} | TradingView: {r.get('tv_score',0):.1f} | "
                                    f"Bullish TFs: {r.get('tv_bullish_timeframes',0)} | "
                                    f"60s: {r.get('v15_reignition_bridge_price_60s',0):+.2f}% | "
                                    f"Trade accel: {r.get('v15_reignition_bridge_accel',0):.2f}x | "
                                    f"Vol: {r.get('v15_reignition_bridge_volume_ratio',0):.2f}x | "
                                    f"Buy pressure: {r.get('v15_reignition_bridge_buy_pressure',0)*100:.1f}% | "
                                    f"Accel slope: {r.get('v15_reignition_bridge_accel_slope',0):+.2f}x | Price: {r.get('price',0)} | "
                                    "🔵 Structure remained strong and short-term momentum has re-ignited — ignition confirmation follows separately."
                                )
                                old["last_reignition_alert"]=now
                            old["last_reignition_score"]=bs
                            old["last_reignition_stage"]=str(r.get("v15_reignition_bridge_stage","") or "")

                        # Dedicated PUMP MOMENTUM Telegram alerts sit between Ignition and BUY.
                        # They measure live pump intensity, not entry quality. This keeps a strong
                        # short-term mover visible even when BUY SETUP QUALITY is intentionally lower.
                        pump_momentum_candidates=[]
                        for r in rows:
                            pm=float(r.get("pump_momentum_score",0) or 0)
                            label=str(r.get("pump_momentum_label","") or "")
                            v15_stage=str(r.get("v15_stage","") or "")
                            btc_off=bool(r.get("v15_btc_risk_off",False))
                            if btc_off or v15_stage=="AVOID" or pm<PUMP_MOMENTUM_ALERT_MIN:
                                continue
                            old=state[r["symbol"]]
                            prev=float(old.get("last_pump_momentum_score",0) or 0)
                            crossed_min=prev<PUMP_MOMENTUM_ALERT_MIN and pm>=PUMP_MOMENTUM_ALERT_MIN
                            jumped=pm-prev>=PUMP_MOMENTUM_ALERT_JUMP
                            crossed_extreme=pm>=PUMP_MOMENTUM_ALERT_EXTREME and prev<PUMP_MOMENTUM_ALERT_EXTREME
                            if crossed_min or jumped or crossed_extreme:
                                rr=dict(r)
                                rr["_pump_momentum_delta"]=pm-prev
                                pump_momentum_candidates.append(rr)

                        pump_momentum_candidates=sorted(
                            pump_momentum_candidates,
                            key=lambda r: (
                                r.get("pump_momentum_score",0),
                                r.get("_pump_momentum_delta",0),
                                r.get("v15_score",0)
                            ),
                            reverse=True
                        )[:PUMP_MOMENTUM_ALERT_TOP]

                        for r in pump_momentum_candidates:
                            s=r["symbol"];old=state[s];now=time.time()
                            pm=float(r.get("pump_momentum_score",0) or 0)
                            if now-float(old.get("last_pump_momentum_alert",0) or 0)>=PUMP_MOMENTUM_ALERT_COOLDOWN:
                                label=str(r.get("pump_momentum_label","NORMAL") or "NORMAL")
                                icon="🚀" if pm>=PUMP_MOMENTUM_ALERT_EXTREME else "🔥"
                                await telegram(
                                    f"{icon} PUMP MOMENTUM | {s} | {label} | {pm:.0f}/100\n"
                                    f"V15: {r.get('v15_score',0):.0f}/100 | Stage: {r.get('v15_stage','')} | "
                                    f"V15 Opportunity: {r.get('v15_opportunity_score',0):.0f} | Confirmation: {r.get('v15_confirmation_score',0):.0f}\n"
                                    f"60s: {r.get('price_60s',0):+.2f}% | Volume: {r.get('volume_ratio',0):.2f}x | "
                                    f"Trade accel: {r.get('trade_accel',0):.2f}x\n"
                                    f"Buy pressure: {r.get('buy_pressure',0)*100:.1f}% | RS 5m: {r.get('v15_relative_strength_5m',0):+.2f}% | "
                                    f"RS 15m: {r.get('v15_relative_strength_15m',0):+.2f}%\n"
                                    f"Momentum change: {r.get('_pump_momentum_delta',0):+.0f} points | "
                                    f"Price: {r.get('price',0)}\n"
                                    f"Drivers: {r.get('pump_momentum_reasons','MOMENTUM BUILDING')}\n"
                                    "🚀 Pump-intensity alert — BUY confirmation is evaluated separately."
                                )
                                old["last_pump_momentum_alert"]=now
                            old["last_pump_momentum_score"]=pm
                            old["last_pump_momentum_label"]=label

                        # Dedicated BUY Telegram alerts require strong setup quality,
                        # confirmation, higher-timeframe confirmation, a ready V15 stage,
                        # no BTC risk-off, and no exhaustion watch/alert.
                        buy_candidates=[]
                        for r in rows:
                            decision=str(r.get("buy_decision","") or "")
                            q=float(r.get("buy_setup_quality",0) or 0)
                            conf=float(r.get("v15_confirmation_score",0) or 0)
                            opp=float(r.get("v15_opportunity_score",0) or 0)
                            tvs=float(r.get("tv_score",0) or 0)
                            bull_tf=int(r.get("tv_bullish_timeframes",0) or 0)
                            ex=float(r.get("exhaustion_score",0) or 0)
                            stage=str(r.get("v15_stage","") or "")
                            v12_conf=bool(r.get("v12_confirmation",False))
                            btc_off=bool(r.get("v15_btc_risk_off",False))
                            ready_stage=stage in ("PRE_PUMP","EARLY_PUMP","CONFIRMED")
                            strong=(
                                decision=="BUY" and q>=BUY_ALERT_MIN_QUALITY and
                                conf>=BUY_ALERT_MIN_CONFIRMATION and opp>=BUY_ALERT_MIN_OPPORTUNITY and
                                tvs>=BUY_ALERT_MIN_TV and bull_tf>=BUY_ALERT_MIN_BULL_TF and
                                ex<BUY_ALERT_MAX_EXHAUSTION and not btc_off and ready_stage and
                                (v12_conf or opp>=70)
                            )
                            if strong:
                                rr=dict(r);rr["_buy_alert_quality"]=q;buy_candidates.append(rr)
                        buy_candidates=sorted(
                            buy_candidates,
                            key=lambda r:(r.get("_buy_alert_quality",0),r.get("v15_confirmation_score",0),r.get("tv_score",0)),
                            reverse=True
                        )[:BUY_ALERT_TOP]
                        for r in buy_candidates:
                            s=r["symbol"];old=state[s];now=time.time();q=float(r.get("_buy_alert_quality",0) or 0)
                            changed=(old.get("last_buy_decision")!="BUY" or q-float(old.get("last_buy_quality",0) or 0)>=5)
                            if changed and now-float(old.get("last_buy_alert",0) or 0)>=BUY_ALERT_COOLDOWN:
                                await telegram(
                                    f"🟢 BUY SETUP | {s} | BUY\
"
                                    f"BUY SETUP QUALITY: {q:.0f}/100 | {r.get('buy_setup_quality_label','')}\
"
                                    f"V15: {r.get('v15_score',0):.0f}/100 | Stage: {r.get('v15_stage','')}\
"
                                    f"Opportunity: {r.get('v15_opportunity_score',0):.0f} | Confirmation: {r.get('v15_confirmation_score',0):.0f}\
"
                                    f"TradingView: {r.get('tv_score',0):.1f} | Bullish TFs: {r.get('tv_bullish_timeframes',0)}\
"
                                    f"V12 confirmation: {'YES' if r.get('v12_confirmation') else 'NO'} | Exhaustion: {r.get('exhaustion_score',0):.0f}\
"
                                    f"Volume: {r.get('volume_ratio',0):.2f}x | Trade accel: {r.get('trade_accel',0):.2f}x | Buy pressure: {r.get('buy_pressure',0)*100:.1f}%\
"
                                    f"RS 5m: {r.get('v15_relative_strength_5m',0):+.2f}% | RS 15m: {r.get('v15_relative_strength_15m',0):+.2f}%\
"
                                    f"Price: {r.get('price',0)}\
"
                                    f"Reason: {r.get('buy_decision_reasons','HIGH-QUALITY CONFLUENCE')}\
"
                                    "⚠️ Scanner decision only — confirm execution conditions before entering."
                                )
                                old["last_buy_alert"]=now
                        for r in rows:
                            old=state[r["symbol"]]
                            old["last_buy_quality"]=float(r.get("buy_setup_quality",0) or 0)
                            old["last_buy_decision"]=str(r.get("buy_decision","") or "")
                        # V15.6 post-BUY deterioration monitor.
                        # Episodes are created only when the actual V15.6
                        # CONFIRMED-IGNITION / BUY Telegram is sent above.
                        # The V15.6 BUY decision and alert-selection lane is untouched.
                        for r in rows:
                            result=v156_deterioration.observe(r["symbol"],r,time.time())
                            if result and result.get("alert"):
                                await telegram(
                                    f"🔴 V15.6 CONFIRMED DETERIORATION | {r['symbol']} | BUY episode {result['episode_id']} | BUY setup degrading\\n"
                                    f"Price: {r.get('price',0)} | 10s: {result.get('price_10s',0):+.2f}% | 60s: {result.get('price_60s',0):+.2f}%\\n"
                                    f"Volume: {result.get('volume_ratio',0):.2f}x | Trade accel: {result.get('trade_accel',0):.2f}x | Buy pressure: {result.get('buy_pressure',0)*100:.1f}%\\n"
                                    f"RS 5m: {result.get('rs5',0):+.2f}% | RSI 5m/30m/1H/4H: {result.get('rsi5',0):.1f}/{result.get('rsi30',0):.1f}/{result.get('rsi1h',0):.1f}/{result.get('rsi4h',0):.1f} | Exhaustion: {result.get('exhaustion',0):.0f}/100 | Drawdown: {result.get('drawdown',0):+.2f}%\\n"
                                    f"Breakdown families: {result.get('family_count',0)}/4 | Core breakdown: {result.get('core_family_count',0)}/3 | RSI bearish: {result.get('rsi_bear_count',0)}/4\\n"
                                    f"Confirmed after {result.get('bad_streak',0)} time-separated observations | Episode age: {result.get('episode_age',0):.0f}s\\n"
                                    "⚠️ Post-BUY monitoring alert — persistent multi-factor deterioration detected."
                                )
                        exhaustion_candidates=[r for r in rows if r.get("exhaustion_alert") and r.get("exhaustion_score",0)>=EXHAUSTION_ALERT_SCORE and r.get("v15_score",0)>=55]
                        exhaustion_candidates=sorted(exhaustion_candidates,key=lambda r:(r.get("exhaustion_score",0),r.get("v15_score",0)),reverse=True)[:TOP_ALERTS]
                        for r in exhaustion_candidates:
                            s=r["symbol"];old=state[s];now=time.time()
                            changed=(r.get("exhaustion_state")!=old.get("last_exhaustion_state") or r.get("exhaustion_score",0)-float(old.get("last_exhaustion_score",0))>=5)
                            if changed and now-old["last_exhaustion_alert"]>=EXHAUSTION_COOLDOWN:
                                await telegram(
                                    f"⚠️ EXHAUSTION MOMENTUM | {s} | {r['exhaustion_state']} | Exhaustion {r['exhaustion_score']}/100\n"
                                    f"V15 {r['v15_score']}/100 | Stage: {r['v15_stage']} | Extension: {r['exhaustion_extension']:.2f}% | Rollover: {r['exhaustion_rollover']:.0f}\n"
                                    f"Buy pressure: {r['buy_pressure']*100:.1f}% | Vol: {r['volume_ratio']:.2f}x | Trade accel: {r['trade_accel']:.2f}x\n"
                                    f"Book imbalance: {r['book_imbalance']:+.2f} | RS 5m: {r['v15_relative_strength_5m']:+.2f}% | RS 15m: {r['v15_relative_strength_15m']:+.2f}%\n"
                                    "⚠️ Momentum is extended and showing deterioration signals; confirmation of reversal is still required."
                                )
                                old["last_exhaustion_alert"]=now
                            old["last_exhaustion_score"]=r.get("exhaustion_score",0);old["last_exhaustion_state"]=r.get("exhaustion_state","")
                        accum_candidates=[r for r in rows if r.get("accumulation_score",0)>=ACCUM_ALERT_SCORE and r.get("accumulation_quality") and r.get("accumulation_stage") in ("ACCUMULATION WATCH","ACCUMULATION ALERT") and r.get("score",0)>=70]
                        accum_candidates=sorted(accum_candidates,key=lambda r:(r.get("accumulation_score",0),r.get("score",0)),reverse=True)[:TOP_ALERTS]
                        for r in accum_candidates:
                            s=r["symbol"];old=state[s];now=time.time();prev=float(old.get("last_accum_score",0))
                            changed=(r["accumulation_score"]-prev>=5 or old.get("last_accum_stage")!=r["accumulation_stage"])
                            if changed and now-old["last_accum_alert"]>=COOLDOWN:
                                await telegram(
                                    f"🟣 ACCUMULATION / PRE-PUMP | {s} | {r['accumulation_stage']} | Accum {r['accumulation_score']}/100\n"
                                    f"Pump score: {r['score']}/100 | 1m: {r['price_1m']:.2f}% | 10s: {r['price_10s']:.2f}%\n"
                                    f"Buy pressure: {r['accum_buy_pressure']*100:.1f}% | Trade accel: {r['accum_trade_accel']:.2f}x | Vol ratio: {r['accum_volume_ratio']:.2f}x\n"
                                    f"Book imbalance: {r['accum_book_imbalance']:+.2f} | Trades/10s: {r['accum_trades_10s']}\n"
                                    f"Price: {r['price']}\n"
                                    "⚠️ Early signal — confirmation still required."
                                )
                                old["last_accum_alert"]=now
                            old["last_accum_score"]=r["accumulation_score"];old["last_accum_stage"]=r["accumulation_stage"]
                        with open("data/latest.json","w") as f:json.dump({"updated":time.time(),"rows":rows},f,indent=2)
                        with open("data/latest.json","w") as f:json.dump({"updated":time.time(),"rows":rows},f,indent=2)
                        # V15.8 Tier-1B Participation Ignition.
                        # Separate from Tier-1; does not weaken or replace the 55/1.5/1.0 rule.
                        participation_candidates=[]
                        if V158_PARTICIPATION_IGNITION_ENABLED:
                            for rr in rows:
                                part=v158_participation_ignition(rr)
                                rr.update(part)
                                if part.get("v158_participation_ignition"):
                                    participation_candidates.append(rr)
                                    sx=str(rr.get("symbol","")).upper()
                                    state[sx]["v158_participation_active"]=True
                                    state[sx]["v158_participation_score"]=float(part.get("v158_participation_score",0) or 0)
                                    state[sx]["v158_participation_last"]=time.time()
                        participation_candidates=sorted(participation_candidates,key=lambda x:float(x.get("v158_participation_score",0) or 0),reverse=True)
                        try:
                            with open("data/v158_participation_ignition.json","w") as pf:
                                json.dump({"updated":time.time(),"tier":"TIER_1B_PARTICIPATION_IGNITION","thresholds":{"trade_z_min":V158_PARTICIPATION_MIN_TRADE_Z,"volume_z_min":V158_PARTICIPATION_MIN_VOLUME_Z,"buy_pressure_min":V158_PARTICIPATION_MIN_BUY,"trade_accel_min":V158_PARTICIPATION_MIN_ACCEL},"count":len(participation_candidates),"candidates":participation_candidates[:20]},pf,indent=2)
                        except Exception:
                            pass

                        directional_candidates=[]
                        if V158_DIRECTIONAL_ACCELERATION_ENABLED:
                            for rr in rows:
                                direction=v158_directional_ignition(rr)
                                rr.update(direction)
                                if direction.get("v158_directional_ignition"):
                                    directional_candidates.append(rr)
                                    sx=str(rr.get("symbol","")).upper()
                                    state[sx]["v158_directional_ignition_active"]=True
                                    state[sx]["v158_directional_ignition_score"]=float(direction.get("v158_directional_score",0) or 0)
                                    state[sx]["v158_directional_ignition_last"]=time.time()
                        directional_candidates=sorted(directional_candidates,key=lambda x:float(x.get("v158_directional_score",0) or 0),reverse=True)
                        try:
                            with open("data/v158_directional_ignition.json","w") as df:
                                json.dump({"updated":time.time(),"tier":"TIER_1C_DIRECTIONAL_IGNITION","prerequisite":"TIER_1B_PARTICIPATION_IGNITION","thresholds":{"buy_slope_min":V158_DIRECTIONAL_MIN_BUY_SLOPE,"price_10s_min_pct":V158_DIRECTIONAL_MIN_PRICE_10S,"price_60s_min_pct":V158_DIRECTIONAL_MIN_PRICE_60S,"cvd_z_min":V158_DIRECTIONAL_MIN_CVD_Z,"price_impact_z_max":V158_DIRECTIONAL_MAX_PRICE_IMPACT_Z},"count":len(directional_candidates),"candidates":directional_candidates[:20]},df,indent=2)
                        except Exception:
                            pass
                        # V15.7-only learning persistence. Legacy V15/V15.4/V15.6
                        # learning files are intentionally no longer written.
                        now_v157=time.time()
                        if now_v157-history_last_write >= max(HISTORY_SAMPLE_INTERVAL,30.0):
                            persist_v157_observations(rows,fast_pump_candidates,state,books,alerted_symbol=fast_alerted_symbol,ts=now_v157)
                            history_last_write=now_v157
                        await asyncio.sleep(1)
            finally:
                if discovery_task and not discovery_task.done():
                    discovery_task.cancel()
                    try:await discovery_task
                    except asyncio.CancelledError:pass
                for _s,_t in list(v158_dynamic_tasks.items()):
                    if not _t.done():_t.cancel()
                if v158_dynamic_tasks:await asyncio.gather(*list(v158_dynamic_tasks.values()),return_exceptions=True)
                if not sync.done():
                    sync.cancel()
                    try:await sync
                    except asyncio.CancelledError:pass
            rows=[r for s in list(symbols) if (r:=score(s))]
            for rr in rows:
                md=v158_discovery.items.get(str(rr.get("symbol","")).upper(),{})
                rr["v158_discovery_score"]=float(md.get("score",0) or 0)
                rr["v158_discovery_velocity_pct_s"]=float(md.get("velocity_pct_s",0) or 0)
                rr["v158_discovery_trade_anomaly"]=float(md.get("trade_anomaly",0) or 0)
                rr["v158_dynamic_promoted"]=str(rr.get("symbol","")).upper() in v158_discovery.active()
            rows.sort(key=lambda z:z["score"],reverse=True)
            with open("data/latest.json","w") as f:json.dump({"updated":time.time(),"rows":rows},f,indent=2)

# V15 production deployment active
if __name__=="__main__":asyncio.run(main())