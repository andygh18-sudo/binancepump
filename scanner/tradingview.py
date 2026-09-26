import asyncio
import os
import time

TV_URL = os.getenv("TRADINGVIEW_SCAN_URL", "https://scanner.tradingview.com/crypto/scan")
TV_TIMEOUT = float(os.getenv("TRADINGVIEW_TIMEOUT", "8"))
TV_BATCH = int(os.getenv("TRADINGVIEW_BATCH", "120"))
INTERVALS = {"30m": "30", "1h": "60", "4h": "240", "1d": "1D"}
BASE_FIELDS = ["close", "RSI", "EMA20", "EMA50", "ADX", "volume", "change", "Recommend.All"]

def _f(value):
    try: return float(value)
    except (TypeError, ValueError): return None

def _rsi_quality(rsi):
    if rsi is None: return 50.0
    if 50 <= rsi <= 72: return 100.0
    if 45 <= rsi < 50: return 75.0
    if 72 < rsi <= 82: return 70.0
    if 82 < rsi <= 88: return 45.0
    if rsi < 30: return 35.0
    if rsi < 45: return 45.0
    return 25.0

def _recommend_quality(rec):
    if rec is None: return 50.0
    return max(0.0, min(100.0, (rec + 1.0) * 50.0))

def _row_score(row):
    close,e20,e50 = row.get("close"),row.get("ema20"),row.get("ema50")
    trend=50.0
    if close is not None and e20 is not None and e50 is not None:
        if close > e20 > e50: trend=100.0
        elif close > e20: trend=75.0
        elif close > e50: trend=60.0
        elif close < e20 < e50: trend=0.0
        else: trend=35.0
    adx=row.get("adx"); adx_q=50.0 if adx is None else max(0.0,min(100.0,(adx-10.0)*2.0))
    score=round(0.40*_recommend_quality(row.get("recommend"))+0.30*trend+0.20*_rsi_quality(row.get("rsi"))+0.10*adx_q)
    return max(0,min(100,score))

async def _scan(session,symbols,interval):
    suffix=INTERVALS[interval]
    # TradingView's crypto scanner reliably exposes daily technicals through
    # the default (unsuffixed) fields; use an explicit suffix for intraday
    # frames. This avoids the 1D scanner column returning null values.
    columns=[field if interval=="1d" else f"{field}|{suffix}" for field in BASE_FIELDS]
    payload={"symbols":{"tickers":[f"BINANCE:{s}" for s in symbols],"query":{"types":[]}},"columns":columns,"range":[0,len(symbols)]}
    try:
        async with session.post(TV_URL,json=payload,headers={"User-Agent":"Binance-Pump-Radar/15.0","Origin":"https://www.tradingview.com","Referer":"https://www.tradingview.com/"},timeout=TV_TIMEOUT) as response:
            if response.status!=200: return interval,{}
            data=await response.json(content_type=None)
    except Exception:
        return interval,{}
    out={}
    for item in data.get("data",[]):
        symbol=str(item.get("s",""))
        if ":" not in symbol: continue
        sym=symbol.split(":",1)[1].upper(); values=item.get("d",[])
        if len(values)!=len(columns): continue
        raw=dict(zip(BASE_FIELDS,values))
        row={"close":_f(raw.get("close")),"rsi":_f(raw.get("RSI")),"ema20":_f(raw.get("EMA20")),"ema50":_f(raw.get("EMA50")),"adx":_f(raw.get("ADX")),"volume":_f(raw.get("volume")),"change":_f(raw.get("change")),"recommend":_f(raw.get("Recommend.All"))}
        row["score"]=_row_score(row);row["bullish_alignment"]=bool(row["close"] is not None and row["ema20"] is not None and row["ema50"] is not None and row["close"]>row["ema20"]>row["ema50"])
        out[sym]=row
    return interval,out

async def fetch_tradingview_signals(session,symbols):
    symbols=[str(s).upper() for s in symbols if str(s).upper().endswith("USDT")]
    if not symbols: return {},time.time()
    merged={s:{} for s in symbols}
    for start in range(0,len(symbols),TV_BATCH):
        batch=symbols[start:start+TV_BATCH]
        results=await asyncio.gather(*(_scan(session,batch,iv) for iv in INTERVALS))
        for interval,rows in results:
            for sym,row in rows.items(): merged.setdefault(sym,{})[interval]=row
    final={}
    for sym,tf in merged.items():
        if not tf: continue
        scores=[tf[iv]["score"] for iv in INTERVALS if iv in tf]; recs=[tf[iv]["recommend"] for iv in INTERVALS if iv in tf and tf[iv]["recommend"] is not None]
        bullish=sum(1 for iv in INTERVALS if tf.get(iv,{}).get("bullish_alignment"))
        final[sym]={"tv_score":round(sum(scores)/len(scores),1) if scores else 0.0,"tv_bullish_timeframes":bullish,"tv_recommendation":round(sum(recs)/len(recs),3) if recs else None,"tv_30m_rsi":tf.get("30m",{}).get("rsi"),"tv_1h_rsi":tf.get("1h",{}).get("rsi"),"tv_4h_rsi":tf.get("4h",{}).get("rsi"),"tv_1d_rsi":tf.get("1d",{}).get("rsi"),"tv_30m_trend":bool(tf.get("30m",{}).get("bullish_alignment")),"tv_1h_trend":bool(tf.get("1h",{}).get("bullish_alignment")),"tv_4h_trend":bool(tf.get("4h",{}).get("bullish_alignment")),"tv_1d_trend":bool(tf.get("1d",{}).get("bullish_alignment")),"tv_30m_adx":tf.get("30m",{}).get("adx"),"tv_1h_adx":tf.get("1h",{}).get("adx"),"tv_4h_adx":tf.get("4h",{}).get("adx"),"tv_1d_adx":tf.get("1d",{}).get("adx"),"tv_30m_change":tf.get("30m",{}).get("change"),"tv_1h_change":tf.get("1h",{}).get("change"),"tv_4h_change":tf.get("4h",{}).get("change"),"tv_1d_change":tf.get("1d",{}).get("change")}
        score=final[sym]["tv_score"]
        final[sym]["tv_confirmation"]="STRONG" if score>=72 and bullish>=2 else "CONFIRM" if score>=60 and bullish>=1 else "BEARISH" if score<=35 else "NEUTRAL"
    return final,time.time()
