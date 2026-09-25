#!/usr/bin/env python3
"""Pump Replay / Backtest Engine v14."""
import argparse,json,os,time,bisect
from datetime import datetime,timezone
import pandas as pd,requests
BASE=os.getenv("BINANCE_REST_BASE","https://data-api.binance.vision")
COLS=["open_time","open","high","low","close","volume","close_time","quote_volume","trades","taker_buy_base","taker_buy_quote","ignore"]

def ms(dt): return int(pd.Timestamp(dt,tz="UTC").timestamp()*1000)
def fetch(symbol,start,end):
    rows=[];cur=ms(start);stop=ms(end)
    while cur<stop:
        r=requests.get(BASE+"/api/v3/klines",params={"symbol":symbol,"interval":"1m","startTime":cur,"endTime":stop,"limit":1000},timeout=20);r.raise_for_status()
        b=r.json()
        if not b: break
        rows+=b;cur=int(b[-1][0])+60000;time.sleep(.03)
        if len(b)<1000: break
    if not rows:return pd.DataFrame(columns=COLS)
    x=pd.DataFrame(rows,columns=COLS).drop_duplicates("open_time")
    for c in COLS:
        if c!="ignore":x[c]=pd.to_numeric(x[c],errors="coerce")
    x["time"]=pd.to_datetime(x.open_time,unit="ms",utc=True)
    return x.sort_values("open_time").reset_index(drop=True)

# NOTE: core feature/classifier functions intentionally remain unchanged from the
# repository's V14 implementation. The publishing bug was that calibration was
# hard-coded to data/v13_calibration.json.
