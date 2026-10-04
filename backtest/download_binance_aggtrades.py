#!/usr/bin/env python3
"""Download Binance Spot daily aggTrades into normalized replay JSONL.

This intentionally downloads trades only. It does not fabricate historical L2,
so depth-dependent V15.8 layers remain disabled unless genuine depth events are
provided separately.
"""
from __future__ import annotations
import argparse,datetime as dt,io,json,os,urllib.request,zipfile
from pathlib import Path

def days(a,b):
    x=dt.date.fromisoformat(a); y=dt.date.fromisoformat(b)
    while x<=y:
        yield x;x+=dt.timedelta(days=1)

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--symbols",required=True,help="comma-separated symbols")
    p.add_argument("--start",required=True);p.add_argument("--end",required=True)
    p.add_argument("--out",default="data/replay/aggtrades")
    a=p.parse_args(); root=Path(a.out);root.mkdir(parents=True,exist_ok=True)
    total=0
    for symbol in [x.strip().upper() for x in a.symbols.split(",") if x.strip()]:
        out=root/(symbol+".jsonl"); n=0
        with out.open("w",encoding="utf-8") as dst:
            for day in days(a.start,a.end):
                name=f"{symbol}-aggTrades-{day.isoformat()}.zip"
                url=f"https://data.binance.vision/data/spot/daily/aggTrades/{symbol}/{name}"
                try:
                    with urllib.request.urlopen(url,timeout=60) as r: blob=r.read()
                    with zipfile.ZipFile(io.BytesIO(blob)) as z:
                        csvname=z.namelist()[0]
                        import csv
                        with z.open(csvname) as raw:
                            txt=io.TextIOWrapper(raw,encoding="utf-8",newline="")
                            for row in csv.reader(txt):
                                if not row or row[0]=="agg_trade_id": continue
                                try:
                                    # Binance aggTrades columns:
                                    # aggId,price,qty,firstTradeId,lastTradeId,time,isBuyerMaker
                                    dst.write(json.dumps({"ts":int(row[5]),"event":"trade","symbol":symbol,
                                      "price":float(row[1]),"qty":float(row[2]),
                                      "is_buyer_maker":row[6].lower() in ("true","1")})+"\n")
                                    n+=1
                                except (ValueError,IndexError):
                                    continue
                except Exception as e:
                    print(f"SKIP {symbol} {day}: {e}")
        print(f"{symbol}: {n} trades -> {out}")
        total+=n
    print(f"TOTAL trades: {total}")

if __name__=="__main__":main()
