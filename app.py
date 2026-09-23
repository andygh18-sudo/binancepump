import streamlit as st,json,os,pandas as pd
st.set_page_config(page_title="Binance Pump Scanner v7.6 Option B",layout="wide")
st.title("⚡ Binance Pump Scanner v7.6 — GitHub + Streamlit")
st.caption("GitHub Actions periodic scanner • synchronized local order book • Telegram alerts")
path="data/latest.json"
if not os.path.exists(path):
    st.warning("No scan has been published yet. Enable the GitHub Actions workflow.")
    st.stop()
with open(path) as f:d=json.load(f)
rows=d.get("rows",[]);df=pd.DataFrame(rows)
if df.empty:st.info("No usable market data in the latest run.");st.stop()
st.metric("Last scan",pd.to_datetime(d.get("updated",0),unit="s").strftime("%Y-%m-%d %H:%M:%S UTC"))
cols=["symbol","price","score","stage","entry","sell","price_1m","price_10s","volume_ratio","trade_accel","buy_pressure","book_imbalance","spread_bps","book_ready","book_gaps"]
x=df[cols].copy();x["buy_pressure"]=(x["buy_pressure"]*100).round(1)
for c in ["price_1m","price_10s","volume_ratio","trade_accel","book_imbalance","spread_bps"]:x[c]=x[c].round(2)
st.dataframe(x,use_container_width=True,height=700)
st.subheader("Top PRE-PUMP / momentum candidates")
p=df[df["stage"].isin(["PRE-PUMP","EARLY MOMENTUM","BREAKOUT","CONFIRMED PUMP"])].head(15)
st.dataframe(p[cols].reset_index(drop=True),use_container_width=True)
