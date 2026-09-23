import streamlit as st,json,os,pandas as pd

st.set_page_config(page_title="Binance Pump Scanner v7.6 Option B",layout="wide")
st.title("⚡ Binance Pump Scanner v7.6 — GitHub + Streamlit")
st.caption("GitHub Actions periodic scanner • synchronized local order book • Telegram alerts")

path="data/latest.json"
if not os.path.exists(path):
    st.warning("No scan has been published yet. Enable the GitHub Actions workflow.")
    st.stop()

with open(path) as f:
    d=json.load(f)

rows=d.get("rows",[])
df=pd.DataFrame(rows)
if df.empty:
    st.info("No usable market data in the latest run.")
    st.stop()

st.metric("Last scan",pd.to_datetime(d.get("updated",0),unit="s").strftime("%Y-%m-%d %H:%M:%S UTC"))

if "score" in df.columns:
    df=df.sort_values("score",ascending=False).reset_index(drop=True)

cols=["symbol","price","score","stage","entry","sell","price_1m","price_10s","volume_ratio","trade_accel","buy_pressure","book_imbalance","spread_bps","book_ready","book_gaps"]
available=[c for c in cols if c in df.columns]
x=df[available].copy()
if "buy_pressure" in x.columns:
    x["buy_pressure"]=(x["buy_pressure"]*100).round(1)
for c in ["price_1m","price_10s","volume_ratio","trade_accel","book_imbalance","spread_bps"]:
    if c in x.columns:
        x[c]=x[c].round(2)

st.dataframe(x,use_container_width=True,height=700)

st.subheader("Top PRE-PUMP / momentum candidates")

target_stages=["PRE-PUMP","EARLY MOMENTUM","BREAKOUT","CONFIRMED PUMP"]
p=df[df["stage"].isin(target_stages)].sort_values("score",ascending=False).head(15)

if p.empty:
    near=df[df["score"]>=25].sort_values("score",ascending=False).head(15)
    st.info("No symbol currently meets the PRE-PUMP threshold (score 45+). Showing the strongest near-candidates instead.")
    p=near

if p.empty:
    st.warning("No near-candidates are currently available.")
else:
    st.dataframe(p[available].reset_index(drop=True),use_container_width=True)
