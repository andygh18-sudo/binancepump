import streamlit as st
import json
import os
import pandas as pd
import numpy as np
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="Binance Pump Radar",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Refresh every 15 seconds so newly published scanner data appears automatically.
st_autorefresh(interval=15_000, key="pump_scanner_auto_refresh")

st.markdown("""
<style>
.block-container {padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1600px;}
[data-testid="stMetric"] {background: rgba(128,128,128,.08); padding: 12px 14px; border-radius: 12px;}
.signal-card {padding: 14px; border: 1px solid rgba(128,128,128,.25); border-radius: 12px; margin-bottom: 10px;}
.small-muted {color: #888; font-size: .85rem;}
</style>
""", unsafe_allow_html=True)

st.title("⚡ Binance Pump Radar")
st.caption("Real-time early-pump intelligence • Binance USDT markets • automatic refresh every 15 seconds")

path = "data/latest.json"
if not os.path.exists(path):
    st.warning("No scan has been published yet. Enable the GitHub Actions workflow.")
    st.stop()

try:
    with open(path) as f:
        d = json.load(f)
except Exception as e:
    st.error(f"Unable to read scanner data: {e}")
    st.stop()

rows = d.get("rows", [])
df = pd.DataFrame(rows)

if df.empty:
    st.info("No usable market data in the latest run.")
    st.stop()

if "score" not in df.columns:
    st.error("Scanner data does not contain a score column.")
    st.stop()

df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0)
df = df.sort_values("score", ascending=False).reset_index(drop=True)

# Numeric normalization
numeric_cols = [
    "price", "price_1m", "price_10s", "volume_ratio", "trade_accel",
    "buy_pressure", "book_imbalance", "spread_bps", "score"
]
for c in numeric_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

st.sidebar.header("⚙️ Radar Filters")
min_score = st.sidebar.slider("Minimum score", 0, 100, 38)
stages = ["BUILDING", "PRE-PUMP", "EARLY MOMENTUM", "BREAKOUT", "CONFIRMED PUMP"]
selected_stages = st.sidebar.multiselect("Stages", stages, default=stages)
min_volume_ratio = st.sidebar.number_input("Minimum volume ratio", min_value=0.0, value=1.0, step=0.1)
only_book_ready = st.sidebar.checkbox("Order book ready only", value=False)
show_acceleration = st.sidebar.checkbox("Show score acceleration", value=True)

view = df[df["score"] >= min_score].copy()
if "stage" in view.columns and selected_stages:
    view = view[view["stage"].isin(selected_stages)]
if "volume_ratio" in view.columns:
    view = view[view["volume_ratio"].fillna(0) >= min_volume_ratio]
if only_book_ready and "book_ready" in view.columns:
    view = view[view["book_ready"] == True]

# Historical score acceleration
history_path = "data/history.jsonl"
history_records = []
if os.path.exists(history_path):
    with open(history_path, errors="ignore") as hf:
        for line in hf:
            try:
                item = json.loads(line)
                if isinstance(item, dict) and item.get("rows"):
                    history_records.append(item)
            except Exception:
                continue

score_history = {}
for item in history_records[-120:]:
    ts = item.get("ts")
    for r in item.get("rows", []):
        sym = str(r.get("symbol", ""))
        if sym:
            score_history.setdefault(sym, []).append({
                "ts": ts,
                "score": float(r.get("score", 0) or 0),
                "stage": r.get("stage", "")
            })

def acceleration_for(symbol):
    points = score_history.get(str(symbol), [])
    if len(points) < 2:
        return 0.0, 0.0, 0
    current = points[-1]["score"]
    previous = points[-2]["score"]
    lookback = points[-7]["score"] if len(points) >= 7 else points[0]["score"]
    return current - previous, current - lookback, len(points)

# Human-readable order-book status
if "book_ready" in df.columns:
    def book_status(row):
        ready = bool(row.get("book_ready", False))
        gaps = int(row.get("book_gaps", 0) or 0)
        if ready and gaps == 0:
            return "🟢 READY"
        if ready and gaps > 0:
            return "🟡 RESYNCING"
        return "🟡 SYNCING"
    df["book_status"] = df.apply(book_status, axis=1)

if show_acceleration and "symbol" in df.columns:
    accel = df["symbol"].map(lambda s: acceleration_for(s)[0])
    accel5 = df["symbol"].map(lambda s: acceleration_for(s)[1])
    df["score_delta"] = accel
    df["score_delta_5"] = accel5
    df["acceleration"] = df["score_delta"].map(
        lambda x: "🔥 SURGING" if x >= 5 else "🟢 RISING" if x > 0 else "🟡 STABLE" if x == 0 else "🔴 FALLING"
    )

# Header metrics
updated = d.get("updated", 0)
try:
    last_scan = pd.to_datetime(updated, unit="s").strftime("%H:%M:%S UTC")
except Exception:
    last_scan = "Unknown"

active = df[df["score"] >= min_score]
early = df[df.get("stage", pd.Series(index=df.index, dtype=str)).isin(["PRE-PUMP", "EARLY MOMENTUM"])]
breakouts = df[df.get("stage", pd.Series(index=df.index, dtype=str)).isin(["BREAKOUT", "CONFIRMED PUMP"])]
building = df[df.get("stage", pd.Series(index=df.index, dtype=str)) == "BUILDING"]

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("🔥 Active Signals", len(active))
c2.metric("⚡ Pre-Pump / Early", len(early))
c3.metric("🚀 Breakouts", len(breakouts))
c4.metric("🏗️ Building", len(building))
c5.metric("🏆 Top Score", f"{df['score'].max():.0f}")
c6.metric("🕒 Last Scan", last_scan)

if "book_status" in df.columns:
    ready_count = int((df["book_status"] == "🟢 READY").sum())
    syncing_count = int((df["book_status"] != "🟢 READY").sum())
    st.caption(f"📚 Order books: **{ready_count} ready** • **{syncing_count} syncing/resyncing**")

st.divider()

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🔥 Live Radar", "⚡ Pre-Pump", "🗺️ Heatmap", "🔎 Coin Analysis", "📜 History"
])

def format_signal_table(frame):
    cols = [
        "symbol", "price", "score", "stage", "book_status", "entry", "sell",
        "price_1m", "price_10s", "volume_ratio", "trade_accel",
        "buy_pressure", "book_imbalance", "spread_bps", "book_ready",
        "score_delta", "score_delta_5", "acceleration"
    ]
    # Keep only columns that exist and de-duplicate defensively.
    cols = list(dict.fromkeys(c for c in cols if c in frame.columns))
    x = frame.loc[:, cols].copy()
    if "buy_pressure" in x.columns:
        x["buy_pressure"] = x["buy_pressure"] * 100
    # PyArrow used by st.dataframe requires unique column names.
    if x.columns.duplicated().any():
        x = x.loc[:, ~x.columns.duplicated()].copy()
    return x

with tab1:
    st.subheader("🔥 Live Pump Radar")
    st.caption("Ranked by current pump score. Focus first on score acceleration and PRE-PUMP/EARLY MOMENTUM stages.")

    radar = view.head(20).copy()
    if radar.empty:
        st.info("No symbols match the current filters.")
    else:
        st.dataframe(
            format_signal_table(radar),
            use_container_width=True,
            height=620,
            hide_index=True,
            column_config={
                "score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%d"),
                "buy_pressure": st.column_config.NumberColumn("Buy %", format="%.1f%%"),
                "volume_ratio": st.column_config.NumberColumn("Vol Ratio", format="%.2fx"),
                "price_1m": st.column_config.NumberColumn("1m %", format="%.2f"),
                "price_10s": st.column_config.NumberColumn("10s %", format="%.2f"),
                "book_imbalance": st.column_config.NumberColumn("Book Imb", format="%.2f"),
                "score_delta": st.column_config.NumberColumn("Δ Score", format="%+.0f"),
                "score_delta_5": st.column_config.NumberColumn("5-Scan Δ", format="%+.0f"),
                "book_status": st.column_config.TextColumn("Order Book"),
            },
        )

    if show_acceleration and "score_delta_5" in radar.columns:
        st.subheader("🔥 Fastest Score Acceleration")
        accelerating = radar.sort_values("score_delta_5", ascending=False).head(10)
        if not accelerating.empty:
            st.dataframe(
                accelerating[[c for c in ["symbol", "score", "score_delta", "score_delta_5", "stage", "volume_ratio"] if c in accelerating.columns]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%d"),
                    "score_delta": st.column_config.NumberColumn("Δ Scan", format="%+.0f"),
                    "score_delta_5": st.column_config.NumberColumn("Δ 5 Scans", format="%+.0f"),
                    "volume_ratio": st.column_config.NumberColumn("Volume", format="%.2fx"),
                },
            )

    st.subheader("📈 Strongest Momentum")
    momentum = view.head(8).copy()
    if not momentum.empty:
        chart_cols = [c for c in ["symbol", "score", "volume_ratio"] if c in momentum.columns]
        st.bar_chart(momentum.set_index("symbol")[chart_cols[1:]] if len(chart_cols) > 1 else momentum.set_index("symbol")[["score"]])

with tab2:
    st.subheader("⚡ Early Pump Opportunities")
    st.caption("Coins that have entered PRE-PUMP or EARLY MOMENTUM, before the scanner classifies them as a confirmed breakout.")

    early_view = df[df.get("stage", pd.Series(index=df.index, dtype=str)).isin(["BUILDING", "PRE-PUMP", "EARLY MOMENTUM"])].copy()
    early_view = early_view[early_view["score"] >= min_score].sort_values("score", ascending=False).head(20)

    if early_view.empty:
        st.info("No early-stage candidates currently meet the selected threshold.")
    else:
        st.dataframe(
            format_signal_table(early_view),
            use_container_width=True,
            height=650,
            hide_index=True,
            column_config={
                "score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%d"),
                "buy_pressure": st.column_config.NumberColumn("Buy %", format="%.1f%%"),
                "volume_ratio": st.column_config.NumberColumn("Vol Ratio", format="%.2fx"),
            },
        )

with tab3:
    st.subheader("🗺️ Binance Pump Heatmap")
    st.caption("A market-wide view of the scanner's current stages and scores.")

    heat = df.copy()
    heat["stage"] = heat.get("stage", "QUIET")
    heat = heat.head(60)

    if not heat.empty:
        display_cols = [c for c in ["symbol", "score", "stage", "volume_ratio", "price_1m"] if c in heat.columns]
        st.dataframe(
            heat[display_cols],
            use_container_width=True,
            height=700,
            hide_index=True,
            column_config={
                "score": st.column_config.ProgressColumn("Pump Score", min_value=0, max_value=100, format="%d"),
                "volume_ratio": st.column_config.NumberColumn("Volume", format="%.2fx"),
                "price_1m": st.column_config.NumberColumn("1m Change", format="%.2f%%"),
            },
        )

with tab4:
    st.subheader("🔎 Coin Analysis")
    symbols = df["symbol"].astype(str).tolist()
    selected = st.selectbox("Select coin", symbols)

    coin = df[df["symbol"].astype(str) == selected].iloc[0]

    score = float(coin.get("score", 0))
    stage = str(coin.get("stage", "UNKNOWN"))
    price = coin.get("price", np.nan)

    a, b, c, e = st.columns(4)
    a.metric("Pump Score", f"{score:.0f}/100")
    b.metric("Stage", stage)
    c.metric("Price", f"{price:g}" if pd.notna(price) else "—")
    e.metric("Volume Ratio", f"{float(coin.get('volume_ratio', 0)):.2f}x")

    st.progress(min(max(int(score), 0), 100), text=f"Pump Score {score:.0f}/100")

    delta_now, delta_lookback, history_count = acceleration_for(selected)
    ac1, ac2, ac3 = st.columns(3)
    ac1.metric("Score Change", f"{delta_now:+.0f}")
    ac2.metric("5-Scan Change", f"{delta_lookback:+.0f}")
    ac3.metric("History Points", history_count)

    if history_count >= 2:
        st.subheader("📈 Score Acceleration")
        points = score_history[selected]
        chart = pd.DataFrame({
            "Time": [pd.to_datetime(p["ts"], unit="s") for p in points[-30:]],
            "Score": [p["score"] for p in points[-30:]],
        }).set_index("Time")
        st.line_chart(chart, height=260)
        trajectory = "🔥 SURGING" if delta_now >= 5 else "🟢 RISING" if delta_now > 0 else "🟡 STABLE" if delta_now == 0 else "🔴 FALLING"
        st.info(f"Current score trajectory: **{trajectory}**. A rising score means momentum is strengthening; a falling score means momentum is weakening.")

    m1, m2, m3, m4 = st.columns(4)
    bp = coin.get("buy_pressure", np.nan)
    m1.metric("Buy Pressure", f"{bp*100:.1f}%" if pd.notna(bp) else "—")
    m2.metric("Trade Acceleration", f"{float(coin.get('trade_accel', 0)):.2f}x")
    m3.metric("Book Imbalance", f"{float(coin.get('book_imbalance', 0)):.2f}")
    m4.metric("Spread", f"{float(coin.get('spread_bps', 0)):.2f} bps")

    st.subheader("Signal Lifecycle")
    lifecycle = ["BUILDING", "PRE-PUMP", "EARLY MOMENTUM", "BREAKOUT", "CONFIRMED PUMP"]
    current_idx = lifecycle.index(stage) if stage in lifecycle else -1
    lifecycle_df = pd.DataFrame({
        "Stage": lifecycle,
        "Status": ["● CURRENT" if i == current_idx else ("✓ PASSED" if i < current_idx else "○ NEXT") for i in range(len(lifecycle))]
    })
    st.dataframe(lifecycle_df, use_container_width=True, hide_index=True)

    st.subheader("📚 Order Book Status")
    selected_book_status = str(coin.get("book_status", "🟡 SYNCING"))
    if selected_book_status == "🟢 READY":
        st.success("🟢 Order book is synchronized and its imbalance/spread metrics can be used confidently.")
    elif selected_book_status == "🟡 RESYNCING":
        st.warning("🟡 Order book detected a synchronization gap and is being rebuilt. Treat order-book metrics cautiously.")
    else:
        st.info("🟡 Order book is still synchronizing. Price/volume/trade signals may be available before book confirmation.")

    st.subheader("📊 Momentum Evidence")
    evidence = {
        "1m Price Change": coin.get("price_1m", np.nan),
        "10s Price Change": coin.get("price_10s", np.nan),
        "Volume Ratio": coin.get("volume_ratio", np.nan),
        "Trade Acceleration": coin.get("trade_accel", np.nan),
        "Buy Pressure": bp,
        "Order Book Imbalance": coin.get("book_imbalance", np.nan),
        "Spread (bps)": coin.get("spread_bps", np.nan),
    }
    edf = pd.DataFrame({"Metric": list(evidence.keys()), "Value": list(evidence.values())})
    st.dataframe(edf, use_container_width=True, hide_index=True)

with tab5:
    st.subheader("📜 Signal History")
    history_path = "data/history.jsonl"

    if os.path.exists(history_path):
        history = []
        with open(history_path, errors="ignore") as f:
            for line in f:
                try:
                    item = json.loads(line)
                    history.extend(item.get("rows", []) if isinstance(item, dict) else [])
                except Exception:
                    continue

        hdf = pd.DataFrame(history)
        if not hdf.empty and "score" in hdf.columns:
            hdf["score"] = pd.to_numeric(hdf["score"], errors="coerce")
            if "symbol" in hdf.columns:
                top_history = hdf.sort_values("score", ascending=False).head(100)
                hcols = [c for c in ["symbol", "score", "stage", "price_1m", "volume_ratio", "buy_pressure"] if c in top_history.columns]
                st.dataframe(top_history[hcols], use_container_width=True, height=650, hide_index=True)
            else:
                st.info("History is available but does not contain symbol data.")
        else:
            st.info("No usable history data yet.")
    else:
        st.info("No history file has been published yet.")

st.divider()
st.caption(f"Dashboard refreshes automatically every 15 seconds • Showing {len(df)} scanned symbols • Historical acceleration uses the latest published scan records • Last scan: {last_scan}")
