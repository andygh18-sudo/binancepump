import streamlit as st
import json
import os
import pandas as pd
import numpy as np
import urllib.request
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="Binance Pump Radar",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st_autorefresh(interval=15_000, key="pump_scanner_auto_refresh")

st.markdown("""
<style>
.block-container {padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1600px;}
[data-testid="stMetric"] {background: rgba(128,128,128,.08); padding: 12px 14px; border-radius: 12px;}
.signal-card {padding: 14px; border: 1px solid rgba(128,128,128,.25); border-radius: 12px; margin-bottom: 10px;}
.small-muted {color: #888; font-size: .85rem;}
</style>
""", unsafe_allow_html=True)

st.title("⚡ Binance Pump Radar — V15")
st.caption("V15 opportunity + confirmation engine • Binance USDT markets • automatic refresh every 15 seconds")

path = "data/latest.json"
RAW_BASE = "https://raw.githubusercontent.com/andygh18-sudo/binancepump/main/data/"

def load_json_data(filename):
    local_path = os.path.join("data", filename)
    # GitHub Actions publishes fresh scan data after each scan. Streamlit
    # deployments can keep an older checked-out copy, so GitHub is the
    # authoritative source and the local file is only a fallback.
    try:
        url = RAW_BASE + filename + "?t=" + str(int(pd.Timestamp.utcnow().timestamp()))
        req = urllib.request.Request(url, headers={"Cache-Control": "no-cache", "User-Agent": "Binance-Pump-Radar"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8")), "github"
    except Exception as github_error:
        if os.path.exists(local_path):
            try:
                with open(local_path) as f:
                    return json.load(f), "local fallback"
            except Exception:
                pass
        return None, f"GitHub data unavailable: {github_error}"

d, data_source = load_json_data("latest.json")
if d is None:
    st.error(f"Unable to load live scanner data from the repository: {data_source}")
    st.stop()

st.caption(f"📡 **V15 LIVE** • Data source: **GitHub Actions scan ({data_source})** • Scanner update: {pd.to_datetime(d.get('updated', 0), unit='s', errors='coerce')}")

rows = d.get("rows", [])
df = pd.DataFrame(rows)

if df.empty:
    st.info("No usable market data in the latest run.")
    st.stop()

# V15 is the primary dashboard model; legacy score/stage fields are supporting diagnostics only.
if "v15_score" not in df.columns or "v15_stage" not in df.columns:
    st.error("Latest scanner data does not contain V15 fields. Run the V15 GitHub Actions scanner first.")
    st.stop()

for c in ["v15_score","v15_opportunity_score","v15_confirmation_score","v15_relative_strength_5m","v15_relative_strength_15m","v15_streak","exhaustion_score","exhaustion_extension","exhaustion_rollover","exhaustion_buy_stress","exhaustion_book_stress","exhaustion_rs_fade","exhaustion_efficiency_stress","exhaustion_symptoms","exhaustion_volume_ratio","exhaustion_trade_accel"]:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

df["score"] = df["v15_score"]
df["stage"] = df["v15_stage"].fillna("NEUTRAL")
df = df.sort_values(["v15_score","v15_confirmation_score","v15_opportunity_score"], ascending=False).reset_index(drop=True)

numeric_cols = [
    "price", "price_1m", "price_10s", "volume_ratio", "trade_accel",
    "buy_pressure", "book_imbalance", "spread_bps", "score",
    "v15_score", "v15_opportunity_score", "v15_confirmation_score",
    "v15_relative_strength_5m", "v15_relative_strength_15m", "v15_streak"
]
for c in numeric_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

st.sidebar.header("⚙️ V15 Radar Filters")
min_score = st.sidebar.slider("Minimum V15 score", 0, 100, 0)
stages = ["NEUTRAL", "WATCH", "PRE_PUMP", "EARLY_PUMP", "CONFIRMED", "AVOID"]
selected_stages = st.sidebar.multiselect("Stages", stages, default=stages)
min_volume_ratio = st.sidebar.number_input("Minimum volume ratio", min_value=0.0, value=0.0, step=0.1)
only_book_ready = st.sidebar.checkbox("Order book ready only", value=False)
show_acceleration = st.sidebar.checkbox("Show score acceleration", value=True)

view = df[df["v15_score"] >= min_score].copy()
if "stage" in view.columns and selected_stages:
    view = view[view["stage"].isin(selected_stages)]
if "volume_ratio" in view.columns:
    view = view[view["volume_ratio"].fillna(0) >= min_volume_ratio]
if only_book_ready and "book_ready" in view.columns:
    view = view[view["book_ready"] == True]

history_path = "data/history.jsonl"
history_records = []
history_text = None
if os.path.exists(history_path):
    try:
        with open(history_path, errors="ignore") as hf:
            history_text = hf.read()
    except Exception:
        history_text = None
if history_text is None:
    try:
        with urllib.request.urlopen(RAW_BASE + "history.jsonl?t=" + str(int(pd.Timestamp.utcnow().timestamp())), timeout=10) as resp:
            history_text = resp.read().decode("utf-8", errors="ignore")
    except Exception:
        history_text = None
if history_text:
    for line in history_text.splitlines():
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

def velocity_for(symbol):
    points = score_history.get(str(symbol), [])
    if len(points) < 2:
        return 0.0
    p0, p1 = points[-2], points[-1]
    try:
        hours = max((float(p1["ts"]) - float(p0["ts"])) / 3600.0, 1/60)
    except Exception:
        hours = 0.25
    return (p1["score"] - p0["score"]) / hours

def recent_signal_info(symbol):
    points = score_history.get(str(symbol), [])
    if len(points) < 2:
        return 0.0, "", ""
    prev, curr = points[-2], points[-1]
    delta = curr["score"] - prev["score"]
    prev_stage = str(prev.get("stage", ""))
    curr_stage = str(curr.get("stage", ""))
    return delta, prev_stage, curr_stage

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

if "symbol" in df.columns:
    accel = df["symbol"].map(lambda s: acceleration_for(s)[0])
    accel5 = df["symbol"].map(lambda s: acceleration_for(s)[1])
    df["score_delta"] = accel
    df["score_delta_5"] = accel5
    df["score_velocity"] = df["symbol"].map(velocity_for)
    df["acceleration"] = df["score_delta"].map(
        lambda x: "🔥 SURGING" if x >= 5 else "🟢 RISING" if x > 0 else "🟡 STABLE" if x == 0 else "🔴 FALLING"
    )
    # Compare current volume ratio with the previous published scan.
    volume_lookup = {}
    for item in history_records[-120:]:
        for r in item.get("rows", []):
            sym = str(r.get("symbol", ""))
            if sym:
                volume_lookup.setdefault(sym, []).append(float(r.get("volume_ratio", 0) or 0))
    def volume_accel(symbol):
        vals = volume_lookup.get(str(symbol), [])
        if len(vals) < 2:
            return 0.0
        return vals[-1] - vals[-2]
    df["volume_accel"] = df["symbol"].map(volume_accel)

updated = d.get("updated", 0)
try:
    last_scan = pd.to_datetime(updated, unit="s").strftime("%H:%M:%S UTC")
except Exception:
    last_scan = "Unknown"

active = df[df["score"] >= min_score]
early = df[df.get("stage", pd.Series(index=df.index, dtype=str)).isin(["PRE-PUMP", "EARLY MOMENTUM"])]
breakouts = df[df.get("stage", pd.Series(index=df.index, dtype=str)).isin(["BREAKOUT", "CONFIRMED PUMP"])]
building = df[df.get("stage", pd.Series(index=df.index, dtype=str)) == "BUILDING"]

c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
c1.metric("🔥 Active Signals", len(active))
c2.metric("⚡ Pre-Pump / Early", len(early))
c3.metric("🚀 Breakouts", len(breakouts))
c4.metric("👀 Watch", len(building))
c5.metric("🏆 Top V15", f"{df['v15_score'].max():.0f}")
c6.metric("⚠️ Exhaustion", int((df.get("exhaustion_state", pd.Series(index=df.index)).eq("EXHAUSTION ALERT")).sum()))
c7.metric("🕒 Last Scan", last_scan)

if "book_status" in df.columns:
    ready_count = int((df["book_status"] == "🟢 READY").sum())
    syncing_count = int((df["book_status"] != "🟢 READY").sum())
    st.caption(f"📚 Order books: **{ready_count} ready** • **{syncing_count} syncing/resyncing**")

st.success(f"V15 scanner data loaded: {len(df)} symbols • source: {data_source} • top V15 score: {df['v15_score'].max():.0f}")

st.subheader("📡 Live Scanner Snapshot")
st.dataframe(df.head(20), use_container_width=True, hide_index=True)

st.subheader("🎯 Entry Signal Monitor")
st.caption("V15 is authoritative here: PRE-PUMP/EARLY_PUMP indicate opportunity; CONFIRMED requires stronger confirmation. Legacy V11/V12 values are supporting diagnostics.")

entry_df = df.copy()
for c in ["v15_score","v15_opportunity_score","v15_confirmation_score","v15_streak","v15_relative_strength_5m","v15_relative_strength_15m","price_1m","volume_ratio","buy_pressure"]:
    if c in entry_df.columns:
        entry_df[c] = pd.to_numeric(entry_df[c], errors="coerce").fillna(0)

entry_df["entry_signal"] = "⚪ V15 WATCH"
entry_df.loc[entry_df.get("v15_early_candidate", pd.Series(False, index=entry_df.index)).astype(bool), "entry_signal"] = "🟡 V15 PRE-PUMP"
entry_df.loc[entry_df.get("v15_confirmed", pd.Series(False, index=entry_df.index)).astype(bool), "entry_signal"] = "🟢 V15 CONFIRMED"
entry_df.loc[entry_df.get("v15_stage", pd.Series("", index=entry_df.index)).eq("AVOID"), "entry_signal"] = "🔴 V15 AVOID"
entry_df.loc[
    (entry_df.get("stage", pd.Series("", index=entry_df.index)).isin(["BUILDING"])) &
    (entry_df["score"] >= 20),
    "entry_signal"
] = "🟡 WATCH"
entry_df.loc[
    (entry_df.get("stage", pd.Series("", index=entry_df.index)).isin(["PRE-PUMP"])) &
    (entry_df["score"] >= 70) &
    (entry_df.get("hybrid_score", pd.Series(0, index=entry_df.index)) >= 70) &
    (entry_df.get("v12_confirmation", pd.Series(False, index=entry_df.index)).astype(bool)) &
    (entry_df.get("v12_efficiency", pd.Series(0, index=entry_df.index)) >= 0.35) &
    (entry_df["price_1m"] < 5),
    "🟢 ENTRY ZONE"
] = "🟢 ENTRY ZONE"
entry_df.loc[
    (entry_df.get("stage", pd.Series("", index=entry_df.index)).isin(["EARLY MOMENTUM"])) &
    (entry_df["score"] >= 70) &
    (entry_df.get("hybrid_score", pd.Series(0, index=entry_df.index)) >= 70) &
    (entry_df.get("v12_confirmation", pd.Series(False, index=entry_df.index)).astype(bool)) &
    (entry_df.get("v12_efficiency", pd.Series(0, index=entry_df.index)) >= 0.35) &
    (entry_df["price_1m"] < 5),
    "🟢 CONFIRMATION ENTRY"
] = "🟢 CONFIRMATION ENTRY"
entry_df.loc[
    (entry_df["price_1m"] >= 5) | (entry_df["score"] >= 92),
    "entry_signal"
] = "🔴 CHASE / WAIT"

entry_view = entry_df[~entry_df["entry_signal"].eq("🔴 V15 AVOID")].copy()
entry_view = entry_view.sort_values(["v15_score","v15_confirmation_score","v15_opportunity_score"], ascending=[False,False,False]).head(20)
entry_cols = [c for c in ["symbol","entry_signal","v15_score","v15_opportunity_score","v15_confirmation_score","v15_stage","v15_streak","v15_relative_strength_5m","v15_relative_strength_15m","v15_btc_risk_off","v11_score","v12_score","v12_confirmation","v12_efficiency","price_1m","volume_ratio","buy_pressure","book_ready"] if c in entry_view.columns]
st.dataframe(entry_view[entry_cols], use_container_width=True, hide_index=True, column_config={
    "v15_score": st.column_config.ProgressColumn("V15", min_value=0, max_value=100, format="%d"),
    "v15_opportunity_score": st.column_config.ProgressColumn("Opportunity", min_value=0, max_value=100, format="%d"),
    "v15_confirmation_score": st.column_config.ProgressColumn("Confirmation", min_value=0, max_value=100, format="%d"),
    "v11_score": st.column_config.NumberColumn("V11", format="%.0f"),
    "v12_score": st.column_config.NumberColumn("V12", format="%.0f"),
    "v12_efficiency": st.column_config.NumberColumn("Efficiency", format="%.2f"),
    "price_1m": st.column_config.NumberColumn("1m %", format="%.2f"),
    "volume_ratio": st.column_config.NumberColumn("Volume", format="%.2fx"),
    "buy_pressure": st.column_config.NumberColumn("Buy %", format="%.1f%%"),
})

st.divider()

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "🔥 Live Radar", "🟣 Accumulation", "⚡ Pre-Pump", "🗺️ Heatmap", "🔎 Coin Analysis", "📜 History", "⚠️ Exhaustion"
])

def format_signal_table(frame):
    cols = [
        "symbol", "price", "v15_score", "v15_opportunity_score", "v15_confirmation_score",
        "v15_stage", "v15_streak", "v15_relative_strength_5m", "v15_relative_strength_15m",
        "v15_btc_risk_off", "v15_early_candidate", "v15_confirmed", "exhaustion_score", "exhaustion_state",
        "score", "stage", "book_status", "entry", "sell",
        "price_1m", "price_10s", "volume_ratio", "trade_accel",
        "buy_pressure", "book_imbalance", "spread_bps", "book_ready",
        "accumulation_score", "accumulation_stage", "accum_buy_pressure", "accum_trade_accel", "accum_volume_ratio", "accum_book_imbalance",
        "score_delta", "score_delta_5", "score_velocity", "volume_accel", "acceleration", "chase_risk", "exhaustion_score", "exhaustion_state", "exhaustion_extension", "exhaustion_rollover"
    ]
    cols = list(dict.fromkeys(c for c in cols if c in frame.columns))
    x = frame.loc[:, cols].copy()
    if "buy_pressure" in x.columns:
        x["buy_pressure"] = x["buy_pressure"] * 100
    if x.columns.duplicated().any():
        x = x.loc[:, ~x.columns.duplicated()].copy()
    return x

with tab1:
    st.subheader("🔥 Live Pump Radar")
    st.caption("Ranked by V15 score. Opportunity detects developing setups; confirmation validates stronger structure. V11/V12 remain supporting evidence.")

    radar = view.head(20).copy()
    if radar.empty:
        # Keep the Live Pump Radar populated even when the sidebar filters are too strict.
        # The filtered count remains visible in Active Signals above.
        radar = df.head(20).copy()
        st.warning("No symbols match the current filters — showing the top 20 live symbols instead. Adjust the sidebar filters to narrow the radar.")
    if not radar.empty:
        st.dataframe(
            format_signal_table(radar),
            use_container_width=True,
            height=620,
            hide_index=True,
            column_config={
                "v15_score": st.column_config.ProgressColumn("V15", min_value=0, max_value=100, format="%d"),
                "v15_opportunity_score": st.column_config.ProgressColumn("Opportunity", min_value=0, max_value=100, format="%d"),
                "v15_confirmation_score": st.column_config.ProgressColumn("Confirmation", min_value=0, max_value=100, format="%d"),
                "buy_pressure": st.column_config.NumberColumn("Buy %", format="%.1f%%"),
                "volume_ratio": st.column_config.NumberColumn("Vol Ratio", format="%.2fx"),
                "price_1m": st.column_config.NumberColumn("1m %", format="%.2f"),
                "price_10s": st.column_config.NumberColumn("10s %", format="%.2f"),
                "book_imbalance": st.column_config.NumberColumn("Book Imb", format="%.2f"),
                "score_delta": st.column_config.NumberColumn("Δ Score", format="%+.0f"),
                "score_delta_5": st.column_config.NumberColumn("5-Scan Δ", format="%+.0f"),
                "score_velocity": st.column_config.NumberColumn("Score/hr", format="%+.1f"),
                "volume_accel": st.column_config.NumberColumn("Vol Δ", format="%+.2fx"),
                "chase_risk": st.column_config.TextColumn("Entry Risk"),
                "book_status": st.column_config.TextColumn("Order Book"),
                "exhaustion_score": st.column_config.ProgressColumn("Exhaustion", min_value=0, max_value=100, format="%d"),
                "exhaustion_extension": st.column_config.NumberColumn("Extension %", format="%.2f"),
                "exhaustion_rollover": st.column_config.NumberColumn("Rollover", format="%.0f"),
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

    # True momentum ranking, independent of the score ordering.
    st.subheader("🚨 Newly Detected Signals")
    st.caption("Signals that have accelerated since the previous published scan. These are shown independently of the sidebar minimum-score filter.")

    new_rows = []
    for _, row in df.iterrows():
        delta, prev_stage, curr_stage = recent_signal_info(row.get("symbol", ""))
        if delta >= 5 or (prev_stage != curr_stage and curr_stage in ["PRE-PUMP", "EARLY MOMENTUM", "BREAKOUT", "CONFIRMED PUMP"]):
            new_rows.append({
                "symbol": row.get("symbol", ""),
                "score": row.get("score", 0),
                "score_delta": delta,
                "score_velocity": row.get("score_velocity", 0),
                "stage": row.get("stage", ""),
                "previous_stage": prev_stage,
                "volume_ratio": row.get("volume_ratio", 0),
                "volume_accel": row.get("volume_accel", 0),
            })
    new_signals = pd.DataFrame(new_rows).sort_values(["score_delta", "score"], ascending=False).head(10) if new_rows else pd.DataFrame()

    if new_signals.empty:
        st.info("No newly accelerated signals detected in the latest scan interval.")
    else:
        st.dataframe(
            new_signals,
            use_container_width=True,
            hide_index=True,
            column_config={
                "score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%d"),
                "score_delta": st.column_config.NumberColumn("Δ Score", format="%+.0f"),
                "score_velocity": st.column_config.NumberColumn("Score/hr", format="%+.1f"),
                "volume_ratio": st.column_config.NumberColumn("Volume", format="%.2fx"),
                "volume_accel": st.column_config.NumberColumn("Vol Δ", format="%+.2fx"),
            },
        )

    st.subheader("🎯 Best Early Pump Setups")
    st.caption("Developing signals with strengthening momentum, volume and buying pressure while penalising already-extended moves.")

    early = df.copy()
    def bounded_series(series, low=None, high=None):
        x = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
        if low is None:
            low = float(x.quantile(0.05)) if len(x) else 0.0
        if high is None:
            high = float(x.quantile(0.95)) if len(x) else 1.0
        if high <= low:
            high = low + 1.0
        return ((x.clip(low, high) - low) / (high - low) * 100).clip(0, 100)

    early["e_score_accel"] = bounded_series(early.get("score_delta_5", pd.Series(0, index=early.index)), 0, max(5.0, float(early.get("score_delta_5", pd.Series(0)).quantile(0.95) if len(early) else 5.0)))
    early["e_volume_accel"] = bounded_series(early.get("volume_accel", pd.Series(0, index=early.index)), 0, max(1.0, float(early.get("volume_accel", pd.Series(0)).quantile(0.95) if len(early) else 1.0)))
    early["e_buy"] = bounded_series(early.get("buy_pressure", pd.Series(0.5, index=early.index)), 0, 1)
    early["e_trade"] = bounded_series(early.get("trade_accel", pd.Series(0, index=early.index)))
    early["e_book"] = bounded_series(early.get("book_imbalance", pd.Series(0, index=early.index)), -1, 1)
    early["e_1m"] = bounded_series(early.get("price_1m", pd.Series(0, index=early.index)))
    early["e_10s"] = bounded_series(early.get("price_10s", pd.Series(0, index=early.index)))

    extension = (
        (pd.to_numeric(early.get("price_1m", pd.Series(0, index=early.index)), errors="coerce").fillna(0).clip(lower=0) / 5 * 100)
        .clip(0, 100)
    )
    early["early_entry_quality"] = (
        early["e_score_accel"] * 0.20 +
        early["e_volume_accel"] * 0.20 +
        early["e_buy"] * 0.15 +
        early["e_trade"] * 0.15 +
        early["e_book"] * 0.10 +
        early["e_1m"] * 0.10 +
        early["e_10s"] * 0.05 +
        (100 - extension) * 0.05
    )
    early["chase_risk"] = np.where(
        (pd.to_numeric(early.get("price_1m", pd.Series(0, index=early.index)), errors="coerce").fillna(0) >= 5) |
        (pd.to_numeric(early.get("score", 0), errors="coerce").fillna(0) >= 92),
        "🔴 CHASING",
        np.where(
            (pd.to_numeric(early.get("price_1m", pd.Series(0, index=early.index)), errors="coerce").fillna(0) >= 3) |
            (pd.to_numeric(early.get("score", 0), errors="coerce").fillna(0) >= 82),
            "🟡 EXTENDED",
            "🟢 EARLY"
        )
    )

    early = early[
        (pd.to_numeric(early.get("score", 0), errors="coerce").fillna(0) >= 20) &
        (early.get("stage", pd.Series("", index=early.index)).isin(["BUILDING", "PRE-PUMP", "EARLY MOMENTUM", "BREAKOUT"]))
    ].sort_values(["early_entry_quality", "score_delta_5"], ascending=False).head(10)

    if early.empty:
        st.info("No developing early-pump setups are available yet.")
    else:
        early_display = early[[
            "symbol", "early_entry_quality", "score", "stage", "chase_risk",
            "score_delta", "score_delta_5", "score_velocity",
            "price_1m", "volume_ratio", "volume_accel", "buy_pressure",
            "trade_accel", "book_imbalance"
        ]].copy()
        early_display["buy_pressure"] = early_display["buy_pressure"] * 100

        st.dataframe(
            early_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "early_entry_quality": st.column_config.ProgressColumn("Entry Quality", min_value=0, max_value=100, format="%.0f"),
                "score": st.column_config.ProgressColumn("Pump Score", min_value=0, max_value=100, format="%d"),
                "score_delta": st.column_config.NumberColumn("Δ Score", format="%+.0f"),
                "score_delta_5": st.column_config.NumberColumn("5-Scan Δ", format="%+.0f"),
                "score_velocity": st.column_config.NumberColumn("Score/hr", format="%+.1f"),
                "price_1m": st.column_config.NumberColumn("1m %", format="%.2f"),
                "volume_ratio": st.column_config.NumberColumn("Volume", format="%.2fx"),
                "volume_accel": st.column_config.NumberColumn("Vol Δ", format="%+.2fx"),
                "buy_pressure": st.column_config.NumberColumn("Buy %", format="%.1f%%"),
                "trade_accel": st.column_config.NumberColumn("Trade Accel", format="%.2fx"),
                "book_imbalance": st.column_config.NumberColumn("Book Imb", format="%.2f"),
            },
        )

    st.subheader("📈 Strongest Momentum")
    st.caption("Market-wide ranking of current momentum, independent of the sidebar minimum-score filter.")

    momentum = df.copy()
    momentum["m_score"] = bounded_series(momentum["score"], 0, 100)
    momentum["m_1m"] = bounded_series(momentum.get("price_1m", pd.Series(0, index=momentum.index)))
    momentum["m_10s"] = bounded_series(momentum.get("price_10s", pd.Series(0, index=momentum.index)))
    momentum["m_vol"] = bounded_series(momentum.get("volume_ratio", pd.Series(0, index=momentum.index)), 0, max(2.0, float(momentum.get("volume_ratio", pd.Series(0)).quantile(0.95) if len(momentum) else 2.0)))
    momentum["m_trade"] = bounded_series(momentum.get("trade_accel", pd.Series(0, index=momentum.index)))
    momentum["m_buy"] = bounded_series(momentum.get("buy_pressure", pd.Series(0.5, index=momentum.index)), 0, 1)
    momentum["m_book"] = bounded_series(momentum.get("book_imbalance", pd.Series(0, index=momentum.index)), -1, 1)
    momentum["m_accel"] = bounded_series(momentum.get("score_delta_5", pd.Series(0, index=momentum.index)))
    momentum["momentum_score"] = (
        momentum["m_score"] * 0.25 +
        momentum["m_1m"] * 0.15 +
        momentum["m_10s"] * 0.10 +
        momentum["m_vol"] * 0.15 +
        momentum["m_trade"] * 0.10 +
        momentum["m_buy"] * 0.10 +
        momentum["m_book"] * 0.05 +
        momentum["m_accel"] * 0.10
    )
    momentum = momentum[pd.to_numeric(momentum["score"], errors="coerce").fillna(0) >= 20].sort_values("momentum_score", ascending=False).head(10)

    if momentum.empty:
        st.info("No sufficient momentum data is available yet.")
    else:
        momentum_display = momentum[[
            "symbol", "momentum_score", "score", "stage",
            "price_1m", "price_10s", "volume_ratio", "trade_accel",
            "buy_pressure", "book_imbalance", "score_delta_5"
        ]].copy()
        momentum_display["buy_pressure"] = momentum_display["buy_pressure"] * 100

        st.dataframe(
            momentum_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "momentum_score": st.column_config.ProgressColumn("Momentum", min_value=0, max_value=100, format="%.0f"),
                "score": st.column_config.ProgressColumn("Pump Score", min_value=0, max_value=100, format="%d"),
                "price_1m": st.column_config.NumberColumn("1m %", format="%.2f"),
                "price_10s": st.column_config.NumberColumn("10s %", format="%.2f"),
                "volume_ratio": st.column_config.NumberColumn("Volume", format="%.2fx"),
                "trade_accel": st.column_config.NumberColumn("Trade Accel", format="%.2fx"),
                "buy_pressure": st.column_config.NumberColumn("Buy %", format="%.1f%%"),
                "book_imbalance": st.column_config.NumberColumn("Book Imb", format="%.2f"),
                "score_delta_5": st.column_config.NumberColumn("5-Scan Δ", format="%+.0f"),
            },
        )

        chart = momentum.set_index("symbol")[["momentum_score"]].sort_values("momentum_score")
        st.bar_chart(chart, height=320)

with tab2:
    st.subheader("🟣 Accumulation / Pre-Pump Detector")
    st.caption("Designed to detect participation building before the normal Pump Score reaches 45. It emphasizes buying pressure, trade acceleration, rising activity and limited price extension.")

    accum = df.copy()
    if "accumulation_score" in accum.columns:
        accum["accumulation_score"] = pd.to_numeric(accum["accumulation_score"], errors="coerce").fillna(0)
        accum = accum[accum.get("accumulation_quality", False) == True].sort_values(["accumulation_score","score"], ascending=False).head(20)
    else:
        accum = pd.DataFrame()

    if accum.empty:
        st.info("No high-quality accumulation setups detected in the latest scan.")
    else:
        ad = accum[[c for c in ["symbol","accumulation_score","accumulation_stage","score","stage","price_1m","accum_buy_pressure","accum_trade_accel","accum_volume_ratio","accum_book_imbalance","accum_trades_10s"] if c in accum.columns]].copy()
        if "accum_buy_pressure" in ad.columns:
            ad["accum_buy_pressure"] = ad["accum_buy_pressure"] * 100
        st.dataframe(ad, use_container_width=True, hide_index=True, column_config={
            "accumulation_score": st.column_config.ProgressColumn("Accumulation", min_value=0, max_value=100, format="%d"),
            "score": st.column_config.ProgressColumn("Pump Score", min_value=0, max_value=100, format="%d"),
            "price_1m": st.column_config.NumberColumn("1m %", format="%.2f"),
            "accum_buy_pressure": st.column_config.NumberColumn("Buy %", format="%.1f%%"),
            "accum_trade_accel": st.column_config.NumberColumn("Trade Accel", format="%.2fx"),
            "accum_volume_ratio": st.column_config.NumberColumn("Vol Ratio", format="%.2fx"),
            "accum_book_imbalance": st.column_config.NumberColumn("Book Imb", format="%+.2f"),
        })

    st.info("🟣 **How to read it:** ACCUMULATION WATCH = participation is building; ACCUMULATION ALERT = stronger pre-pump structure. It is an early-warning signal, not confirmation of a pump.")

with tab3:
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

with tab4:
    st.subheader("🗺️ Binance Pump Heatmap")
    st.caption("Visual intensity view across score, short-term momentum, volume expansion, buying pressure and score acceleration.")

    heat = df.copy().head(60)
    heat["price_1m"] = pd.to_numeric(heat.get("price_1m", 0), errors="coerce").fillna(0)
    heat["volume_ratio"] = pd.to_numeric(heat.get("volume_ratio", 0), errors="coerce").fillna(0)
    heat["buy_pressure"] = pd.to_numeric(heat.get("buy_pressure", 0), errors="coerce").fillna(0)
    heat["score_delta_5"] = pd.to_numeric(heat.get("score_delta_5", 0), errors="coerce").fillna(0)

    heat["Momentum"] = bounded_series(heat["price_1m"])
    heat["Volume Intensity"] = bounded_series(heat["volume_ratio"], 0, max(2.0, float(heat["volume_ratio"].quantile(0.95) if len(heat) else 2.0)))
    heat["Buy Intensity"] = heat["buy_pressure"].clip(0, 1) * 100
    heat["Score Accel"] = bounded_series(heat["score_delta_5"], 0, max(5.0, float(heat["score_delta_5"].quantile(0.95) if len(heat) else 5.0)))

    if not heat.empty:
        heat_display = heat[[
            "symbol", "score", "stage", "Momentum",
            "Volume Intensity", "Buy Intensity", "Score Accel", "price_1m", "volume_ratio"
        ]].copy()

        st.dataframe(
            heat_display,
            use_container_width=True,
            height=700,
            hide_index=True,
            column_config={
                "score": st.column_config.ProgressColumn("Pump Score", min_value=0, max_value=100, format="%d"),
                "Momentum": st.column_config.ProgressColumn("Price Momentum", min_value=0, max_value=100, format="%.0f"),
                "Volume Intensity": st.column_config.ProgressColumn("Volume Intensity", min_value=0, max_value=100, format="%.0f"),
                "Buy Intensity": st.column_config.ProgressColumn("Buy Pressure", min_value=0, max_value=100, format="%.0f%%"),
                "Score Accel": st.column_config.ProgressColumn("Score Accel", min_value=0, max_value=100, format="%.0f"),
                "price_1m": st.column_config.NumberColumn("1m %", format="%.2f"),
                "volume_ratio": st.column_config.NumberColumn("Volume", format="%.2fx"),
            },
        )


with tab5:
    st.subheader("🔎 Coin Analysis")
    symbols = df["symbol"].astype(str).tolist()
    selected = st.selectbox("Select coin", symbols)

    coin = df[df["symbol"].astype(str) == selected].iloc[0]

    score = float(coin.get("score", 0))
    stage = str(coin.get("stage", "UNKNOWN"))
    price = coin.get("price", np.nan)

    a, b, c, e = st.columns(4)
    a.metric("V15 Score", f"{score:.0f}/100")
    b.metric("Stage", stage)
    c.metric("Price", f"{price:g}" if pd.notna(price) else "—")
    e.metric("Volume Ratio", f"{float(coin.get('volume_ratio', 0)):.2f}x")

    if "exhaustion_score" in coin.index:
        ex1, ex2, ex3 = st.columns(3)
        ex1.metric("⚠️ Exhaustion", f"{float(coin.get('exhaustion_score',0)):.0f}/100")
        ex2.metric("Exhaustion State", str(coin.get("exhaustion_state","NORMAL")))
        ex3.metric("Extension", f"{float(coin.get('exhaustion_extension',0)):.2f}%")
        if str(coin.get("exhaustion_state","NORMAL")) == "EXHAUSTION ALERT":
            st.warning("⚠️ Momentum exhaustion alert: extension and deterioration symptoms are elevated.")

    if "accumulation_score" in coin.index:
        st.metric("🟣 Accumulation Score", f"{float(coin.get('accumulation_score', 0)):.0f}/100")
        st.caption(f"Accumulation status: **{coin.get('accumulation_stage', 'MONITOR')}**")

    st.progress(min(max(int(score), 0), 100), text=f"V15 Score {score:.0f}/100")

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
    lifecycle = ["NEUTRAL", "WATCH", "PRE_PUMP", "EARLY_PUMP", "CONFIRMED", "AVOID"]
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

    st.subheader("🧠 V15 Signal Breakdown")
    v15_metrics = pd.DataFrame({
        "Component": ["Opportunity", "Confirmation", "Stage", "Streak", "RS 5m", "RS 15m", "BTC Risk-Off", "Early Candidate", "Confirmed"],
        "Value": [
            coin.get("v15_opportunity_score", 0), coin.get("v15_confirmation_score", 0),
            coin.get("v15_stage", "—"), coin.get("v15_streak", 0),
            coin.get("v15_relative_strength_5m", 0), coin.get("v15_relative_strength_15m", 0),
            coin.get("v15_btc_risk_off", False), coin.get("v15_early_candidate", False),
            coin.get("v15_confirmed", False)
        ]
    })
    st.dataframe(v15_metrics, use_container_width=True, hide_index=True)

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

with tab7:
    st.subheader("⚠️ Exhaustion Momentum Alert")
    st.caption("Detects an extended momentum move that is beginning to lose follow-through. This is a reversal-risk warning, not an automatic short or sell signal.")

    ex = df.copy()
    for col in ["exhaustion_score","exhaustion_extension","exhaustion_rollover","exhaustion_buy_stress","exhaustion_book_stress","exhaustion_rs_fade","exhaustion_efficiency_stress","exhaustion_symptoms","exhaustion_volume_ratio","exhaustion_trade_accel","v15_score"]:
        if col in ex.columns:
            ex[col] = pd.to_numeric(ex[col], errors="coerce").fillna(0)

    if "exhaustion_state" not in ex.columns:
        st.warning("Exhaustion data is not present in the latest scanner run. Run the updated V15 workflow first.")
    else:
        alerts = ex[ex["exhaustion_state"].eq("EXHAUSTION ALERT")].sort_values(["exhaustion_score","v15_score"], ascending=False)
        watches = ex[ex["exhaustion_state"].eq("EXHAUSTION WATCH")].sort_values(["exhaustion_score","v15_score"], ascending=False)
        extended = ex[ex["exhaustion_state"].eq("EXTENDED / MONITOR")].sort_values(["exhaustion_score","v15_score"], ascending=False)

        e1,e2,e3,e4 = st.columns(4)
        e1.metric("🚨 Alert", len(alerts))
        e2.metric("🟠 Watch", len(watches))
        e3.metric("🟡 Extended", len(extended))
        e4.metric("Highest Exhaustion", f"{ex['exhaustion_score'].max():.0f}/100")

        st.markdown("### 🚨 Active Exhaustion Alerts")
        if alerts.empty:
            st.success("No active exhaustion alerts in the latest scan.")
        else:
            display = alerts[[c for c in ["symbol","exhaustion_score","exhaustion_state","v15_score","v15_stage","exhaustion_extension","exhaustion_rollover","exhaustion_buy_stress","exhaustion_book_stress","exhaustion_rs_fade","exhaustion_symptoms","price_1m","price_10s","volume_ratio","trade_accel","buy_pressure","book_imbalance"] if c in alerts.columns]].copy()
            if "buy_pressure" in display.columns:
                display["buy_pressure"] *= 100
            st.dataframe(display, use_container_width=True, hide_index=True, column_config={
                "exhaustion_score": st.column_config.ProgressColumn("Exhaustion", min_value=0, max_value=100, format="%d"),
                "v15_score": st.column_config.ProgressColumn("V15", min_value=0, max_value=100, format="%d"),
                "exhaustion_extension": st.column_config.NumberColumn("Extension %", format="%.2f"),
                "exhaustion_rollover": st.column_config.NumberColumn("Rollover", format="%.0f"),
                "exhaustion_buy_stress": st.column_config.NumberColumn("Buy Stress", format="%.0f"),
                "exhaustion_book_stress": st.column_config.NumberColumn("Book Stress", format="%.0f"),
                "exhaustion_rs_fade": st.column_config.NumberColumn("RS Fade", format="%.0f"),
                "buy_pressure": st.column_config.NumberColumn("Buy %", format="%.1f%%"),
                "price_1m": st.column_config.NumberColumn("1m %", format="%.2f"),
                "price_10s": st.column_config.NumberColumn("10s %", format="%.2f"),
                "volume_ratio": st.column_config.NumberColumn("Volume", format="%.2fx"),
                "trade_accel": st.column_config.NumberColumn("Trade Accel", format="%.2fx"),
                "book_imbalance": st.column_config.NumberColumn("Book Imbalance", format="%+.2f"),
            })

        st.markdown("### 🟠 Exhaustion Watch")
        watch_display = ex[ex["exhaustion_state"].isin(["EXHAUSTION WATCH","EXTENDED / MONITOR"])].head(20)
        if watch_display.empty:
            st.info("No extended momentum setups currently require monitoring.")
        else:
            st.dataframe(watch_display[[c for c in ["symbol","exhaustion_score","exhaustion_state","v15_score","v15_stage","exhaustion_extension","exhaustion_rollover","exhaustion_symptoms","price_1m","volume_ratio","buy_pressure","book_imbalance"] if c in watch_display.columns]], use_container_width=True, hide_index=True, column_config={
                "exhaustion_score": st.column_config.ProgressColumn("Exhaustion", min_value=0, max_value=100, format="%d"),
                "v15_score": st.column_config.ProgressColumn("V15", min_value=0, max_value=100, format="%d"),
                "exhaustion_extension": st.column_config.NumberColumn("Extension %", format="%.2f"),
                "exhaustion_rollover": st.column_config.NumberColumn("Rollover", format="%.0f"),
                "buy_pressure": st.column_config.NumberColumn("Buy %", format="%.1f%%"),
                "price_1m": st.column_config.NumberColumn("1m %", format="%.2f"),
                "volume_ratio": st.column_config.NumberColumn("Volume", format="%.2fx"),
                "book_imbalance": st.column_config.NumberColumn("Book Imbalance", format="%+.2f"),
            })

        st.info("How to read it: **Exhaustion Alert** means the move is extended and multiple deterioration symptoms are present. Use it alongside price structure and confirmation; it does not predict a reversal by itself.")

with tab6:
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
st.caption(f"Dashboard runs on V15 as the primary model • V11/V12 are supporting diagnostics • Refreshes every 15 seconds • Showing {len(df)} scanned symbols • Historical acceleration uses the latest published scan records • Last scan: {last_scan}")
