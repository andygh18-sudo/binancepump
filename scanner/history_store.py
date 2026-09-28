import json
from pathlib import Path
from time import time

HISTORY_ROOT = Path("data/history")
MICROSTRUCTURE_ROOT = Path("data/microstructure")
MICROSTRUCTURE_SAMPLE_INTERVAL = 15.0

# One historical sample every 5 minutes gives useful multi-day coverage while
# keeping each symbol file comfortably small enough for GitHub/API retrieval.
HISTORY_FIELDS = [
    "symbol","price","price_1m","price_10s","price_60s",
    "score","stage","entry","sell",
    "volume_ratio","trade_accel","buy_pressure","book_imbalance","spread_bps","book_ready",
    "early_pump_score","early_pump_stage","early_pump_quality",
    "relative_strength_5m","relative_strength_15m","btc_ret_5m","btc_ret_15m","false_positive_penalty",
    "accumulation_score","accumulation_stage","accumulation_quality","accum_buy_pressure",
    "accum_trade_accel","accum_volume_ratio","accum_book_imbalance","accum_price_10s","accum_trades_10s",
    "v15_model","v15_alert","v15_score","v15_stage","v15_opportunity_score","v15_confirmation_score",
    "v15_early_candidate","v15_confirmed","v15_streak","v15_btc_risk_off",
    "v15_relative_strength_5m","v15_relative_strength_15m",
    "v15_regime","v15_reignition_score","v15_reignition_watch",
    "v15_reignition_bridge_score","v15_reignition_bridge_stage","v15_reignition_bridge_armed",
    "v15_reignition_bridge_trigger","v15_reignition_bridge_price_60s","v15_reignition_bridge_accel",
    "v15_reignition_bridge_volume_ratio","v15_reignition_bridge_buy_pressure","v15_reignition_bridge_accel_slope",
    "v15_tv_score","v15_tv_adjustment","tv_confirmation","tv_bullish_timeframes",
    "tv_30m_rsi","tv_1h_rsi","tv_4h_rsi","tv_1d_rsi","tv_1w_rsi","tv_1m_rsi",
    "v15_ignition_score","v15_ignition_stage","v15_ignition_alert","v15_ignition_signals",
    "v15_ignition_confirmations","v15_ignition_accel","v15_trade_accel_slope","v15_buy_pressure_slope",
    "v15_ignition_rs5","v15_ignition_rs15","v15_ignition_volume_ratio","v15_ignition_trades_10s",
    "v15_ignition_samples_5m","v15_ignition_window_seconds","v15_ignition_score_delta_5m",
    "v15_trade_accel_delta_5m","v15_buy_pressure_delta_5m","v15_ignition_rs5_delta_5m",
    "v15_ignition_price_change_5m","v15_ignition_rising_ratio_5m","v15_ignition_persistence_5m",
    "v15_ignition_early_samples_5m","v15_ignition_trajectory_score","v15_ignition_trajectory_stage",
    "v15_ignition_trajectory_confirmed",
    "v12_score","v12_efficiency","hybrid_score","hybrid_path","hybrid_grade",
    "exhaustion_score","exhaustion_state","exhaustion_alert","exhaustion_extension",
    "exhaustion_rollover","exhaustion_symptoms",
    "v154_score","v154_early_buy","v154_stage","v154_participation_gate","v154_price_gate","v154_structure_gate","v154_persistence_status","v154_persistence_failures","v154_persistence_seconds","v154_exhaustion_veto","v154_disqualifiers","v154_book_imbalance","v154_spread_bps","v154_reignition_bonus","v154_alert","v154_event_id"
]

def _record(row, ts):
    return {"ts": float(ts), **{k: row.get(k) for k in HISTORY_FIELDS if k in row}}

def append_microstructure_history(rows, ts=None):
    """Append 15-second V15.3 microstructure samples only for armed/high-score symbols."""
    MICROSTRUCTURE_ROOT.mkdir(parents=True, exist_ok=True)
    stamp = time() if ts is None else float(ts)
    fields = [
        "symbol","price","price_10s","price_60s","volume_ratio","trade_accel","buy_pressure",
        "book_imbalance","spread_bps","score","v15_score","v15_stage","v15_opportunity_score",
        "v15_confirmation_score","v15_btc_risk_off","v15_regime","v15_reignition_score",
        "v15_reignition_watch","v15_reignition_bridge_score","v15_reignition_bridge_stage",
        "v15_reignition_bridge_armed","v15_reignition_bridge_trigger","v15_reignition_bridge_price_60s",
        "v15_reignition_bridge_accel","v15_reignition_bridge_volume_ratio",
        "v15_reignition_bridge_buy_pressure","v15_reignition_bridge_accel_slope",
        "v15_ignition_score","v15_ignition_stage","v15_ignition_signals","v15_ignition_confirmations",
        "v15_ignition_accel","v15_trade_accel_slope","v15_buy_pressure_slope","v15_ignition_rs5",
        "v15_ignition_rs15","v15_ignition_trades_10s","v15_ignition_trajectory_score",
        "accumulation_score","tv_confirmation","tv_bullish_timeframes",
        "v154_score","v154_early_buy","v154_stage","v154_participation_gate","v154_price_gate","v154_structure_gate","v154_persistence_status","v154_persistence_failures","v154_persistence_seconds","v154_exhaustion_veto","v154_disqualifiers","v154_book_imbalance","v154_spread_bps","v154_reignition_bonus","v154_alert","v154_event_id"
    ]
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        v15 = float(row.get("v15_score", 0) or 0)
        bridge_armed = bool(row.get("v15_reignition_bridge_armed", False))
        bridge_score = float(row.get("v15_reignition_bridge_score", 0) or 0)
        if not (bridge_armed or v15 >= 70 or bridge_score >= 60):
            continue
        record = {"ts": stamp, **{k: row.get(k) for k in fields if k in row}}
        path = MICROSTRUCTURE_ROOT / f"{symbol}.jsonl"
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":")) + "\n")


def append_scan_history(rows, ts=None):
    """Append one compact V15.2 sample per symbol."""
    HISTORY_ROOT.mkdir(parents=True, exist_ok=True)
    stamp = time() if ts is None else float(ts)
    grouped = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).strip().upper()
        if symbol:
            grouped[symbol] = _record(row, stamp)
    for symbol, record in grouped.items():
        path = HISTORY_ROOT / f"{symbol}.jsonl"
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":")) + "\n")
