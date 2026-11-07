"""Independent V15.8 top-10 liquidity and aggressive-buyer monitor.

This is a ranking/visibility layer only. It does not change V15/V15.6/V15.7/V15.8
signal gates or send Telegram alerts.
"""
from __future__ import annotations

def _num(row, key, default=0.0):
    try:
        value = float(row.get(key, default) or 0.0)
        return value if value == value and abs(value) != float("inf") else default
    except (TypeError, ValueError):
        return default

def rank_liquidity_buyers(rows, quote_volumes=None, limit=10, min_quote_volume=100000.0):
    quote_volumes = quote_volumes or {}
    ranked = []
    for source in rows or []:
        if not isinstance(source, dict):
            continue
        row = dict(source)
        symbol = str(row.get("symbol", "")).upper()
        if not symbol.endswith("USDT") or not symbol.isascii() or not symbol.isalnum():
            continue
        quote_volume = _num(row, "quote_volume_24h_usdt", _num(quote_volumes, symbol))
        if quote_volume < min_quote_volume or not bool(row.get("book_ready", False)):
            continue
        buy = _num(row, "buy_pressure")
        accel = _num(row, "trade_accel")
        trades = _num(row, "buyer_monitor_trades_10s")
        spread = _num(row, "spread_bps")
        bid_depth = _num(row, "bid_depth_usdt")
        ask_depth = _num(row, "ask_depth_usdt")
        depth = bid_depth + ask_depth
        if trades < 3 or spread <= 0 or spread > 25 or depth <= 0:
            continue
        cvd_z = _num(row, "adaptive_cvd_z")
        activity_z = _num(row, "adaptive_trade_z")
        # Scores reward depth and turnover, then sustained aggressive buying.
        liquidity_score = min(100.0, 35.0 * min(quote_volume / 20_000_000.0, 1.0)
                              + 35.0 * min(depth / 250_000.0, 1.0)
                              + 30.0 * max(0.0, 1.0 - spread / 25.0))
        buyer_score = (40.0 * max(0.0, min((buy - 0.45) / 0.35, 1.0))
                       + 25.0 * max(0.0, min((accel - 0.75) / 2.25, 1.0))
                       + 20.0 * max(0.0, min((cvd_z + 1.0) / 3.0, 1.0))
                       + 15.0 * max(0.0, min((activity_z + 1.0) / 3.0, 1.0)))
        combined = 0.55 * liquidity_score + 0.45 * buyer_score
        row.update({
            "liquidity_buyer_quote_volume_24h": round(quote_volume, 2),
            "liquidity_buyer_depth_usdt": round(depth, 2),
            "liquidity_buyer_spread_bps": round(spread, 3),
            "liquidity_buyer_trades_10s": int(trades),
            "liquidity_buyer_ratio": round(buy, 4),
            "liquidity_buyer_liquidity_score": round(liquidity_score, 1),
            "liquidity_buyer_flow_score": round(buyer_score, 1),
            "liquidity_buyer_combined_score": round(combined, 1),
        })
        ranked.append(row)
    ranked.sort(key=lambda r: (
        _num(r, "liquidity_buyer_combined_score"),
        _num(r, "liquidity_buyer_quote_volume_24h"),
        _num(r, "liquidity_buyer_depth_usdt")
    ), reverse=True)
    out = ranked[:max(1, int(limit))]
    for index, row in enumerate(out, 1):
        row["liquidity_buyer_rank"] = index
    return out
