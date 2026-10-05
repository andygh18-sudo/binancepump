"""V15.7 hard-negative and boundary-label utilities.

Research-only: this module never changes live scanner decisions. It converts
past V15.7 observations into leakage-safe forward-outcome labels and identifies
near-boundary/hard-negative observations whose pre-event feature signature is
close to successful pump observations.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Dict, List, Optional, Sequence, Tuple


DEFAULT_HORIZON_SECONDS = 600.0
DEFAULT_PUMP_TARGET_PCT = 3.0
DEFAULT_NEAR_TARGET_PCT = 1.0
DEFAULT_SIGNAL_SCORE_MIN = 55.0

# Decision-time features only: no future/outcome fields are used for distance.
FEATURES: Tuple[str, ...] = (
    "v158_score", "v158_adaptive_score", "adaptive_trade_z",
    "adaptive_volume_z", "adaptive_trade_size_z", "adaptive_cvd_z",
    "adaptive_intensity_z", "adaptive_price_impact_z",
    "adaptive_regime_change", "adaptive_participation_z",
    "adaptive_synchronized_flow_z", "v158_directional_score",
    "v158_liquidity_score", "v158_sweep_score",
    "v158_price_impact_efficiency", "cross_section_percentile",
    "cross_section_route_score",
)


@dataclass(frozen=True)
class Outcome:
    label: str
    max_return_pct: float
    horizon_seconds: float
    first_hit_seconds: Optional[float]


def _f(row: dict, key: str, default: float = 0.0) -> float:
    try:
        value = float(row.get(key, default))
        return value if value == value else default
    except (TypeError, ValueError):
        return default


def _price(row: dict) -> Optional[float]:
    value = _f(row, "price", 0.0)
    return value if value > 0 else None


def _feature_vector(row: dict) -> Tuple[float, ...]:
    return tuple(_f(row, key) for key in FEATURES)


def classify_outcome(
    rows: Sequence[dict],
    index: int,
    horizon_seconds: float = DEFAULT_HORIZON_SECONDS,
    pump_target_pct: float = DEFAULT_PUMP_TARGET_PCT,
    near_target_pct: float = DEFAULT_NEAR_TARGET_PCT,
    signal_score_min: float = DEFAULT_SIGNAL_SCORE_MIN,
) -> Optional[Outcome]:
    """Label one observation using strictly future prices from the same symbol."""
    row = rows[index]
    base = _price(row)
    if base is None:
        return None
    t0 = _f(row, "ts", 0.0)
    if t0 <= 0:
        return None

    future = []
    for candidate in rows[index + 1:]:
        ts = _f(candidate, "ts", 0.0)
        if ts <= t0:
            continue
        age = ts - t0
        if age > horizon_seconds:
            break
        price = _price(candidate)
        if price is not None:
            future.append((age, (price / base - 1.0) * 100.0))

    # Require complete forward coverage; otherwise the label would be censored.
    if not future or future[-1][0] < horizon_seconds:
        return None

    max_ret = max(ret for _, ret in future)
    first_hit = next((age for age, ret in future if ret >= near_target_pct), None)
    score = max(_f(row, "v158_score"), _f(row, "v158_adaptive_score"))

    if max_ret >= pump_target_pct:
        label = "PUMP"
    elif max_ret >= near_target_pct:
        label = "NEAR_PUMP"
    elif score >= signal_score_min:
        label = "FAILED_IGNITION"
    else:
        label = "NON_EVENT"

    return Outcome(label, round(max_ret, 6), horizon_seconds, first_hit)


def add_labels(
    records: Sequence[dict],
    horizon_seconds: float = DEFAULT_HORIZON_SECONDS,
    pump_target_pct: float = DEFAULT_PUMP_TARGET_PCT,
    near_target_pct: float = DEFAULT_NEAR_TARGET_PCT,
    signal_score_min: float = DEFAULT_SIGNAL_SCORE_MIN,
) -> List[dict]:
    """Return labeled copies of a V15.7 observation stream."""
    grouped: Dict[str, List[dict]] = {}
    for record in records:
        symbol = str(record.get("symbol", "")).upper()
        if symbol:
            grouped.setdefault(symbol, []).append(record)

    output: List[dict] = []
    for symbol_rows in grouped.values():
        symbol_rows.sort(key=lambda r: _f(r, "ts"))
        for index, row in enumerate(symbol_rows):
            outcome = classify_outcome(
                symbol_rows, index, horizon_seconds, pump_target_pct,
                near_target_pct, signal_score_min
            )
            if outcome is None:
                continue
            item = dict(row)
            item["outcome_label"] = outcome.label
            item["outcome_max_return_pct"] = outcome.max_return_pct
            item["outcome_horizon_seconds"] = outcome.horizon_seconds
            item["outcome_first_hit_seconds"] = outcome.first_hit_seconds
            output.append(item)
    return output


def mark_hard_negatives(
    records: Sequence[dict],
    distance_threshold: float = 3.0,
) -> List[dict]:
    """Mark failed/near-boundary rows close to successful-pump signatures.

    Distance uses only decision-time features. PUMP rows form the reference
    signature set; FAILED_IGNITION rows are hard negatives and NEAR_PUMP rows
    are boundary examples when close enough to a PUMP signature.
    """
    positives = [r for r in records if r.get("outcome_label") == "PUMP"]
    if not positives:
        return [dict(r, hard_negative=False, boundary_example=False,
                     hard_negative_distance=None) for r in records]

    vectors = [_feature_vector(r) for r in positives]
    means = [
        sum(v[j] for v in vectors) / len(vectors)
        for j in range(len(FEATURES))
    ]
    sds = []
    for j in range(len(FEATURES)):
        var = sum((v[j] - means[j]) ** 2 for v in vectors) / len(vectors)
        sds.append(sqrt(var) if var > 1e-12 else 1.0)

    def distance(row: dict) -> float:
        v = _feature_vector(row)
        return min(
            sqrt(sum(((v[j] - p[j]) / sds[j]) ** 2
                     for j in range(len(FEATURES))))
            for p in vectors
        )

    output = []
    for row in records:
        item = dict(row)
        if row.get("outcome_label") in {"FAILED_IGNITION", "NEAR_PUMP"}:
            d = distance(row)
            item["hard_negative_distance"] = round(d, 6)
            item["hard_negative"] = bool(
                row.get("outcome_label") == "FAILED_IGNITION"
                and d <= distance_threshold
            )
            item["boundary_example"] = bool(
                row.get("outcome_label") == "NEAR_PUMP"
                and d <= distance_threshold
            )
        else:
            item["hard_negative_distance"] = None
            item["hard_negative"] = False
            item["boundary_example"] = False
        output.append(item)
    return output
