"""Deterministic tests for V15.7 hard-negative labeling."""
from scanner.v157_hard_negatives import add_labels, mark_hard_negatives


def _row(ts, price, score=70, **extra):
    row = {"event": "OBS", "symbol": "TESTUSDT", "ts": ts,
           "price": price, "v158_score": score}
    row.update(extra)
    return row


def test_pump_label_uses_only_future_prices():
    rows = [_row(1, 100), _row(61, 101), _row(121, 103), _row(601, 102)]
    labeled = add_labels(rows, horizon_seconds=600)
    assert labeled[0]["outcome_label"] == "PUMP"
    assert labeled[0]["outcome_max_return_pct"] == 3.0


def test_failed_ignition_is_distinct_from_non_event():
    rows = [_row(1, 100, score=70), _row(601, 100.2)]
    labeled = add_labels(rows, horizon_seconds=600)
    assert labeled[0]["outcome_label"] == "FAILED_IGNITION"


def test_hard_negative_marks_failed_boundary_signature():
    pump = _row(1, 100, score=80, adaptive_trade_z=4, adaptive_volume_z=3)
    pump_future = _row(601, 104, score=80, adaptive_trade_z=4, adaptive_volume_z=3)
    failed = _row(1201, 100, score=79, adaptive_trade_z=3.9, adaptive_volume_z=3.1)
    failed_future = _row(1801, 100.2, score=79, adaptive_trade_z=3.9, adaptive_volume_z=3.1)
    rows = add_labels([pump, pump_future, failed, failed_future], horizon_seconds=600)
    marked = mark_hard_negatives(rows, distance_threshold=3.0)
    failed_row = next(r for r in marked if r["ts"] == 1201)
    assert failed_row["outcome_label"] == "FAILED_IGNITION"
    assert failed_row["hard_negative"] is True
    assert failed_row["boundary_example"] is False


def test_near_pump_is_boundary_not_hard_negative():
    pump = _row(1, 100, score=80, adaptive_trade_z=4, adaptive_volume_z=3)
    pump_future = _row(601, 104, score=80, adaptive_trade_z=4, adaptive_volume_z=3)
    near = _row(1201, 100, score=79, adaptive_trade_z=3.9, adaptive_volume_z=3.1)
    near_future = _row(1801, 101, score=79, adaptive_trade_z=3.9, adaptive_volume_z=3.1)
    rows = add_labels([pump, pump_future, near, near_future], horizon_seconds=600)
    marked = mark_hard_negatives(rows, distance_threshold=3.0)
    near_row = next(r for r in marked if r["ts"] == 1201)
    assert near_row["outcome_label"] == "NEAR_PUMP"
    assert near_row["boundary_example"] is True
    assert near_row["hard_negative"] is False
