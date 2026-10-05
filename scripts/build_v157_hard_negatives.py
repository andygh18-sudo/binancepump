#!/usr/bin/env python3
"""Build a leakage-safe V15.7 hard-negative/boundary dataset.

Research-only; it does not alter live scanner gates or alerts.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from scanner.v157_hard_negatives import (
    DEFAULT_HORIZON_SECONDS,
    DEFAULT_NEAR_TARGET_PCT,
    DEFAULT_PUMP_TARGET_PCT,
    DEFAULT_SIGNAL_SCORE_MIN,
    add_labels,
    mark_hard_negatives,
)


def load(path: Path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict) and row.get("symbol") and row.get("event") == "OBS":
                yield row


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/v157_observations.jsonl")
    parser.add_argument("--output", default="data/v157_hard_negatives.jsonl")
    parser.add_argument("--horizon", type=float, default=DEFAULT_HORIZON_SECONDS)
    parser.add_argument("--pump-target", type=float, default=DEFAULT_PUMP_TARGET_PCT)
    parser.add_argument("--near-target", type=float, default=DEFAULT_NEAR_TARGET_PCT)
    parser.add_argument("--signal-score-min", type=float, default=DEFAULT_SIGNAL_SCORE_MIN)
    parser.add_argument("--distance", type=float, default=3.0)
    args = parser.parse_args()

    records = list(load(Path(args.input)))
    labeled = add_labels(
        records,
        horizon_seconds=args.horizon,
        pump_target_pct=args.pump_target,
        near_target_pct=args.near_target,
        signal_score_min=args.signal_score_min,
    )
    labeled = mark_hard_negatives(labeled, distance_threshold=args.distance)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for row in labeled:
            handle.write(json.dumps(row, separators=(",", ":"), allow_nan=False) + "\n")

    counts = Counter(row["outcome_label"] for row in labeled)
    hard = sum(bool(row.get("hard_negative")) for row in labeled)
    boundary = sum(bool(row.get("boundary_example")) for row in labeled)
    print(f"input_observations={len(records)}")
    print(f"labeled_observations={len(labeled)}")
    print(f"labels={dict(sorted(counts.items()))}")
    print(f"hard_negatives={hard}")
    print(f"boundary_examples={boundary}")
    print(f"output={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
