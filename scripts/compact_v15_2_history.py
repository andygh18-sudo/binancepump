import json
from pathlib import Path
from time import time

ROOT = Path("data")
HISTORY_ROOT = ROOT / "history"
LEGACY = ROOT / "history.jsonl"
INDEX = ROOT / "history_index.json"
MAX_SAMPLES = 1000
RECENT_INDEX_POINTS = 12

def append_record(symbol, record):
    HISTORY_ROOT.mkdir(parents=True, exist_ok=True)
    path = HISTORY_ROOT / f"{symbol}.jsonl"
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, separators=(",", ":")) + "\n")

def migrate_legacy():
    if not LEGACY.exists() or LEGACY.stat().st_size == 0:
        return 0
    migrated = 0
    for line in LEGACY.read_text(errors="ignore").splitlines():
        try:
            item = json.loads(line)
        except Exception:
            continue
        ts = item.get("ts")
        for row in item.get("rows", []) or []:
            if isinstance(row, dict) and row.get("symbol"):
                append_record(str(row["symbol"]).upper(), {"ts": ts, **row})
                migrated += 1
    LEGACY.unlink()
    return migrated

def compact_symbol(path):
    rows = []
    for line in path.read_text(errors="ignore").splitlines():
        try:
            row = json.loads(line)
            if isinstance(row, dict) and row.get("symbol"):
                rows.append(row)
        except Exception:
            continue
    seen = set()
    unique = []
    for row in rows:
        key = (row.get("ts"), row.get("symbol"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    unique.sort(key=lambda r: float(r.get("ts", 0) or 0))
    unique = unique[-MAX_SAMPLES:]
    path.write_text(
        "\n".join(json.dumps(r, separators=(",", ":")) for r in unique) + ("\n" if unique else ""),
        encoding="utf-8",
    )
    return unique

migrated = migrate_legacy()
HISTORY_ROOT.mkdir(parents=True, exist_ok=True)
index = {
    "version": "V15.2",
    "generated_at": time(),
    "sample_interval_seconds": 300,
    "max_samples_per_symbol": MAX_SAMPLES,
    "symbols": {},
}
for path in sorted(HISTORY_ROOT.glob("*.jsonl")):
    rows = compact_symbol(path)
    if not rows:
        path.unlink(missing_ok=True)
        continue
    index["symbols"][path.stem] = {
        "path": f"data/history/{path.name}",
        "samples": len(rows),
        "first_ts": rows[0].get("ts"),
        "last_ts": rows[-1].get("ts"),
        "recent": [
            {
                "ts": r.get("ts"),
                "score": r.get("score", r.get("v15_score", 0)),
                "stage": r.get("stage", r.get("v15_stage", "")),
                "volume_ratio": r.get("volume_ratio", 0),
                "price_60s": r.get("price_60s", 0),
                "v15_ignition_stage": r.get("v15_ignition_stage", "NORMAL"),
                "v15_ignition_score": r.get("v15_ignition_score", 0),
            }
            for r in rows[-RECENT_INDEX_POINTS:]
        ],
    }

INDEX.write_text(json.dumps(index, indent=2), encoding="utf-8")
print(f"V15.2 history ready: {len(index['symbols'])} symbols, {migrated} legacy records migrated.")
