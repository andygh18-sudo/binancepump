import json
from collections import defaultdict, deque
from pathlib import Path

path = Path("data/ignition_history.jsonl")
if not path.exists():
    raise SystemExit(0)

per_symbol = defaultdict(lambda: deque(maxlen=60))
for line in path.read_text(errors="ignore").splitlines():
    try:
        item = json.loads(line)
        ts = item.get("ts")
        for row in item.get("rows", []):
            if isinstance(row, dict) and row.get("symbol"):
                sample = dict(row)
                sample["ts"] = ts
                per_symbol[row["symbol"]].append(sample)
    except Exception:
        continue

samples = [sample for values in per_symbol.values() for sample in values]
samples.sort(key=lambda x: (x.get("ts") or 0, x.get("symbol", "")))
path.write_text("\n".join(json.dumps(x, separators=(",", ":")) for x in samples) + ("\n" if samples else ""))
print(f"Retained {len(samples)} ignition samples across {len(per_symbol)} symbols; 60 samples/symbol max.")
