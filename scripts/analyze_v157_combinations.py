"""Outcome-based analysis of V15.7 feature-combination signatures.

Research-only. Reads labeled V15.7 observations produced by the hard-negative
pipeline and ranks combinations by PUMP rate, boundary rate and failure rate.
"""
from __future__ import annotations
import argparse
import json
from collections import defaultdict
from pathlib import Path
from scanner.v157_learning import feature_combo_signature


def load(path: Path):
    rows=[]
    with path.open(encoding="utf-8") as f:
        for line in f:
            try:
                r=json.loads(line)
                if r.get("outcome_label"): rows.append(r)
            except json.JSONDecodeError:
                continue
    return rows


def analyze(rows, min_samples=10):
    groups=defaultdict(list)
    for r in rows:
        groups[str(r.get("v157_combo_signature") or feature_combo_signature(r))].append(r)
    out=[]
    for sig,items in groups.items():
        n=len(items)
        if n<min_samples: continue
        pumps=sum(r.get("outcome_label")=="PUMP" for r in items)
        near=sum(r.get("outcome_label")=="NEAR_PUMP" for r in items)
        failed=sum(r.get("outcome_label")=="FAILED_IGNITION" for r in items)
        avg=sum(float(r.get("outcome_max_return_pct",0) or 0) for r in items)/n
        out.append({
            "signature":sig,"samples":n,"pump_rate":round(pumps/n,4),
            "near_pump_rate":round(near/n,4),"failed_rate":round(failed/n,4),
            "positive_rate":round((pumps+near)/n,4),
            "avg_max_return_pct":round(avg,4),
        })
    return sorted(out,key=lambda x:(x["pump_rate"],x["positive_rate"],x["samples"]),reverse=True)


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--input",default="data/v157_hard_negatives.jsonl")
    p.add_argument("--output",default="data/v157_combo_learning.json")
    p.add_argument("--min-samples",type=int,default=10)
    args=p.parse_args()
    rows=load(Path(args.input))
    ranking=analyze(rows,args.min_samples)
    payload={"version":"v15.7-combo-learning-1","samples":len(rows),
             "min_samples":args.min_samples,"signatures":ranking}
    Path(args.output).parent.mkdir(parents=True,exist_ok=True)
    Path(args.output).write_text(json.dumps(payload,indent=2),encoding="utf-8")
    print(json.dumps({"labeled_samples":len(rows),"signatures":len(ranking),
                      "top":ranking[:10]},indent=2))


if __name__=="__main__":
    main()
