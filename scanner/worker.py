"""V15 worker bootstrap.

Loads the last clean V15.2 worker source and injects the local V15.4 engine.
This keeps the production worker source recoverable while V15.4 remains
experimental and parallel to V15.
"""
import urllib.request

SOURCE_URL = "https://raw.githubusercontent.com/andygh18-sudo/binancepump/cb0276aeb5786b4b8d8cf8fbaf5ff4d29e656b6c/scanner/worker.py"

def _load():
    req=urllib.request.Request(SOURCE_URL,headers={"User-Agent":"Binance-Pump-Scanner"})
    with urllib.request.urlopen(req,timeout=20) as r:
        return r.read().decode("utf-8")

src=_load()
src=src.replace(
    "from .history_store import append_scan_history, append_microstructure_history",
    "from .history_store import append_scan_history, append_microstructure_history\nfrom .v154 import evaluate as v154_evaluate"
)
src=src.replace(
    "async def telegram(msg):",
    '''def apply_v154(rows):
    events=[]
    for r in rows:
        s=str(r.get("symbol","")).upper()
        if not s:
            continue
        result,event_record=v154_evaluate(r,state[s].setdefault("v154",{}))
        r.update(result)
        if event_record:
            events.append(event_record)
    return events

def persist_v154_events(events):
    if not events:
        return
    os.makedirs("data",exist_ok=True)
    with open("data/v154_events.jsonl","a",encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event,separators=(",",":"))+"\\n")

async def telegram(msg):'''
)
src=src.replace(
    'rows=[r for s in symbols if (r:=score(s))];rows.sort(key=lambda z:z["score"],reverse=True)',
    'rows=[r for s in symbols if (r:=score(s))]\n            v154_events=apply_v154(rows)\n            persist_v154_events(v154_events)\n            rows.sort(key=lambda z:z["score"],reverse=True)'
)
exec(compile(src,"scanner/worker.py","exec"),globals(),globals())
