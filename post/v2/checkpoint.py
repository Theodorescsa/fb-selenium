# =========================
# Checkpoint / Output
# =========================
import os, json
from configs import *
from datetime import datetime
def load_checkpoint():
    if not os.path.exists(CHECKPOINT):
        return {"cursor": None, "seen_ids": [], "vars_template": {}, "ts": None,
                "mode": None, "slice_to": None, "slice_from": None, "year": None,
                "page": None, "min_created": None}
    try:
        with open(CHECKPOINT, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"cursor": None, "seen_ids": [], "vars_template": {}, "ts": None,
                "mode": None, "slice_to": None, "slice_from": None, "year": None,
                "page": None, "min_created": None}

def save_checkpoint(**kw):
    data = load_checkpoint()
    data.update(kw)
    data["ts"] = datetime.now().isoformat(timespec="seconds")
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def append_ndjson(items):
    if not items: return
    with open(OUT_NDJSON, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

def normalize_seen_ids(seen_ids):
    return set(seen_ids or [])
