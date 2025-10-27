from get_posts_fb_automation_v2 import merge_two_posts
import os
def consolidate_ndjson_inplace(path):
    if not os.path.exists(path):
        return
    import json
    by_rid = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                it = json.loads(line)
            except Exception:
                continue
            rid = it.get("rid") or it.get("id") or it.get("link") or f"_no_rid_{hash(line)}"
            if rid in by_rid:
                by_rid[rid] = merge_two_posts(by_rid[rid], it)
            else:
                by_rid[rid] = it

    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as g:
        for it in by_rid.values():
            g.write(json.dumps(it, ensure_ascii=False) + "\n")
    os.replace(tmp, path)

consolidate_ndjson_inplace("posts_all.ndjson")