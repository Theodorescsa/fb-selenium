import os, re, json, urllib.parse, datetime
from pathlib import Path

import pandas as pd

# =========================
# CONFIG — nhớ sửa GROUP_URL
# =========================
CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"
PROFILE_NAME  = "Profile 5"
REMOTE_PORT   = 9222

# GROUP_URL     = "https://web.facebook.com/groups/laptrinhvienit"   # <— ĐỔI Ở ĐÂY
GROUP_URL     = "https://www.facebook.com/thoibao.de"               # <— ĐỔI Ở ĐÂY
KEEP_LAST     = 350
OUT_NDJSON    = "posts_all.ndjson"
RAW_DUMPS_DIR = "raw_dumps"
CHECKPOINT    = r"E:\NCS\fb-selenium\checkpoint.json"

os.makedirs(RAW_DUMPS_DIR, exist_ok=True)

REACTION_KEYS = {
    "LIKE": "like", "LOVE": "love", "HAHA": "haha", "WOW": "wow",
    "SAD": "sad", "ANGRY": "angry", "CARE": "care"
}
CURSOR_KEYS   = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}
POST_URL_RE   = re.compile(r"https?://(?:web\.)?facebook\.com/groups/[^/]+/(?:permalink|posts)/(\d+)/?$", re.I)
HASHTAG_RE    = re.compile(r"(#\w+)", re.UNICODE)

# =========================================
# Class 1: Utils — helpers, JSON, cursor, IO
# =========================================
class Utils:
    @staticmethod
    def deep_iter(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                yield k, v
                if isinstance(v, (dict, list)):
                    yield from Utils.deep_iter(v)
        elif isinstance(obj, list):
            for v in obj:
                yield from Utils.deep_iter(v)

    @staticmethod
    def deep_get_first(obj, want_keys):
        want = {k.lower() for k in want_keys}
        for k, v in Utils.deep_iter(obj):
            if isinstance(k, str) and k.lower() in want:
                return k, v
        return None, None

    # ---------- JSON chunk tools ----------
    @staticmethod
    def _strip_xssi_prefix(s: str) -> str:
        if not s: return s
        s2 = s.lstrip()
        s2 = re.sub(r'^\s*for\s*\(\s*;\s*;\s*\)\s*;\s*', '', s2)
        s2 = re.sub(r"^\s*\)\]\}'\s*", '', s2)
        return s2

    @staticmethod
    def iter_json_values(s: str):
        dec = json.JSONDecoder()
        i, n = 0, len(s)
        while i < n:
            m = re.search(r'\S', s[i:])
            if not m: break
            j = i + m.start()
            try:
                obj, k = dec.raw_decode(s, j); yield obj; i = k
            except json.JSONDecodeError:
                chunk = Utils._strip_xssi_prefix(s[j:])
                if chunk == s[j:]: break
                try:
                    obj, k_rel = dec.raw_decode(chunk, 0); yield obj; i = j + k_rel
                except json.JSONDecodeError:
                    break

    @staticmethod
    def choose_best_graphql_obj(objs):
        objs = list(objs)
        if not objs: return None
        with_data = [o for o in objs if isinstance(o, dict) and 'data' in o]
        pick = with_data or objs
        return max(pick, key=lambda o: len(json.dumps(o, ensure_ascii=False)))

    # ---------- forms / variables ----------
    @staticmethod
    def parse_form(body_str):
        qs = urllib.parse.parse_qs(body_str, keep_blank_values=True)
        return {k:(v[0] if isinstance(v, list) else v) for k,v in qs.items()}

    @staticmethod
    def get_vars_from_form(form_dict):
        try:
            return json.loads(form_dict.get("variables", "{}")) if form_dict else {}
        except:
            return {}

    @staticmethod
    def make_vars_template(vars_dict):
        if not isinstance(vars_dict, dict): return {}
        t = dict(vars_dict)
        for k in list(t.keys()):
            if k in CURSOR_KEYS: del t[k]
        return t

    @staticmethod
    def merge_vars(base_vars, template_vars):
        if not isinstance(base_vars, dict): base_vars = {}
        if not isinstance(template_vars, dict): template_vars = {}
        out = dict(base_vars)
        for k, v in template_vars.items():
            if k in CURSOR_KEYS: continue
            out[k] = v
        return out

    # ---------- cursor scan ----------
    @staticmethod
    def deep_collect_cursors(obj):
        found = []
        def dive(o):
            if isinstance(o, dict):
                pi = o.get("page_info") or o.get("pageInfo")
                if isinstance(pi, dict):
                    ec = pi.get("end_cursor") or pi.get("endCursor")
                    if isinstance(ec, str) and len(ec) >= 10:
                        found.append(("page_info.end_cursor", ec))
                edges = o.get("edges")
                if isinstance(edges, list) and edges:
                    last = edges[-1]
                    if isinstance(last, dict):
                        cur = last.get("cursor")
                        if isinstance(cur, str) and len(cur) >= 10:
                            found.append(("edges[-1].cursor", cur))
                for k, v in o.items():
                    if k in CURSOR_KEYS and isinstance(v, str) and len(v) >= 10:
                        found.append((k, v))
                    dive(v)
            elif isinstance(o, list):
                for v in o: dive(v)
        dive(obj)
        priority = {"page_info.end_cursor": 3, "end_cursor": 3, "endCursor": 3, "edges[-1].cursor": 2}
        found.sort(key=lambda kv: (priority.get(kv[0], 1), len(kv[1])), reverse=True)
        uniq, seenv = [], set()
        for k, v in found:
            if v not in seenv:
                uniq.append((k, v)); seenv.add(v)
        return uniq

    @staticmethod
    def deep_find_has_next(obj):
        res = []
        def dive(o):
            if isinstance(o, dict):
                pi = o.get("page_info") or o.get("pageInfo")
                if isinstance(pi, dict):
                    hn = pi.get("has_next_page");  hn = pi.get("hasNextPage") if hn is None else hn
                    if isinstance(hn, bool): res.append(hn)
                for v in o.values(): dive(v)
            elif isinstance(o, list):
                for v in o: dive(v)
        dive(obj)
        if any(res): return True
        if res and not any(res): return False
        return None

    # ---------- scoring / key / normalize ----------
    @staticmethod
    def norm_str(x):
        return ("" if x is None else str(x)).strip().lower()

    @staticmethod
    def safe_int(x):
        try:
            return int(x)
        except:
            return 0

    @staticmethod
    def md5_16(text):
        import hashlib
        try:
            return hashlib.md5((text or "").encode("utf-8")).hexdigest()[:16]
        except:
            return "0"*16

    @staticmethod
    def canonical_key(obj: dict) -> str:
        """
        Khóa chuẩn để gộp trùng: ưu tiên link → rid → author_id|created_time|md5(content).
        """
        link = Utils.norm_str(obj.get("link"))
        rid  = Utils.norm_str(obj.get("rid"))
        if link: return link
        if rid:  return rid
        return f'{obj.get("author_id") or ""}|{Utils.safe_int(obj.get("created_time"))}|{Utils.md5_16(obj.get("content"))}'

    @staticmethod
    def has_meaningful_content(obj: dict) -> bool:
        content = obj.get("content")
        link    = obj.get("link")
        return bool((content and str(content).strip()) or (link and str(link).strip()))

    @staticmethod
    def score_interactions(obj: dict) -> int:
        fields = ["like","love","haha","wow","sad","angry","care","comment","share"]
        total = 0
        for c in fields:
            try:
                total += int(obj.get(c, 0) or 0)
            except:
                pass
        return total

    @staticmethod
    def as_json_list(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "[]"
        if isinstance(x, (list, tuple)):
            return json.dumps(list(x), ensure_ascii=False)
        try:
            obj = json.loads(x)
            if isinstance(obj, (list, tuple)):
                return json.dumps(list(obj), ensure_ascii=False)
        except:
            pass
        if isinstance(x, str) and x.strip():
            return json.dumps([x], ensure_ascii=False)
        return "[]"

    @staticmethod
    def as_int01(v):
        if isinstance(v, bool): return 1 if v else 0
        if isinstance(v, (int, float)): return int(v)
        return 0

    # ---------- file ops ----------
    @staticmethod
    def append_ndjson(path, items):
        if not items: return
        with open(path, "a", encoding="utf-8") as f:
            for it in items:
                f.write(json.dumps(it, ensure_ascii=False) + "\n")
    @staticmethod
    def filter_link_and_dedupe_ndjson(in_path: str, out_path: str, require_url: bool = True) -> dict:
        """
        - Chỉ giữ object có 'link' (tùy chọn: bắt buộc là URL http/https)
        - Bỏ object không có content/link meaningful (dựa vào Utils.has_meaningful_content)
        - Dedupe theo Utils.canonical_key -> chọn bản có score_interactions cao hơn,
          nếu bằng thì chọn created_time mới hơn.
        - Ghi NDJSON, mỗi object 1 dòng.
        """
        import json
        from collections import OrderedDict

        def _has_link(o) -> bool:
            link = o.get("link")
            if not isinstance(link, str):
                return False
            link = link.strip()
            if not link:
                return False
            return (link.startswith("http://") or link.startswith("https://")) if require_url else True

        kept_map: "OrderedDict[str, dict]" = OrderedDict()  # giữ thứ tự gặp đầu tiên
        total = bad_json = no_link = no_meaning = 0
        replaced = 0

        with open(in_path, "r", encoding="utf-8") as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    bad_json += 1
                    continue

                total += 1
                if not _has_link(obj):
                    no_link += 1
                    continue
                if not Utils.has_meaningful_content(obj):
                    no_meaning += 1
                    continue

                k = Utils.canonical_key(obj)
                if k in kept_map:
                    a, b = kept_map[k], obj
                    sa, sb = Utils.score_interactions(a), Utils.score_interactions(b)
                    cta = Utils.safe_int(a.get("created_time"))
                    ctb = Utils.safe_int(b.get("created_time"))
                    if (sb, ctb) > (sa, cta):
                        kept_map[k] = b
                        replaced += 1
                else:
                    kept_map[k] = obj

        with open(out_path, "w", encoding="utf-8") as fout:
            for obj in kept_map.values():
                fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

        return {
            "total_in": total,
            "bad_json": bad_json,
            "filtered_no_link": no_link,
            "filtered_not_meaningful": no_meaning,
            "duplicates_kept": len(kept_map),
            "duplicates_replaced": replaced,
            "written": len(kept_map),
            "out_path": out_path,
        }
    # ---------- checkpoint ----------
    @staticmethod
    def load_checkpoint(path):
        if not os.path.exists(path):
            return {"cursor": None, "seen_ids": [], "last_doc_id": None, "last_query_name": None, "vars_template": {}, "ts": None}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {"cursor": None, "seen_ids": [], "last_doc_id": None, "last_query_name": None, "vars_template": {}, "ts": None}

    @staticmethod
    def save_checkpoint(path, cursor, seen_ids, last_doc_id=None, last_query_name=None, vars_template=None):
        data = {
            "cursor": cursor,
            "seen_ids": list(seen_ids)[:200000],
            "last_doc_id": last_doc_id,
            "last_query_name": last_query_name,
            "vars_template": vars_template or {},
            "ts": datetime.datetime.now().isoformat(timespec="seconds")
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    @staticmethod
    def normalize_seen_ids(seen_ids):
        return set(seen_ids or [])