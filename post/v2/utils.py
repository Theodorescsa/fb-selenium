import json, re, urllib
from typing import Optional, List, Dict
from configs import *

def _coerce_epoch(v):
    try:
        vv = float(v)
        if vv > 10_000_000_000:  # ms -> s
            vv = vv / 1000.0
        return int(vv)
    except Exception:
        return None

def _normalize_cookie(c: dict) -> Optional[dict]:
    if not isinstance(c, dict): 
        return None
    name  = c.get("name")
    value = c.get("value")
    if not name or value is None:
        return None

    domain = c.get("domain")
    host_only = c.get("hostOnly", False)
    if domain:
        domain = domain.strip()
        if host_only and domain.startswith("."):
            domain = domain.lstrip(".")
    if not domain:
        domain = "facebook.com"

    if not any(domain.endswith(d) or ("."+domain).endswith(d) for d in ALLOWED_COOKIE_DOMAINS):
        return None

    path = c.get("path") or "/"
    secure    = bool(c.get("secure", True))
    httpOnly  = bool(c.get("httpOnly", c.get("httponly", False)))

    expiry = c.get("expiry", None)
    if expiry is None:
        expiry = c.get("expirationDate", None)
    if expiry is None:
        expiry = c.get("expires", None)
    expiry = _coerce_epoch(expiry) if expiry is not None else None

    out = {
        "name": name,
        "value": value,
        "domain": domain,
        "path": path,
        "secure": secure,
        "httpOnly": httpOnly,
    }
    if expiry is not None:
        out["expiry"] = expiry
    return out

# =========================
# Request matching / parsing
# =========================
def parse_form(body_str: str) -> Dict[str, str]:
    qs = urllib.parse.parse_qs(body_str, keep_blank_values=True)
    return {k: (v[0] if isinstance(v, list) else v) for k, v in qs.items()}

def is_group_feed_req(rec):
    if "/api/graphql/" not in (rec.get("url") or ""): return False
    if (rec.get("method") or "").upper() != "POST": return False
    body = rec.get("body") or ""
    if "fb_api_req_friendly_name=" in body:
        if re.search(r"(?:GroupComet|CometGroup|GroupsComet|ProfileComet|Comet).*?(?:Feed|Timeline|Stories).*?(?:Pagination|Refetch)", body, re.I):
            return True
    try:
        v = parse_form(body).get("variables","")
        vj = json.loads(urllib.parse.unquote_plus(v))
        if any(k in vj for k in ["groupID","groupIDV2","id","actorID","profileID","pageID"]):
            if any(k in vj for k in ["after","cursor","endCursor","afterCursor","feedAfterCursor","beforeTime","afterTime"]):
                return True
    except:
        pass
    return False

# =========================
# JSON helpers
# =========================
def _strip_xssi_prefix(s: str) -> str:
    if not s: return s
    s2 = s.lstrip()
    s2 = re.sub(r'^\s*for\s*\(\s*;\s*;\s*\)\s*;\s*', '', s2)
    s2 = re.sub(r"^\s*\)\]\}'\s*", '', s2)
    return s2

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
            chunk = _strip_xssi_prefix(s[j:])
            if chunk == s[j:]: break
            try:
                obj, k_rel = dec.raw_decode(chunk, 0); yield obj; i = j + k_rel
            except json.JSONDecodeError:
                break

def choose_best_graphql_obj(objs):
    objs = list(objs)
    if not objs: return None
    with_data = [o for o in objs if isinstance(o, dict) and 'data' in o]
    pick = with_data or objs
    return max(pick, key=lambda o: len(json.dumps(o, ensure_ascii=False)))

def current_cursor_from_form(form):
    try:
        v = json.loads(form.get("variables", "{}"))
    except Exception:
        return None
    for k in ["cursor","after","endCursor","afterCursor","feedAfterCursor"]:
        c = v.get(k)
        if isinstance(c, str) and len(c) > 10:
            return c
    return None

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

def deep_find_has_next(obj):
    res = []
    o = obj
    def dive(o):
        if isinstance(o, dict):
            pi = o.get("page_info") or o.get("pageInfo")
            if isinstance(pi, dict):
                hn = pi.get("has_next_page");  hn = pi.get("hasNextPage") if hn is None else hn
                if isinstance(hn, bool): res.append(hn)
            for v in o.values(): dive(v)
        elif isinstance(o, list):
            for v in o: dive(v)
    dive(o)
    if any(res): return True
    if res and not any(res): return False
    return None

def deep_collect_timestamps(obj) -> List[int]:
    keys_hint = {"creation_time","created_time","creationTime","createdTime"}
    out = []
    def as_epoch_s(x):
        try:
            v = int(x)
            if v > 10_000_000_000: v //= 1000
            if 1104537600 <= v <= 4102444800:  # 2005..2100
                return v
        except: pass
        return None
    def dive(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in keys_hint:
                    vv = as_epoch_s(v)
                    if vv: out.append(vv)
                dive(v)
        elif isinstance(obj, list):
            for v in obj: dive(v)
    dive(obj)
    return out

# =========================
# Variables template helpers
# =========================
def get_vars_from_form(form_dict):
    try:
        return json.loads(form_dict.get("variables", "{}")) if form_dict else {}
    except:
        return {}

def make_vars_template(vars_dict):
    if not isinstance(vars_dict, dict): return {}
    t = dict(vars_dict)
    for k in list(t.keys()):
        if k in CURSOR_KEYS: del t[k]
    return t

def merge_vars(base_vars, template_vars):
    if not isinstance(base_vars, dict): base_vars = {}
    if not isinstance(template_vars, dict): template_vars = {}
    out = dict(base_vars)
    for k, v in template_vars.items():
        if k in CURSOR_KEYS: continue
        out[k] = v
    return out

def strip_cursors_from_vars(v: dict) -> dict:
    if not isinstance(v, dict): return {}
    return {k: v for k, v in v.items() if k not in CURSOR_KEYS}

def update_vars_for_next_cursor(form: dict, next_cursor: str, vars_template: dict = None):
    try:
        base = json.loads(form.get("variables", "{}"))
    except Exception:
        base = {}
    if vars_template:
        base = merge_vars(base, vars_template)
    changed = False
    if "cursor" in base:
        base["cursor"] = next_cursor; changed = True
    if not changed:
        for key in ["after","endCursor","afterCursor","feedAfterCursor"]:
            if key in base:
                base[key] = next_cursor; changed = True
    if not changed:
        base["cursor"] = next_cursor
    if "count" in base and isinstance(base["count"], int):
        base["count"] = max(base["count"], 10)
    form["variables"] = json.dumps(base, separators=(",", ":"))
    return form

