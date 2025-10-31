# -*- coding: utf-8 -*-
"""
Facebook GraphQL Feed Crawler — Cursor-only + Crash-proof Resume + Branch Discovery

- Chỉ dùng cursor (KHÔNG time-slice).
- Tự khám phá nhiều nhánh GraphQL (doc_id/friendly_name) và CHỌN NHÁNH ĐI SÂU NHẤT (gần với run ~6k trước đây).
- Checkpoint "pre-commit cursor" trước mỗi request → resume chính xác tại post sau crash.
- Ghi NDJSON an toàn (staging -> fsync -> append).
- Seed lại seen_ids từ NDJSON khi resume (nếu checkpoint chưa kịp cập nhật).
- Khi stall: soft-refetch (no-UI), hoặc rotate sang doc_id dự phòng đã khám phá.

⚠️ Chỉ crawl nội dung bạn có quyền truy cập. Tôn trọng Điều khoản nền tảng.
"""
import argparse
import os, re, json, time, random, datetime, urllib.parse, socket
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode
from pathlib import Path

from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException as _SETimeout

# ==== custom utils của bạn (đã có sẵn trong get_info.py) ====
from get_info import *
from get_info import _all_urls_from_text
from get_info import _dig_attachment_urls

# =========================
# CONFIG
# =========================
HERE = Path(__file__).resolve().parent

GROUP_URL     = "https://www.facebook.com/thoibao.de"

COOKIES_PATH         = HERE / "authen" / "cookies.json"
LOCALSTORAGE_PATH    = HERE / "authen" / "localstorage.json"
SESSIONSTORAGE_PATH  = HERE / "authen" / "sessionstorage.json"

PROXY_URL = ""  # để trống nếu không dùng

KEEP_LAST     = 400
OUT_NDJSON    = "posts_all.ndjson"
RAW_DUMPS_DIR = "raw_dumps"
CHECKPOINT    = "checkpoint.json"

# Branch discovery
DISCOVER_SCROLLS      = 8      # số lần cuộn để thu nhiều request feed
BRANCH_PROBE_PAGES    = 6      # mỗi nhánh test nhanh tối đa N trang
BRANCH_MIN_NEW_THRESH = 5      # tổng bài mới tối thiểu coi nhánh có triển vọng
STALL_REFETCH_ROUNDS  = 3
STALL_FASTFWD_HOPS    = 2

os.makedirs(RAW_DUMPS_DIR, exist_ok=True)


POST_URL_RE = re.compile(
    r"""https?://(?:web\.)?facebook\.com/
        (?:
            groups/[^/]+/(?:permalink|posts)/\d+
          | [A-Za-z0-9.\-]+/posts/\d+
          | [A-Za-z0-9.\-]+/reel/\d+
          | photo(?:\.php)?\?(?:.*(?:fbid|story_fbid|video_id)=\d+)
          | .*?/pfbid[A-Za-z0-9]+
        )
    """, re.I | re.X
)

# =========================
# Hook /api/graphql/
# =========================
def install_early_hook(driver, keep_last=KEEP_LAST):
    HOOK_SRC = r"""
    (function(){
      if (window.__gqlHooked) return;
      window.__gqlHooked = true;
      window.__gqlReqs = [];
      function headersToObj(h){try{
        if (!h) return {};
        if (h instanceof Headers){const o={}; h.forEach((v,k)=>o[k]=v); return o;}
        if (Array.isArray(h)){const o={}; for (const [k,v] of h) o[k]=v; return o;}
        return (typeof h==='object')?h:{};}catch(e){return {}}
      }
      function pushRec(rec){try{
        const q = window.__gqlReqs; q.push(rec);
        if (q.length > __KEEP_LAST__) q.splice(0, q.length - __KEEP_LAST__);
      }catch(e){}}
      const origFetch = window.fetch;
      window.fetch = async function(input, init){
        const url = (typeof input==='string') ? input : (input&&input.url)||'';
        const method = (init&&init.method)||'GET';
        const body = (init && typeof init.body==='string') ? init.body : '';
        const hdrs = headersToObj(init && init.headers);
        let rec = null;
        if (url.includes('/api/graphql/') && method==='POST'){
          rec = {kind:'fetch', url, method, headers:hdrs, body:String(body)};
        }
        const res = await origFetch(input, init);
        if (rec){
          try{ rec.responseText = await res.clone().text(); }catch(e){ rec.responseText = null; }
          pushRec(rec);
        }
        return res;
      };
      const XO = XMLHttpRequest.prototype.open, XS = XMLHttpRequest.prototype.send;
      XMLHttpRequest.prototype.open = function(m,u,a){ this.__m=m; this.__u=u; return XO.apply(this, arguments); };
      XMLHttpRequest.prototype.send = function(b){
        this.__b = (typeof b==='string')?b:'';
        this.addEventListener('load', ()=>{
          try{
            if ((this.__u||'').includes('/api/graphql/') && (this.__m||'')==='POST'){
              pushRec({kind:'xhr', url:this.__u, method:this.__m, headers:{}, body:String(this.__b),
                       responseText:(typeof this.responseText==='string'?this.responseText:null)});
            }
          }catch(e){}
        });
        return XS.apply(this, arguments);
      };
    })();
    """.replace("__KEEP_LAST__", str(keep_last))
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": HOOK_SRC})
    driver.execute_script(HOOK_SRC)

def gql_count(d): return d.execute_script("return (window.__gqlReqs||[]).length")
def get_gql_at(d, i): return d.execute_script("return (window.__gqlReqs||[])[arguments[0]]", i)

def wait_next_req(d, start_idx, matcher, timeout=25, poll=0.25):
    end = time.time() + timeout
    cur = start_idx
    while time.time() < end:
        n = gql_count(d)
        while cur < n:
            req = get_gql_at(d, cur)
            if req and matcher(req): return (cur, req)
            cur += 1
        time.sleep(poll)
    return None

# =========================
# Chrome + selenium-wire
# =========================
def start_driver_with_proxy(proxy_url: str, headless: bool = False) -> webdriver.Chrome:
    chrome_opts = Options()
    if headless:
        chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--window-size=1920,1080")
    chrome_opts.add_argument("--disable-extensions")
    chrome_opts.add_argument("--disable-background-networking")
    chrome_opts.add_argument("--disable-popup-blocking")
    chrome_opts.add_argument("--no-first-run")
    chrome_opts.add_argument("--no-default-browser-check")
    chrome_opts.add_argument("--disable-background-timer-throttling")
    chrome_opts.add_argument("--disable-backgrounding-occluded-windows")
    chrome_opts.add_argument("--disable-renderer-backgrounding")

    sw_options = None
    if proxy_url:
        sw_options = {
            "proxy": {
                "http":  proxy_url,
                "https": proxy_url,
                "no_proxy": "localhost,127.0.0.1",
            },
        }

    driver = webdriver.Chrome(options=chrome_opts, seleniumwire_options=sw_options)
    driver.scopes = [r".*"] 
    return driver

# =========================
# Auth bootstrap (optional)
# =========================
ALLOWED_COOKIE_DOMAINS = {".facebook.com","facebook.com","m.facebook.com","web.facebook.com"}

def _coerce_epoch(v):
    try:
        vv = float(v)
        if vv > 10_000_000_000: vv = vv/1000.0
        return int(vv)
    except Exception:
        return None

def _normalize_cookie(c: dict) -> Optional[dict]:
    if not isinstance(c, dict): return None
    name  = c.get("name"); value = c.get("value")
    if not name or value is None: return None

    domain = c.get("domain")
    host_only = c.get("hostOnly", False)
    if domain:
        domain = domain.strip()
        if host_only and domain.startswith("."): domain = domain.lstrip(".")
    if not domain: domain = "facebook.com"
    if not any(domain.endswith(d) or ("."+domain).endswith(d) for d in ALLOWED_COOKIE_DOMAINS):
        return None

    path = c.get("path") or "/"
    secure   = bool(c.get("secure", True))
    httpOnly = bool(c.get("httpOnly", c.get("httponly", False)))
    expiry   = c.get("expiry") or c.get("expirationDate") or c.get("expires")
    expiry   = _coerce_epoch(expiry) if expiry is not None else None

    out = {"name":name,"value":value,"domain":domain,"path":path,"secure":secure,"httpOnly":httpOnly}
    if expiry is not None: out["expiry"] = expiry
    return out

def _add_cookies_safely(driver, cookies_path: Path):
    with open(cookies_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if isinstance(raw, dict) and "cookies" in raw: raw = raw["cookies"]
    if not isinstance(raw, list): raise ValueError("File cookies không phải mảng JSON.")
    added = 0
    for c in raw:
        nc = _normalize_cookie(c)
        if not nc: continue
        try: driver.add_cookie(nc); added += 1
        except Exception: pass
    return added

def _set_kv_storage(driver, kv_path: Path, storage: str = "localStorage"):
    with open(kv_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        for k, v in data.items():
            driver.execute_script(f"{storage}.setItem(arguments[0], arguments[1]);", k, v)

def bootstrap_auth(d):
    d.get("https://www.facebook.com/"); time.sleep(1.0)
    if COOKIES_PATH.exists():
        try:
            count = _add_cookies_safely(d, COOKIES_PATH)
            d.get("https://www.facebook.com/"); time.sleep(1.0)
            print(f"[AUTH] Added cookies: {count}")
        except Exception as e:
            print("[WARN] bootstrap cookies:", e)
    # if LOCALSTORAGE_PATH.exists():
    #     try:
    #         d.get("https://www.facebook.com/"); _set_kv_storage(d, LOCALSTORAGE_PATH, "localStorage")
    #         d.get("https://www.facebook.com/"); time.sleep(0.8)
    #     except Exception as e: print("[WARN] bootstrap localStorage:", e)
    # if SESSIONSTORAGE_PATH.exists():
    #     try:
    #         d.get("https://www.facebook.com/"); _set_kv_storage(d, SESSIONSTORAGE_PATH, "sessionStorage")
    #         d.get("https://www.facebook.com/"); time.sleep(0.8)
    #     except Exception as e: print("[WARN] bootstrap sessionStorage:", e)
    try:
        ck = {c["name"]: c.get("value") for c in d.get_cookies()}
        print(f"[AUTH] c_user={'c_user' in ck}, xs={'xs' in ck}")
    except Exception: pass

# =========================
# Request helpers
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
            if any(k in vj for k in ["after","cursor","endCursor","afterCursor","feedAfterCursor"]):
                return True
    except: pass
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

# =========================
# Cursor helpers
# =========================
def fetch_via_wire(driver, form):
    url = "https://www.facebook.com/api/graphql/"
    body = urlencode(form)
    resp = driver.request("POST", url, data=body,
        headers={"Content-Type":"application/x-www-form-urlencoded",
                 "Origin":"https://www.facebook.com",
                 "Referer":"https://www.facebook.com/"},
        timeout=25)
    return getattr(resp, "text", "")

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
                if k in CURSOR_KEYS_DEEP and isinstance(v, str) and len(v) >= 10:
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

# =========================
# Variables helpers
# =========================
def get_vars_from_form(form_dict):
    try:
        return json.loads(form_dict.get("variables", "{}")) if form_dict else {}
    except:
        return {}

def make_vars_template(vars_dict):
    """Tạo bản template của variables, bỏ hết khóa cursor."""
    if not isinstance(vars_dict, dict):
        return {}
    t = dict(vars_dict)
    for k in list(t.keys()):
        if k in CURSOR_KEYS_FORM:   # ✅ chỉ cần bỏ khóa paging ở tầng form
            del t[k]
    return t


def merge_vars(base_vars, template_vars):
    """Gộp hai dict variables, bỏ qua các khóa cursor."""
    if not isinstance(base_vars, dict):
        base_vars = {}
    if not isinstance(template_vars, dict):
        template_vars = {}
    out = dict(base_vars)
    for k, v in template_vars.items():
        if k in CURSOR_KEYS_FORM:   # ✅ tương tự, không merge cursor
            continue
        out[k] = v
    return out


def strip_cursors_from_vars(v: dict) -> dict:
    if not isinstance(v, dict): return {}
    return {k: v for k, v in v.items() if k not in CURSOR_KEYS_FORM}

def update_vars_for_next_cursor(form: dict, next_cursor: str, vars_template: dict = None):
    try:
        base = json.loads(form.get("variables", "{}"))
    except Exception:
        base = {}
    if vars_template:
        base = merge_vars(base, vars_template)
    changed = False
    for key in CURSOR_KEYS_FORM:
        if key in base:
            base[key] = next_cursor; changed = True
    if not changed:
        base["cursor"] = next_cursor
    if "count" in base and isinstance(base["count"], int):
        base["count"] = max(base["count"], 20)
    form["variables"] = json.dumps(base, separators=(",", ":"))
    return form

def strip_cursors_from_form_on_form(form, vars_template):
    """Sinh ra một form mới giữ nguyên meta (av, __a, fb_api_caller_class...) nhưng thay 'variables' = template không-cursor."""
    newf = dict(form)  # shallow copy
    vt = vars_template or {}
    try:
        newf["variables"] = json.dumps(vt, ensure_ascii=False, separators=(',',':'))
    except Exception:
        # cuối cùng vẫn cố serialize tối giản
        newf["variables"] = "{}"
    # Xoá các key paging “ngoài ruộng” nếu có
    for k in list(newf.keys()):
        if k.lower() in CURSOR_KEYS_FORM:
            newf.pop(k, None)
    return newf


# =========================
# JS fetch in page (giữ cookie)
# =========================
def js_fetch_in_page(driver, form_dict, extra_headers=None, timeout_ms=20000):
    script = r"""
        const done = arguments[arguments.length - 1];
        (async () => {
          try {
            const form = arguments[0] || {};
            const extra = arguments[1] || {};
            const timeout = arguments[2] || 20000;
            const ctrl = new AbortController();
            const to = setTimeout(() => ctrl.abort('timeout'), timeout);
            const headers = Object.assign({"Content-Type":"application/x-www-form-urlencoded"}, extra);
            const body = new URLSearchParams(form).toString();
            if (!location.host.includes('facebook.com')) {
              clearTimeout(to);
              return done(JSON.stringify({ok:false, error:"bad_origin:"+location.href}));
            }
            const res = await fetch("/api/graphql/", {
              method: "POST", headers, body, credentials: "include", signal: ctrl.signal
            });
            const text = await res.text();
            clearTimeout(to);
            done(JSON.stringify({ok:true, status:res.status, text}));
          } catch (e) {
            done(JSON.stringify({ok:false, error: (e && e.message) ? e.message : String(e)}));
          }
        })();
    """
    driver.set_script_timeout(max(5, int(timeout_ms/1000) + 10))
    raw = driver.execute_async_script(script, form_dict, extra_headers or {}, int(timeout_ms))
    try:
        obj = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        raise RuntimeError(f"js_fetch_in_page: bad_return {raw!r}")
    if not obj.get("ok"):
        raise RuntimeError(f"js_fetch_in_page: {obj.get('error')}")
    return obj.get("text", "")

# =========================
# Soft-refetch & reload
# =========================
def soft_refetch_form_and_cursor(driver, form, vars_template):
    try:
        base = json.loads(form.get("variables", "{}"))
    except Exception:
        base = {}
    base = merge_vars(base, vars_template)
    base = strip_cursors_from_vars(base)
    new_form = dict(form)
    new_form["variables"] = json.dumps(base, separators=(",", ":"))
    txt = js_fetch_in_page(driver, new_form, extra_headers={
        "Cache-Control":"no-cache","Pragma":"no-cache",
    })
    if not txt or len(txt) < 50:
        print("[FETCH] js_fetch empty/short, fallback wire")
        txt = fetch_via_wire(driver, form)
    obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
    if not obj: return None, None, None, None
    cursors = deep_collect_cursors(obj)
    new_has_next = deep_find_has_next(obj)
    if new_has_next is None: new_has_next = bool(cursors)
    new_cursor = cursors[0][1] if cursors else None
    if new_cursor:
        new_form = update_vars_for_next_cursor(new_form, new_cursor, vars_template)
    return new_form, new_cursor, new_has_next, obj

def reload_and_refresh_form(d, group_url, cursor, vars_template, timeout=25, poll=0.25):
    d.get(group_url); time.sleep(1.5)
    for _ in range(6):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));"); time.sleep(0.5)
    nxt = wait_next_req(d, 0, is_group_feed_req, timeout=timeout, poll=poll)
    if not nxt: return None, None, None
    _, req = nxt
    new_form = parse_form(req.get("body", ""))
    friendly = urllib.parse.parse_qs(req.get("body","")).get("fb_api_req_friendly_name", [""])[0]
    new_doc_id = new_form.get("doc_id")
    if cursor: new_form = update_vars_for_next_cursor(new_form, cursor, vars_template)
    return new_form, friendly, new_doc_id

def fast_forward_cursor(driver, form, vars_template, hops=2):
    cur_form = form
    last_cursor = None
    for _ in range(max(1, hops)):
        nf, nc, nh, _ = soft_refetch_form_and_cursor(driver, cur_form, vars_template)
        if not nf or not nc: break
        cur_form = nf; last_cursor = nc
    return cur_form, last_cursor

# =========================
# Post extractors (giữ như bạn đã dùng)
# =========================
def _is_story_node(n: dict) -> bool:
    if n.get("__typename") == "Story": return True
    if n.get("__isFeedUnit") == "Story": return True
    if "post_id" in n or "comet_sections" in n: return True
    return False

def _looks_like_group_post(n: dict) -> bool:
    if not _is_story_node(n): return False
    url = n.get("wwwURL") or n.get("url") or ""
    pid = n.get("id") or ""
    if POST_URL_RE.match(url): return True
    if (isinstance(pid, str) and pid.startswith("Uzpf")) or n.get("post_id"): return True
    return False

def _extract_url_digits(url: str) -> Optional[str]:
    if not url: return None
    try:
        path = urlparse(url).path.lower()
    except:
        path = url.lower()
    m = re.search(r"/(?:reel|posts|permalink)/(\d+)", path)
    if m: return m.group(1)
    qs = parse_qs(urlparse(url).query)
    for k in ("fbid","story_fbid","video_id","photo_id","id","v"):
        v = qs.get(k)
        if v and v[0] and v[0].isdigit():
            return v[0]
    return None

def _dig_text(o):
    texts = []
    def take(x):
        if isinstance(x, str):
            t = x.strip()
            if t and t.lower() not in {"see more", "xem thêm"}:
                texts.append(t)
    def dive(v):
        if isinstance(v, dict):
            if "text" in v and isinstance(v["text"], str): take(v["text"])
            if "message" in v and isinstance(v["message"], dict):
                if isinstance(v["message"].get("text"), str): take(v["message"]["text"])
            if "body" in v and isinstance(v["body"], dict):
                if isinstance(v["body"].get("text"), str): take(v["body"]["text"])
            if "savable_description" in v and isinstance(v["savable_description"], dict):
                if isinstance(v["savable_description"].get("text"), str): take(v["savable_description"]["text"])
            for k in ("title","subtitle","headline","label","contextual_message"):
                val = v.get(k)
                if isinstance(val, dict) and isinstance(val.get("text"), str): take(val["text"])
                elif isinstance(val, str): take(val)
            for vv in v.values(): dive(vv)
        elif isinstance(v, list):
            for it in v: dive(it)
    dive(o)
    uniq, seen = [], set()
    for t in texts:
        if t not in seen:
            uniq.append(t); seen.add(t)
    return uniq

def _extract_share_texts(n: dict):
    actor_texts, attached_texts = [], []
    if isinstance(n.get("message"), dict) and isinstance(n["message"].get("text"), str):
        actor_texts.append(n["message"]["text"])
    cs = n.get("comet_sections") or {}
    if isinstance(cs, dict):
        msg = cs.get("message")
        if isinstance(msg, dict):
            t = msg.get("text")
            if isinstance(t, str) and t.strip():
                actor_texts.append(t)
        attached = cs.get("attached_story") or cs.get("content") or {}
        if isinstance(attached, dict):
            story = attached.get("story") if isinstance(attached.get("story"), dict) else attached
            if isinstance(story, dict):
                if isinstance(story.get("message"), dict) and isinstance(story["message"].get("text"), str):
                    attached_texts.append(story["message"]["text"])
                attached_texts.extend(_dig_text(story))
    if not actor_texts: actor_texts.extend(_dig_text(n))
    def _uniq_keep(seq):
        out, seen = [], set()
        for s in seq:
            s2 = s.strip()
            if s2 and s2 not in seen:
                out.append(s2); seen.add(s2)
        return out
    actor_texts = _uniq_keep(actor_texts)
    attached_texts = _uniq_keep(attached_texts)
    if actor_texts and attached_texts:
        combined = actor_texts[0]
        if combined not in attached_texts:
            combined = combined + "\n\n" + attached_texts[0]
    elif actor_texts:
        combined = actor_texts[0]
    elif attached_texts:
        combined = attached_texts[0]
    else:
        combined = None
    return (actor_texts[0] if actor_texts else None,
            attached_texts[0] if attached_texts else None,
            combined)

def _get_text_from_node(n: dict):
    _, _, combined = _extract_share_texts(n)
    return combined

def collect_post_summaries(obj, out, group_url=GROUP_URL):
    if isinstance(obj, dict):
        if _looks_like_group_post(obj):
            post_id_api = obj.get("post_id")
            fb_id      = obj.get("id")
            url        = obj.get("wwwURL") or obj.get("url")
            url_digits = _extract_url_digits(url)
            rid        = post_id_api or url_digits or fb_id
            author_id, author_name, author_link, avatar, type_label = extract_author(obj)
            actor_text, attached_text, text_combined = _extract_share_texts(obj)
            image_urls, video_urls = extract_media(obj)
            counts = extract_reactions_and_counts(obj)
            smart_is_share, smart_link, smart_type, origin_id, share_meta = extract_share_flags_smart(obj, actor_text or text_combined)
            created_candidates = []
            def _deep_ts(o):
                if isinstance(o, dict):
                    for k, v in o.items():
                        if k in {"creation_time","created_time","creationTime","createdTime"}:
                            try:
                                t = int(v); created_candidates.append(t)
                            except: pass
                        _deep_ts(v)
                elif isinstance(o, list):
                    for v in o: _deep_ts(v)
            _deep_ts(obj)
            created = max(created_candidates) if created_candidates else extract_created_time(obj)
            is_share, link_share, type_share, origin_id_fallback = extract_share_flags(obj)
            hashtags = extract_hashtags(text_combined)
            out_links = list(dict.fromkeys(_all_urls_from_text(text_combined or "") + _dig_attachment_urls(obj)[0]))
            out_domains = []
            for u in out_links:
                try:
                    host = urlparse(u).netloc.lower().split(":")[0]
                    if host: out_domains.append(host)
                except: pass
            out_domains = list(dict.fromkeys(out_domains))
            source_id = None
            _k, _v = deep_get_first(obj, {"group_id","groupID","groupIDV2"})
            if _v: source_id = _v
            if not source_id:
                try:
                    slug = re.search(r"/groups/([^/?#]+)", group_url).group(1)
                    source_id = slug
                except: pass

            item = {
                "id": fb_id,
                "rid": rid,
                "type": type_label,
                "link": url,
                "author_id": author_id,
                "author": author_name,
                "author_link": author_link,
                "avatar": avatar,
                "created_time": created,
                "content": text_combined,
                "image_url": image_urls,
                "like": counts["like"],
                "comment": counts["comment"],
                "haha": counts["haha"],
                "wow": counts["wow"],
                "sad": counts["sad"],
                "love": counts["love"],
                "angry": counts["angry"],
                "care": counts["care"],
                "share": counts["share"],
                "hashtag": hashtags,
                "video": video_urls,
                "source_id": source_id,
                "is_share": smart_is_share,
                "link_share": smart_link,
                "type_share": smart_type,
                "origin_id": origin_id,
                "out_links": out_links,
                "out_domains": out_domains,
            }
            if share_meta: item["share_meta"] = share_meta
            if smart_is_share:
                item["content_parts"] = {"actor_text": actor_text,"attached_text": attached_text}
            out.append(item)
        for v in obj.values():
            collect_post_summaries(v, out, group_url)
    elif isinstance(obj, list):
        for v in obj:
            collect_post_summaries(v, out, group_url)

def filter_only_feed_posts(items):
    keep = []
    for it in items or []:
        link = (it.get("link") or "").strip()
        fb_id = (it.get("id") or "").strip()
        rid = (it.get("rid") or "").strip()
        if rid or (link and POST_URL_RE.match(link)) or (fb_id and fb_id.startswith("Uzpf")):
            keep.append(it)
    return keep

# =========================
# Dedupe/merge
# =========================
def _norm_link(u: str) -> Optional[str]:
    if not u or not isinstance(u, str): return None
    try:
        p = urlparse(u)
        host = p.netloc.lower()
        if host.endswith("facebook.com"): host = "facebook.com"
        path = (p.path or "").rstrip("/")
        if re.search(r"/(?:reel|posts|permalink)/\d+$", path.lower()):
            return urlunparse(("https", host, path.lower(), "", "", ""))
        return None
    except Exception:
        return None

def _all_join_keys(it: dict) -> List[str]:
    keys, seen = [], set()
    for k in (it.get("rid"), it.get("id"), _extract_url_digits(it.get("link") or ""), _norm_link(it.get("link") or "")):
        if isinstance(k, str) and k and (k not in seen):
            keys.append(k); seen.add(k)
    return keys

def _best_primary_key(it: dict) -> Optional[str]:
    rid = it.get("rid"); link = it.get("link"); _id = it.get("id")
    digits = _extract_url_digits(link) if link else None
    norm   = _norm_link(link) if link else None
    for k in (rid, _id, digits, norm):
        if isinstance(k, str) and k.strip(): return k.strip()
    return None

def merge_two_posts(a: dict, b: dict) -> dict:
    if not a: return b or {}
    if not b: return a or {}
    m = dict(a)
    m["id"]   = m.get("id")   or b.get("id")
    m["rid"]  = m.get("rid")  or b.get("rid")
    m["link"] = m.get("link") or b.get("link")
    ca, cb = m.get("created_time"), b.get("created_time")
    try:
        m["created_time"] = max(int(ca) if ca else 0, int(cb) if cb else 0) or (ca or cb)
    except: m["created_time"] = ca or cb
    return m

def coalesce_posts(items: List[dict]) -> List[dict]:
    groups, key2group, seq = {}, {}, 0
    def _new_gid():
        nonlocal seq; seq += 1; return f"g{seq}"
    for it in (items or []):
        keys = _all_join_keys(it)
        gid = None
        for k in keys:
            if k in key2group:
                gid = key2group[k]; break
        if gid is None:
            gid = _new_gid(); groups[gid] = it
        else:
            groups[gid] = merge_two_posts(groups[gid], it)
        for k in _all_join_keys(groups[gid]):
            key2group[k] = gid
    return list(groups.values())

# =========================
# Checkpoint / Output (crash-proof)
# =========================
def load_checkpoint():
    if not os.path.exists(CHECKPOINT):
        return {"cursor": None, "seen_ids": [], "vars_template": {},
                "ts": None, "branch": None}
    try:
        with open(CHECKPOINT, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"cursor": None, "seen_ids": [], "vars_template": {},
                "ts": None, "branch": None}

def save_checkpoint(**kw):
    data = load_checkpoint()
    data.update(kw)
    data["ts"] = datetime.datetime.now().isoformat(timespec="seconds")
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def append_ndjson_atomic(items, out_path=OUT_NDJSON):
    if not items: return 0
    dir_ = os.path.dirname(out_path) or "."
    os.makedirs(dir_, exist_ok=True)
    tmp_path = out_path + ".staging"
    with open(tmp_path, "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")
        f.flush(); os.fsync(f.fileno())
    with open(tmp_path, "r", encoding="utf-8") as fsrc, open(out_path, "a", encoding="utf-8") as fdst:
        for line in fsrc: fdst.write(line)
        fdst.flush(); os.fsync(fdst.fileno())
    try: os.remove(tmp_path)
    except: pass
    return len(items)

def rebuild_seen_ids_from_ndjson(path=OUT_NDJSON, max_lines=None):
    seen = set()
    if not os.path.exists(path): return seen
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            if max_lines and i > max_lines: break
            try:
                it = json.loads(line)
            except Exception:
                continue
            for k in _all_join_keys(it):
                seen.add(k)
    return seen

def save_cursor_precommit(next_cursor, seen_ids, vars_template, branch):
    save_checkpoint(
        cursor=next_cursor,
        seen_ids=list(seen_ids),
        vars_template=vars_template,
        branch=branch
    )

# =========================
# Paginate 1 window (cursor-only)
# =========================
def paginate_window_cursor_only(d, form, vars_template, seen_ids: set,
                                page_limit: Optional[int]=None,
                                branch_name: str = "default") -> Tuple[int, Optional[str]]:
    """
    Trả về: (total_new, last_good_cursor)
    """
    last_good_cursor = current_cursor_from_form(form) or None
    prev_cursor = None
    total_new = 0
    no_progress_rounds = 0
    cursor_stall_rounds = 0

    page = 0
    while True:
        page += 1
        # === Pre-commit cursor trước khi request ===
        pre_cursor = current_cursor_from_form(form)
        if pre_cursor:
            save_cursor_precommit(pre_cursor, seen_ids, vars_template, branch_name)

        # === Fetch 1 trang ===
        max_tries = 3
        txt, obj = None, None
        for attempt in range(1, max_tries+1):
            try:
                txt = js_fetch_in_page(d, form, extra_headers={}, timeout_ms=20000)
                if not txt or len(txt) < 50:
                    print("[FETCH] js_fetch empty/short, fallback wire")
                    txt = fetch_via_wire(d, form)
                break
            except (_SETimeout, RuntimeError) as e:
                if "bad_origin:" in str(e):
                    d.get(GROUP_URL); time.sleep(1.2)
                    try:
                        txt = js_fetch_in_page(d, form, extra_headers={}, timeout_ms=20000); break
     
                    except Exception: pass
                print(f"[WARN] fetch page try {attempt}/{max_tries} failed: {e}")
                time.sleep(random.uniform(0.8, 1.6))
                if attempt == 2:
                    new_form, boot_cursor, boot_has_next, _ = soft_refetch_form_and_cursor(d, form, vars_template)
                    if new_form: form = new_form
                if attempt == max_tries:
                    form2, friendly2, docid2 = reload_and_refresh_form(d, GROUP_URL, None, vars_template)
                    if form2: form = form2
                    try:
                        txt = js_fetch_in_page(d, form, extra_headers={}, timeout_ms=25000)
                        if not txt or len(txt) < 50:
                            print("[FETCH] js_fetch empty/short, fallback wire")
                            txt = fetch_via_wire(d, form)
                        break
                    except Exception:
                        try:
                            txt = fetch_via_wire(d, form)
                            if txt: break
                        except Exception: pass
                        raise

        obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
        if not obj:
            print(f"[{branch_name}] p{page} parse fail → stop.")
            break

        # === Parse & write ===
        page_posts = []
        collect_post_summaries(obj, page_posts)
        raw_len = len(page_posts)
        page_posts = coalesce_posts(filter_only_feed_posts(page_posts))
        kept_len = len(page_posts)

        written_this_round = set()
        fresh = []
        for p in page_posts:
            pk = _best_primary_key(p)
            if pk and (pk not in seen_ids) and (pk not in written_this_round):
                fresh.append(p); written_this_round.add(pk)
        if fresh:
            append_ndjson_atomic(fresh)
            for p in fresh:
                for k in _all_join_keys(p): seen_ids.add(k)
            total_new += len(fresh)
            no_progress_rounds = 0
        else:
            no_progress_rounds += 1

        # === Cursor/next ===
        cursors = deep_collect_cursors(obj)
        has_next = deep_find_has_next(obj)
        if not fresh and has_next: cursor_stall_rounds += 1
        else: cursor_stall_rounds = 0

        if cursor_stall_rounds >= 6:
            print(f"[{branch_name}] STALL → fast-forward {STALL_FASTFWD_HOPS} hops")
            ff_form, ff_cursor = fast_forward_cursor(d, form, vars_template, hops=STALL_FASTFWD_HOPS)
            if ff_form:
                form = ff_form
                if ff_cursor:
                    last_good_cursor = ff_cursor
                    prev_cursor = ff_cursor
            cursor_stall_rounds = 0
            time.sleep(random.uniform(0.6, 1.0))
            continue

        if has_next is None: has_next = bool(cursors)
        new_cursor = cursors[0][1] if cursors else None

        if not fresh and new_cursor:
            form = update_vars_for_next_cursor(form, new_cursor, vars_template)
            last_good_cursor = new_cursor
            prev_cursor = new_cursor
            try:
                v = json.loads(form.get("variables","{}"))
                if isinstance(v.get("count"), int):
                    v["count"] = min(max(v["count"] + 10, 20), 60)
                    form["variables"] = json.dumps(v, separators=(",",":"))
            except: pass
            time.sleep(random.uniform(0.3, 0.6))
            continue

        if new_cursor and prev_cursor == new_cursor:
            print(f"[{branch_name}] cursor lặp lại → soft-refetch")
            nf, bc, bh, _ = soft_refetch_form_and_cursor(d, form, vars_template)
            if nf and (bc or bh):
                form = nf
                if bc: new_cursor = bc
            else:
                f2, _, _ = reload_and_refresh_form(d, GROUP_URL, (last_good_cursor or current_cursor_from_form(form)), vars_template)
                if f2:
                    form = f2
                    try:
                        v = json.loads(form.get("variables","{}"))
                        if isinstance(v.get("count"), int):
                            v["count"] = min(max(v["count"] + 10, 20), 60)
                            form["variables"] = json.dumps(v, separators=(",",":"))
                    except: pass
                    no_progress_rounds = 0
                    time.sleep(random.uniform(0.8, 1.3))
                    continue

        if new_cursor:
            last_good_cursor = new_cursor
            prev_cursor = new_cursor

        print(f"[{branch_name}] p{page} raw={raw_len} kept={kept_len} (new {len(fresh)}), total_new={total_new}, next={has_next}")

        save_checkpoint(
            cursor=last_good_cursor,
            seen_ids=list(seen_ids),
            vars_template=vars_template,
            branch=branch_name
        )

        MAX_NO_NEXT_ROUNDS = 3
        if not has_next and no_progress_rounds >= MAX_NO_NEXT_ROUNDS:
            print(f"[{branch_name}] next=False x{no_progress_rounds} → soft-refetch")
            refetch_ok = False
            for attempt in range(1, 3):
                new_form, boot_cursor, boot_has_next, boot_obj = soft_refetch_form_and_cursor(d, form, vars_template)
                if new_form and (boot_cursor or boot_has_next):
                    form = new_form
                    if boot_cursor: last_good_cursor = boot_cursor
                    no_progress_rounds = 0
                    refetch_ok = True
                    break
                time.sleep(random.uniform(1.0, 2.0))
            if not refetch_ok:
                print(f"[{branch_name}] soft-refetch failed → stop pagination.")
                break

        if new_cursor:
            form = update_vars_for_next_cursor(form, new_cursor, vars_template)

        if page_limit and page >= page_limit: break
        time.sleep(random.uniform(0.7, 1.2))

    return total_new, last_good_cursor

# =========================
# Branch discovery (lấy nhánh đi sâu)
# =========================
# ==== helpers: parse body safely (handles form, JSON, batch) ====
import json, urllib.parse

def _parse_graphql_body(req_body: bytes) -> list[dict]:
    """Return a list of GraphQL calls: [{"doc_id":..., "variables":{...}, "friendly_name":...}, ...]"""
    out = []
    if not req_body:
        return out
    body = req_body.decode("utf-8", errors="ignore")

    # 1) form-encoded: doc_id=...&variables=...
    if "doc_id=" in body and "variables=" in body and "&" in body:
        parsed = urllib.parse.parse_qs(body)
        doc_ids = parsed.get("doc_id", [])
        vars_   = parsed.get("variables", [])
        for i, d in enumerate(doc_ids):
            v = vars_[i] if i < len(vars_) else vars_[-1] if vars_ else "{}"
            try:
                vj = json.loads(v)
            except:
                vj = {}
            out.append({"doc_id": d, "variables": vj})
        return out

    # 2) batch: queries={...}
    if "queries=" in body:
        parsed = urllib.parse.parse_qs(body)
        qs = parsed.get("queries", [])
        if qs:
            try:
                qj = json.loads(qs[0])
                # qj is dict of key -> {doc_id, variables, ...}
                for k, v in qj.items():
                    out.append({
                        "doc_id": v.get("doc_id"),
                        "variables": v.get("variables", {}),
                        "friendly_name": v.get("friendly_name") or v.get("query_name")
                    })
            except:
                pass
        return out

    # 3) raw JSON (rare but handle)
    try:
        j = json.loads(body)
        if isinstance(j, dict) and "doc_id" in j:
            out.append({"doc_id": j.get("doc_id"), "variables": j.get("variables", {})})
        elif isinstance(j, list):
            for it in j:
                if isinstance(it, dict):
                    out.append({"doc_id": it.get("doc_id"), "variables": it.get("variables", {})})
    except:
        pass

    return out


import json, urllib.parse, re

CURSOR_KEYS_FORM = {"cursor","after","endCursor","afterCursor","feedAfterCursor","before","beforeCursor"}
CURSOR_KEYS_DEEP = {
  "cursor", "after", "before", "end_cursor", "start_cursor",
  "timeline_cursor", "timeline_section_cursor", "page_info",
  "has_next_page", "last_fetched_cursor", "pagination_token"
}
# dùng CURSOR_KEYS_FORM cho update/strip trên form.variables
# dùng CURSOR_KEYS_DEEP cho deep_strip_cursors(...) và deep_collect_cursors(...)

def parse_graphql_form(req):
    """
    Trả về tuple (form_dict, variables_dict, doc_id/opname)
    Hỗ trợ: application/x-www-form-urlencoded và graphqlbatch.
    """
    body = None
    try:
        body = req.body.decode("utf-8", "ignore") if isinstance(req.body, (bytes, bytearray)) else str(req.body)
    except Exception:
        body = str(req.body)

    form = {}
    ct = (req.headers.get("content-type") or req.headers.get("Content-Type") or "").lower()
    if ct.startswith("application/x-www-form-urlencoded"):
        form = {k: v[0] if isinstance(v, list) else v for k, v in urllib.parse.parse_qs(body).items()}
    else:
        # đôi khi FB gửi raw "av=...&__a=...&variables=..."
        if "=" in body and "&" in body:
            form = {k: v[0] if isinstance(v, list) else v for k, v in urllib.parse.parse_qs(body).items()}
        else:
            # graphqlbatch: body là JSON lines
            try:
                j = json.loads(body)
                if isinstance(j, dict):
                    form = j
                elif isinstance(j, list) and j and isinstance(j[0], dict):
                    form = j[0]
            except Exception:
                pass

    # Lấy variables
    variables = {}
    raw_vars = form.get("variables") or form.get("query_params") or form.get("queries")  # tuỳ biến
    if isinstance(raw_vars, str):
        try:
            variables = json.loads(raw_vars)
        except Exception:
            # Có thể là dạng queries={... "variables": {...}}
            m = re.search(r'"variables"\s*:\s*(\{.*\})', raw_vars)
            if m:
                try: variables = json.loads(m.group(1))
                except: pass
    elif isinstance(raw_vars, dict):
        variables = raw_vars

    # Lấy doc_id hoặc op_name
    doc_id = form.get("doc_id") or form.get("docid") or form.get("docId")
    op_name = form.get("operation_name") or form.get("operationName") or form.get("fb_api_req_friendly_name")

    return form, variables, (doc_id or op_name)

def deep_strip_cursors(obj):
    """Xoá/cắt các field cursor trong variables để tạo template ổn định."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            lk = k.lower()
            if lk in CURSOR_KEYS_DEEP:
                # xoá luôn
                continue
            if lk == "edges" and isinstance(v, list):
                # cắt edges giữ structure rỗng
                out[k] = []
                continue
            out[k] = deep_strip_cursors(v)
        return out
    elif isinstance(obj, list):
        return [deep_strip_cursors(x) for x in obj]
    else:
        return obj

def discover_branches_from_requests(driver):
    """
    Duyệt driver.requests (selenium-wire) để gom candidate branches.
    Mỗi branch: { 'doc_id'|'op_name', 'form', 'vars_template', 'cursor_keys_hint', ... }
    """
    branches = []
    seen = set()
    for req in getattr(driver, "requests", []):
        url = req.url or ""
        if not re.search(r'/(api/graphql|ajax/browse/graphql|api/graphqlbatch)', url):
            continue
        try:
            form, variables, key = parse_graphql_form(req)
        except Exception:
            continue
        if not key:
            continue
        sig = (key, form.get("av"), form.get("doc_id"))
        if not (form.get("doc_id") or key): 
            continue
        if sig in seen:
            continue
        seen.add(sig)

        vars_template = deep_strip_cursors(variables) if variables else {}
        if not form:
            # vẫn tạo form tối thiểu nếu parse lỗi
            form = {}
        # Chuẩn hoá: đảm bảo có 'doc_id' nếu có
        if "doc_id" not in form and isinstance(key, str) and key.isdigit():
            form["doc_id"] = key

        branches.append({
            "doc_id": form.get("doc_id"),
            "op_name": form.get("fb_api_req_friendly_name") or form.get("operation_name"),
            "friendly": form.get("fb_api_req_friendly_name") or form.get("operation_name") or form.get("doc_id"),
            "form": form,
            "vars_template": vars_template,
            "url": url,
        })

    return branches


def score_branch_quick(d, br, seen_ids_seed=None, page_limit=2):
    form = br.get("form")
    vars_template = br.get("vars_template")
    if not form or not vars_template:
        raise ValueError("branch thiếu form/vars_template")

    # form sạch cursor để probe trang 1
    form0 = strip_cursors_from_form_on_form(form, vars_template)
    form0, boot_cursor = fast_forward_cursor(d, form0, vars_template, hops=1)
    tmp_seen = set(seen_ids_seed)  # không làm bẩn seen chính
    try:
        fresh, _ = paginate_window_cursor_only(
            d, form0, br["vars_template"], tmp_seen,
            page_limit=page_limit,
            branch_name=f"probe:{br.get('friendly') or br.get('doc_id') or 'x'}"
        )
        early_stall = (fresh < BRANCH_MIN_NEW_THRESH)
        return fresh, early_stall
    except Exception as e:
        print("[PROBE] error:", e)
        return 0, True

def choose_best_branch(d, branches, seen_ids_seed=None):
    # 🔧 Lọc nhánh thiếu metadata tối thiểu
    branches = [b for b in branches if b.get("form") and b.get("vars_template")]
    if not branches:
        raise RuntimeError("Không tìm thấy nhánh feed nào có đủ metadata (form + vars_template).")

    best = None
    best_score = (-1, 10**9)  # (fresh, stall) -> fresh max, stall min
    for br in branches:
        try:
            fresh, stall = score_branch_quick(d, br, seen_ids_seed, page_limit=BRANCH_PROBE_PAGES)
        except Exception as e:
            print(f"[WARN] Bỏ qua nhánh {br.get('doc_id') or br.get('op_name')} do lỗi probe: {e}")
            continue
        if (fresh, -stall) > (best_score[0], -best_score[1]):
            best_score = (fresh, stall)
            best = br
    if not best:
        raise RuntimeError("Không có nhánh nào probe thành công.")
    return best


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--resume", action="store_true", help="Tiếp tục từ cursor/branch trong checkpoint.")
    ap.add_argument("--page-limit", type=int, default=None, help="Giới hạn số trang crawl (test).")
    ap.add_argument("--headless", action="store_true", help="Chạy headless.")
    args = ap.parse_args()

    # Khởi chạy Chrome
    CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    USER_DATA_DIR = r"E:\NCS\Userdata"
    PROFILE_NAME  = "Profile 5"
    REMOTE_PORT   = 9222

    # try:
    #     d = start_driver(
    #         chrome_path=CHROME_PATH,
    #         user_data_dir=USER_DATA_DIR,
    #         profile_name=PROFILE_NAME,
    #         port=REMOTE_PORT,
    #         headless=args.headless
    #     )
    # except Exception:
    d = start_driver_with_proxy(PROXY_URL, headless=args.headless)

    d.set_script_timeout(40)
    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception: pass
    bootstrap_auth(d)
    try:
        install_early_hook(d, keep_last=KEEP_LAST)
    except Exception as e:
        print("[WARN] install_early_hook:", e)

    # Seed seen_ids
    state = load_checkpoint()
    seen_ids = set(state.get("seen_ids") or [])
    if args.resume and not seen_ids:
        seen_ids = rebuild_seen_ids_from_ndjson(OUT_NDJSON, max_lines=None)
        print(f"[SEED] seen_ids from NDJSON = {len(seen_ids)}")

    # === RESUME PATH ===
    if args.resume and state.get("branch") and state.get("cursor"):
        print(f"[RESUME] branch={state['branch']} cursor={str(state['cursor'])[:30]}...")
        # Reload một request feed để lấy form mới theo branch hiện có
        d.get(GROUP_URL); time.sleep(1.2)
        for _ in range(6):
            d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));"); time.sleep(0.5)
        nxt = wait_next_req(d, 0, is_group_feed_req, timeout=25, poll=0.25)
        if not nxt:
            form2, friendly2, docid2 = reload_and_refresh_form(d, GROUP_URL, state["cursor"], state.get("vars_template") or {})
            if not form2:
                print("[RESUME] reload_and_refresh_form cũng fail → thử scroll thêm lần nữa")
                d.get(GROUP_URL); time.sleep(1.2)
                for _ in range(10):
                    d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));"); time.sleep(0.4)
                nxt = wait_next_req(d, 0, is_group_feed_req, timeout=25, poll=0.25)
                if not nxt:
                    raise RuntimeError("Resume: không bắt được feed req sau reload+scroll.")
            else:
                base_form = form2
        else:
            _, req = nxt
            base_form = parse_form(req.get("body",""))

        _, req = nxt
        base_form = parse_form(req.get("body",""))
        vars_now  = get_vars_from_form(base_form)
        template_now = make_vars_template(vars_now)
        effective_template = state.get("vars_template") or template_now
        # gắn cursor từ checkpoint
        form = update_vars_for_next_cursor(base_form, state["cursor"], vars_template=effective_template)
        total, last_cur = paginate_window_cursor_only(
            d, form, effective_template, seen_ids,
            page_limit=args.page_limit,
            branch_name=state["branch"]
        )
        save_checkpoint(cursor=last_cur, seen_ids=list(seen_ids),
                        vars_template=effective_template, branch=state["branch"])
        print(f"[DONE] total new (resume) = {total} → {OUT_NDJSON}")
        raise SystemExit(0)

    # === FRESH RUN: discover branches & pick deepest ===
    d.get(GROUP_URL); time.sleep(1.2)
    for _ in range(6):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));"); time.sleep(0.6)

    branches = discover_branches_from_requests(d)
    # seed nhẹ từ file nếu chưa có
    if not seen_ids:
        seen_ids = rebuild_seen_ids_from_ndjson(OUT_NDJSON, max_lines=200000)

    best = choose_best_branch(d, branches, seen_ids_seed=seen_ids)

    # Tạo form sạch cursor cho nhánh best
    base_form = best["form"]
    effective_template = best["vars_template"] or make_vars_template(get_vars_from_form(base_form))
    form0 = strip_cursors_from_form_on_form(base_form, effective_template)

    # (Optional) vớt vài bài mới ở head
    try:
        txt = js_fetch_in_page(d, form0, {}, 15000)
        if not txt or len(txt) < 50:
            print("[FETCH] js_fetch empty/short, fallback wire")
            txt = fetch_via_wire(d, form0)
        obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
        buf = []
        collect_post_summaries(obj, buf)
        buf = coalesce_posts(filter_only_feed_posts(buf))
        fresh_head = []
        written = set()
        for p in buf:
            pk = _best_primary_key(p)
            if pk and pk not in seen_ids and pk not in written:
                fresh_head.append(p); written.add(pk)
        if fresh_head:
            append_ndjson_atomic(fresh_head)
            for p in fresh_head:
                for k in _all_join_keys(p): seen_ids.add(k)
            print(f"[HEAD] grabbed {len(fresh_head)} at head")
    except Exception: pass

    # Bắt đầu paginate theo cursor
    total, last_cur = paginate_window_cursor_only(
        d, form0, effective_template, seen_ids,
        page_limit=args.page_limit,
        branch_name=best.get("friendly") or (best.get("doc_id") or "best")
    )

    save_checkpoint(cursor=last_cur, seen_ids=list(seen_ids),
                    vars_template=effective_template,
                    branch=best.get("friendly") or (best.get("doc_id") or "best"))
    print(f"[DONE] total new = {total} → {OUT_NDJSON}")
