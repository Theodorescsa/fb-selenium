# -*- coding: utf-8 -*-
"""
Facebook GraphQL Feed Crawler — Cursor-first (no time-slice)
- Hook sớm /api/graphql để "hứng" request và response.
- Resume bằng cursor + seen_ids trong checkpoint (nhưng KHÔNG ép dùng cursor cũ nếu có thể bám head).
- Head-probe: luôn "đặt chân" vào trang đầu hiện tại để lấy endCursor mới nhất trước khi paginate.
- Khi tiến độ chậm / next=False nhiều vòng: thử soft-refetch (không đụng UI), rồi hard-reload form/doc_id.
- Fast-forward (nhảy 1-2 hops) khi fresh=0 nhưng has_next=True lặp lại.

⚠️ Chỉ crawl nội dung bạn có quyền truy cập. Tôn trọng Điều khoản sử dụng của nền tảng.
"""
import argparse
import os, re, json, time, random, datetime, urllib.parse, socket
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode
from pathlib import Path

from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException as _SETimeout

# ==== custom utils bạn đã có trong get_info.py (yêu cầu file này tồn tại) ====
from get_info import *
from get_info import _all_urls_from_text
from get_info import _dig_attachment_urls

# =========================
# CONFIG — chỉnh theo máy bạn
# =========================

HERE = Path(__file__).resolve().parent

# Page/Group/Profile gốc bạn muốn crawl
GROUP_URL     = "https://www.facebook.com/thoibao.de"

# (Optional) Nếu muốn nạp login thủ công từ file, set path 2 hằng dưới; nếu không, để None:
COOKIES_PATH         = HERE / "authen" / "cookies.json"
LOCALSTORAGE_PATH    = HERE / "authen" / "localstorage.json"
SESSIONSTORAGE_PATH  = HERE / "authen" / "sessionstorage.json"

# Proxy tuỳ chọn cho selenium-wire (để trống nếu không dùng)
PROXY_URL = ""

# Lưu trữ
KEEP_LAST     = 350
OUT_NDJSON    = "posts_all.ndjson"
RAW_DUMPS_DIR = "raw_dumps"
CHECKPOINT    = "checkpoint.json"

os.makedirs(RAW_DUMPS_DIR, exist_ok=True)

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
def _wait_port(host: str, port: int, timeout: float = 20.0, poll: float = 0.1) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except Exception:
            time.sleep(poll)
    return False

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
            # "verify_ssl": False,
        }

    driver = webdriver.Chrome(options=chrome_opts, seleniumwire_options=sw_options)
    driver.scopes = [r".*"]
    return driver

# =========================
# bootstrap_auth — nạp cookies/localStorage nếu có
# =========================
ALLOWED_COOKIE_DOMAINS = {".facebook.com", "facebook.com", "m.facebook.com", "web.facebook.com"}

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

def _add_cookies_safely(driver, cookies_path: Path):
    with open(cookies_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if isinstance(raw, dict) and "cookies" in raw:
        raw = raw["cookies"]
    if not isinstance(raw, list):
        raise ValueError("File cookies không phải mảng JSON.")

    added = 0
    for c in raw:
        nc = _normalize_cookie(c)
        if not nc:
            continue
        try:
            driver.add_cookie(nc)
            added += 1
        except Exception:
            pass
    return added

def _set_kv_storage(driver, kv_path: Path, storage: str = "localStorage"):
    with open(kv_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        for k, v in data.items():
            driver.execute_script(f"{storage}.setItem(arguments[0], arguments[1]);", k, v)

def bootstrap_auth(d):
    d.get("https://www.facebook.com/")
    time.sleep(1.0)

    if COOKIES_PATH and os.path.exists(COOKIES_PATH):
        try:
            count = _add_cookies_safely(d, Path(COOKIES_PATH))
            d.get("https://www.facebook.com/")
            time.sleep(1.0)
            print(f"[AUTH] Added cookies: {count}")
        except Exception as e:
            print("[WARN] bootstrap cookies:", e)

    # if LOCALSTORAGE_PATH and os.path.exists(LOCALSTORAGE_PATH):
    #     try:
    #         d.get("https://www.facebook.com/")
    #         _set_kv_storage(d, Path(LOCALSTORAGE_PATH), "localStorage")
    #         d.get("https://www.facebook.com/")
    #         time.sleep(0.8)
    #     except Exception as e:
    #         print("[WARN] bootstrap localStorage:", e)

    # if SESSIONSTORAGE_PATH and os.path.exists(SESSIONSTORAGE_PATH):
    #     try:
    #         d.get("https://www.facebook.com/")
    #         _set_kv_storage(d, Path(SESSIONSTORAGE_PATH), "sessionStorage")
    #         d.get("https://www.facebook.com/")
    #         time.sleep(0.8)
    #     except Exception as e:
    #         print("[WARN] bootstrap sessionStorage:", e)

    try:
        all_cookies = {c["name"]: c.get("value") for c in d.get_cookies()}
        has_cuser = "c_user" in all_cookies
        has_xs    = "xs" in all_cookies
        print(f"[AUTH] c_user={has_cuser}, xs={has_xs}")
    except Exception:
        pass

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

# =========================
# Cursor / next helpers
# =========================
CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}

def fetch_via_wire(driver, form):
    url = "https://www.facebook.com/api/graphql/"
    body = urlencode(form)
    resp = driver.request(
        "POST", url,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.facebook.com",
            "Referer": "https://www.facebook.com/"
        },
        timeout=25
    )
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

# =========================
# JS fetch with page cookies
# =========================
def js_fetch_in_page(driver, form_dict, extra_headers=None, timeout_ms=20000):
    """
    Chạy fetch ngay TRONG context page (giữ cookie), có timeout bằng AbortController.
    Trả về text body. Ném RuntimeError nếu fail.
    """
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
              method: "POST",
              headers,
              body,
              credentials: "include",
              signal: ctrl.signal
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
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
    if not obj:
        return None, None, None, None

    cursors = deep_collect_cursors(obj)
    new_has_next = deep_find_has_next(obj)
    if new_has_next is None: 
        new_has_next = bool(cursors)
    new_cursor = cursors[0][1] if cursors else None

    if new_cursor:
        new_form = update_vars_for_next_cursor(new_form, new_cursor, vars_template)

    return new_form, new_cursor, new_has_next, obj

def reload_and_refresh_form(d, group_url, cursor, vars_template, timeout=25, poll=0.25):
    d.get(group_url); time.sleep(1.5)
    for _ in range(4):
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
        if not nf or not nc: 
            break
        cur_form = nf
        last_cursor = nc
    return cur_form, last_cursor

# =========================
# Post collectors (ưu tiên rid + link + created_time)
# =========================
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
            for k in ("title", "subtitle", "headline", "label", "contextual_message"):
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
    if not actor_texts:
        actor_texts.extend(_dig_text(n))
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
            created_candidates = deep_collect_timestamps(obj)
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
            _k, _v = deep_get_first(obj, {"group_id", "groupID", "groupIDV2"})
            if _v: source_id = _v
            if not source_id:
                try:
                    slug = re.search(r"/groups/([^/?#]+)", group_url).group(1)
                    source_id = slug
                except:
                    pass

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
            if share_meta:
                item["share_meta"] = share_meta
            if smart_is_share:
                item["content_parts"] = {
                    "actor_text": actor_text,
                    "attached_text": attached_text
                }
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
# Dedupe/merge (rid + normalized link)
# =========================
def _norm_link(u: str) -> Optional[str]:
    if not u or not isinstance(u, str):
        return None
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
# Checkpoint / Output
# =========================
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
    data["ts"] = datetime.datetime.now().isoformat(timespec="seconds")
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def append_ndjson(items):
    if not items: return
    with open(OUT_NDJSON, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

def normalize_seen_ids(seen_ids):
    return set(seen_ids or [])

# =========================
# Paginate 1 window (NO time slice khi gọi từ cursor-only)
# =========================
def paginate_window(d, form, vars_template, seen_ids: set,
                    t_from: Optional[int]=None, t_to: Optional[int]=None,
                    page_limit: Optional[int]=None) -> Tuple[int, Optional[int], bool]:
    last_good_cursor = current_cursor_from_form(form) or None
    cursor_stall_rounds = 0
    prev_cursor = None

    total_new = 0
    min_created = None
    no_progress_rounds = 0

    mode_str = "time" if (t_from is not None or t_to is not None) else "warmup"
    if mode_str == "time":
        print(f"[MODE] Time-slice window: from={t_from} to={t_to}")

    if (t_from is not None) or (t_to is not None):
        # không dùng trong cursor-only, nhưng giữ để tái sử dụng
        base = json.loads(form.get("variables","{}")) if form.get("variables") else {}
        known_keys = set(base.keys())
        cand_after = "afterTime"  if "afterTime"  in known_keys else "after_time"
        cand_before= "beforeTime" if "beforeTime" in known_keys else "before_time"
        base = merge_vars(base, vars_template)
        if t_from is not None:  base[cand_after]  = int(t_from)
        if t_to   is not None:  base[cand_before] = int(t_to)
        if "count" in base and isinstance(base["count"], int):
            base["count"] = max(base["count"], 10)
        form["variables"] = json.dumps(base, separators=(",", ":"))

    page = 0
    has_next = False
    cursor_for_reload = None

    while True:
        page += 1
        max_tries = 3
        last_err = None
        for attempt in range(1, max_tries+1):
            try:
                txt = js_fetch_in_page(d, form, extra_headers={}, timeout_ms=20000)
                break
            except (_SETimeout, RuntimeError) as e:
                last_err = e
                if "bad_origin:" in str(e):
                    d.get(GROUP_URL); time.sleep(1.2)
                    try:
                        txt = js_fetch_in_page(d, form, extra_headers={}, timeout_ms=20000)
                        break
                    except Exception:
                        pass
                print(f"[WARN] fetch page try {attempt}/{max_tries} failed: {e}")
                time.sleep(random.uniform(0.8, 1.6))

                if attempt == 2:
                    new_form, boot_cursor, boot_has_next, _ = soft_refetch_form_and_cursor(d, form, vars_template)
                    if new_form:
                        form = new_form
                        if (t_from is not None) or (t_to is not None):
                            base = json.loads(form.get("variables","{}")) if form.get("variables") else {}
                            known_keys = set(base.keys())
                            cand_after = "afterTime"  if "afterTime"  in known_keys else "after_time"
                            cand_before= "beforeTime" if "beforeTime" in known_keys else "before_time"
                            base = merge_vars(base, vars_template)
                            if t_from is not None:  base[cand_after]  = int(t_from)
                            if t_to   is not None:  base[cand_before] = int(t_to)
                            form["variables"] = json.dumps(base, separators=(",", ":"))

                if attempt == max_tries:
                    form2, friendly2, docid2 = reload_and_refresh_form(d, GROUP_URL, None, vars_template)
                    if form2:
                        form = form2
                        if (t_from is not None) or (t_to is not None):
                            base = json.loads(form.get("variables","{}")) if form.get("variables") else {}
                            known_keys = set(base.keys())
                            cand_after = "afterTime"  if "afterTime"  in known_keys else "after_time"
                            cand_before= "beforeTime" if "beforeTime" in known_keys else "before_time"
                            base = merge_vars(base, vars_template)
                            if t_from is not None:  base[cand_after]  = int(t_from)
                            if t_to   is not None:  base[cand_before] = int(t_to)
                            form["variables"] = json.dumps(base, separators=(",", ":"))
                    try:
                        txt = js_fetch_in_page(d, form, extra_headers={}, timeout_ms=25000)
                        break
                    except Exception:
                        try:
                            txt = fetch_via_wire(d, form)
                            if txt: break
                        except Exception:
                            pass
                        raise

        obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
        with open(os.path.join(RAW_DUMPS_DIR, f"slice_{t_from or 'None'}_{t_to or 'None'}_p{page}.json"), "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

        if not obj:
            print(f"[SLICE {t_from}->{t_to}] parse fail → stop slice.")
            break

        page_posts = []
        collect_post_summaries(obj, page_posts)
        page_posts = coalesce_posts(filter_only_feed_posts(page_posts))

        written_this_round = set()
        fresh = []
        for p in page_posts:
            pk = _best_primary_key(p)
            if pk and (pk not in seen_ids) and (pk not in written_this_round):
                fresh.append(p); written_this_round.add(pk)

        if fresh:
            append_ndjson(fresh)
            for p in fresh:
                for k in _all_join_keys(p): seen_ids.add(k)
            total_new += len(fresh)
            no_progress_rounds = 0
        else:
            no_progress_rounds += 1

        for p in page_posts:
            ct = p.get("created_time")
            if isinstance(ct, int):
                if (min_created is None) or (ct < min_created):
                    min_created = ct

        cursors = deep_collect_cursors(obj)
        has_next = deep_find_has_next(obj)
        if not fresh and has_next:
            cursor_stall_rounds += 1
        else:
            cursor_stall_rounds = 0
        if cursor_stall_rounds >= 6:
            print("[STALL] next=True & fresh=0 nhiều vòng → fast-forward 2 hops")
            ff_form, ff_cursor = fast_forward_cursor(d, form, vars_template, hops=2)
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
            except: 
                pass
            time.sleep(random.uniform(0.3, 0.6))
            continue

        if new_cursor:
            cursor_for_reload = new_cursor
        elif not cursor_for_reload:
            cursor_for_reload = current_cursor_from_form(form)

        if new_cursor and prev_cursor == new_cursor:
            print(f"[WARN] cursor lặp lại → thử soft-refetch")
            nf, bc, bh, _ = soft_refetch_form_and_cursor(d, form, vars_template)
            if nf and (bc or bh):
                form = nf
                if bc:
                    new_cursor = bc
                    print(f"[FIX] lấy được cursor mới sau refetch.")
            else:
                f2, _, _ = reload_and_refresh_form(d, GROUP_URL, (last_good_cursor or current_cursor_from_form(form)), vars_template)
                if f2:
                    form = f2
                    try:
                        v = json.loads(form.get("variables","{}"))
                        if isinstance(v.get("count"), int):
                            v["count"] = min(max(v["count"] + 10, 20), 60)
                            form["variables"] = json.dumps(v, separators=(",",":"))
                    except:
                        pass
                    no_progress_rounds = 0
                    time.sleep(random.uniform(0.8, 1.3))
                    continue

        if new_cursor:
            last_good_cursor = new_cursor
            prev_cursor = new_cursor

        print(f"[SLICE {t_from or '-inf'}→{t_to or '+inf'}] p{page} got {len(page_posts)} (new {len(fresh)}), total_new={total_new}, next={has_next}")

        save_checkpoint(
            cursor=last_good_cursor,
            seen_ids=list(seen_ids),
            vars_template=vars_template,
            mode=mode_str,
            slice_from=t_from,
            slice_to=t_to,
            year=(datetime.datetime.utcfromtimestamp(t_to).year
                  if (t_to and mode_str == "time") else None),
            page=page,
            min_created=min_created
        )

        MAX_NO_NEXT_ROUNDS = 3
        if not has_next and no_progress_rounds >= MAX_NO_NEXT_ROUNDS:
            print(f"[PAGE#{page}] next=False x{no_progress_rounds} → soft-refetch doc_id/variables (no UI)")
            if not cursor_for_reload:
                cursor_for_reload = last_good_cursor or current_cursor_from_form(form)

            save_checkpoint(
                cursor=cursor_for_reload or last_good_cursor,
                seen_ids=list(seen_ids),
                vars_template=vars_template,
                mode=mode_str,
                slice_from=t_from,
                slice_to=t_to,
                year=(datetime.datetime.utcfromtimestamp(t_to).year
                    if (t_to and mode_str == "time") else None),
                page=page,
                min_created=min_created
            )

            refetch_ok = False
            for attempt in range(1, 3):
                new_form, boot_cursor, boot_has_next, boot_obj = soft_refetch_form_and_cursor(
                    d, form, vars_template
                )
                if new_form and (boot_cursor or boot_has_next):
                    form = new_form
                    if boot_cursor:
                        cursor = boot_cursor
                    has_next = bool(boot_has_next)
                    no_progress_rounds = 0
                    refetch_ok = True
                    print(f"[PAGE#{page}] soft-refetch OK (attempt {attempt}) → has_next={has_next} | cursor={str(cursor)[:24] if cursor else None}")
                    break
                time.sleep(random.uniform(1.0, 2.0))

            if not refetch_ok:
                print(f"[PAGE#{page}] soft-refetch failed → stop pagination.")
                break

        if new_cursor:
            form = update_vars_for_next_cursor(form, new_cursor, vars_template)
            cursor_for_reload = new_cursor

        if page_limit and page >= page_limit:
            break

        time.sleep(random.uniform(0.7, 1.4))

    return total_new, min_created, bool(has_next)

# =========================
# Head-probe: bám đầu feed hiện tại
# =========================
def probe_head(driver, base_form, vars_template, k=5):
    try:
        v = json.loads(base_form.get("variables","{}"))
    except: 
        v = {}
    v = strip_cursors_from_vars(merge_vars(v, vars_template))
    form0 = dict(base_form); form0["variables"] = json.dumps(v, separators=(",",":"))

    txt = js_fetch_in_page(driver, form0, extra_headers={
        "Cache-Control": "no-cache", "Pragma": "no-cache",
    })
    obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
    if not obj: 
        return None, [], None

    page_posts = []
    collect_post_summaries(obj, page_posts)
    page_posts = coalesce_posts(filter_only_feed_posts(page_posts))
    top = page_posts[:k]

    cursors = deep_collect_cursors(obj)
    head_cursor = cursors[0][1] if cursors else None

    if head_cursor:
        form1 = update_vars_for_next_cursor(form0, head_cursor, vars_template)
    else:
        form1 = form0

    return form1, top, head_cursor

# =========================
# Runner: THUẦN CURSOR
# =========================
def strip_cursors_from_form_on_form(form, vars_template=None):
    """
    Tạo bản copy của form, loại bỏ tất cả các trường cursor/endCursor/after... trong phần variables.
    Giữ lại các biến khác (ví dụ: id, count, scale, viewerID...).
    """
    import json

    # Lấy ra biến variables từ form
    try:
        v = json.loads(form.get("variables", "{}"))
    except Exception:
        v = {}

    # Các key cần xoá
    CURSOR_KEYS = {
        "cursor", "after", "endCursor", "afterCursor",
        "feedAfterCursor", "before", "beforeCursor"
    }

    # Hàm đệ quy loại bỏ key trong dict con
    def _strip(o):
        if isinstance(o, dict):
            new = {}
            for k, val in o.items():
                if k in CURSOR_KEYS:
                    continue
                new[k] = _strip(val)
            return new
        elif isinstance(o, list):
            return [_strip(x) for x in o]
        else:
            return o

    cleaned = _strip(v)

    # Merge lại template (nếu có)
    if vars_template:
        try:
            cleaned = merge_vars(cleaned, vars_template)
        except Exception:
            pass

    # Trả lại form mới (copy)
    new_form = dict(form)
    new_form["variables"] = json.dumps(cleaned, separators=(",", ":"))
    return new_form

def run_cursor_only(d, form, vars_template, seen_ids, page_limit=None, resume=False):
    """
    Cursor-only paging. Nếu resume=True => KHÔNG boot ở head, đi thẳng từ checkpoint cursor.
    """
    total = 0

    # === (A) HEAD-BOOT CHỈ KHI resume=False ===
    if not resume:
        fresh_head = 0
        try:
            # 1 nhát head nhanh để vớt bài siêu mới (tuỳ bạn giữ hay bỏ)
            txt = js_fetch_in_page(d, strip_cursors_from_form_on_form(form, vars_template(form)), {}, 15000)  # pseudo
            obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
            buf = []
            collect_post_summaries(obj, buf)
            buf = coalesce_posts(filter_only_feed_posts(buf))
            written = []
            for p in buf:
                pk = _best_primary_key(p)
                if pk and pk not in seen_ids:
                    written.append(p)
                    for k in _all_join_keys(p): seen_ids.add(k)
            append_ndjson(written)
            fresh_head = len(written)
            if fresh_head:
                print(f"[HEAD] grabbed {fresh_head} fresh at head")
        except Exception:
            pass  # không quan trọng
        total += fresh_head
    else:
        print("[RESUME] Skip head-probe; continue strictly from checkpoint cursor.")

    # === (B) CHẠY PAGINATION THEO CURSOR ===
    # Không set time window; để [-inf, +inf]
    add, _, _ = paginate_window(
        d, form, vars_template, seen_ids,
        t_from=None, t_to=None,
        page_limit=page_limit
    )
    total += add
    return total

# =========================
# MAIN
# =========================
# from get_posts_fb_automation import start_driver
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--resume", action="store_true",
                    help="Tiếp tục từ cursor trong checkpoint thay vì bám head.")
    ap.add_argument("--page-limit", type=int, default=None,
                    help="Giới hạn số trang để test (None = không giới hạn).")
    args = ap.parse_args()

    # CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    # USER_DATA_DIR = r"E:\NCS\Userdata"
    # PROFILE_NAME  = "Profile 5"
    # REMOTE_PORT   = 9222

    # # Nếu bạn đã có start_driver(...), dùng nó; không thì đổi lại start_driver_with_proxy(PROXY_URL, headless=False)
    # d = start_driver(
    #     chrome_path=CHROME_PATH,
    #     user_data_dir=USER_DATA_DIR,
    #     profile_name=PROFILE_NAME,
    #     port=REMOTE_PORT,
    #     headless=False
    # )
    d = start_driver_with_proxy(PROXY_URL, headless=False)
    d.set_script_timeout(40)
    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception:
        pass

    # Nếu đang dùng profile thật (USER_DATA_DIR), có thể bỏ bootstrap_auth.
    bootstrap_auth(d)

    try:
        install_early_hook(d, keep_last=KEEP_LAST)
    except Exception as e:
        print("[WARN] install_early_hook:", e)

    d.get(GROUP_URL); time.sleep(1.2)
    for _ in range(6):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));"); time.sleep(0.6)

    nxt = wait_next_req(d, 0, is_group_feed_req, timeout=25, poll=0.25)
    if not nxt:
        raise RuntimeError("Không bắt được request feed. Hãy cuộn thêm/kiểm tra quyền.")
    _, first_req = nxt
    form         = parse_form(first_req.get("body", ""))
    friendly     = urllib.parse.parse_qs(first_req.get("body","")).get("fb_api_req_friendly_name", [""])[0]
    vars_now     = get_vars_from_form(form)
    template_now = make_vars_template(vars_now)

    state = load_checkpoint()
    seen_ids      = normalize_seen_ids(state.get("seen_ids"))
    cursor_ckpt   = state.get("cursor")                 # cursor đã lưu lần trước (last_good_cursor)
    vars_template = state.get("vars_template") or template_now
    effective_template = vars_template or template_now

    # ✅ Resume đúng vị trí (nếu có --resume và có cursor trong checkpoint)
    if args.resume and cursor_ckpt:
        form = update_vars_for_next_cursor(form, cursor_ckpt, vars_template=effective_template)
        print(f"[RESUME] Dùng lại cursor từ checkpoint: {str(cursor_ckpt)[:40]}...")

    # 🔁 Chạy crawl theo cursor-only (không time-slice)
    total_got = run_cursor_only(
        d, form, effective_template, seen_ids,
        page_limit=args.page_limit,
        resume=args.resume   # ✅ quan trọng
    )

    # Lưu checkpoint cuối (giữ seen_ids & template; cursor đã được cập nhật trong quá trình paginate)
    save_checkpoint(cursor=None, seen_ids=list(seen_ids), vars_template=effective_template,
                    mode=None, slice_from=None, slice_to=None, year=None)
    print(f"[DONE] total new written (cursor-only) = {total_got} → {OUT_NDJSON}")
