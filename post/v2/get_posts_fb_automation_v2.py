# -*- coding: utf-8 -*-
"""
Facebook Group GraphQL crawler — proxy-only, resume-first, no UI scrolling except boot.
- Selenium-Wire + proxy
- Load cookies + localStorage + sessionStorage
- Early hook /api/graphql to buffer requests
- Resume by cursor; coalesce multi-keys; soft-refetch when has_next stalls

⚠️ Crawl nơi bạn có quyền. Tôn trọng ToS.
"""

# =============== Imports ===============
import os, re, json, time, random, datetime, urllib.parse, socket
from typing import Dict, Any, List, Optional
from pathlib import Path

from seleniumwire import webdriver                      # pip install selenium-wire
from selenium.webdriver.chrome.options import Options
from get_info import (deep_get_first, extract_author, extract_created_time, 
                      extract_hashtags, extract_media, extract_reactions_and_counts, 
                      extract_share_flags   )

# =============== Config ===============
HERE = Path(__file__).resolve().parent
PROXY_URL            = ""
GROUP_URL            = "https://www.facebook.com/thoibao.de"

KEEP_LAST            = 350
OUT_NDJSON           = "posts_all.ndjson"
RAW_DUMPS_DIR        = "raw_dumps"
CHECKPOINT           = "checkpoint.json"
POST_URL_RE = re.compile(r"https?://(?:web\.)?facebook\.com/groups/[^/]+/(?:permalink|posts)/(\d+)/?$", re.I)


# ✅ FIX: point to authen folder under this script directory
COOKIES_PATH         = HERE / "authen" / "cookies.json"
LOCALSTORAGE_PATH    = HERE / "authen" / "localstorage.json"
SESSIONSTORAGE_PATH  = HERE / "authen" / "sessionstorage.json"

CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}
FB_ORIGINS = [
    "https://www.facebook.com",
    "https://web.facebook.com",
    "https://m.facebook.com",
]
os.makedirs(HERE / RAW_DUMPS_DIR, exist_ok=True)

# ====== CDP setter fallback (edit to your own if you already have one) ======
def _cdp_set_cookie_raw(driver, cookie: Dict[str, Any], url: Optional[str] = None) -> bool:
    """
    Sets a cookie via DevTools. Accepts fields like:
    name, value, domain, path, expires/expiry, httpOnly, secure, sameSite.
    If url is provided, DevTools requires it OR domain/path pair.
    """
    try:
        params = {"name": cookie["name"], "value": cookie.get("value", "")}
        # Prefer URL if passed (simplifies site-scoped cookies)
        if url:
            params["url"] = url
        else:
            if "domain" in cookie: params["domain"] = cookie["domain"]
            if "path" in cookie:   params["path"] = cookie["path"]

        # Expiry mapping
        exp = cookie.get("expiry", cookie.get("expires"))
        if isinstance(exp, (int, float)): params["expires"] = float(exp)

        if "httpOnly" in cookie: params["httpOnly"] = bool(cookie["httpOnly"])
        if "secure"   in cookie: params["secure"]   = bool(cookie["secure"])
        if "sameSite" in cookie:
            ss = str(cookie["sameSite"]).capitalize()
            if ss in {"Lax","Strict","None"}: params["sameSite"] = ss

        driver.execute_cdp_cmd("Network.setCookie", params)
        return True
    except Exception:
        return False

# ------------------ Loaders (robust to many formats) ------------------
def _read_text(p: Path) -> Optional[str]:
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        return None

def _load_json_file(p: Path) -> Optional[Any]:
    if not p or not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def _normalize_cookies(any_obj: Any) -> List[Dict[str, Any]]:
    """
    Supports:
    - EditThisCookie array: [{name,value,domain,path,expirationDate,...}]
    - Selenium cookies list: [{name,value,domain,path,expiry,...}]
    - Your format: {"cookies":[...]}
    - Netscape cookie text (fallback)
    """
    cookies: List[Dict[str, Any]] = []

    # Dict wrapper {"cookies":[...]}
    if isinstance(any_obj, dict) and "cookies" in any_obj and isinstance(any_obj["cookies"], list):
        any_obj = any_obj["cookies"]

    # Array JSON
    if isinstance(any_obj, list):
        for c in any_obj:
            if not isinstance(c, dict): continue
            name = c.get("name")
            value = c.get("value", c.get("content", ""))
            if not name: continue
            ck = {
                "name": name,
                "value": value,
                "domain": c.get("domain") or c.get("host", ".facebook.com"),
                "path": c.get("path", "/"),
                "secure": bool(c.get("secure", True)),
                "httpOnly": bool(c.get("httpOnly", False)),
            }
            # expiry / expirationDate
            if "expiry" in c: ck["expiry"] = c["expiry"]
            elif "expires" in c: ck["expiry"] = c["expires"]
            elif "expirationDate" in c: ck["expiry"] = c["expirationDate"]
            # sameSite variants
            ss = c.get("sameSite") or c.get("sameSiteAttribute")
            if ss: ck["sameSite"] = ss
            cookies.append(ck)
        return cookies

    # Netscape cookie.txt fallback
    if isinstance(any_obj, str):
        lines = any_obj.splitlines()
        for line in lines:
            if not line or line.startswith("#"): continue
            parts = line.split("\t")
            if len(parts) >= 7:
                domain, _flag, path, secure, expires, name, value = parts[:7]
                ck = {
                    "name": name,
                    "value": value.strip(),
                    "domain": domain.strip(),
                    "path": path.strip() if path else "/",
                    "secure": (secure.strip().upper()=="TRUE"),
                }
                try:
                    ck["expiry"] = int(expires)
                except Exception:
                    pass
                cookies.append(ck)
        return cookies

    return cookies

def _load_cookies_any_format(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        print(f"[AUTH] Cookie file not found: {path}")
        return []
    # Try JSON
    obj = _load_json_file(path)
    if obj is not None:
        return _normalize_cookies(obj)
    # Try Netscape text
    txt = _read_text(path)
    if txt:
        return _normalize_cookies(txt)
    return []

def _load_localstorage_any_format(path: Path) -> Dict[str, Any]:
    """
    Supports:
    - Dict: {"key1":"value1", ...}
    - Array: [{"key":"foo","value":"bar"}, ...]
    """
    js = _load_json_file(path)
    if js is None: return {}
    if isinstance(js, dict):
        return js
    if isinstance(js, list):
        out = {}
        for item in js:
            if isinstance(item, dict) and "key" in item:
                out[item["key"]] = item.get("value", "")
        return out
    return {}
def _cdp_set_cookie(driver,cookie: Dict[str, Any], url: Optional[str] = None) -> bool:
    assert driver is not None
    driver.execute_cdp_cmd("Network.enable", {})
    name  = cookie.get("name") or cookie.get("Name")
    value = cookie.get("value") or cookie.get("Value")
    domain = cookie.get("domain") or cookie.get("Domain")
    path = cookie.get("path") or cookie.get("Path") or "/"
    secure = bool(cookie.get("secure") or cookie.get("Secure") or False)
    httpOnly = bool(cookie.get("httpOnly") or cookie.get("HttpOnly") or False)
    same_site_raw = (cookie.get("sameSite") or cookie.get("SameSite") or "")
    if isinstance(same_site_raw, str):
        same_site_raw = same_site_raw.capitalize()
    sameSite = same_site_raw if same_site_raw in ("Lax","Strict","None") else None
    expires = cookie.get("expires") or cookie.get("expiry") or cookie.get("expirationDate")
    if isinstance(expires, str):
        try: expires = float(expires)
        except: expires = None
    if isinstance(expires, (int, float)) and expires <= 0:
        expires = None

    params: Dict[str, Any] = {
        "name": name, "value": value,
        "path": path,
        "secure": secure, "httpOnly": httpOnly,
    }
    if sameSite: params["sameSite"] = sameSite
    if isinstance(expires, (int, float)): params["expires"] = expires

    if url:
        params["url"] = url
    elif domain:
        params["domain"] = domain
    else:
        params["url"] = "https://www.facebook.com/"

    try:
        res = driver.execute_cdp_cmd("Network.setCookie", params)
        return bool(res.get("success"))
    except Exception:
        return False

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

def parse_form(body_str: str) -> Dict[str, str]:
    qs = urllib.parse.parse_qs(body_str, keep_blank_values=True)
    return {k: (v[0] if isinstance(v, list) else v) for k, v in qs.items()}

# =============== Selenium boot (proxy-only) ===============
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

    sw_options = None
    if proxy_url:
        sw_options = {
            "proxy": {
                "http":  proxy_url,
                "https": proxy_url,
                "no_proxy": "localhost,127.0.0.1",
            },
            # "verify_ssl": False,  # nếu proxy self-signed
        }

    driver = webdriver.Chrome(options=chrome_opts, seleniumwire_options=sw_options)
    driver.scopes = ['.*']  # hook all; có thể thu hẹp sau
    return driver

# =============== Session bootstrap (cookies + storage) ===============
def _coerce_expiry(ts) -> Optional[int]:
    try:
        if ts is None: return None
        return int(ts)
    except Exception:
        return None

def apply_cookies(driver, cookies: List[Dict[str, Any]], base_url="https://www.facebook.com/"):
    driver.get(base_url); time.sleep(0.5)
    driver.delete_all_cookies()
    for c in (cookies or []):
        try:
            ck = {
                "name":   c.get("name"),
                "value":  c.get("value", ""),
                "domain": c.get("domain", ".facebook.com"),
                "path":   c.get("path", "/"),
                "secure": bool(c.get("secure", True)),
            }
            exp = _coerce_expiry(c.get("expires") or c.get("expiry"))
            if exp: ck["expiry"] = exp
            if "sameSite" in c: ck["sameSite"] = c["sameSite"]
            if "httpOnly" in c: ck["httpOnly"] = bool(c["httpOnly"])
            try:
                driver.add_cookie(ck)
            except Exception:
                ck.pop("sameSite", None); ck.pop("httpOnly", None)
                driver.add_cookie(ck)
        except Exception as e:
            print("[WARN] add_cookie failed:", e)
    driver.get(base_url); time.sleep(0.7)

def apply_storage(driver, local_kv: Dict[str, str] = None, session_kv: Dict[str, str] = None, origin="https://www.facebook.com/"):
    driver.get(origin); time.sleep(0.4)
    if local_kv:
        driver.execute_script("""
            const data = arguments[0] || {};
            Object.entries(data).forEach(([k,v]) => { try{ localStorage.setItem(k, String(v)); }catch(e){} });
        """, local_kv)
    if session_kv:
        driver.execute_script("""
            const data = arguments[0] || {};
            Object.entries(data).forEach(([k,v]) => { try{ sessionStorage.setItem(k, String(v)); }catch(e){} });
        """, session_kv)
    driver.get(origin); time.sleep(0.4)

def bootstrap_auth(driver):
    print(f"[AUTH] Using:\n - COOKIES_PATH={COOKIES_PATH}\n - LOCALSTORAGE_PATH={LOCALSTORAGE_PATH}")
    cookies_list = _load_cookies_any_format(COOKIES_PATH)
    print(f"[AUTH] Loaded {len(cookies_list)} cookies from file.")

    # 1) Hit the domain so LS is available, also enables add_cookie fallback if needed
    driver.get("https://www.facebook.com/")
    time.sleep(1.0)

    # 2) Set cookies via CDP; mirror across origins to be safe
    ok = 0
    for c in cookies_list:
        if _cdp_set_cookie_raw(driver, c):
            ok += 1
    ok_host = 0
    for origin in FB_ORIGINS:
        for c in cookies_list:
            if _cdp_set_cookie_raw(driver, c, url=origin + "/"):
                ok_host += 1
    print(f"[AUTH] cookies set: domain_ok={ok}/{len(cookies_list)} | host_mirrors={ok_host}")

    # 3) LocalStorage
    ls_dict = _load_localstorage_any_format(LOCALSTORAGE_PATH)
    if ls_dict:
        driver.execute_script("window.localStorage.clear();")
        script = """
            const data = arguments[0];
            for (const [k,v] of Object.entries(data)) {
                try { localStorage.setItem(k, (typeof v === 'string') ? v : JSON.stringify(v)); } catch(e) {}
            }
            return Object.keys(data).length;
        """
        set_count = driver.execute_script(script, ls_dict)
        print(f"[AUTH] Injected {set_count} localStorage keys.")
        driver.refresh()
        time.sleep(0.8)
    else:
        print("[AUTH] No localStorage keys loaded (file empty or wrong format).")

    print(f"[AUTH] Ready at {driver.execute_script('return location.href;')}")

# =============== Early hook /api/graphql/ ===============
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

# =============== Hook buffer helpers ===============
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

# =============== Request matching ===============
def is_group_feed_req(rec):
    if "/api/graphql/" not in (rec.get("url") or ""): return False
    if (rec.get("method") or "").upper() != "POST": return False
    body = rec.get("body") or ""
    if "fb_api_req_friendly_name=" in body:
        if re.search(r"(?:GroupComet|CometGroup|GroupsComet).*(?:Feed|Stories).*Pagination", body, re.I):
            return True
    try:
        v = parse_form(body).get("variables","")
        vj = json.loads(urllib.parse.unquote_plus(v))
        if any(k in vj for k in ["groupID","groupIDV2","id"]) and any(
            k in vj for k in ["after","cursor","endCursor","afterCursor","feedAfterCursor"]
        ):
            return True
    except:
        pass
    return False

# =============== Cursor helpers ===============
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

# =============== Variables template helpers ===============
def strip_cursors_from_vars(v: dict) -> dict:
    if not isinstance(v, dict): return {}
    return {k: v for k, v in v.items() if k not in CURSOR_KEYS}

def get_vars_from_form(form_dict):  # safe load
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

# =============== JS fetch with current page cookies ===============
def js_fetch_in_page(driver, form_dict, extra_headers=None):
    script = """
    const url = "/api/graphql/";
    const form = arguments[0];
    const extra = arguments[1] || {};
    const headers = Object.assign({"Content-Type":"application/x-www-form-urlencoded"}, extra);
    const body = new URLSearchParams(form).toString();
    return fetch(url, {method:"POST", headers, body, credentials:"include"}).then(r=>r.text());
    """
    return driver.execute_script(script, form_dict, extra_headers or {})

def soft_refetch_form_and_cursor(driver, form, effective_template):
    try:
        base = json.loads(form.get("variables", "{}"))
    except Exception:
        base = {}
    base = merge_vars(base, effective_template)
    base = strip_cursors_from_vars(base)

    new_form = dict(form)
    new_form["variables"] = json.dumps(base, separators=(",", ":"))

    txt = js_fetch_in_page(driver, new_form, extra_headers={})
    obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
    if not obj: return None, None, None, None

    cursors = deep_collect_cursors(obj)
    new_has_next = deep_find_has_next(obj)
    if new_has_next is None: new_has_next = bool(cursors)
    new_cursor = cursors[0][1] if cursors else None
    return new_form, new_cursor, new_has_next, obj

# =============== Choose best GraphQL payload ===============
def choose_best_graphql_obj(objs):
    objs = list(objs)
    if not objs: return None
    with_data = [o for o in objs if isinstance(o, dict) and 'data' in o]
    pick = with_data or objs
    return max(pick, key=lambda o: len(json.dumps(o, ensure_ascii=False)))

# =============== Collect & coalesce posts ===============
def _get_text_from_node(n: dict):
    if isinstance(n.get("message"), dict):
        t = n["message"].get("text")
        if t: return t
    if isinstance(n.get("body"), dict):
        t = n["body"].get("text")
        if t: return t
    return None

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

def _extract_url_digits(url: str):
    if not url: return None
    m = POST_URL_RE.match(url)
    return m.group(1) if m else None

def collect_post_summaries(obj, out, group_url=GROUP_URL):
    if isinstance(obj, dict):
        if _looks_like_group_post(obj):
            post_id_api = obj.get("post_id")
            fb_id      = obj.get("id")
            url        = obj.get("wwwURL") or obj.get("url")
            url_digits = _extract_url_digits(url)
            rid        = post_id_api or url_digits or fb_id

            author_id, author_name, author_link, avatar, type_label = extract_author(obj)
            text = _get_text_from_node(obj)
            image_urls, video_urls = extract_media(obj)
            counts = extract_reactions_and_counts(obj)
            created = extract_created_time(obj)
            is_share, link_share, type_share, origin_id = extract_share_flags(obj)
            hashtags = extract_hashtags(text)

            # source_id
            source_id = None
            _k, _v = deep_get_first(obj, {"group_id", "groupID", "groupIDV2"})
            if _v: source_id = _v
            if not source_id:
                try:
                    slug = re.search(r"/groups/([^/?#]+)", group_url).group(1)
                    source_id = slug
                except: pass

            out.append({
                "id": fb_id,
                "rid": rid,
                "type": type_label,
                "link": url,
                "author_id": author_id,
                "author": author_name,
                "author_link": author_link,
                "avatar": avatar,
                "created_time": created,
                "content": text,
                "image_url": image_urls,
                "like": counts["like"], "comment": counts["comment"],
                "haha": counts["haha"], "wow": counts["wow"], "sad": counts["sad"],
                "love": counts["love"], "angry": counts["angry"], "care": counts["care"],
                "share": counts["share"],
                "hashtag": hashtags,
                "video": video_urls,
                "source_id": source_id,
                "is_share": is_share,
                "link_share": link_share,
                "type_share": type_share,
            })
        for v in obj.values():
            collect_post_summaries(v, out, group_url)
    elif isinstance(obj, list):
        for v in obj: collect_post_summaries(v, out, group_url)

# --- merge helpers ---
def _pick_text(a, b):
    if not b: return a
    if not a: return b
    return b if isinstance(b, str) and isinstance(a, str) and len(b) > len(a) else (b or a)

def _pick_non_empty(a, b):
    return b if b not in (None, "", [], {}) else a

def _merge_arrays(a, b):
    out, seen = [], set()
    for arr in (a or [], b or []):
        for x in arr:
            if x not in seen:
                out.append(x); seen.add(x)
    return out

def _merge_counts(a, b, keys):
    out = dict(a or {})
    for k in keys:
        out[k] = max((a or {}).get(k, 0), (b or {}).get(k, 0))
    return out

def _prefer_type(t1, t2):
    rank = {"facebook page": 3, "facebook profile": 3, "facebook group": 3, "story": 1, None: 0}
    return t2 if rank.get(t2,0) >= rank.get(t1,0) else t1

COUNT_KEYS = ["like","comment","haha","wow","sad","love","angry","care","share"]

def merge_two_posts(a: dict, b: dict) -> dict:
    if not a: return b or {}
    if not b: return a or {}
    m = dict(a)
    m["id"]  = m.get("id")  or b.get("id")
    m["rid"] = m.get("rid") or b.get("rid")
    m["type"] = _prefer_type(m.get("type"), b.get("type"))
    m["link"]        = _pick_non_empty(m.get("link"),        b.get("link"))
    m["author_id"]   = _pick_non_empty(m.get("author_id"),   b.get("author_id"))
    m["author"]      = _pick_non_empty(m.get("author"),      b.get("author"))
    m["author_link"] = _pick_non_empty(m.get("author_link"), b.get("author_link"))
    m["avatar"]      = _pick_non_empty(m.get("avatar"),      b.get("avatar"))

    ct_a, ct_b = m.get("created_time"), b.get("created_time")
    try:
        m["created_time"] = max(int(ct_a) if ct_a is not None else 0,
                                int(ct_b) if ct_b is not None else 0) or (ct_a or ct_b)
    except: m["created_time"] = ct_a or ct_b

    m["content"]  = _pick_text(m.get("content"), b.get("content"))
    m["image_url"]= _merge_arrays(m.get("image_url"), b.get("image_url"))
    m["video"]    = _merge_arrays(m.get("video"),     b.get("video"))
    m["hashtag"]  = _merge_arrays(m.get("hashtag"),   b.get("hashtag"))

    counts_a = {k: m.get(k, 0) for k in COUNT_KEYS}
    counts_b = {k: b.get(k, 0) for k in COUNT_KEYS}
    m.update(_merge_counts(counts_a, counts_b, COUNT_KEYS))

    m["source_id"] = _pick_non_empty(m.get("source_id"), b.get("source_id"))
    m["is_share"]   = bool(m.get("is_share")) or bool(b.get("is_share"))
    m["link_share"] = _pick_non_empty(m.get("link_share"), b.get("link_share"))
    m["type_share"] = _pick_non_empty(m.get("type_share"), b.get("type_share"))
    return m

# --- keying for dedupe/merge ---
from urllib.parse import urlparse, urlunparse

def _norm_link(u: str) -> Optional[str]:
    if not u or not isinstance(u, str): return None
    try:
        p = urlparse(u); host = p.netloc.lower()
        if host.endswith("facebook.com"): host = "facebook.com"
        path = (p.path or "").rstrip("/")
        return urlunparse(("https", host, path.lower(), "", "", ""))
    except Exception:
        return u

def _extract_digits_from_fb_link(u: str) -> Optional[str]:
    if not u: return None
    try: path = urlparse(u).path.lower()
    except: path = u.lower()
    m = re.search(r"/(?:reel|posts|permalink)/(\d+)", path)
    return m.group(1) if m else None

def _best_primary_key(it: dict) -> Optional[str]:
    rid = it.get("rid"); _id = it.get("id"); link = it.get("link")
    norm = _norm_link(link) if link else None
    digits = _extract_digits_from_fb_link(link) if link else None
    for k in (rid, _id, digits, norm):
        if isinstance(k, str) and k.strip():
            return k.strip()
    return None

def _all_join_keys(it: dict) -> List[str]:
    keys = []
    rid = it.get("rid"); _id = it.get("id"); link = it.get("link")
    if isinstance(rid, str) and rid.strip(): keys.append(rid.strip())
    if isinstance(_id,  str) and _id.strip(): keys.append(_id.strip())
    d = _extract_digits_from_fb_link(link) if link else None
    if d: keys.append(d)
    norm = _norm_link(link) if link else None
    if norm: keys.append(norm)
    seen, out = set(), []
    for k in keys:
        if k not in seen: out.append(k); seen.add(k)
    return out

def coalesce_posts(items: List[dict]) -> List[dict]:
    groups, key2group, seq = {}, {}, 0
    def _new_gid():
        nonlocal seq; seq += 1; return f"g{seq}"
    for it in items or []:
        keys = _all_join_keys(it)
        gid = None
        for k in keys:
            if k in key2group: gid = key2group[k]; break
        if gid is None:
            gid = _new_gid(); groups[gid] = it
        else:
            groups[gid] = merge_two_posts(groups[gid], it)
        for k in _all_join_keys(groups[gid]):
            key2group[k] = gid
    return list(groups.values())

def filter_only_group_posts(items):
    keep = []
    for it in items:
        url = (it.get("link") or "").strip()
        fb_id = (it.get("id") or "").strip()
        if POST_URL_RE.match(url) or (isinstance(fb_id, str) and fb_id.startswith("Uzpf")) or it.get("post_id"):
            keep.append(it)
    return keep

# =============== Optional UFI booster for reels ===============
def try_refetch_reel_ufi(driver, base_form: dict, video_id: str, timeout=8.0):
    if not video_id: return {}
    vars_min = {"feedbackTargetID": video_id, "scale": 1}
    form2 = dict(base_form); form2["variables"] = json.dumps(vars_min, separators=(",", ":"))
    _ = js_fetch_in_page(driver, form2, extra_headers={})
    start_idx = max(0, gql_count(driver) - 50)
    def _ufi_req(rec):
        if "/api/graphql/" not in (rec.get("url") or ""): return False
        body = rec.get("body") or ""
        if video_id not in body: return False
        txt = rec.get("responseText") or ""
        return ("top_reactions" in txt) or ("reaction_count" in txt) or ("total_comment_count" in txt)
    hit = wait_next_req(driver, start_idx, _ufi_req, timeout=timeout, poll=0.25)
    if not hit: return {}
    _, req = hit
    txt = req.get("responseText") or ""
    obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
    if not obj: return {}
    return extract_reactions_and_counts(obj) or {}

# =============== State (checkpoint & output) ===============
def load_checkpoint():
    if not os.path.exists(CHECKPOINT):
        return {"cursor": None, "seen_ids": [], "last_doc_id": None, "last_query_name": None, "vars_template": {}, "ts": None}
    try:
        with open(CHECKPOINT, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"cursor": None, "seen_ids": [], "last_doc_id": None, "last_query_name": None, "vars_template": {}, "ts": None}

def save_checkpoint(cursor, seen_ids, last_doc_id=None, last_query_name=None, vars_template=None):
    data = {
        "cursor": cursor,
        "seen_ids": list(seen_ids)[:200000],
        "last_doc_id": last_doc_id,
        "last_query_name": last_query_name,
        "vars_template": vars_template or {},
        "ts": datetime.datetime.now().isoformat(timespec="seconds")
    }
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def append_ndjson(items):
    if not items: return
    with open(OUT_NDJSON, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

def normalize_seen_ids(seen_ids):  # keep prior ids
    return set(seen_ids or [])

def reload_and_refresh_form(d, group_url, cursor, effective_template, timeout=25, poll=0.25):
    d.get(group_url); time.sleep(1.5)
    for _ in range(4):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));"); time.sleep(0.5)
    nxt = wait_next_req(d, 0, is_group_feed_req, timeout=timeout, poll=poll)
    if not nxt: return None, None, None
    _, req = nxt
    new_form = parse_form(req.get("body", ""))
    new_friendly = urllib.parse.parse_qs(req.get("body","")).get("fb_api_req_friendly_name", [""])[0]
    new_doc_id = new_form.get("doc_id")
    new_form = update_vars_for_next_cursor(new_form, cursor, vars_template=effective_template)
    return new_form, new_friendly, new_doc_id

# =============== Main ===============
if __name__ == "__main__":
    d = start_driver_with_proxy(PROXY_URL, headless=False)

    try:
        install_early_hook(d, keep_last=KEEP_LAST)
    except Exception as e:
        print("[WARN] install_early_hook:", e)

    bootstrap_auth(d)

    d.get(GROUP_URL); time.sleep(1.2)
    for _ in range(6):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
        time.sleep(0.6)

    # Boot request
    nxt = wait_next_req(d, 0, is_group_feed_req, timeout=25, poll=0.25)
    if not nxt:
        raise RuntimeError("Không bắt được request feed của group. Hãy cuộn thêm / kiểm tra quyền vào group.")

    _, first_req = nxt
    form         = parse_form(first_req.get("body", ""))
    friendly     = urllib.parse.parse_qs(first_req.get("body", "")).get("fb_api_req_friendly_name", [""])[0]
    vars_now     = get_vars_from_form(form)
    template_now = make_vars_template(vars_now)

    state         = load_checkpoint()
    seen_ids      = normalize_seen_ids(state.get("seen_ids", []))
    cursor        = state.get("cursor")
    vars_template = state.get("vars_template") or {}
    total_written = 0

    effective_template = vars_template or template_now

    # Page#1 (or resume)
    if cursor:
        print(f"[RESUME] Using saved cursor → jump directly. cursor={str(cursor)[:24]}..., friendly={friendly}")
        has_next, page = True, 0
    else:
        raw0 = first_req.get("responseText") or ""
        obj0 = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(raw0)))
        if not obj0:
            open(os.path.join(RAW_DUMPS_DIR, "page1_raw.txt"), "w", encoding="utf-8").write(raw0)
            raise RuntimeError("Không parse được trang đầu; đã dump raw_dumps/page1_raw.txt")

        page_posts = []
        collect_post_summaries(obj0, page_posts)
        page_posts = coalesce_posts(filter_only_group_posts(page_posts))

        cursors = deep_collect_cursors(obj0)
        has_next = deep_find_has_next(obj0)
        if has_next is None: has_next = bool(cursors)
        end_cursor = cursors[0][1] if cursors else None
        if end_cursor: cursor = end_cursor

        fresh = []
        for p in page_posts:
            pk = _best_primary_key(p)
            if pk and pk not in seen_ids:
                fresh.append(p)

        if fresh:
            append_ndjson(fresh)
            for p in fresh:
                for k in _all_join_keys(p): seen_ids.add(k)
            total_written += len(fresh)

        print(f"[PAGE#1] got {len(page_posts)} (new {len(fresh)}), next={bool(has_next)}")
        save_checkpoint(cursor, seen_ids, last_doc_id=form.get('doc_id'), last_query_name=friendly, vars_template=template_now)
        page = 1

    # Pagination loop
    no_progress_rounds = 0
    while True:
        page += 1
        if cursor:
            form = update_vars_for_next_cursor(form, cursor, vars_template=effective_template)

        txt = js_fetch_in_page(d, form, extra_headers={})
        obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))

        with open(os.path.join(RAW_DUMPS_DIR, f"page{page}_obj.json"), "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

        if not obj:
            open(os.path.join(RAW_DUMPS_DIR, f"page{page}_raw.txt"), "w", encoding="utf-8").write(txt)
            print(f"[PAGE#{page}] parse fail → dumped raw, break.")
            break

        page_posts = []
        collect_post_summaries(obj, page_posts)
        page_posts = coalesce_posts(filter_only_group_posts(page_posts))

        cursors = deep_collect_cursors(obj)
        has_next = deep_find_has_next(obj)
        if has_next is None: has_next = bool(cursors)
        new_cursor = cursors[0][1] if cursors else None
        if new_cursor: cursor = new_cursor

        # fresh by primary key
        written_this_round, fresh_dedup = set(), []
        for p in page_posts:
            pk = _best_primary_key(p)
            if pk and pk not in seen_ids and pk not in written_this_round:
                fresh_dedup.append(p); written_this_round.add(pk)

        if fresh_dedup:
            append_ndjson(fresh_dedup)
            for p in fresh_dedup:
                for k in _all_join_keys(p): seen_ids.add(k)
            total_written += len(fresh_dedup)
            no_progress_rounds = 0
        else:
            no_progress_rounds += 1

        print(f"[PAGE#{page}] got {len(page_posts)} (new {len(fresh_dedup)}), total={total_written}, next={bool(has_next)} | cursor={str(cursor)[:24] if cursor else None}")
        save_checkpoint(cursor, seen_ids, last_doc_id=form.get('doc_id'), last_query_name=friendly, vars_template=effective_template)

        # soft-refetch when stalled
        if not has_next and no_progress_rounds >= 3:
            print(f"[PAGE#{page}] next=False x{no_progress_rounds} → soft-refetch form/variables")
            save_checkpoint(cursor, seen_ids, last_doc_id=form.get('doc_id'), last_query_name=friendly, vars_template=effective_template)

            refetch_ok = False
            for attempt in range(1, 3):
                new_form, boot_cursor, boot_has_next, _ = soft_refetch_form_and_cursor(d, form, effective_template)
                if new_form and (boot_cursor or boot_has_next):
                    form = new_form
                    if boot_cursor: cursor = boot_cursor
                    has_next = bool(boot_has_next)
                    no_progress_rounds = 0
                    refetch_ok = True
                    print(f"[PAGE#{page}] soft-refetch OK (attempt {attempt}) → has_next={has_next} | cursor={str(cursor)[:24] if cursor else None}")
                    break
                time.sleep(random.uniform(1.0, 2.0))

            if not refetch_ok:
                print(f"[PAGE#{page}] soft-refetch failed → stop.")
                break

        time.sleep(random.uniform(0.7, 1.5))

    print(f"[DONE] wrote {total_written} posts → {OUT_NDJSON}")
    print(f"[INFO] resume later with checkpoint: {CHECKPOINT}")
