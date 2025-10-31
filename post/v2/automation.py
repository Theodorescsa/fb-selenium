import time, json, urllib, os
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode



from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException as _SETimeout

from configs import *
from utils import _normalize_cookie, _strip_xssi_prefix, choose_best_graphql_obj, deep_collect_cursors, deep_find_has_next, is_group_feed_req, iter_json_values, merge_vars, parse_form, strip_cursors_from_vars, update_vars_for_next_cursor
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
            # "verify_ssl": False,
        }

    driver = webdriver.Chrome(options=chrome_opts, seleniumwire_options=sw_options)
    driver.scopes = [r".*"]
    return driver


# =========================
# bootstrap_auth — nạp cookies/localStorage nếu có
# =========================
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
    try:
        all_cookies = {c["name"]: c.get("value") for c in d.get_cookies()}
        has_cuser = "c_user" in all_cookies
        has_xs    = "xs" in all_cookies
        print(f"[AUTH] c_user={has_cuser}, xs={has_xs}")
    except Exception:
        pass

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
# Fetch /api/graphql/
# =========================
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