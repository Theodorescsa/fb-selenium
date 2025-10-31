import json, time, urllib.parse, subprocess, re, socket
from typing import List, Dict, Any, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def _wait_port(host: str, port: int, timeout: float = 15.0, poll: float = 0.1) -> bool:
    """Return True if (host,port) becomes connectable within timeout."""
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

def start_driver(chrome_path,
                 user_data_dir,
                 profile_name,
                 port=9222,
                 headless: bool = True,
                 timeout: float = 15.0):
    """
    Start a real Chrome process and attach Selenium via remote debugging.

    Args:
        chrome_path: path to chrome/chromium executable.
        user_data_dir: profile dir (keeps cookies/session).
        profile_name: profile directory name (e.g. 'Default' or 'Profile 1').
        port: remote debugging port.
        headless: if True, start Chrome in headless (background) mode.
        timeout: seconds to wait for remote port to become available.

    Returns:
        webdriver.Chrome instance (connected to the launched Chrome).
    """
    # build CLI args for Chrome instance
    # keep remote-debugging-port + user profile. Add headless flags optionally.
    args = [
        chrome_path,
        f'--remote-debugging-port={port}',
        f'--user-data-dir={user_data_dir}',
        f'--profile-directory={profile_name}',
        # useful flags to make an isolated, stable environment:
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-extensions',
        '--disable-background-networking',
        '--disable-popup-blocking',
        '--disable-default-apps',
        '--disable-infobars'
    ]

    if headless:
        # prefer new headless mode; adjust window size
        args += [
            '--headless=new',
            '--disable-gpu',
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--window-size=1920,1080'
        ]

    # Launch Chrome (separate process) that Selenium will attach to.
    proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Wait for remote debugging port to be ready
    ok = _wait_port('127.0.0.1', port, timeout=timeout)
    if not ok and headless:
        # fallback: try again without headless (some sites require non-headless)
        proc.kill()
        time.sleep(0.5)
        # try non-headless
        args = [
            chrome_path,
            f'--remote-debugging-port={port}',
            f'--user-data-dir={user_data_dir}',
            f'--profile-directory={profile_name}',
            '--no-first-run',
            '--no-default-browser-check',
            '--disable-extensions',
            '--disable-background-networking',
            '--disable-popup-blocking',
            '--disable-default-apps',
            '--disable-infobars',
            '--window-size=1920,1080'
        ]
        proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ok = _wait_port('127.0.0.1', port, timeout=timeout)
        if not ok:
            proc.kill()
            raise RuntimeError(f"Chrome remote debugging port {port} not available after fallback start.")

    if not ok:
        proc.kill()
        raise RuntimeError(f"Chrome remote debugging port {port} not available.")

    # Attach Selenium to the running Chrome via debuggerAddress
    options = Options()
    options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")

    # Important: do NOT also set options.headless here — we're attaching to the launched Chrome.
    driver = webdriver.Chrome(options=options)
    return driver


def install_early_hook(driver):
    HOOK_SRC = r"""
    (function(){
      if (window.__gqlHooked) return;
      window.__gqlHooked = true;
      window.__gqlReqs = [];
      function headersToObj(h){
        try{
          if (!h) return {};
          if (h instanceof Headers){ const o={}; h.forEach((v,k)=>o[k]=v); return o; }
          if (Array.isArray(h)){ const o={}; for(const [k,v] of h) o[k]=v; return o; }
          return (typeof h==='object') ? h : {};
        }catch(e){ return {}; }
      }
      const pushRec = (rec)=>{ try{ (window.__gqlReqs||[]).push(rec); }catch(e){} };
      const origFetch = window.fetch;
      window.fetch = async function(input, init){
        const url = (typeof input==='string') ? input : (input && input.url) || '';
        const method = (init && init.method) || 'GET';
        const body = (init && typeof init.body==='string') ? init.body : '';
        const hdrs = headersToObj(init && init.headers);
        let rec = null;
        if (url.includes('/api/graphql/') && method === 'POST'){
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
        this.__b = (typeof b==='string') ? b : '';
        this.addEventListener('load', ()=>{
          try{
            if ((this.__u||'').includes('/api/graphql/') && (this.__m||'')==='POST'){
              (window.__gqlReqs||[]).push({
                kind:'xhr', url:this.__u, method:this.__m, headers:{},
                body:String(this.__b),
                responseText:(typeof this.responseText==='string'?this.responseText:null)
              });
            }
          }catch(e){}
        });
        return XS.apply(this, arguments);
      };
    })();
    """
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": HOOK_SRC})

def hook_graphql(driver):
    js = r"""
    (function() {
      if (window.__gqlHooked) return;
      window.__gqlHooked = true;
      window.__gqlReqs = window.__gqlReqs || [];

      // wrap fetch
      const _fetch = window.fetch;
      window.fetch = function(input, init) {
        try {
          const url = (typeof input === 'string') ? input : (input && input.url) || '';
          const method = (init && init.method) || 'GET';
          let body = (init && init.body) || '';
          if (body instanceof URLSearchParams) body = body.toString();
          if (String(url).includes('/api/graphql/')) {
            window.__gqlReqs.push({ts:Date.now(), type:'fetch', url:String(url), method:String(method), body:String(body||'')});
          }
        } catch(e) {}
        return _fetch.apply(this, arguments);
      };

      // wrap XHR
      const _open = XMLHttpRequest.prototype.open;
      const _send = XMLHttpRequest.prototype.send;
      XMLHttpRequest.prototype.open = function(method, url) {
        this.__gql_meta = { url: String(url||''), method: String(method||'GET') };
        return _open.apply(this, arguments);
      };
      XMLHttpRequest.prototype.send = function(body) {
        try {
          const meta = this.__gql_meta || {};
          if (String(meta.url).includes('/api/graphql/')) {
            window.__gqlReqs.push({ts:Date.now(), type:'xhr', url:String(meta.url), method:String(meta.method||'GET'), body:String(body||'')});
          }
        } catch(e) {}
        return _send.apply(this, arguments);
      };
    })();
    """
    driver.execute_script(js)

# =========================
# Utils (GraphQL buffer)
# =========================
def gql_count(driver):
    return driver.execute_script("return (window.__gqlReqs||[]).length")

def get_gql_at(driver, i):
    return driver.execute_script("return (window.__gqlReqs||[])[arguments[0]]", i)

def wait_next_comment_req(driver, start_idx, timeout=10, poll=0.2):
    """Đợi đúng 1 request comment mới sau mốc start_idx."""
    end = time.time() + timeout
    cur = start_idx
    while time.time() < end:
        n = gql_count(driver)
        while cur < n:
            req = get_gql_at(driver, cur)
            if req and match_comment_req(req):
                return (cur, req)
            cur += 1
        time.sleep(poll)
    return None

# =========================
# FB GraphQL comment match & parsing
# =========================
def parse_form(body_str):
    qs = urllib.parse.parse_qs(body_str, keep_blank_values=True)
    return {k:(v[0] if isinstance(v, list) else v) for k,v in qs.items()}

friendly_name_whitelist = [
    "CommentsListComponentsPaginationQuery",
    "UFI2CommentsProviderPaginationQuery",
    "CometUFI.*Comments.*Pagination",
]

def match_comment_req(rec):
    if "/api/graphql/" not in rec.get("url",""): return False
    if rec.get("method") != "POST": return False
    body = rec.get("body","") or ""
    if "fb_api_req_friendly_name=" in body:
        if "fb_api_req_friendly_name=CommentsListComponentsPaginationQuery" in body: return True
        if "fb_api_req_friendly_name=UFI2CommentsProviderPaginationQuery" in body: return True
        if re.search(r"fb_api_req_friendly_name=CometUFI[^&]*Comments[^&]*Pagination", body): return True
    if "variables=" in body:
        try:
            v = parse_form(body).get("variables","")
            vj = json.loads(urllib.parse.unquote_plus(v))
            keys = set(vj.keys())
            signs = {"commentable_object_id","commentsAfterCursor","feedLocation","focusCommentID","feedbackSource"}
            if keys & signs: return True
        except:
            pass
    return False

def find_pageinfo(obj):
    if isinstance(obj, dict):
        if "page_info" in obj and isinstance(obj["page_info"], dict):
            pi = obj["page_info"]
            return pi.get("end_cursor"), pi.get("has_next_page")
        for v in obj.values():
            c = find_pageinfo(v)
            if c: return c
    elif isinstance(obj, list):
        for v in obj:
            c = find_pageinfo(v)
            if c: return c
    return (None, None)

def extract_comment_texts(obj, out):
    if isinstance(obj, dict):
        if "body" in obj and isinstance(obj["body"], dict) and "text" in obj["body"]:
            out.append(obj["body"]["text"])
        if "message" in obj and isinstance(obj["message"], dict) and "text" in obj["message"]:
            out.append(obj["message"]["text"])
        for v in obj.values():
            extract_comment_texts(v, out)
    elif isinstance(obj, list):
        for v in obj:
            extract_comment_texts(v, out)

def extract_comments_from_resptext(resp_text):
    texts = []
    try:
        obj = json.loads(resp_text)
    except:
        return texts, None, None, None
    extract_comment_texts(obj, texts)
    end_cursor, has_next = find_pageinfo(obj)
    total = None
    try:
        c = obj["data"]["node"]["comment_rendering_instance_for_feed_location"]["comments"]
        total = c.get("count") or c.get("total_count")
    except:
        pass
    return texts, end_cursor, total, obj
# =========================
# UI interactions (scroll/click)
# =========================
def click_view_more_if_any(driver, max_clicks=1):
    xps = [
        "//div[@role='button'][contains(.,'Xem thêm bình luận') or contains(.,'Xem thêm phản hồi')]",
        "//span[contains(.,'Xem thêm bình luận') or contains(.,'Xem thêm phản hồi')]/ancestor::div[@role='button']",
        "//div[@role='button'][contains(.,'View more comments') or contains(.,'View more replies')]",
        "//span[contains(.,'View more comments') or contains(.,'View more replies')]/ancestor::div[@role='button']",
    ]
    clicks = 0
    for xp in xps:
        for b in driver.find_elements(By.XPATH, xp):
            if clicks >= max_clicks: return clicks
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
                time.sleep(0.15)
                b.click()
                clicks += 1
                time.sleep(0.35)
            except: pass
    return clicks

def scroll_to_last_comment(driver):
    js = r"""
    (function(){
      const cands = Array.from(document.querySelectorAll("div[role='article'][aria-label]"));
      let nodes = cands.filter(n => /Bình luận/i.test(n.getAttribute('aria-label')||""));
      if (nodes.length === 0) nodes = cands.filter(n => /(Comment|Comments)/i.test(n.getAttribute('aria-label')||""));
      if (nodes.length === 0) return false;
      nodes[nodes.length - 1].scrollIntoView({behavior: 'instant', block: 'center'});
      window.scrollBy(0, Math.floor(window.innerHeight*0.1));
      return true;
    })();
    """
    return bool(driver.execute_script(js))

def wait_first_comment_request(driver, start_idx, timeout=10, poll=0.2):
    end = time.time() + timeout
    i = start_idx
    print("Waiting for first comment request after index", start_idx)
    
    # while time.time() < end:
    print("time.time() < end",time.time() < end)
    n = driver.execute_script("return (window.__gqlReqs||[]).length")
    print("n",n)
    while i <= n:
        rec = driver.execute_script("return (window.__gqlReqs||[])[arguments[0]]", i)
        i += 1
        if rec and match_comment_req(rec):
            return rec
    time.sleep(poll)
    # return None

# =========================
# Replay GraphQL inside the page (keeps auth/cookies)
# =========================
def graphql_post_in_page(driver, url: str, form_params: dict, override_vars: dict):
    fp = dict(form_params)
    fp["variables"] = json.dumps(override_vars, separators=(',',':'), ensure_ascii=False)
    body = urllib.parse.urlencode(fp)
    js = r"""
    const url = arguments[0], body = arguments[1], cb = arguments[2];
    fetch(url, {
      method:'POST', credentials:'include',
      headers:{'content-type':'application/x-www-form-urlencoded'},
      body
    }).then(r=>r.text()).then(t=>cb({ok:true,text:t}))
      .catch(e=>cb({ok:false,err:String(e)}));
    """
    driver.set_script_timeout(120)
    ret = driver.execute_async_script(js, url, body)
    if not ret or not ret.get("ok"):
        raise RuntimeError("Replay GraphQL failed: %s" % (ret and ret.get('err')))
    return ret["text"]

def pick_reply_template_from_page(driver):
    """
    Lấy cái request GraphQL dùng để load REPLIES (Depth1).
    Ưu tiên mấy friendly name kiểu Depth1CommentsListPaginationQuery.
    """
    reqs = driver.execute_script("return window.__gqlReqs || []") or []
    # duyệt từ cuối lên đầu để lấy request mới nhất
    for r in reversed(reqs):
        body = r.get("body") or ""
        form = parse_form(body)
        friendly = form.get("fb_api_req_friendly_name", "") or ""
        vars_str = urllib.parse.unquote_plus(form.get("variables","") or "")
        try:
            vars_obj = json.loads(vars_str) if vars_str else {}
        except Exception:
            vars_obj = {}

        # vài pattern tên thường gặp
        if (
            "Depth1CommentsListPaginationQuery" in friendly
            or "CommentRepliesList" in friendly
            or "CommentReplies" in friendly
            or ("repliesAfterCount" in vars_obj)
        ):
            # đây mới là template reply thật
            return r.get("url"), form, vars_obj

    # không tìm được → trả None, caller sẽ fallback
    return None, None, None