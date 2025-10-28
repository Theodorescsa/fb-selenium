# -*- coding: utf-8 -*-
import json, time, urllib.parse, subprocess, re, socket, os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from get_comment_fb_utils import set_sort_to_all_comments

# =========================
# Config
# =========================
CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"
PROFILE_NAME  = "Profile 5"
POST_URL      = "https://web.facebook.com/share/p/17W2LptXYM/"
OUT_FILE      = "comments_batch1.json"
CHECKPOINT    = "checkpoint_comments.json"

CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}

# =========================
# Boot
# =========================
def _wait_port(host: str, port: int, timeout: float = 15.0, poll: float = 0.1) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except Exception:
            time.sleep(poll)
    return False

def start_driver(chrome_path,
                 user_data_dir,
                 profile_name,
                 port=9222,
                 headless: bool = False,
                 timeout: float = 15.0):
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
        '--disable-infobars'
    ]
    if headless:
        args += ['--headless=new','--disable-gpu','--no-sandbox','--disable-dev-shm-usage','--window-size=1920,1080']

    proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ok = _wait_port('127.0.0.1', port, timeout=timeout)
    if not ok and headless:
        proc.kill()
        time.sleep(0.5)
        args = [
            chrome_path,
            f'--remote-debugging-port={port}',
            f'--user-data-dir={user_data_dir}',
            f'--profile-directory={profile_name}',
            '--no-first-run','--no-default-browser-check',
            '--disable-extensions','--disable-background-networking',
            '--disable-popup-blocking','--disable-default-apps','--disable-infobars',
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

    options = Options()
    options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")
    return webdriver.Chrome(options=options)

# =========================
# Hooks
# =========================
def install_early_hook(driver):
    # Robust: capture both request meta and responseText for fetch/XHR
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
      const push = (rec)=>{ try{ (window.__gqlReqs||[]).push(rec); }catch(e){} };
      const origFetch = window.fetch;
      window.fetch = async function(input, init){
        const url = (typeof input==='string') ? input : (input && input.url) || '';
        const method = (init && init.method) || 'GET';
        let body = (init && init.body) || '';
        if (body instanceof URLSearchParams) body = body.toString();
        const hdrs = headersToObj(init && init.headers);
        let rec = null;
        if (String(url).includes('/api/graphql/') && method === 'POST'){
          rec = {kind:'fetch', url:String(url), method:String(method), headers:hdrs, body:String(body||'')};
        }
        const res = await origFetch(input, init);
        if (rec){
          try{ rec.responseText = await res.clone().text(); }catch(e){ rec.responseText = null; }
          push(rec);
        }
        return res;
      };
      const XO = XMLHttpRequest.prototype.open, XS = XMLHttpRequest.prototype.send;
      XMLHttpRequest.prototype.open = function(m,u){
        this.__m=m; this.__u=u; return XO.apply(this, arguments);
      };
      XMLHttpRequest.prototype.send = function(b){
        this.__b = (typeof b==='string') ? b : '';
        this.addEventListener('load', ()=>{
          try{
            if ((this.__u||'').includes('/api/graphql/') && (this.__m||'')==='POST'){
              (window.__gqlReqs||[]).push({
                kind:'xhr', url:String(this.__u), method:String(this.__m), headers:{},
                body:String(this.__b||''), responseText:(typeof this.responseText==='string'?this.responseText:null)
              });
            }
          }catch(e){}
        });
        return XS.apply(this, arguments);
      };
      // ensure array exists
      if (!Array.isArray(window.__gqlReqs)) window.__gqlReqs = [];
    })();
    """
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": HOOK_SRC})

def ensure_hook_ready(driver, tries=40, sleep=0.1):
    for _ in range(tries):
        try:
            ok = driver.execute_script("return Array.isArray(window.__gqlReqs)")
            if ok: return True
        except: pass
        time.sleep(sleep)
    return False

# =========================
# Match & parse helpers
# =========================
def parse_form(body_str):
    qs = urllib.parse.parse_qs(body_str, keep_blank_values=True)
    return {k:(v[0] if isinstance(v, list) else v) for k,v in qs.items()}

def match_comment_req(rec):
    if "/api/graphql/" not in rec.get("url",""): return False
    if (rec.get("method") or rec.get("kind") in {"xhr","fetch"}) != "POST": pass  # tolerate missing
    body = rec.get("body","") or ""
    if "fb_api_req_friendly_name=" in body and "Comments" in body:
        return True
    if "variables=" in body:
        try:
            v = parse_form(body).get("variables","")
            vj = json.loads(urllib.parse.unquote_plus(v))
            keys = set(vj.keys())
            signs = {"commentable_object_id","commentsAfterCursor","feedLocation","focusCommentID","feedbackSource"}
            if keys & signs: return True
        except: pass
    return False

def gql_count(driver):
    return driver.execute_script("return (window.__gqlReqs||[]).length")

def get_gql_at(driver, i):
    return driver.execute_script("return (window.__gqlReqs||[])[arguments[0]]", i)

def wait_next_comment_req(driver, start_idx, timeout=10, poll=0.2):
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

def detect_cursor_key(variables: dict) -> str | None:
    for k in variables.keys():
        if k in CURSOR_KEYS: return k
    # fallback: look inside nested dicts
    for k,v in variables.items():
        if isinstance(v, dict):
            ck = detect_cursor_key(v)
            if ck: return f"{k}.{ck}"
    return None

def strip_cursors_from_vars(v: dict) -> dict:
    if not isinstance(v, dict): return {}
    out = {}
    for k, val in v.items():
        if k in CURSOR_KEYS:
            continue
        if isinstance(val, dict):
            out[k] = strip_cursors_from_vars(val)
        else:
            out[k] = val
    return out

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
# UI nudges (click/scroll)
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

# =========================
# Replay inside page
# =========================
def make_form_body_from_template(vars_template: dict, doc_id: str|None, friendly: str|None, cursor_key: str|None, cursor_val: str|None) -> str:
    # update variables with cursor if we have both key and value
    v = json.loads(json.dumps(vars_template))
    if cursor_key and cursor_val:
        # support dot-path cursor key (e.g. "feedback.comments.after")
        target = v
        parts = cursor_key.split(".")
        for p in parts[:-1]:
            target = target.setdefault(p, {})
        target[parts[-1]] = cursor_val
    payload = {
        "av": "",  # optional
        "doc_id": doc_id or "",
        "variables": urllib.parse.quote_plus(json.dumps(v, separators=(',',':'), ensure_ascii=False)),
        "fb_api_req_friendly_name": friendly or "",
        "__a": "1"
    }
    # important: we need classic form encoding (not JSON)
    return "&".join([f"{k}={str(payload[k])}" for k in payload if payload[k]!=""])

def replay_once(driver, body_str: str) -> str|None:
    js = r"""
    const body = arguments[0];
    return fetch("/api/graphql/", {
      method: "POST",
      headers: {"content-type":"application/x-www-form-urlencoded"},
      body
    }).then(r=>r.text()).catch(_=>null);
    """
    try:
        return driver.execute_script(js, body_str)
    except:
        return None

# =========================
# Checkpoint I/O
# =========================
def load_checkpoint(path: str):
    if not os.path.exists(path): return None
    try:
        return json.load(open(path,"r",encoding="utf-8"))
    except:
        return None

def save_checkpoint(path: str, data: dict):
    try:
        json.dump(data, open(path,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
    except Exception as e:
        print("[WARN] cannot save checkpoint:", e)

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    d = start_driver(CHROME_PATH, USER_DATA_DIR, PROFILE_NAME)
    install_early_hook(d)

    d.get(POST_URL)
    if not ensure_hook_ready(d):  # make sure __gqlReqs exists
        raise SystemExit("[FATAL] hook not ready.")

    time.sleep(1.2)
    set_sort_to_all_comments(d)

    # If we have checkpoint, try to resume immediately by replaying
    ck = load_checkpoint(CHECKPOINT)

    texts = []
    total_target = None
    vars_template = None
    friendly = None
    doc_id = None
    cursor_key = None
    cursor_val = None

    if ck and all(k in ck for k in ("vars_template","friendly","doc_id","cursor_key","cursor")):
        vars_template = ck["vars_template"]
        friendly = ck["friendly"]
        doc_id = ck["doc_id"]
        cursor_key = ck["cursor_key"]
        cursor_val = ck["cursor"]

        print("[RESUME] Using checkpoint to continue via replay…")
        # do a small prime scroll to ensure page context is warm
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.4));")
        time.sleep(0.6)
    else:
        print("[INIT] No checkpoint, prime the page to capture ONE new UFI request…")
        # do exactly 4 cycles like you requested
        baseline_idx = gql_count(d)
        for _ in range(4):
            if click_view_more_if_any(d, max_clicks=1) == 0:
                if not scroll_to_last_comment(d):
                    d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.8));")
            time.sleep(2)

        nxt = wait_next_comment_req(d, baseline_idx, timeout=10, poll=0.2)
        if not nxt:
            # Dump everything for debug
            all_reqs = d.execute_script("return window.__gqlReqs || []")
            json.dump(all_reqs, open("gql_all_dump.json","w",encoding="utf-8"), ensure_ascii=False, indent=2)
            raise SystemExit("[WARN] Không bắt được request comment sau 4 vòng nudge. Đã dump gql_all_dump.json")

        idx, req = nxt
        body = (req.get("body") or "")
        resp = (req.get("responseText") or "")

        # Extract variables / friendly / doc_id
        form = parse_form(body)
        friendly = form.get("fb_api_req_friendly_name")
        doc_id = form.get("doc_id")
        try:
            raw_vars = form.get("variables","")
            raw_vars = urllib.parse.unquote_plus(raw_vars) if raw_vars else "{}"
            var_obj = json.loads(raw_vars)
        except:
            var_obj = {}

        cursor_key = detect_cursor_key(var_obj)
        vars_template = strip_cursors_from_vars(var_obj)

        # Use first response to seed comments / end_cursor
        batch_texts, end_cursor, total_target, obj0 = extract_comments_from_resptext(resp)
        if obj0:
            json.dump(obj0, open(OUT_FILE,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
        if batch_texts:
            texts.extend(batch_texts)
        if not total_target:
            total_target = max(10, len(texts))

        # Save initial checkpoint
        save_checkpoint(CHECKPOINT, {
            "vars_template": vars_template,
            "friendly": friendly,
            "doc_id": doc_id,
            "cursor_key": cursor_key,
            "cursor": end_cursor or "",
            "ts": time.time()
        })
        cursor_val = end_cursor or ""

        print(f"[BATCH#1] got {len(batch_texts)} comments; end_cursor? {bool(end_cursor)}; target≈{total_target}")

    # =========================
    # Replay paginate loop
    # =========================
    if total_target is None:
        total_target = 999999  # if resume without prior total, just keep going

    rounds, guard = 0, 1000
    while rounds < guard:
        rounds += 1
        if not cursor_key:
            print("[STOP] No cursor_key was detected in variables; cannot paginate.")
            break

        body = make_form_body_from_template(vars_template, doc_id, friendly, cursor_key, cursor_val)
        resp_text = replay_once(d, body)
        if not resp_text:
            print(f"[{rounds}] Replay failed (no text). Retrying light scroll to refresh ctx…")
            d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.2));")
            time.sleep(0.5)
            continue

        batch_texts, end_cursor, maybe_total, obj = extract_comments_from_resptext(resp_text)
        if obj and rounds == 1 and not os.path.exists(OUT_FILE):
            json.dump(obj, open(OUT_FILE,"w",encoding="utf-8"), ensure_ascii=False, indent=2)

        added = len(batch_texts or [])
        if added:
            texts.extend(batch_texts)
        if maybe_total:
            total_target = maybe_total

        print(f"[{rounds}] +{added} → total={len(texts)} / target≈{total_target} ; cursor? {bool(end_cursor)}")

        # save progress & checkpoint
        try:
            json.dump({"texts": texts, "target": total_target}, open(OUT_FILE,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
        except Exception as e:
            print("[WARN] cannot write OUT_FILE:", e)

        save_checkpoint(CHECKPOINT, {
            "vars_template": vars_template,
            "friendly": friendly,
            "doc_id": doc_id,
            "cursor_key": cursor_key,
            "cursor": end_cursor or "",
            "ts": time.time()
        })

        if not end_cursor:
            print("[DONE] No more pages.")
            break

        cursor_val = end_cursor  # advance

    print(f"[FIN] Collected {len(texts)} comments (target≈{total_target}). Saved {OUT_FILE} & checkpoint {CHECKPOINT}")
