import json, time, urllib.parse, subprocess, re, socket
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service

from get_comment_fb_utils import set_sort_to_all_comments

# =========================
# Config
# =========================
CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"
PROFILE_NAME  = "Profile 5"
POST_URL      = "https://web.facebook.com/share/p/17W2LptXYM/"
OUT_FILE      = "comments_batch1.json"

# =========================
# Boot & Hooks
# =========================
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

def start_driver(chrome_path,
                 user_data_dir,
                 profile_name,
                 port=9222,
                 headless: bool = False,
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

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    d = start_driver(CHROME_PATH, USER_DATA_DIR, PROFILE_NAME)
    install_early_hook(d)

    d.get(POST_URL)
    time.sleep(2)
    set_sort_to_all_comments(d)
    hook_graphql(d)

    # ép FB tạo UFI nếu chưa có
    time.sleep(0.8)
    for _ in range(3):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.8));")
        time.sleep(0.4)

    # tìm request comment đầu để suy biến form (nếu cần)
    all_reqs = d.execute_script("return window.__gqlReqs || []")
    comm_reqs = [r for r in all_reqs if match_comment_req(r)]
    if not comm_reqs:
        try:
            d.find_element(By.XPATH, "//div[contains(@aria-label,'Viết bình luận') or contains(@aria-label,'Write a comment')]")
            time.sleep(0.6)
        except: pass
        for _ in range(5):
            d.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.6)
        all_reqs = d.execute_script("return window.__gqlReqs || []")
        comm_reqs = [r for r in all_reqs if match_comment_req(r)]

    if not comm_reqs:
        print("[WARN] Chưa bắt trúng API comment. Mở đúng permalink & nhấn 'Xem thêm bình luận/ phản hồi'.")
        json.dump(all_reqs, open("gql_all_dump.json","w",encoding="utf-8"), ensure_ascii=False, indent=2)
        raise SystemExit(0)

    # Batch đầu tiên: đọc trực tiếp từ responseText (đã hook) thay vì replay
    first_resptext = comm_reqs[0].get("responseText") or ""
    texts, _, total_target, obj0 = extract_comments_from_resptext(first_resptext)
    # Lưu batch đầu (để nguyên object đầu cho debug), có thể đổi sang chỉ lưu texts nếu muốn
    json.dump(obj0, open(OUT_FILE,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"[BATCH#1] comments extracted: {len(texts)}")

    if not total_target:
        total_target = max(10, len(texts))
    print(f"[INFO] Target ≈ {total_target} comments")

    baseline_idx = gql_count(d)
    rounds, guard = 0, 200

    while len(texts) < total_target and rounds < guard:
        rounds += 1

        # 1) click “Xem thêm …” nếu có, else kéo đến comment cuối
        if click_view_more_if_any(d, max_clicks=1) == 0:
            if not scroll_to_last_comment(d):
                d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.8));")
        time.sleep(2)

        # 2) đợi đúng 1 request comment mới
        nxt = wait_next_comment_req(d, baseline_idx, timeout=8, poll=0.2)
        if not nxt:
            print(f"[{rounds}] Không thấy request mới sau khi cuộn/click, thử lại…")
            continue

        idx, req = nxt
        baseline_idx = idx + 1

        resp_text = req.get("responseText") or ""
        batch_texts, _, maybe_total, _ = extract_comments_from_resptext(resp_text)
        if batch_texts:
            texts.extend(batch_texts)
            print(f"[{rounds}] +{len(batch_texts)} → total={len(texts)}/{total_target}")
        else:
            print(f"[{rounds}] Parse OK nhưng không thấy text mới.")

        if maybe_total:
            total_target = maybe_total or total_target

        # 3) lưu tiến độ (nhẹ nhàng, chỉ texts + target)
        try:
            json.dump({"texts": texts, "target": total_target}, open(OUT_FILE,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
        except Exception as e:
            print("[WARN] Không thể ghi file:", e)

    print(f"[DONE] Collected {len(texts)} comments (target ~{total_target}). Saved to {OUT_FILE}")
