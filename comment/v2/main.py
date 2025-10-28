# -*- coding: utf-8 -*-
"""
V2 – FB comments crawler via GraphQL replay (no continuous scrolling)
- Bước 1: set sort "All comments" như cũ.
- Bước 2: cuộn 1 lần để FB bắn 1 request comment đầu.
- Bước 3: lấy request đó, tách form, lưu vars_template (bỏ cursor).
- Bước 4: REPLAY GraphQL trong chính page context bằng fetch(), paginate bằng end_cursor.
- Bước 5: lưu checkpoint (cursor + vars_template + cursor_key + doc_id + friendly).
"""

import json, time, re, urllib.parse, os
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
# Reuse helpers (cursor keys + parse)
# =========================
CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","commentsAfterCursor","feedAfterCursor","cursor"}

def parse_form(body_str: str):
    qs = urllib.parse.parse_qs(body_str, keep_blank_values=True)
    return {k:(v[0] if isinstance(v, list) else v) for k,v in qs.items()}



# =========================
# Checkpoint
# =========================
def load_checkpoint(path="checkpoint_comments.json"):
    if os.path.exists(path):
        try:
            return json.load(open(path,"r",encoding="utf-8"))
        except: return {}
    return {}

def save_checkpoint(data: dict, path="checkpoint_comments.json"):
    tmp = path + ".tmp"
    with open(tmp,"w",encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

# =========================
# GraphQL capture + bootstrap
# =========================
def match_comment_req(rec: dict):
    url = rec.get("url","")
    if "/api/graphql/" not in url: return False
    if rec.get("method") != "POST": return False
    body = rec.get("body","") or ""
    if "fb_api_req_friendly_name=" in body:
        if "CommentsListComponentsPaginationQuery" in body: return True
        elif "UFI2CommentsProviderPaginationQuery" in body: return True
        elif re.search(r"fb_api_req_friendly_name=CometUFI[^&]*Comments[^&]*Pagination", body): return True
    if "variables=" in body:
        try:
            v = parse_form(body).get("variables","")
            vj = json.loads(urllib.parse.unquote_plus(v))
            keys = set(vj.keys())
            signs = {"commentable_object_id","commentsAfterCursor","feedLocation","focusCommentID","feedbackSource","after","afterCursor"}
            if keys & signs: return True
        except: pass
    return False
def wait_first_comment_request(driver, start_idx, timeout=10, poll=0.2):
    end = time.time() + timeout
    i = start_idx
    print("Waiting for first comment request after index", start_idx)
    
    while time.time() < end:
        print("time.time() < end",time.time() < end)
        n = driver.execute_script("return (window.__gqlReqs||[]).length")
        print("n",n)
        while i <= n:
            rec = driver.execute_script("return (window.__gqlReqs||[])[arguments[0]]", i)
            i += 1
            if rec and match_comment_req(rec):
                return rec
        time.sleep(poll)
    return None



def strip_cursors_from_vars(v: dict) -> dict:
    if not isinstance(v, dict): return {}
    return {k: v for k, v in v.items() if k not in CURSOR_KEYS}

def detect_cursor_key(original_vars: dict) -> str:
    # Ưu tiên key cursor đang dùng trong variables ban đầu
    for k in original_vars.keys():
        if k in CURSOR_KEYS:
            return k
    # fallback hay gặp trong UFI
    return "commentsAfterCursor"

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

# =========================
# V2 main routine for comments
# =========================
def crawl_comments_v2(driver, out_json="comments_v2.json", checkpoint_path="checkpoint_comments.json", max_pages=None):
    """
    Steps:
    - Assume page is at permalink; sort set to All comments.
    - Trigger exactly ONE scroll to cause first comment request.
    - Capture the first comment request form -> vars_template (no cursor).
    - If checkpoint has cursor+template, reuse; else init from this form.
    - Replay GraphQL in a loop (no more scrolling) until has_next_page=False (or max_pages reached).
    - Save texts + checkpoint each page.
    """
    # 1) ensure one lightweight scroll to produce first request
    baseline = driver.execute_script("return (window.__gqlReqs||[]).length")
    driver.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.8));")

    time.sleep(1.0)

    first_req = wait_first_comment_request(driver, 0, timeout=12, poll=0.2)

    url = first_req.get("url")
    form = parse_form(first_req.get("body",""))
    # variables gốc
    orig_vars_str = urllib.parse.unquote_plus(form.get("variables","") or "")
    try:
        orig_vars = json.loads(orig_vars_str) if orig_vars_str else {}
    except Exception:
        orig_vars = {}

    cursor_key = detect_cursor_key(orig_vars)
    vars_template = strip_cursors_from_vars(orig_vars)

    # doc_id / friendly (giữ nguyên để replay đúng tài liệu)
    doc_id = form.get("doc_id")
    friendly = form.get("fb_api_req_friendly_name")

    # 2) load checkpoint (nếu có)
    ck = load_checkpoint(checkpoint_path)
    if ck and ck.get("doc_id") == doc_id and ck.get("friendly") == friendly:
        # resume
        last_cursor = ck.get("cursor")
        saved_template = ck.get("vars_template") or {}
        saved_cursor_key = ck.get("cursor_key") or cursor_key
        if saved_template:
            vars_template = saved_template
        if saved_cursor_key:
            cursor_key = saved_cursor_key
    else:
        # init checkpoint fresh
        ck = {
            "cursor": None,
            "vars_template": vars_template,
            "cursor_key": cursor_key,
            "doc_id": doc_id,
            "friendly": friendly,
            "ts": time.time()
        }
        save_checkpoint(ck, checkpoint_path)

    # 3) paginate via replay
    all_texts = []
    pages = 0
    current_cursor = ck.get("cursor")

    while True:
        pages += 1
        if max_pages and pages > max_pages:
            break

        # compose variables = template + cursor(if any)
        use_vars = dict(vars_template)
        use_vars.setdefault("commentsAfterCount", 50)  # 25–100 đều ok
        if current_cursor:
            use_vars[cursor_key] = current_cursor

        # replay
        resp_text = graphql_post_in_page(driver, url, form, use_vars)
        batch_texts, end_cursor, total_target, _ = extract_full_posts_from_resptext(resp_text)

        if batch_texts:
            all_texts.extend(batch_texts)
            print(f"[V2] Page {pages}: +{len(batch_texts)} comments (cursor={bool(current_cursor)})")

        # update checkpoint
        ck["cursor"] = end_cursor
        ck["vars_template"] = vars_template
        ck["cursor_key"] = cursor_key
        ck["ts"] = time.time()
        save_checkpoint(ck, checkpoint_path)

        # save progress
        try:
            json.dump({"texts": all_texts, "pages": pages, "target": total_target},
                      open(out_json,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
        except Exception as e:
            print("[WARN] Không thể ghi file:", e)

        # stop if no next page
        if not end_cursor:
            print("[V2] Hết trang (không còn end_cursor).")
            break

        # advance
        current_cursor = end_cursor

    print(f"[V2] DONE. Collected {len(all_texts)} comment texts → {out_json}. Checkpoint at {checkpoint_path}.")
    return all_texts

# =========================
# MAIN
# =========================
from extract_comment_utils import extract_full_posts_from_resptext
from get_comment_fb_utils import (
                                 set_sort_to_all_comments
                                 )
from get_comment_fb_automation import (
                                 start_driver,
                                 install_early_hook,
                                 hook_graphql)
CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"
PROFILE_NAME  = "Profile 5"
POST_URL      = "https://web.facebook.com/share/p/17W2LptXYM/"
OUT_FILE      = "comments_batch1.json"
REMOTE_PORT  = 9222
def scroll_element_by_xpath(driver, xpath, fraction=0.8):
    """
    Cuộn phần tử có scroll (scrollTop) theo tỉ lệ chiều cao của chính nó.
    fraction: 0..1 (0.8 = cuộn ~80% chiều cao viewport của phần tử)
    Return: True nếu cuộn được (scrollTop thay đổi), False nếu không scrollable.
    """
    js = r"""
    const el = document.evaluate(arguments[0], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
    if (!el) return {ok:false, reason:'not_found'};
    const cs = getComputedStyle(el);
    const oy = cs.overflowY;
    const scrollable = (['auto','scroll'].includes(oy) || el === document.scrollingElement || el === document.body);
    const canScroll = el.scrollHeight > el.clientHeight + 2;
    if (!(scrollable && canScroll)) return {ok:false, reason:'not_scrollable', h:el.scrollHeight, c:el.clientHeight};

    const dy = Math.floor(el.clientHeight * arguments[1]);
    const before = el.scrollTop;
    el.scrollTop = before + dy;
    const after = el.scrollTop;
    return {ok:(after !== before), before, after, dy, h:el.scrollHeight, c:el.clientHeight};
    """
    ret = driver.execute_script(js, xpath, float(fraction))
    return bool(ret and ret.get("ok"))
if __name__ == "__main__":
    d = start_driver(CHROME_PATH, USER_DATA_DIR, PROFILE_NAME, port=REMOTE_PORT)
    install_early_hook(d)

    d.get(POST_URL)
    time.sleep(2)
    hook_graphql(d)
    time.sleep(0.5)

    set_sort_to_all_comments(d)

    # ép FB tạo UFI nếu chưa có
    time.sleep(0.8)
        # 1) click “Xem thêm …” nếu có, else kéo đến comment cuối
    for _ in range(4):
        if click_view_more_if_any(d, max_clicks=1) == 0:
            if not scroll_to_last_comment(d):
                d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.8));")
        time.sleep(2)
    texts = crawl_comments_v2(
        d,
        out_json="comments_v2.json",
        checkpoint_path="checkpoint_comments.json",
        max_pages=None  # hoặc đặt số trang tối đa để giới hạn
    )