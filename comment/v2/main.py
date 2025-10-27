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

def _deep_iter(o):
    if isinstance(o, dict):
        for k,v in o.items():
            yield k,v
            if isinstance(v, (dict,list)):
                yield from _deep_iter(v)
    elif isinstance(o, list):
        for v in o:
            yield from _deep_iter(v)

def find_pageinfo(obj):
    if isinstance(obj, dict):
        if "page_info" in obj and isinstance(obj["page_info"], dict):
            pi = obj["page_info"]
            return pi.get("end_cursor") or pi.get("endCursor"), pi.get("has_next_page")
        for v in obj.values():
            c = find_pageinfo(v)
            if c: return c
    elif isinstance(obj, list):
        for v in obj:
            c = find_pageinfo(v)
            if c: return c
    return (None, None)

def extract_comment_texts(obj, out_list):
    if isinstance(obj, dict):
        if "body" in obj and isinstance(obj["body"], dict) and "text" in obj["body"]:
            txt = obj["body"]["text"]
            if isinstance(txt, str) and txt:
                out_list.append(txt)
        if "message" in obj and isinstance(obj["message"], dict) and "text" in obj["message"]:
            txt = obj["message"]["text"]
            if isinstance(txt, str) and txt:
                out_list.append(txt)
        for v in obj.values():
            extract_comment_texts(v, out_list)
    elif isinstance(obj, list):
        for v in obj:
            extract_comment_texts(v, out_list)

# ---- REPLACE these helpers ----

def _iter_all_dicts(o):
    if isinstance(o, dict):
        yield o
        for v in o.values():
            yield from _iter_all_dicts(v)
    elif isinstance(o, list):
        for v in o:
            yield from _iter_all_dicts(v)

def _read_pi(pi: dict):
    if not isinstance(pi, dict):
        return (None, None)
    # key variants
    end_cur = (pi.get("end_cursor") or pi.get("endCursor") or
               pi.get("start_cursor") or pi.get("startCursor"))
    has_next = pi.get("has_next_page")
    if has_next is None:
        has_next = pi.get("hasNextPage")
    return end_cur, has_next

def find_pageinfo_any(obj):
    """
    Tìm page_info theo đúng connection:
      { edges: [...], page_info: {...} }
    hoặc biến thể:
      { nodes: [...], page_info: {...} }
    Hỗ trợ cả camelCase 'pageInfo'.
    """
    # 1) Ưu tiên connection chuẩn
    for d in _iter_all_dicts(obj):
        if not isinstance(d, dict):
            continue
        # các khóa connection phổ biến
        has_edges_or_nodes = ("edges" in d and isinstance(d.get("edges"), list)) or \
                             ("nodes" in d and isinstance(d.get("nodes"), list))
        if not has_edges_or_nodes:
            continue

        pi = d.get("page_info") or d.get("pageInfo")
        if isinstance(pi, dict):
            end_cur, has_next = _read_pi(pi)
            if end_cur or has_next is not None:
                return end_cur, has_next

    # 2) Fallback: bất cứ nơi nào có page_info/pageInfo
    for d in _iter_all_dicts(obj):
        if not isinstance(d, dict):
            continue
        pi = d.get("page_info") or d.get("pageInfo")
        if isinstance(pi, dict):
            end_cur, has_next = _read_pi(pi)
            if end_cur or has_next is not None:
                return end_cur, has_next

    # 3) Cực đoan: lượm end_cursor xuất hiện rải rác
    for d in _iter_all_dicts(obj):
        if not isinstance(d, dict):
            continue
        for k in ("end_cursor", "endCursor"):
            if k in d and isinstance(d[k], str):
                return d[k], None

    return (None, None)

def extract_comments_from_resptext(resp_text: str):
    texts = []
    try:
        obj = json.loads(resp_text)
    except:
        return texts, None, None, None

    # payload có thể là mảng (batched GraphQL)
    payloads = obj if isinstance(obj, list) else [obj]

    end_cursor = None
    has_next = None
    total = None

    for pay in payloads:
        # gom text bình luận
        extract_comment_texts(pay, texts)

        # lấy page_info theo connection
        ec, hn = find_pageinfo_any(pay)
        if ec:
            end_cursor = ec
        if hn is not None:
            has_next = hn

        # total count (nếu có)
        try:
            c = pay["data"]["node"]["comment_rendering_instance_for_feed_location"]["comments"]
            total = c.get("count") or c.get("total_count") or total
        except:
            pass

    return texts, end_cursor, total, obj

def force_load_comments(driver):
    js = r"""
    (function(){
      const dlg = document.querySelector('div[role="dialog"]');
      if(!dlg) return false;
      const commentBox = dlg.querySelector("div[aria-label*='Viết bình luận'], div[aria-label*='Write a comment']");
      if(commentBox){
        commentBox.scrollIntoView({block:'center'});
        commentBox.focus();
        return true;
      }
      const seeBtns = dlg.querySelectorAll("div[role='button']");
      for(const b of seeBtns){
        if(b.innerText.includes('Xem bình luận') || b.innerText.includes('View comments')){
          b.click(); return true;
        }
      }
      return false;
    })();
    """
    return driver.execute_script(js)

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
def wait_first_comment_request(driver, start_idx, timeout=10, poll=0.2):
    end = time.time() + timeout
    i = start_idx
    while time.time() < end:
        n = driver.execute_script("return (window.__gqlReqs||[]).length")
        while i < n:
            rec = driver.execute_script("return (window.__gqlReqs||[])[arguments[0]]", i)
            i += 1
            if rec and match_comment_req(rec):
                return rec
        time.sleep(poll)
    return None

def match_comment_req(rec: dict):
    url = rec.get("url","")
    if "/api/graphql/" not in url: return False
    if rec.get("method") != "POST": return False
    body = rec.get("body","") or ""
    if "fb_api_req_friendly_name=" in body:
        if "CommentsListComponentsPaginationQuery" in body: return True
        if "UFI2CommentsProviderPaginationQuery" in body: return True
        if re.search(r"fb_api_req_friendly_name=CometUFI[^&]*Comments[^&]*Pagination", body): return True
    if "variables=" in body:
        try:
            v = parse_form(body).get("variables","")
            vj = json.loads(urllib.parse.unquote_plus(v))
            keys = set(vj.keys())
            signs = {"commentable_object_id","commentsAfterCursor","feedLocation","focusCommentID","feedbackSource","after","afterCursor"}
            if keys & signs: return True
        except: pass
    return False

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
    """
    - driver: Selenium webdriver
    - url: original /api/graphql/ endpoint
    - form_params: original parsed form (doc_id, fb_api_req_friendly_name, variables, ... )
    - override_vars: new variables dict (will be JSON-encoded & urlencoded)
    Returns: responseText (str)
    """
    fp = dict(form_params)  # shallow copy
    # encode variables
    fp["variables"] = urllib.parse.quote_plus(json.dumps(override_vars, separators=(',',':'), ensure_ascii=False))
    # Build body by preserving keys order-ish (not strictly required)
    body_pairs = []
    for k, v in fp.items():
        if k == "variables":
            body_pairs.append(f"variables={v}")
        else:
            body_pairs.append(f"{k}={urllib.parse.quote_plus(str(v))}")
    body = "&".join(body_pairs)

    js = r"""
    const url = arguments[0];
    const body = arguments[1];
    const cb = arguments[2];
    fetch(url, {
      method: 'POST',
      credentials: 'include',
      headers: {'content-type': 'application/x-www-form-urlencoded'},
      body: body
    }).then(r => r.text()).then(t => cb({ok:true, text:t})).catch(e => cb({ok:false, err:String(e)}));
    """
    # Use async script to wait for fetch
    driver.set_script_timeout(120)
    ret = driver.execute_async_script(js, url, body)
    if not ret or not ret.get("ok"):
        raise RuntimeError("Replay GraphQL failed: %s" % (ret and ret.get("err")))
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

    first_req = wait_first_comment_request(driver, baseline, timeout=10, poll=0.2)
    if not first_req:
        raise TimeoutException("Không bắt được request comment đầu (hãy đảm bảo đã mở đúng permalink & sort 'All comments').")

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
        if current_cursor:
            use_vars[cursor_key] = current_cursor

        # replay
        resp_text = graphql_post_in_page(driver, url, form, use_vars)
        batch_texts, end_cursor, total_target, _ = extract_comments_from_resptext(resp_text)

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
from get_comment_fb_utils import (
                                 set_sort_to_all_comments
                                 )
from get_comment_fb_automation import (
                                 start_driver,
                                 install_early_hook,
                                 hook_graphql,
                                 parse_form,
                                 match_comment_req)
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
    set_sort_to_all_comments(d)
    hook_graphql(d)

    # ép FB tạo UFI nếu chưa có
    time.sleep(0.8)
        # 1) click “Xem thêm …” nếu có, else kéo đến comment cuối
    for _ in range(10):
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