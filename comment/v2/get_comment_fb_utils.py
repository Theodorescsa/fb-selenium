from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, StaleElementReferenceException
)
from selenium.common.exceptions import NoSuchElementException

import time, os, json, urllib, re

from configs import CURSOR_KEYS
def _iter_all_dicts(o):
    if isinstance(o, dict):
        yield o
        for v in o.values():
            yield from _iter_all_dicts(v)
    elif isinstance(o, list):
        for v in o:
            yield from _iter_all_dicts(v)
def find_pageinfo_any(pay):
    # 1) page_info cổ điển
    paths = [
        ("data","node","comment_rendering_instance_for_feed_location","comments","page_info"),
        ("data","node","feedback","comment_rendering_instance","comments","page_info"),
    ]
    for p in paths:
        try:
            pi = pay
            for k in p: pi = pi[k]
            if isinstance(pi, dict):
                ec = pi.get("end_cursor") or pi.get("endCursor")
                hn = pi.get("has_next_page") or pi.get("hasNextPage")
                if ec or (hn is not None):
                    return ec, bool(hn)
        except Exception:
            pass

    # 2) NEW: lấy expansion_token từ CHÍNH list top-level
    def _last_token_from_edges(edges):
        last_tok = None
        for e in edges or []:
            n = (e or {}).get("node") or {}
            fb = n.get("feedback") or {}
            ei = fb.get("expansion_info") or {}
            tok = ei.get("expansion_token")
            if tok:
                last_tok = tok
        return last_tok

    # 2a) đường comment_rendering_instance_for_feed_location
    try:
        edges = (pay["data"]["node"]
                    ["comment_rendering_instance_for_feed_location"]["comments"]
                    .get("edges", []))
        tok = _last_token_from_edges(edges)
        if tok: return tok, True
    except Exception:
        pass

    # 2b) fallback feedback.comment_rendering_instance
    try:
        edges = (pay["data"]["node"]["feedback"]
                    ["comment_rendering_instance"]["comments"]
                    .get("edges", []))
        tok = _last_token_from_edges(edges)
        if tok: return tok, True
    except Exception:
        pass

    return None, None



# ====== Generic helpers ======
def _wait(d, timeout=10):
    return WebDriverWait(d, timeout)

def scroll_into_view(d, el, block="center"):
    d.execute_script(f"arguments[0].scrollIntoView({{block:'{block}'}});", el)

def safe_click(d, el, sleep_after=0.25):
    """Scroll → try click (handles intercept/stale)."""
    try:
        scroll_into_view(d, el, "center")
        time.sleep(0.15)
        el.click()
        time.sleep(sleep_after)
        return True
    except (ElementClickInterceptedException, StaleElementReferenceException):
        try:
            d.execute_script("arguments[0].click()", el)
            time.sleep(sleep_after)
            return True
        except Exception:
            return False

def find_first(container, xpaths):
    """Find the first displayed+enabled element matching any xpath (relative to container)."""
    for xp in xpaths:
        try:
            el = container.find_element(By.XPATH, xp)
            if el.is_displayed() and el.is_enabled():
                return el
        except Exception:
            pass
    return None

def wait_first(d, container, xpaths, timeout=8, poll=0.2):
    """Wait until any of xpaths appears (displayed+enabled)."""
    end = time.time() + timeout
    while time.time() < end:
        el = find_first(container, xpaths)
        if el:
            return el
        time.sleep(poll)
    raise TimeoutException("Element for any provided xpath not found/ready.")

# ====== Post dialog ======
def get_post_dialog(driver, timeout=10):
    """Lấy dialog mới nhất (post mở kiểu popup)."""
    return _wait(driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, "(//div[@role='dialog'])[last()]"))
    )

# ====== Sort menu (scoped to dialog) ======
# Text variants (add more locales if cần)
SORT_BUTTON_TEXTS = [
    "Phù hợp nhất",      # VI
    "Most relevant",     # EN
]
ALL_COMMENTS_TEXTS = [
    "Tất cả bình luận",  # VI
    "All comments",      # EN
]

def _button_with_span_text_xpath(texts):
    print("Đang tìm xpath button với span text...", texts)
    # .//div[@role='button'][.//span[normalize-space()='...']]
    parts = [f".//div[@role='button'][.//span[normalize-space()='{t}']]" for t in texts]
    # fallback contains (ít ưu tiên hơn)
    parts += [f".//div[@role='button'][contains(., '{t}')]" for t in texts]
    return parts

def _menuitem_with_span_text_xpath(texts):
    # Tìm trong popup menu: role=menu / menuitem hoặc button
    parts = [
        f"(//div[@role='menu'] | //div[@role='dialog'] | //div[@role='menuitem'] | //div[@role='listbox'])"
        f"//div[@role='menuitem' or @role='button'][.//span[normalize-space()='{t}']]"
        for t in texts
    ]
    # fallback: bất kỳ nút có span text đó
    parts += [f"(.//div[@role='button'] | .//div[@role='menuitem'])[.//span[normalize-space()='{t}']]" for t in texts]
    return parts

def open_sort_menu_scoped(driver, container, timeout=10):
    """
    Tìm & click nút sort ('Phù hợp nhất' / 'Most relevant').
    Không lệ thuộc class/aria-label dễ vỡ.
    """
    xpaths = _button_with_span_text_xpath(SORT_BUTTON_TEXTS)
    btn = wait_first(driver, container, xpaths, timeout=timeout)
    if not safe_click(driver, btn):
        raise ElementClickInterceptedException("Không click được nút Sort.")
    return True

def choose_all_comments_scoped(driver, container, timeout=10):
    """
    Trong menu vừa mở, chọn 'Tất cả bình luận' / 'All comments'.
    """
    # Scope tìm trong toàn trang vì menu thường mount ở root (không nằm trong dialog container)
    root = driver.find_element(By.XPATH, "/html/body")
    xpaths = _menuitem_with_span_text_xpath(ALL_COMMENTS_TEXTS)
    opt = wait_first(driver, root, xpaths, timeout=timeout)
    if not safe_click(driver, opt):
        raise ElementClickInterceptedException("Không click được option 'All comments'.")
    return True

# ===== Text variants =====
SORT_TEXTS = [
    "Phù hợp nhất", "Most relevant",  # add more locales if needed
]
ALL_COMMENTS_TEXTS = [
    "Tất cả bình luận", "All comments",
]

# ===== Small utils (robust click + multi-xpath wait) =====
def _visible(elem):
    try:
        return elem.is_displayed() and elem.is_enabled()
    except Exception:
        return False

def js_click(driver, el):
    driver.execute_script("arguments[0].click();", el)

def safe_click(driver, el, move_first=True):
    try:
        if move_first:
            try:
                ActionChains(driver).move_to_element(el).pause(0.05).perform()
            except Exception:
                pass
        el.click()
        return True
    except Exception:
        try:
            js_click(driver, el)
            return True
        except Exception:
            return False

def wait_first_xpath_anywhere(driver, xpaths, timeout=10):
    end = time.time() + timeout
    last_err = None
    while time.time() < end:
        for xp in xpaths:
            try:
                el = driver.find_element(By.XPATH, xp)
                if _visible(el):
                    return el
            except Exception as e:
                last_err = e
        time.sleep(0.1)
    if last_err:
        raise last_err
    raise TimeoutException("No element matched any xpath in time.")

# ===== XPath builders (global, not scoped to dialog) =====
def _button_xpaths_for_texts(texts):
    # We’ll try (1) role=button with span text; (2) role=button with innerText; (3) aria-label
    # Plus some FB-specific wrappers.
    xps = []
    for t in texts:
        # exact span match
        xps += [
            f"//div[@role='button'][.//span[normalize-space()='{t}']]",
            f"//div[@role='button'][normalize-space(.)='{t}']",
            f"//div[@role='button'][contains(., '{t}')]",
            f"//*[@role='button' and @aria-label='{t}']",
            f"//*[@role='button' and contains(@aria-label, '{t}')]",
        ]
        # menus sometimes live in composite buttons
        xps += [
            f"//span[normalize-space()='{t}']/ancestor::*[@role='button'][1]",
        ]
    # A couple of generic fallbacks frequently seen on Reels/Video surfaces
    xps += [
        # sort pills near comment header
        "//div[@role='button'][.//span[contains(., 'bình luận')] and .//span[contains(., 'hợp')]]",
        # last visible button inside the last dialog as fallback
        "(//div[@role='dialog'])[last()]//div[@role='button'][.//span][last()]",
    ]
    return xps

def _menuitem_xpaths_for_texts(texts):
    # FB mounts menus at body-level; include role=menuitem/option/button, and listbox options.
    xps = []
    for t in texts:
        xps += [
            # common menu containers
            f"(//div[@role='menu'] | //div[@role='listbox'] | //div[@role='dialog'] | //div[@role='presentation'] | /html/body)"
            f"//div[@role='menuitem' or @role='option' or @role='button'][.//span[normalize-space()='{t}']]",
            f"(//div[@role='menu'] | //div[@role='listbox'] | //div[@role='dialog'] | //div[@role='presentation'] | /html/body)"
            f"//div[@role='menuitem' or @role='option' or @role='button'][contains(., '{t}')]",
            # sometimes text is directly on a span/div
            f"(//div[@role='menu'] | //div[@role='listbox'] | /html/body)//*[normalize-space()='{t}']/ancestor::*[@role='menuitem' or @role='option' or @role='button'][1]",
        ]
    # Fallback: last open menuitem in the last menu
    xps += [
        "(//div[@role='menu'])[last()]//div[@role='menuitem' or @role='option' or @role='button'][last()]"
    ]
    return xps

# ===== New unified flows (work for Post + Video + Reel) =====
def open_sort_menu_unified(driver, timeout=10):
    """
    Globally find and click the Sort button ("Phù hợp nhất"/"Most relevant") regardless of surface.
    """
    xpaths = _button_xpaths_for_texts(SORT_TEXTS)
    btn = wait_first_xpath_anywhere(driver, xpaths, timeout=timeout)
    if not safe_click(driver, btn):
        raise ElementClickInterceptedException("Không click được nút Sort (global).")
    # small pause for menu mount/animation
    time.sleep(1)
    return True

def choose_all_comments_unified(driver, timeout=10):
    """
    After the sort menu opens, pick the 'All comments' option. Search at <body>-level.
    """
    xpaths = _menuitem_xpaths_for_texts(ALL_COMMENTS_TEXTS)
    opt = wait_first_xpath_anywhere(driver, xpaths, timeout=timeout)
    if not safe_click(driver, opt):
        raise ElementClickInterceptedException("Không click được option 'All comments' (global).")
    time.sleep(1)
    return True

def set_sort_to_all_comments_unified(driver, max_retry=2):
    """
    Public API — robust for Post/Video/Reel:
    1) Find global Sort button (any surface), click.
    2) Select 'All comments' from the body-mounted menu.
    """
    print("[SORT] Set to 'All comments' (unified)…")
    last_err = None
    for _ in range(max_retry):
        try:
            open_sort_menu_unified(driver, timeout=10)
            choose_all_comments_unified(driver, timeout=10)
            return True
        except Exception as e:
            last_err = e
            time.sleep(0.6)
    if last_err:
        raise last_err
    return False
def append_ndjson_texts(out_path: str, texts, page_no: int, cursor_val: str | None):
    """
    Ghi mỗi comment 1 dòng NDJSON:
    {"text": "...", "page": 3, "idx": 17, "cursor": "..."}

    - Không dedup ở đây (để đơn giản và giữ đủ dữ liệu thô).
    - Dùng encoding UTF-8, ensure_ascii=False để giữ tiếng Việt.
    """
    if not texts:
        return 0
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    wrote = 0
    with open(out_path, "a", encoding="utf-8") as f:
        for i, t in enumerate(texts):
            # Nếu batch_texts là chuỗi thuần:
            if isinstance(t, str):
                obj = {"text": t, "page": page_no, "idx": i}
            else:
                # Phòng khi sau này bạn đổi parser trả dict (text, author,...)
                obj = dict(t)
                obj.setdefault("page", page_no)
                obj.setdefault("idx", i)
                if "text" not in obj and "body" in obj:
                    obj["text"] = obj.get("body")
            if cursor_val:
                obj["cursor"] = cursor_val
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            wrote += 1
    return wrote
def open_reel_comments_if_present(driver, wait_after=0.6, timeout=6.0):
    """
    Mở panel bình luận cho Reel nếu có.
    Ưu tiên click 'thật' bằng CDP (Input.dispatchMouseEvent) vào đúng tâm nút,
    fallback JS click + click overlay anh em.
    Trả về True nếu đã mở (hoặc đang mở sẵn), False nếu không thấy nút.
    """
    # ===== helpers =====
    def _is_expanded(el):
        try:
            return (el.get_attribute("aria-expanded") or "").lower() == "true"
        except:
            return False

    def _scroll_into_view(el):
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.2)

    def _hard_click_center(el):
        # dùng CDP để bắn mouse events vào tọa độ tuyệt đối của phần tử
        rect = driver.execute_script("""
            const r = arguments[0].getBoundingClientRect();
            return {x: r.left + r.width/2, y: r.top + r.height/2,
                    left:r.left, top:r.top, width:r.width, height:r.height};
        """, el)
        if not rect: 
            return False
        # bring to front (đề phòng tab chưa active)
        try:
            driver.execute_cdp_cmd("Page.bringToFront", {})
        except Exception:
            pass

        # chuyển tọa độ viewport → absolute (Chrome DevTools dùng coords viewport)
        x = rect["x"]
        y = rect["y"]

        try:
            driver.execute_cdp_cmd("Input.dispatchMouseEvent", {
                "type": "mouseMoved", "x": x, "y": y, "buttons": 1
            })
            driver.execute_cdp_cmd("Input.dispatchMouseEvent", {
                "type": "mousePressed", "x": x, "y": y, "button": "left", "clickCount": 1
            })
            driver.execute_cdp_cmd("Input.dispatchMouseEvent", {
                "type": "mouseReleased", "x": x, "y": y, "button": "left", "clickCount": 1
            })
            return True
        except Exception:
            return False

    def _js_click(el):
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False

    def _click_overlay_sibling(el):
        try:
            ov = el.find_element(By.XPATH, "following-sibling::div[@role='none'][1]")
            driver.execute_script("arguments[0].click();", ov)
            return True
        except NoSuchElementException:
            return False
        except Exception:
            return False

    def _opened():
        # mở thành công khi:
        # 1) nút có aria-expanded=true, hoặc
        # 2) xuất hiện container có aria-label chứa 'Bình luận' / 'Comments', hoặc
        # 3) buffer GraphQL tăng
        return True

    # baseline GraphQL buffer để theo dõi có phát request không
    try:
        baseline = driver.execute_script("return (window.__gqlReqs||[]).length") or 0
    except Exception:
        baseline = 0

    # ===== chọn nút ứng viên =====
    XPATHS = [
        # Nút Reel “Bình luận” theo aria-label (VI + EN)
        "//div[@role='button' and @aria-label='Bình luận']",
        "//div[@role='button' and (@aria-label='Comments' or contains(@aria-label,'Comment'))]",

        # Nút có icon bong bóng chat (path bạn gửi) → leo ancestor button
        "//svg[.//path[starts-with(@d,'M12 .5C18.351')]]/ancestor::*[@role='button'][1]",

        # Fallback: nút hiển thị số bình luận kèm icon → leo ancestor button
        "//span[normalize-space(text()) and number(.)=number(.)]/ancestor::*[@role='button'][1]",
    ]

    cand = None
    for xp in XPATHS:
        els = driver.find_elements(By.XPATH, xp)
        if els:
            cand = els[0]; break
    if not cand:
        return False  # không thấy nút → coi như không phải Reel hoặc layout khác

    # Nếu đang expanded thì coi như OK
    if _is_expanded(cand):
        return True

    # Thử theo thứ tự: scroll → hard click (CDP) → JS click → click overlay
    _scroll_into_view(cand)
    clicked = _hard_click_center(cand)
    if not clicked:
        clicked = _js_click(cand)
    if not clicked:
        clicked = _click_overlay_sibling(cand)

    # chờ load một nhịp
    time.sleep(wait_after)

    # Kiểm tra đã mở/đã bắn request chưa
    try:
        now = driver.execute_script("return (window.__gqlReqs||[]).length") or 0
    except Exception:
        now = baseline
    if _is_expanded(cand) or now > baseline:
        return True

    # Thử lần 2 (một số layout cần 2 click mới mở panel)
    _scroll_into_view(cand)
    _hard_click_center(cand)
    time.sleep(wait_after)

    try:
        now2 = driver.execute_script("return (window.__gqlReqs||[]).length") or 0
    except Exception:
        now2 = baseline

    return _is_expanded(cand) or now2 > baseline


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
    # with open(tmp,"w",encoding="utf-8") as f:
    #     json.dump(data, f, ensure_ascii=False, indent=2)
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
import json, re

CURSOR_KEYS = {
    "end_cursor", "endCursor",
    "cursor", "Cursor",
    "expansion_token", "expansionToken",
    "after",  # value thường là endCursor để trang sau
}

def _strip_xssi_globally(s: str) -> str:
    s = s.replace("for (;;);", "")
    s = s.replace(")]}',", "")
    return s.strip()

def _split_top_level_json_objects(s: str) -> list[str]:
    out = []
    start = None
    depth = 0
    in_str = False
    esc = False

    for i, ch in enumerate(s):
        if in_str:
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == '"':
                in_str = False
            continue
        else:
            if ch == '"':
                in_str = True
                if depth == 0 and start is None:
                    # không set start ở đây; chỉ set khi thấy { hoặc [
                    pass
            elif ch in '{[':
                if depth == 0 and start is None:
                    start = i
                depth += 1
            elif ch in '}]':
                if depth > 0:
                    depth -= 1
                    if depth == 0 and start is not None:
                        out.append(s[start:i+1].strip())
                        start = None
    return out if out else [s.strip()]

def _score_cursor_in_json(obj, depth=0):
    """
    Trả về (score, has_any), trong đó score cao hơn khi:
    - có nhiều khóa 'cursor-like'
    - cursor xuất hiện sâu hơn (ưu tiên block thực sự chứa page_info)
    """
    score = 0
    has_any = False

    if isinstance(obj, dict):
        for k, v in obj.items():
            k_l = str(k)
            if k_l in CURSOR_KEYS:
                # chỉ cộng điểm nếu value có ý nghĩa (non-empty / khác null / khác False)
                meaningful = v is not None and v != "" and v is not False
                if meaningful:
                    has_any = True
                    # trọng số theo độ sâu, ưu tiên sâu hơn
                    score += 10 + depth
            # tiếp tục đệ quy
            sc, ha = _score_cursor_in_json(v, depth + 1)
            score += sc
            has_any = has_any or ha

    elif isinstance(obj, list):
        for it in obj:
            sc, ha = _score_cursor_in_json(it, depth + 1)
            score += sc
            has_any = has_any or ha

    # Nếu có cấu trúc quen thuộc page_info → bonus nhẹ
    if isinstance(obj, dict) and "page_info" in obj:
        score += 3

    return score, has_any

def clean_fb_resp_text(resp_text: str) -> str:
    """
    Normalize FB GraphQL responses:
    - Strip XSSI everywhere.
    - Split safely into top-level JSON objects.
    - Parse từng block và CHẤM ĐIỂM theo sự hiện diện 'cursor-like' (đệ quy).
    - Ưu tiên block có score cao nhất; nếu không có, fallback block dài nhất.
    - Guard HTML.
    """
    if not resp_text:
        return ""

    s = _strip_xssi_globally(resp_text)

    s_l = s.lstrip()
    if s_l.startswith("<!DOCTYPE html") or s_l.startswith("<html"):
        raise ValueError("Got HTML instead of JSON (maybe login expired)")

    parts = _split_top_level_json_objects(s)

    parsed = []
    for p in parts:
        try:
            obj = json.loads(p)
            parsed.append((p, obj))
        except json.JSONDecodeError:
            continue

    if not parsed:
        # last chance: thử parse nguyên chuỗi
        json.loads(s)
        return s

    # Chấm điểm cursor cho từng block
    best_p = None
    best_score = -1
    any_has = False

    for p, obj in parsed:
        score, has_any = _score_cursor_in_json(obj)
        if has_any:
            any_has = True
        if score > best_score:
            best_score = score
            best_p = p

    if any_has:
        return best_p.strip()

    # Không block nào có cursor → fallback: block hợp lệ dài nhất
    best_len = -1
    best_p2 = None
    for p, _ in parsed:
        if len(p) > best_len:
            best_len = len(p)
            best_p2 = p
    return best_p2.strip()



def append_ndjson_line(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        
def choose_first_key(candidates):
    for k in candidates:
        return k  # lấy key đầu (đã sắp xếp theo ưu tiên)
    return None

def collect_reply_tokens_from_json(json_resp, out_map):
    """
    Đi qua tree JSON, gom:
    - comment node id  (Y29t...)
    - feedback.id      (ZmVlZGJhY2s6...)
    - feedback.expansion_info.expansion_token
    rồi lưu vào out_map[comment_id] = {token:..., feedback_id:...}
    """
    if not isinstance(json_resp, dict):
        return

    def walk(obj):
        if isinstance(obj, dict):
            # dạng mà ông dán:
            # node -> feedback -> expansion_info -> expansion_token
            if obj.get("__typename") == "Comment" and "feedback" in obj:
                cmt_id = obj.get("id")
                fb = obj.get("feedback") or {}
                fb_id = fb.get("id")
                exp = (fb.get("expansion_info") or {}).get("expansion_token")
                if cmt_id and fb_id and exp:
                    out_map[cmt_id] = {
                        "token": exp,
                        "feedback_id": fb_id,
                    }
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)

    walk(json_resp)
def _normalize_id(item: dict) -> str | None:
    print(item)
    cid = item.get("id") or item.get("id")
    return str(cid).strip() if cid else None