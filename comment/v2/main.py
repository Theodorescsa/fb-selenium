# -*- coding: utf-8 -*-
"""
V2 – FB comments crawler via GraphQL replay (no continuous scrolling)
- Bước 1: set sort "All comments" như cũ.
- Bước 2: cuộn 1 lần để FB bắn 1 request comment đầu.
- Bước 3: lấy request đó, tách form, lưu vars_template (bỏ cursor).
- Bước 4: REPLAY GraphQL trong chính page context bằng fetch(), paginate bằng end_cursor.
- Bước 5: lưu checkpoint (cursor + vars_template + cursor_key + doc_id + friendly).
"""

import json, time, urllib.parse, os, hashlib
from extract_comment_utils import extract_full_posts_from_resptext
from configs import *
from get_comment_fb_utils import (
                                 append_ndjson_line,
                                 build_reply_vars,
                                 clean_fb_resp_text,
                                 detect_cursor_key,
                                 load_checkpoint,
                                 open_reel_comments_if_present,
                                 save_checkpoint,
                                 set_sort_to_all_comments,
                                 strip_cursors_from_vars
                                 )
from get_comment_fb_automation import (
                                 click_view_more_if_any,
                                 graphql_post_in_page,
                                 parse_form,
                                 scroll_to_last_comment,
                                 start_driver,
                                 install_early_hook,
                                 hook_graphql,
                                 wait_first_comment_request)
os.makedirs("raw_dumps", exist_ok=True)
from collections import deque
reply_jobs = deque()
def crawl_replies_for_parent(driver, url, form, base_vars_template, parent_comment_id,
                             out_json, extract_fn, clean_fn,
                             max_reply_pages=None):
    """
    - driver/url/form: như phần top-level
    - base_vars_template: vars_template 'sạch' (không cursor) của phần comments
    - parent_comment_id: id comment cha
    - out_json: file NDJSON
    - extract_fn: ví dụ extract_full_posts_from_resptext
    - clean_fn: ví dụ clean_fb_resp_text
    """

    pages = 0
    current_cursor = None
    seen_cursors = set()
    seen_hash = set()

    while True:
        pages += 1
        if max_reply_pages and pages > max_reply_pages:
            break

        use_vars = build_reply_vars(base_vars_template, parent_comment_id, current_cursor)

        raw_ret = graphql_post_in_page(driver, url, form, use_vars)
        resp_text = raw_ret.get("text") if isinstance(raw_ret, dict) else raw_ret

        # parse/clean an toàn
        try:
            json.loads(resp_text)
            cleaned = resp_text
        except Exception:
            cleaned = clean_fn(resp_text)
            json.loads(cleaned)  # để raise nếu vẫn hỏng

        # extract replies từ response này
        reply_items, end_cursor, _target, _extra = extract_fn(cleaned)

        # Ghi per-reply (dedupe theo nội dung)
        new_cnt = 0
        for i, it in enumerate(reply_items, 1):
            if isinstance(it, dict):
                txt = it.get("text") or it.get("message") or it.get("body")
                # nếu extractor đã có id reply thì nên lấy ra ở đây để dedupe theo id
                rid = it.get("id")
            else:
                txt, rid = str(it), None

            txt = (txt or "").strip()
            if not txt:
                continue

            # ưu tiên dedupe theo id nếu có; else theo hash nội dung
            if rid:
                key = f"id:{rid}"
            else:
                key = "md5:" + hashlib.md5(txt.encode("utf-8")).hexdigest()

            if key in seen_hash:
                continue
            seen_hash.add(key)

            append_ndjson_line(out_json, {
                "is_reply": True,
                "parent_id": parent_comment_id,
                "page": pages,
                "text": txt,
                "reply_id": rid,
                "cursor": end_cursor,
                "ts": time.time()
            })
            new_cnt += 1

        print(f"[V2-REPLIES] parent={parent_comment_id[:12]}… page {pages}: +{new_cnt}/{len(reply_items)}")

        # điều kiện dừng
        if not end_cursor:
            print("[V2-REPLIES] Hết trang replies (no end_cursor).")
            break
        if current_cursor and end_cursor == current_cursor:
            print("[V2-REPLIES] Cursor không tiến → dừng.")
            break
        if end_cursor in seen_cursors:
            print("[V2-REPLIES] Cursor lặp → dừng.")
            break

        seen_cursors.add(end_cursor)
        current_cursor = end_cursor
        
def crawl_comments(driver, out_json="comments.ndjson", checkpoint_path="checkpoint_comments.json", max_pages=None):

    # 1) ensure one lightweight scroll to produce first request
    baseline = driver.execute_script("return (window.__gqlReqs||[]).length")
    # 1) click “Xem thêm …” nếu có, else kéo đến comment cuối
    for _ in range(2):
        if click_view_more_if_any(driver, max_clicks=1) == 0:  # FIX: dùng driver
            if not scroll_to_last_comment(driver):             # FIX: dùng driver
                driver.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.8));")
        time.sleep(2)

    first_req = wait_first_comment_request(driver, baseline, timeout=12, poll=0.2)

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
    seen_text_hash = set()
    seen_cursors = set()
    reply_jobs = deque()  # NEW: hàng đợi crawl replies

    while True:
        pages += 1
        if max_pages and pages > max_pages:
            break

        use_vars = dict(vars_template)
        use_vars.setdefault("commentsAfterCount", 50)
        if current_cursor:
            use_vars[cursor_key] = current_cursor

        # replay
        raw_ret = graphql_post_in_page(driver, url, form, use_vars)
        resp_text = raw_ret.get("text") if isinstance(raw_ret, dict) else raw_ret

        # parse “an toàn”
        try:
            json_resp = json.loads(resp_text)
            cleaned = resp_text
        except Exception as e:
            cleaned = clean_fb_resp_text(resp_text)
            json_resp = json.loads(cleaned)
            os.makedirs("raw_dumps", exist_ok=True)
            with open(f"raw_dumps/page{pages}.txt", "w", encoding="utf-8") as f:
                f.write(resp_text)
            print(f"[WARN] page {pages} parse fail:", e)
            # không continue vì đã parse ok qua cleaned

        # lưu JSON sạch để trace (optional)
        with open(f"raw_dumps/page{pages}.json", "w", encoding="utf-8") as f:
            json.dump(json_resp, f, ensure_ascii=False, indent=2)

        # extract
        batch_texts, end_cursor, total_target, extra = extract_full_posts_from_resptext(cleaned)

        # stop if no next page
        if not end_cursor:
            print("[V2] Hết trang (không còn end_cursor).")
            break

        # guard: cursor không tiến hoặc lặp
        if current_cursor and end_cursor == current_cursor:
            print(f"[FUSE] cursor no-advance at page {pages} (cursor={current_cursor[:20]}...). Stop to avoid loop.")
            break
        if end_cursor in seen_cursors:
            print(f"[FUSE] cursor repeated: {str(end_cursor)[:20]}... Stop.")
            break
        seen_cursors.add(end_cursor)

        print(f"[DBG] cursor_key={cursor_key} current={str(current_cursor)[:24]}... next={str(end_cursor)[:24]}...")

        # ✅ GHI THEO COMMENT — MỖI COMMENT 1 DÒNG + ENQUEUE REPLIES
        if batch_texts:
            new_cnt = 0
            for idx, item in enumerate(batch_texts, 1):
                # lấy text
                if isinstance(item, dict):
                    txt = (
                        item.get("text")
                        or item.get("message")
                        or item.get("body")
                        or json.dumps(item, ensure_ascii=False)
                    )
                    # phát hiện replies
                    parent_id = item.get("id")
                    reply_count = item.get("comment") or item.get("reply_count") or 0
                else:
                    txt = str(item)
                    parent_id, reply_count = None, 0

                txt = (txt or "").strip()
                if not txt:
                    continue

                # dedupe theo nội dung (tạm thời; nếu có id comment nên dedupe theo id)
                h = hashlib.md5(txt.encode("utf-8")).hexdigest()
                if h in seen_text_hash:
                    continue
                seen_text_hash.add(h)

                # ghi NDJSON top-level
                append_ndjson_line(out_json, {
                    "is_reply": False,
                    "parent_id": None,
                    "page": pages,
                    "index_in_page": idx,
                    "text": txt,
                    "cursor": end_cursor,
                    "ts": time.time(),
                    "target": total_target
                })
                new_cnt += 1

                # enqueue replies nếu có
                if parent_id and isinstance(reply_count, int) and reply_count > 0:
                    reply_jobs.append(parent_id)

            all_texts.extend(batch_texts)
            print(f"[V2] Page {pages}: +{new_cnt}/{len(batch_texts)} comments (cursor={bool(current_cursor)})")

        # update checkpoint
        ck["cursor"] = end_cursor
        ck["vars_template"] = vars_template
        ck["cursor_key"] = cursor_key
        ck["ts"] = time.time()
        save_checkpoint(ck, checkpoint_path)

        # ADVANCE cursor (chỉ 1 lần)
        current_cursor = end_cursor

        # # === crawl replies cho các parent vừa phát hiện ===
        # while reply_jobs:
        #     pid = reply_jobs.popleft()
        #     crawl_replies_for_parent(
        #         driver,
        #         url,
        #         form,
        #         vars_template,                 # base template đã strip cursor
        #         parent_comment_id=pid,
        #         out_json=out_json,
        #         extract_fn=extract_full_posts_from_resptext,
        #         clean_fn=clean_fb_resp_text,
        #         max_reply_pages=None           # giới hạn nếu muốn (VD: 3)
        #     )

    print(f"[V2] DONE. Collected {len(all_texts)} comments → {out_json}. Checkpoint at {checkpoint_path}.")
    return all_texts

# =========================
# MAIN
# =========================
def scroll_element_by_xpath(driver, xpath, fraction=0.8):
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
    if "reel" in POST_URL:
        open_reel_comments_if_present(d)
    set_sort_to_all_comments(d)

    # ép FB tạo UFI nếu chưa có
    time.sleep(0.8)
    texts = crawl_comments(
        d,
        out_json="comments.ndjson",
        checkpoint_path="checkpoint_comments.json",
        max_pages=None  # hoặc đặt số trang tối đa để giới hạn
    )