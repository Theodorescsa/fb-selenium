from collections import deque
import json, time, urllib.parse, os, hashlib
from extract_comment_utils import extract_full_posts_from_resptext, extract_replies_from_depth1_resp
from configs import *
from get_comment_fb_utils import (
                                 _split_top_level_json_objects,
                                 _strip_xssi_globally,
                                 append_ndjson_line,
                                 clean_fb_resp_text,
                                 collect_reply_tokens_from_json,
                                 detect_cursor_key,
                                 load_checkpoint,
                                 open_reel_comments_if_present,
                                 save_checkpoint,
                                 set_sort_to_all_comments_unified,
                                 strip_cursors_from_vars
                                 )
from get_comment_fb_automation import (
                                 click_view_more_if_any,
                                 graphql_post_in_page,
                                 parse_form,
                                 pick_reply_template_from_page,
                                 scroll_to_last_comment,
                                 start_driver,
                                 install_early_hook,
                                 hook_graphql,
                                 wait_first_comment_request)
from startdriverproxy import bootstrap_auth, start_driver_with_proxy
os.makedirs("raw_dumps", exist_ok=True)

REPLY_DOC_ID = "25396268633304296"  # từ payload của ông

def crawl_replies_for_parent_expansion(
    driver,
    url,
    form,
    base_reply_vars,
    parent_id,
    parent_token,
    out_json,
    extract_fn,
    clean_fn,
    max_reply_pages=None
):
    pages = 0
    current_token = parent_token

    reply_form = dict(form)
    reply_form["doc_id"] = REPLY_DOC_ID
    reply_form["fb_api_req_friendly_name"] = "Depth1CommentsListPaginationQuery"

    while True:
        pages += 1
        if max_reply_pages and pages > max_reply_pages:
            break

        use_vars = dict(base_reply_vars)
        # dọn field comment-level
        use_vars.pop("commentsAfterCount", None)
        use_vars.pop("commentsAfterCursor", None)
        use_vars.pop("commentsBeforeCount", None)
        use_vars.pop("commentsBeforeCursor", None)

        # query theo FEEDBACK ID
        use_vars["id"] = parent_id
        use_vars["repliesAfterCount"] = 20
        if current_token:
            use_vars["expansionToken"] = current_token

        raw_ret = graphql_post_in_page(driver, url, reply_form, use_vars)
        resp_text = raw_ret.get("text") if isinstance(raw_ret, dict) else raw_ret

        try:
            json.loads(resp_text)
        except Exception:
            resp_text = clean_fn(resp_text)

        # 👇 Lúc này replies là list "full rows"
        replies, next_token = extract_fn(resp_text, parent_id)

        new_cnt = 0
        for r in replies:
            # r đã là dạng comment-row rồi → chỉ thêm metadata để phân biệt reply
            rec = {
                **r,
                "is_reply": True,
                "parent_id": parent_id,
                "page": pages,
                "ts": time.time(),
            }
            append_ndjson_line(out_json, rec)
            new_cnt += 1

        print(f"[V2-REPLIES] parent={parent_id[:12]}… page {pages}: +{new_cnt}/{len(replies)}")

        if not next_token or next_token == current_token:
            print("[V2-REPLIES] Hết trang replies (no new expansion_token).")
            break

        current_token = next_token

def crawl_comments(driver, out_json="comments.ndjson", checkpoint_path="checkpoint_comments.json", max_pages=None):

    # 1) ensure one lightweight scroll to produce first request
    baseline = driver.execute_script("return (window.__gqlReqs||[]).length")
    set_sort_to_all_comments_unified(driver)
    # 1) click “Xem thêm …” nếu có, else kéo đến comment cuối
    for _ in range(1):
        if click_view_more_if_any(driver, max_clicks=1) == 0:  # FIX: dùng driver
            if not scroll_to_last_comment(driver):             # FIX: dùng driver
                driver.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.8));")
        time.sleep(1)

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
        # save_checkpoint(ck, checkpoint_path)

    # 3) paginate via replay
    all_texts = []
    pages = 0
    current_cursor = ck.get("cursor")
    seen_cursors = set()
    reply_jobs = deque()  # NEW: hàng đợi crawl replies

    skip_count = 0
    seen_links = set()
    while True:
        pages += 1
        if max_pages and pages > max_pages:
            break
        if skip_count > 1:
            break
        use_vars = dict(vars_template)
        use_vars.setdefault("commentsAfterCount", 50)
        if current_cursor:
            use_vars[cursor_key] = current_cursor

        # replay
        raw_ret = graphql_post_in_page(driver, url, form, use_vars)
        resp_text = raw_ret.get("text") if isinstance(raw_ret, dict) else raw_ret

        # parse “an toàn”
        reply_token_map = {}
        try:
            # case FB trả JSON sạch
            with open(f"raw_dumps/page{pages}.txt", "w", encoding="utf-8") as f:
                f.write(resp_text)
            json_resp = json.loads(resp_text)
            
            cleaned = resp_text
            reply_token_map = {}
            collect_reply_tokens_from_json(json_resp, reply_token_map)
        except Exception as e:
            # case FB trả 2 JSON dính nhau → dùng hàm clean
            raw = resp_text
            stripped = _strip_xssi_globally(raw)
            parts = _split_top_level_json_objects(stripped)
            if len(parts) > 1:
                cleaned = clean_fb_resp_text(raw)      # ưu tiên block có cursor bằng score đệ quy
                json_resp = json.loads(cleaned)
            else:
                json_resp = json.loads(stripped)
                cleaned = stripped


            # os.makedirs("raw_dumps", exist_ok=True)
            # with open(f"raw_dumps/page{pages}.txt", "w", encoding="utf-8") as f:
            #     f.write(resp_text)

            print(f"[WARN] page {pages} parse fail:", e)
            # không continue vì đã parse ok qua cleaned

        # # lưu JSON sạch để trace (optional)
        with open(f"raw_dumps/page{pages}.json", "w", encoding="utf-8") as f:
            json.dump(json_resp, f, ensure_ascii=False, indent=2)

        # extract
        batch_texts, end_cursor, total_target, extra = extract_full_posts_from_resptext(cleaned)
        if extra and isinstance(extra, dict):
            for job in extra.get("reply_jobs", []):
                # job kiểu: {"id": parent_comment_id, "token": expansion_token}
                reply_jobs.append(job)
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
                    reply_count = (
                        item.get("comment")
                        or item.get("reply_count")
                        or item.get("comments_count")
                        or 0
                    )
                else:
                    txt = str(item)
                    reply_count = 0

                txt = (txt or "").strip()
                if not txt:
                    continue

                # dedupe
                link = item.get("link").strip().lower()

                # Nếu đã có thì bỏ qua
                if link in seen_links:
                    print(f"[SKIP] trùng comment {link or '(no link)'} -> skip")
                    skip_count += 1

                # Nếu chưa có thì thêm vào set
                seen_links.add(link)

                # ghi dòng comment
                rec = {
                    **item,
                    "is_reply": False,
                    "parent_id": None,
                    "page": pages,
                    "index_in_page": idx,
                    "cursor": end_cursor,
                    "ts": time.time(),
                    "target": total_target,
                }
                append_ndjson_line(out_json, rec)
                new_cnt += 1
                if skip_count > 1:
                    break
                # 🟣🟣🟣 ENQUEUE REPLIES Ở ĐÂY
                # extractor mới đã có: item["feedback_id"], item["raw_comment_id"]
                fb_id = item.get("feedback_id")
                raw_cid = item.get("raw_comment_id") or item.get("id")

                if isinstance(reply_count, int) and reply_count > 0:
                    info = None

                    # 1) ưu tiên feedback_id vì crawl replies đang query theo feedback
                    if fb_id:
                        info = reply_token_map.get(fb_id)

                    # 2) thử theo id gốc
                    if not info and raw_cid:
                        info = reply_token_map.get(raw_cid)

                    # 3) thử theo id hiện tại
                    if not info and item.get("id"):
                        info = reply_token_map.get(item["id"])

                    if info:
                        reply_jobs.append({
                            "id": info["feedback_id"],   # để crawl_replies_for_parent_expansion dùng
                            "token": info["token"],
                        })
                    else:
                        print(f"[REPLIES] comment {(raw_cid or fb_id or '')[:12]}… có {reply_count} replies nhưng KHÔNG thấy expansionToken/feedback_id → skip")

            all_texts.extend(batch_texts)
            print(f"[V2] Page {pages}: +{new_cnt}/{len(batch_texts)} comments (cursor={bool(current_cursor)})")

        # update checkpoint
        ck["cursor"] = end_cursor
        ck["vars_template"] = vars_template
        ck["cursor_key"] = cursor_key
        ck["ts"] = time.time()
        # save_checkpoint(ck, checkpoint_path)

        # ADVANCE cursor (chỉ 1 lần)
        current_cursor = end_cursor

        # # === crawl replies cho các parent vừa phát hiện ===
        while reply_jobs:
            job = reply_jobs.popleft()
            parent_id = job["id"]
            parent_token = job.get("token")  # có thể None với mấy comment ít reply

            crawl_replies_for_parent_expansion(
                driver,
                url,
                form,
                base_reply_vars=vars_template,   # ⚠️ dùng template đã strip cursor, KO dùng orig_vars
                parent_id=parent_id,
                parent_token=parent_token,
                out_json=out_json,
                extract_fn=extract_replies_from_depth1_resp,
                clean_fn=clean_fb_resp_text,
                max_reply_pages=None
            )


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
    d = start_driver(CHROME_PATH, USER_DATA_DIR, PROFILE_NAME, port=REMOTE_PORT, headless=False)
    # d = start_driver_with_proxy(PROXY_URL, headless=False)
    d.set_script_timeout(40)
    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception:
        pass

    # bootstrap_auth(d)
    # try:
    #     install_early_hook(d)
    # except Exception as e:
    #     print("[WARN] install_early_hook:", e)

    d.get(POST_URL)
    time.sleep(2)
    hook_graphql(d)
    time.sleep(0.5)
    if "reel" in POST_URL:
        open_reel_comments_if_present(d)
    # set_sort_to_all_comments_unified(d)

    # ép FB tạo UFI nếu chưa có
    time.sleep(0.8)
    texts = crawl_comments(
        d,
        out_json="comments.ndjson",
        checkpoint_path="checkpoint_comments.json",
        max_pages=None  # hoặc đặt số trang tối đa để giới hạn
    )