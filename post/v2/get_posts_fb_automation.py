import argparse
import os, re, json, time, random, datetime, urllib.parse
from typing import List, Optional, Tuple
from urllib.parse import urlparse, urlunparse


from selenium.common.exceptions import TimeoutException as _SETimeout

# ==== custom utils bạn đã có trong get_info.py (yêu cầu file này tồn tại) ====
from get_info import _all_urls_from_text, _dig_attachment_urls, _extract_share_texts, _extract_url_digits, _looks_like_group_post, deep_get_first, extract_author, extract_created_time, extract_hashtags, extract_media, extract_reactions_and_counts, extract_share_flags, extract_share_flags_smart, filter_only_feed_posts

from configs import *
from automation import (fast_forward_cursor, fetch_via_wire, js_fetch_in_page,
                        reload_and_refresh_form, soft_refetch_form_and_cursor)
from checkpoint import append_ndjson, save_checkpoint
from utils import (
                   _strip_xssi_prefix, choose_best_graphql_obj, 
                    current_cursor_from_form, deep_collect_cursors, 
                    deep_collect_timestamps, deep_find_has_next, 
                    iter_json_values, merge_vars, 
                    strip_cursors_from_vars, update_vars_for_next_cursor)
os.makedirs(RAW_DUMPS_DIR, exist_ok=True)

# =========================
# Post collectors (ưu tiên rid + link + created_time)
# =========================

def collect_post_summaries(obj, out, group_url=GROUP_URL):
    if isinstance(obj, dict):
        if _looks_like_group_post(obj):
            post_id_api = obj.get("post_id")
            fb_id      = obj.get("id")
            url        = obj.get("wwwURL") or obj.get("url")
            url_digits = _extract_url_digits(url)
            rid        = post_id_api or url_digits or fb_id
            author_id, author_name, author_link, avatar, type_label = extract_author(obj)

            actor_text, attached_text, text_combined = _extract_share_texts(obj)
            image_urls, video_urls = extract_media(obj)
            counts = extract_reactions_and_counts(obj)
            smart_is_share, smart_link, smart_type, origin_id, share_meta = extract_share_flags_smart(obj, actor_text or text_combined)
            created_candidates = deep_collect_timestamps(obj)
            created = max(created_candidates) if created_candidates else extract_created_time(obj)

            hashtags = extract_hashtags(text_combined)
            out_links = list(dict.fromkeys(_all_urls_from_text(text_combined or "") + _dig_attachment_urls(obj)[0]))
            out_domains = []
            for u in out_links:
                try:
                    host = urlparse(u).netloc.lower().split(":")[0]
                    if host: out_domains.append(host)
                except: pass
            out_domains = list(dict.fromkeys(out_domains))
            source_id = None
            _k, _v = deep_get_first(obj, {"group_id", "groupID", "groupIDV2"})
            if _v: source_id = _v
            if not source_id:
                try:
                    slug = re.search(r"/groups/([^/?#]+)", group_url).group(1)
                    source_id = slug
                except:
                    pass

            item = {
                "id": fb_id,
                "rid": rid,
                "type": type_label,
                "link": url,
                "author_id": author_id,
                "author": author_name,
                "author_link": author_link,
                "avatar": avatar,
                "created_time": created,
                "content": text_combined,
                "image_url": image_urls,
                "like": counts["like"],
                "comment": counts["comment"],
                "haha": counts["haha"],
                "wow": counts["wow"],
                "sad": counts["sad"],
                "love": counts["love"],
                "angry": counts["angry"],
                "care": counts["care"],
                "share": counts["share"],
                "hashtag": hashtags,
                "video": video_urls,
                "source_id": source_id,
                "is_share": smart_is_share,
                "link_share": smart_link,
                "type_share": smart_type,
                "origin_id": origin_id,
                "out_links": out_links,
                "out_domains": out_domains,
            }
            if share_meta:
                item["share_meta"] = share_meta
            if smart_is_share:
                item["content_parts"] = {
                    "actor_text": actor_text,
                    "attached_text": attached_text
                }
            out.append(item)
        for v in obj.values():
            collect_post_summaries(v, out, group_url)
    elif isinstance(obj, list):
        for v in obj:
            collect_post_summaries(v, out, group_url)

# =========================
# Dedupe/merge (rid + normalized link)
# =========================
def _norm_link(u: str) -> Optional[str]:
    if not u or not isinstance(u, str):
        return None
    try:
        p = urlparse(u)
        host = p.netloc.lower()
        if host.endswith("facebook.com"): host = "facebook.com"
        path = (p.path or "").rstrip("/")
        if re.search(r"/(?:reel|posts|permalink)/\d+$", path.lower()):
            return urlunparse(("https", host, path.lower(), "", "", ""))
        return None
    except Exception:
        return None

def _all_join_keys(it: dict) -> List[str]:
    keys, seen = [], set()
    for k in (it.get("rid"), it.get("id"), _extract_url_digits(it.get("link") or ""), _norm_link(it.get("link") or "")):
        if isinstance(k, str) and k and (k not in seen):
            keys.append(k); seen.add(k)
    return keys

def _best_primary_key(it: dict) -> Optional[str]:
    rid = it.get("rid"); link = it.get("link"); _id = it.get("id")
    digits = _extract_url_digits(link) if link else None
    norm   = _norm_link(link) if link else None
    for k in (rid, _id, digits, norm):
        if isinstance(k, str) and k.strip(): return k.strip()
    return None

def merge_two_posts(a: dict, b: dict) -> dict:
    if not a: return b or {}
    if not b: return a or {}
    m = dict(a)
    m["id"]   = m.get("id")   or b.get("id")
    m["rid"]  = m.get("rid")  or b.get("rid")
    m["link"] = m.get("link") or b.get("link")
    ca, cb = m.get("created_time"), b.get("created_time")
    try:
        m["created_time"] = max(int(ca) if ca else 0, int(cb) if cb else 0) or (ca or cb)
    except: m["created_time"] = ca or cb
    return m

def coalesce_posts(items: List[dict]) -> List[dict]:
    groups, key2group, seq = {}, {}, 0
    def _new_gid():
        nonlocal seq; seq += 1; return f"g{seq}"
    for it in (items or []):
        keys = _all_join_keys(it)
        gid = None
        for k in keys:
            if k in key2group:
                gid = key2group[k]; break
        if gid is None:
            gid = _new_gid(); groups[gid] = it
        else:
            groups[gid] = merge_two_posts(groups[gid], it)
        for k in _all_join_keys(groups[gid]):
            key2group[k] = gid
    return list(groups.values())

# =========================
# Paginate 1 window (NO time slice khi gọi từ cursor-only)
# =========================
def paginate_window(d, form, vars_template, seen_ids: set,
                    t_from: Optional[int]=None, t_to: Optional[int]=None,
                    page_limit: Optional[int]=None) -> Tuple[int, Optional[int], bool]:
    last_good_cursor = current_cursor_from_form(form) or None
    cursor_stall_rounds = 0
    prev_cursor = None

    total_new = 0
    min_created = None
    no_progress_rounds = 0

    mode_str = "time" if (t_from is not None or t_to is not None) else "warmup"
    if mode_str == "time":
        print(f"[MODE] Time-slice window: from={t_from} to={t_to}")

    if (t_from is not None) or (t_to is not None):
        # không dùng trong cursor-only, nhưng giữ để tái sử dụng
        base = json.loads(form.get("variables","{}")) if form.get("variables") else {}
        known_keys = set(base.keys())
        cand_after = "afterTime"  if "afterTime"  in known_keys else "after_time"
        cand_before= "beforeTime" if "beforeTime" in known_keys else "before_time"
        base = merge_vars(base, vars_template)
        if t_from is not None:  base[cand_after]  = int(t_from)
        if t_to   is not None:  base[cand_before] = int(t_to)
        if "count" in base and isinstance(base["count"], int):
            base["count"] = max(base["count"], 10)
        form["variables"] = json.dumps(base, separators=(",", ":"))

    page = 0
    has_next = False
    cursor_for_reload = None

    while True:
        page += 1
        max_tries = 3
        last_err = None
        for attempt in range(1, max_tries+1):
            try:
                txt = js_fetch_in_page(d, form, extra_headers={}, timeout_ms=20000)
                break
            except (_SETimeout, RuntimeError) as e:
                last_err = e
                if "bad_origin:" in str(e):
                    d.get(GROUP_URL); time.sleep(1.2)
                    try:
                        txt = js_fetch_in_page(d, form, extra_headers={}, timeout_ms=20000)
                        break
                    except Exception:
                        pass
                print(f"[WARN] fetch page try {attempt}/{max_tries} failed: {e}")
                time.sleep(random.uniform(0.8, 1.6))

                if attempt == 2:
                    new_form, boot_cursor, boot_has_next, _ = soft_refetch_form_and_cursor(d, form, vars_template)
                    if new_form:
                        form = new_form
                        if (t_from is not None) or (t_to is not None):
                            base = json.loads(form.get("variables","{}")) if form.get("variables") else {}
                            known_keys = set(base.keys())
                            cand_after = "afterTime"  if "afterTime"  in known_keys else "after_time"
                            cand_before= "beforeTime" if "beforeTime" in known_keys else "before_time"
                            base = merge_vars(base, vars_template)
                            if t_from is not None:  base[cand_after]  = int(t_from)
                            if t_to   is not None:  base[cand_before] = int(t_to)
                            form["variables"] = json.dumps(base, separators=(",", ":"))

                if attempt == max_tries:
                    form2, friendly2, docid2 = reload_and_refresh_form(d, GROUP_URL, None, vars_template)
                    if form2:
                        form = form2
                        if (t_from is not None) or (t_to is not None):
                            base = json.loads(form.get("variables","{}")) if form.get("variables") else {}
                            known_keys = set(base.keys())
                            cand_after = "afterTime"  if "afterTime"  in known_keys else "after_time"
                            cand_before= "beforeTime" if "beforeTime" in known_keys else "before_time"
                            base = merge_vars(base, vars_template)
                            if t_from is not None:  base[cand_after]  = int(t_from)
                            if t_to   is not None:  base[cand_before] = int(t_to)
                            form["variables"] = json.dumps(base, separators=(",", ":"))
                    try:
                        txt = js_fetch_in_page(d, form, extra_headers={}, timeout_ms=25000)
                        break
                    except Exception:
                        try:
                            txt = fetch_via_wire(d, form)
                            if txt: break
                        except Exception:
                            pass
                        raise

        obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
        with open(os.path.join(RAW_DUMPS_DIR, f"slice_{t_from or 'None'}_{t_to or 'None'}_p{page}.json"), "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

        if not obj:
            print(f"[SLICE {t_from}->{t_to}] parse fail → stop slice.")
            break

        page_posts = []
        collect_post_summaries(obj, page_posts)
        page_posts = coalesce_posts(filter_only_feed_posts(page_posts))

        written_this_round = set()
        fresh = []
        for p in page_posts:
            pk = _best_primary_key(p)
            if pk and (pk not in seen_ids) and (pk not in written_this_round):
                fresh.append(p); written_this_round.add(pk)

        if fresh:
            append_ndjson(fresh)
            for p in fresh:
                for k in _all_join_keys(p): seen_ids.add(k)
            total_new += len(fresh)
            no_progress_rounds = 0
        else:
            no_progress_rounds += 1

        for p in page_posts:
            ct = p.get("created_time")
            if isinstance(ct, int):
                if (min_created is None) or (ct < min_created):
                    min_created = ct

        cursors = deep_collect_cursors(obj)
        has_next = deep_find_has_next(obj)
        if not fresh and has_next:
            cursor_stall_rounds += 1
        else:
            cursor_stall_rounds = 0
        if cursor_stall_rounds >= 6:
            print("[STALL] next=True & fresh=0 nhiều vòng → fast-forward 2 hops")
            ff_form, ff_cursor = fast_forward_cursor(d, form, vars_template, hops=2)
            if ff_form:
                form = ff_form
                if ff_cursor:
                    last_good_cursor = ff_cursor
                    prev_cursor = ff_cursor
            cursor_stall_rounds = 0
            time.sleep(random.uniform(0.6, 1.0))
            continue

        if has_next is None: has_next = bool(cursors)
        new_cursor = cursors[0][1] if cursors else None

        if not fresh and new_cursor:
            form = update_vars_for_next_cursor(form, new_cursor, vars_template)
            last_good_cursor = new_cursor
            prev_cursor = new_cursor
            try:
                v = json.loads(form.get("variables","{}"))
                if isinstance(v.get("count"), int):
                    v["count"] = min(max(v["count"] + 10, 20), 60)
                    form["variables"] = json.dumps(v, separators=(",",":"))
            except: 
                pass
            time.sleep(random.uniform(0.3, 0.6))
            continue

        if new_cursor:
            cursor_for_reload = new_cursor
        elif not cursor_for_reload:
            cursor_for_reload = current_cursor_from_form(form)

        if new_cursor and prev_cursor == new_cursor:
            print(f"[WARN] cursor lặp lại → thử soft-refetch")
            nf, bc, bh, _ = soft_refetch_form_and_cursor(d, form, vars_template)
            if nf and (bc or bh):
                form = nf
                if bc:
                    new_cursor = bc
                    print(f"[FIX] lấy được cursor mới sau refetch.")
            else:
                f2, _, _ = reload_and_refresh_form(d, GROUP_URL, (last_good_cursor or current_cursor_from_form(form)), vars_template)
                if f2:
                    form = f2
                    try:
                        v = json.loads(form.get("variables","{}"))
                        if isinstance(v.get("count"), int):
                            v["count"] = min(max(v["count"] + 10, 20), 60)
                            form["variables"] = json.dumps(v, separators=(",",":"))
                    except:
                        pass
                    no_progress_rounds = 0
                    time.sleep(random.uniform(0.8, 1.3))
                    continue

        if new_cursor:
            last_good_cursor = new_cursor
            prev_cursor = new_cursor

        print(f"[SLICE {t_from or '-inf'}→{t_to or '+inf'}] p{page} got {len(page_posts)} (new {len(fresh)}), total_new={total_new}, next={has_next}")

        save_checkpoint(
            cursor=last_good_cursor,
            seen_ids=list(seen_ids),
            vars_template=vars_template,
            mode=mode_str,
            slice_from=t_from,
            slice_to=t_to,
            year=(datetime.datetime.utcfromtimestamp(t_to).year
                  if (t_to and mode_str == "time") else None),
            page=page,
            min_created=min_created
        )

        MAX_NO_NEXT_ROUNDS = 3
        if not has_next and no_progress_rounds >= MAX_NO_NEXT_ROUNDS:
            print(f"[PAGE#{page}] next=False x{no_progress_rounds} → reload trang & bắt lại feed")

            # cursor để bơm lại sau reload
            reload_cursor = cursor_for_reload or last_good_cursor or current_cursor_from_form(form)

            # lưu checkpoint trước khi reload
            save_checkpoint(
                cursor=reload_cursor,
                seen_ids=list(seen_ids),
                vars_template=vars_template,
                mode=mode_str,
                slice_from=t_from,
                slice_to=t_to,
                year=(datetime.datetime.utcfromtimestamp(t_to).year
                      if (t_to and mode_str == "time") else None),
                page=page,
                min_created=min_created
            )

            # thử reload 2 lần
            reloaded_ok = False
            for attempt in range(1, 3):
                new_form, friendly2, docid2 = reload_and_refresh_form(
                    d,
                    GROUP_URL,
                    reload_cursor,
                    vars_template
                )
                if new_form:
                    # nếu có cursor cũ thì gắn lại
                    if reload_cursor:
                        new_form = update_vars_for_next_cursor(new_form, reload_cursor, vars_template)
                    form = new_form
                    no_progress_rounds = 0
                    cursor_stall_rounds = 0
                    print(f"[PAGE#{page}] reload OK (attempt {attempt}) → tiếp tục từ cursor={str(reload_cursor)[:28] if reload_cursor else None}")
                    reloaded_ok = True
                    break
                time.sleep(random.uniform(1.0, 2.0))

            if not reloaded_ok:
                print(f"[PAGE#{page}] reload thất bại → dừng pagination.")
                break

            # đã reload xong thì quay lại vòng while để fetch trang kế tiếp
            continue

        if new_cursor:
            form = update_vars_for_next_cursor(form, new_cursor, vars_template)
            cursor_for_reload = new_cursor

        if page_limit and page >= page_limit:
            break

        time.sleep(random.uniform(0.7, 1.4))

    return total_new, min_created, bool(has_next)

# =========================
# Head-probe: bám đầu feed hiện tại
# =========================
def probe_head(driver, base_form, vars_template, k=5):
    try:
        v = json.loads(base_form.get("variables","{}"))
    except: 
        v = {}
    v = strip_cursors_from_vars(merge_vars(v, vars_template))
    form0 = dict(base_form); form0["variables"] = json.dumps(v, separators=(",",":"))

    txt = js_fetch_in_page(driver, form0, extra_headers={
        "Cache-Control": "no-cache", "Pragma": "no-cache",
    })
    obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
    if not obj: 
        return None, [], None

    page_posts = []
    collect_post_summaries(obj, page_posts)
    page_posts = coalesce_posts(filter_only_feed_posts(page_posts))
    top = page_posts[:k]

    cursors = deep_collect_cursors(obj)
    head_cursor = cursors[0][1] if cursors else None

    if head_cursor:
        form1 = update_vars_for_next_cursor(form0, head_cursor, vars_template)
    else:
        form1 = form0

    return form1, top, head_cursor

# =========================
# Runner: THUẦN CURSOR
# =========================
def strip_cursors_from_form_on_form(form, vars_template=None):
    """
    Tạo bản copy của form, loại bỏ tất cả các trường cursor/endCursor/after... trong phần variables.
    Giữ lại các biến khác (ví dụ: id, count, scale, viewerID...).
    """
    import json

    # Lấy ra biến variables từ form
    try:
        v = json.loads(form.get("variables", "{}"))
    except Exception:
        v = {}

    # Các key cần xoá
    CURSOR_KEYS = {
        "cursor", "after", "endCursor", "afterCursor",
        "feedAfterCursor", "before", "beforeCursor"
    }

    # Hàm đệ quy loại bỏ key trong dict con
    def _strip(o):
        if isinstance(o, dict):
            new = {}
            for k, val in o.items():
                if k in CURSOR_KEYS:
                    continue
                new[k] = _strip(val)
            return new
        elif isinstance(o, list):
            return [_strip(x) for x in o]
        else:
            return o

    cleaned = _strip(v)

    # Merge lại template (nếu có)
    if vars_template:
        try:
            cleaned = merge_vars(cleaned, vars_template)
        except Exception:
            pass

    # Trả lại form mới (copy)
    new_form = dict(form)
    new_form["variables"] = json.dumps(cleaned, separators=(",", ":"))
    return new_form

def run_cursor_only(d, form, vars_template, seen_ids, page_limit=None, resume=False):
    """
    Cursor-only paging. Nếu resume=True => KHÔNG boot ở head, đi thẳng từ checkpoint cursor.
    """
    total = 0

    # === (A) HEAD-BOOT CHỈ KHI resume=False ===
    if not resume:
        fresh_head = 0
        try:
            txt = js_fetch_in_page(d, strip_cursors_from_form_on_form(form, vars_template(form)), {}, 15000)  # pseudo
            obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
            buf = []
            collect_post_summaries(obj, buf)
            buf = coalesce_posts(filter_only_feed_posts(buf))
            written = []
            for p in buf:
                pk = _best_primary_key(p)
                if pk and pk not in seen_ids:
                    written.append(p)
                    for k in _all_join_keys(p): seen_ids.add(k)
            append_ndjson(written)
            fresh_head = len(written)
            if fresh_head:
                print(f"[HEAD] grabbed {fresh_head} fresh at head")
        except Exception:
            pass 
        total += fresh_head
    else:
        print("[RESUME] Skip head-probe; continue strictly from checkpoint cursor.")

    # === (B) CHẠY PAGINATION THEO CURSOR ===
    # Không set time window; để [-inf, +inf]
    add, _, _ = paginate_window(
        d, form, vars_template, seen_ids,
        t_from=None, t_to=None,
        page_limit=page_limit
    )
    total += add
    return total
