# -*- coding: utf-8 -*-
# Facebook Group GraphQL crawler — V2.2 (resume-first)
# - Nếu checkpoint có cursor: NHẢY THẲNG bằng cursor + vars_template (KHÔNG parse page1)
# - Nếu không có cursor: chạy như thường (parse page1 rồi paginate)
# - Checkpoint: cursor + vars_template + seen_ids + doc_id + friendly + ts
# ⚠️ Chỉ crawl nơi bạn có quyền. Tôn trọng ToS.

import json, re, time, random, urllib.parse, subprocess, os, sys, datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from get_info import *
# =========================
# CONFIG — nhớ sửa GROUP_URL
# =========================
CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"
PROFILE_NAME  = "Profile 5"
REMOTE_PORT   = 9222

# GROUP_URL     = "https://web.facebook.com/groups/laptrinhvienit"  # <— ĐỔI Ở ĐÂY
GROUP_URL     = "https://www.facebook.com/thoibao.de"  # <— ĐỔI Ở ĐÂY
KEEP_LAST     = 350
OUT_NDJSON    = "posts_all.ndjson"
RAW_DUMPS_DIR = "raw_dumps"

CHECKPOINT    = r"E:\NCS\fb-selenium\checkpoint.json"

os.makedirs(RAW_DUMPS_DIR, exist_ok=True)

# =========================
# Boot
# =========================
def start_driver(chrome_path, user_data_dir, profile_name, port=REMOTE_PORT):
    subprocess.Popen([
        chrome_path,
        f'--remote-debugging-port={port}',
        f'--user-data-dir={user_data_dir}',
        f'--profile-directory={profile_name}'
    ])
    time.sleep(2)
    options = Options()
    options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")
    return webdriver.Chrome(options=options)

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
        return (typeof h==='object')?h:{};
      }catch(e){return {}}}
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
          try{ rec.responseText = await res.clone().text(); }
          catch(e){ rec.responseText = null; }
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

# =========================
# Buffer helpers
# =========================
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
# Matching + parsing
# =========================
def parse_form(body_str):
    qs = urllib.parse.parse_qs(body_str, keep_blank_values=True)
    return {k:(v[0] if isinstance(v, list) else v) for k,v in qs.items()}

def is_group_feed_req(rec):
    if "/api/graphql/" not in (rec.get("url") or ""): return False
    if (rec.get("method") or "").upper() != "POST": return False
    body = rec.get("body") or ""
    if "fb_api_req_friendly_name=" in body:
        if re.search(r"(?:GroupComet|CometGroup|GroupsComet).*(?:Feed|Stories).*Pagination", body, re.I):
            return True
    try:
        v = parse_form(body).get("variables","")
        vj = json.loads(urllib.parse.unquote_plus(v))
        if any(k in vj for k in ["groupID","groupIDV2","id"]) and any(
            k in vj for k in ["after","cursor","endCursor","afterCursor","feedAfterCursor"]
        ):
            return True
    except:
        pass
    return False

def _strip_xssi_prefix(s: str) -> str:
    if not s: return s
    s2 = s.lstrip()
    s2 = re.sub(r'^\s*for\s*\(\s*;\s*;\s*\)\s*;\s*', '', s2)
    s2 = re.sub(r"^\s*\)\]\}'\s*", '', s2)
    return s2

def iter_json_values(s: str):
    dec = json.JSONDecoder()
    i, n = 0, len(s)
    while i < n:
        m = re.search(r'\S', s[i:])
        if not m: break
        j = i + m.start()
        try:
            obj, k = dec.raw_decode(s, j); yield obj; i = k
        except json.JSONDecodeError:
            chunk = _strip_xssi_prefix(s[j:])
            if chunk == s[j:]: break
            try:
                obj, k_rel = dec.raw_decode(chunk, 0); yield obj; i = j + k_rel
            except json.JSONDecodeError:
                break

def choose_best_graphql_obj(objs):
    objs = list(objs)
    if not objs: return None
    with_data = [o for o in objs if isinstance(o, dict) and 'data' in o]
    pick = with_data or objs
    return max(pick, key=lambda o: len(json.dumps(o, ensure_ascii=False)))

# =========================
# Cursor extract (tham lam)
# =========================
CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}

def deep_collect_cursors(obj):
    found = []
    def dive(o):
        if isinstance(o, dict):
            pi = o.get("page_info") or o.get("pageInfo")
            if isinstance(pi, dict):
                ec = pi.get("end_cursor") or pi.get("endCursor")
                if isinstance(ec, str) and len(ec) >= 10:
                    found.append(("page_info.end_cursor", ec))
            edges = o.get("edges")
            if isinstance(edges, list) and edges:
                last = edges[-1]
                if isinstance(last, dict):
                    cur = last.get("cursor")
                    if isinstance(cur, str) and len(cur) >= 10:
                        found.append(("edges[-1].cursor", cur))
            for k, v in o.items():
                if k in CURSOR_KEYS and isinstance(v, str) and len(v) >= 10:
                    found.append((k, v))
                dive(v)
        elif isinstance(o, list):
            for v in o: dive(v)
    dive(obj)
    priority = {"page_info.end_cursor": 3, "end_cursor": 3, "endCursor": 3, "edges[-1].cursor": 2}
    found.sort(key=lambda kv: (priority.get(kv[0], 1), len(kv[1])), reverse=True)
    uniq, seenv = [], set()
    for k, v in found:
        if v not in seenv:
            uniq.append((k, v)); seenv.add(v)
    return uniq

def deep_find_has_next(obj):
    res = []
    def dive(o):
        if isinstance(o, dict):
            pi = o.get("page_info") or o.get("pageInfo")
            if isinstance(pi, dict):
                hn = pi.get("has_next_page");  hn = pi.get("hasNextPage") if hn is None else hn
                if isinstance(hn, bool): res.append(hn)
            for v in o.values(): dive(v)
        elif isinstance(o, list):
            for v in o: dive(v)
    dive(obj)
    if any(res): return True
    if res and not any(res): return False
    return None

# =========================
# Story collector + rid
# =========================
POST_URL_RE = re.compile(r"https?://(?:web\.)?facebook\.com/groups/[^/]+/(?:permalink|posts)/(\d+)/?$", re.I)

def _get_text_from_node(n: dict):
    if isinstance(n.get("message"), dict):
        t = n["message"].get("text")
        if t:
            return t
    if isinstance(n.get("body"), dict):
        t = n["body"].get("text")
        if t:
            return t
    return None
def _is_story_node(n: dict) -> bool:
    if n.get("__typename") == "Story": return True
    if n.get("__isFeedUnit") == "Story": return True
    if "post_id" in n or "comet_sections" in n: return True
    return False

def _looks_like_group_post(n: dict) -> bool:
    if not _is_story_node(n): return False
    url = n.get("wwwURL") or n.get("url") or ""
    pid = n.get("id") or ""
    if POST_URL_RE.match(url): return True
    if (isinstance(pid, str) and pid.startswith("Uzpf")) or n.get("post_id"): return True
    return False

def _extract_url_digits(url: str):
    if not url: return None
    m = POST_URL_RE.match(url)
    return m.group(1) if m else None

# === replace collect_post_summaries bằng bản này ===
def collect_post_summaries(obj, out, group_url=GROUP_URL):
    if isinstance(obj, dict):
        if _looks_like_group_post(obj):
            post_id_api = obj.get("post_id")
            fb_id      = obj.get("id")
            url        = obj.get("wwwURL") or obj.get("url")
            url_digits = _extract_url_digits(url)
            rid        = post_id_api or url_digits or fb_id

            # author & type label
            author_id, author_name, author_link, avatar, type_label = extract_author(obj)

            text = _get_text_from_node(obj)

            image_urls, video_urls = extract_media(obj)
            counts = extract_reactions_and_counts(obj)
            created = extract_created_time(obj)
            is_share, link_share, type_share, origin_id = extract_share_flags(obj)
            hashtags = extract_hashtags(text)

            # source_id (group id/slug best-effort)
            source_id = None
            _k, _v = deep_get_first(obj, {"group_id", "groupID", "groupIDV2"})
            if _v: source_id = _v
            if not source_id:
                try:
                    slug = re.search(r"/groups/([^/?#]+)", group_url).group(1)
                    source_id = slug
                except:
                    pass

            out.append({
                "id": fb_id,                 # <- id gốc nếu có
                "rid": rid,                  # id dùng để dedupe
                "type": type_label,          # "facebook page/profile/group"
                "link": url,
                "author_id": author_id,
                "author": author_name,
                "author_link": author_link,
                "avatar": avatar,
                "created_time": created,     # giữ epoch như mẫu
                "content": text,
                "image_url": image_urls,     # mảng
                "like": counts["like"],
                "comment": counts["comment"],
                "haha": counts["haha"],
                "wow": counts["wow"],
                "sad": counts["sad"],
                "love": counts["love"],
                "angry": counts["angry"],
                "care": counts["care"],
                "share": counts["share"],
                "hashtag": hashtags,         # mảng, lowercase
                "video": video_urls,         # mảng
                "source_id": source_id,
                "is_share": is_share,
                "link_share": link_share,
                "type_share": type_share,
            })
        for v in obj.values():
            collect_post_summaries(v, out, group_url)
    elif isinstance(obj, list):
        for v in obj:
            collect_post_summaries(v, out, group_url)


def filter_only_group_posts(items):
    keep = []
    for it in items:
        url = (it.get("url") or "").strip()
        fb_id = (it.get("id") or "").strip()
        if POST_URL_RE.match(url) or (isinstance(fb_id, str) and fb_id.startswith("Uzpf")) or it.get("post_id"):
            keep.append(it)
    return keep

# =========================
# Variables template helpers
# =========================
def get_vars_from_form(form_dict):
    try:
        return json.loads(form_dict.get("variables", "{}")) if form_dict else {}
    except:
        return {}

def make_vars_template(vars_dict):
    if not isinstance(vars_dict, dict): return {}
    t = dict(vars_dict)
    for k in list(t.keys()):
        if k in CURSOR_KEYS: del t[k]
    return t

def merge_vars(base_vars, template_vars):
    if not isinstance(base_vars, dict): base_vars = {}
    if not isinstance(template_vars, dict): template_vars = {}
    out = dict(base_vars)
    for k, v in template_vars.items():
        if k in CURSOR_KEYS: continue
        out[k] = v
    return out

# =========================
# JS fetch with current page cookies
# =========================
def js_fetch_in_page(driver, form_dict, extra_headers=None):
    script = """
    const url = "/api/graphql/";
    const form = arguments[0];
    const extra = arguments[1] || {};
    const headers = Object.assign({"Content-Type":"application/x-www-form-urlencoded"}, extra);
    const body = new URLSearchParams(form).toString();
    return fetch(url, {method:"POST", headers, body, credentials:"include"}).then(r=>r.text());
    """
    return driver.execute_script(script, form_dict, extra_headers or {})

# =========================
# Update variables for next cursor (template-aware)
# =========================
def update_vars_for_next_cursor(form: dict, next_cursor: str, vars_template: dict = None):
    try:
        base = json.loads(form.get("variables", "{}"))
    except Exception:
        base = {}
    if vars_template:
        base = merge_vars(base, vars_template)
    changed = False
    if "cursor" in base:
        base["cursor"] = next_cursor; changed = True
    if not changed:
        for key in ["after","endCursor","afterCursor","feedAfterCursor"]:
            if key in base:
                base[key] = next_cursor; changed = True
    if not changed:
        base["cursor"] = next_cursor
    if "count" in base and isinstance(base["count"], int):
        base["count"] = max(base["count"], 10)
    form["variables"] = json.dumps(base, separators=(",", ":"))
    return form

# =========================
# Checkpoint / Output
# =========================
def load_checkpoint():
    if not os.path.exists(CHECKPOINT):
        return {"cursor": None, "seen_ids": [], "last_doc_id": None, "last_query_name": None, "vars_template": {}, "ts": None}
    try:
        with open(CHECKPOINT, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"cursor": None, "seen_ids": [], "last_doc_id": None, "last_query_name": None, "vars_template": {}, "ts": None}

def save_checkpoint(cursor, seen_ids, last_doc_id=None, last_query_name=None, vars_template=None):
    data = {
        "cursor": cursor,
        "seen_ids": list(seen_ids)[:200000],
        "last_doc_id": last_doc_id,
        "last_query_name": last_query_name,
        "vars_template": vars_template or {},
        "ts": datetime.datetime.now().isoformat(timespec="seconds")
    }
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def append_ndjson(items):
    if not items: return
    with open(OUT_NDJSON, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

# =========================
# seen_ids migration (đảm bảo hợp với 'rid')
# =========================
def normalize_seen_ids(seen_ids):
    # Ở bản cũ bạn có thể đã lưu 'id' thuần; bản mới dùng rid (post_id | urlDigits | id).
    # Không có URL để suy ra digits, nên giữ nguyên chuỗi — Uzpf* vẫn match rid.
    return set(seen_ids or [])

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    d = start_driver(CHROME_PATH, USER_DATA_DIR, PROFILE_NAME, port=REMOTE_PORT)
    install_early_hook(d, keep_last=KEEP_LAST)

    d.get(GROUP_URL)
    time.sleep(1.2)
    for _ in range(6):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
        time.sleep(0.6)

    # Bắt 1 request mới để lấy token/doc_id/variables mới (NHƯNG có thể bỏ qua parse page1)
    nxt = wait_next_req(d, 0, is_group_feed_req, timeout=25, poll=0.25)
    if not nxt:
        raise RuntimeError("Không bắt được request feed của group. Hãy cuộn thêm / kiểm tra quyền vào group.")
    idx, first_req = nxt
    form = parse_form(first_req.get("body", ""))
    friendly    = urllib.parse.parse_qs(first_req.get("body", "")).get("fb_api_req_friendly_name", [""])[0]
    last_doc_id = form.get("doc_id")
    vars_now    = get_vars_from_form(form)
    template_now= make_vars_template(vars_now)

    # Load checkpoint
    state = load_checkpoint()
    seen_ids      = normalize_seen_ids(state.get("seen_ids", []))
    cursor        = state.get("cursor")
    vars_template = state.get("vars_template") or {}
    total_written = 0

    # Chọn template hiệu lực
    effective_template = vars_template or template_now

    # ======== NHÁY THẲNG NẾU CÓ CURSOR ========
    if cursor:
        print(f"[RESUME] Using saved cursor → jump directly. cursor={str(cursor)[:24]}..., friendly={friendly}")
        has_next = True
        page = 0
    else:
        # ======== CHẠY TRUYỀN THỐNG (CHƯA CÓ CURSOR) ========
        raw0 = first_req.get("responseText") or ""
        obj0 = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(raw0)))
        if not obj0:
            open(os.path.join(RAW_DUMPS_DIR, "page1_raw.txt"), "w", encoding="utf-8").write(raw0)
            raise RuntimeError("Không parse được trang đầu; đã dump raw_dumps/page1_raw.txt")

        page_posts = []
        collect_post_summaries(obj0, page_posts)
        page_posts = filter_only_group_posts(page_posts)

        cursors = deep_collect_cursors(obj0)
        has_next = deep_find_has_next(obj0)
        if has_next is None:
            has_next = bool(cursors)

        end_cursor = cursors[0][1] if cursors else None
        if end_cursor:
            cursor = end_cursor

        print(f"[DEBUG] page1 posts={len(page_posts)} | cursors={len(cursors)} | has_next={has_next} | pick={str(end_cursor)[:24] if end_cursor else None}")
        print(f"[DEBUG] doc_id={form.get('doc_id')} | friendly={friendly}")

        fresh = [p for p in page_posts if p.get("rid") and p["rid"] not in seen_ids]
        append_ndjson(fresh)
        for p in fresh:
            if p.get("rid"):
                seen_ids.add(p["rid"])
        total_written += len(fresh)
        print(f"[PAGE#1] got {len(page_posts)} (new {len(fresh)}), next={bool(has_next)}")

        save_checkpoint(cursor, seen_ids, last_doc_id=form.get('doc_id'), last_query_name=friendly, vars_template=template_now)
        page = 1

    # ======== PAGINATE (resume-first) ========
    no_progress_rounds = 0
    while has_next:
        page += 1
        # if page == 20:
        #     print(f"[PAGE#{page}] reached page limit 20, stop.")
        #     break
        if cursor:
            form = update_vars_for_next_cursor(form, cursor, vars_template=effective_template)

        txt = js_fetch_in_page(d, form, extra_headers={})
        obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
        if not obj:
            open(os.path.join(RAW_DUMPS_DIR, f"page{page}_raw.txt"), "w", encoding="utf-8").write(txt)
            print(f"[PAGE#{page}] parse fail → dumped raw, break.")
            break

        page_posts = []
        collect_post_summaries(obj, page_posts)
        page_posts = filter_only_group_posts(page_posts)

        cursors = deep_collect_cursors(obj)
        has_next = deep_find_has_next(obj)
        if has_next is None:
            has_next = bool(cursors)

        new_cursor = cursors[0][1] if cursors else None
        if new_cursor:
            cursor = new_cursor

        fresh = [p for p in page_posts if p.get("rid") and p["rid"] not in seen_ids]
        if fresh:
            append_ndjson(fresh)
            for p in fresh:
                if p.get("rid"):
                    seen_ids.add(p["rid"])
            total_written += len(fresh)
            no_progress_rounds = 0
        else:
            no_progress_rounds += 1

        print(f"[PAGE#{page}] got {len(page_posts)} (new {len(fresh)}), total={total_written}, next={bool(has_next)} | cursor={str(cursor)[:24] if cursor else None}")

        save_checkpoint(cursor, seen_ids, last_doc_id=form.get('doc_id'), last_query_name=friendly, vars_template=effective_template)

        if no_progress_rounds >= 3:
            print(f"[PAGE#{page}] no new items 3 rounds → nudge UI + backoff")
            for _ in range(2):
                d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.8));")
                time.sleep(0.5)
            time.sleep(random.uniform(2.0, 4.0))
            no_progress_rounds = 0

        time.sleep(random.uniform(0.7, 1.5))

    print(f"[DONE] wrote {total_written} posts → {OUT_NDJSON}")
    print(f"[INFO] resume later with checkpoint: {CHECKPOINT}")

    # ======== AFTER LOOP: consolidate (story-only) + dedupe + export to Excel ========
    try:
        rows = []
        if os.path.exists(OUT_NDJSON):
            with open(OUT_NDJSON, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        rows.append(json.loads(line))
                    except:
                        pass
        if rows:
            df = pd.DataFrame(rows)

            # 1) story-only
            df = df[df.get("type").astype(str).str.lower().eq("story")].copy()

            # 2) dedupe (giữ bản có tổng tương tác cao nhất, tie-break created_time mới hơn)
            import hashlib

            def _norm_str(x):
                s = ("" if x is None else str(x)).strip().lower()
                return s

            def _safe_int(x):
                try:
                    return int(x)
                except:
                    return 0

            def _hash_content(text):
                try:
                    b = (text or "").encode("utf-8")
                    return hashlib.md5(b).hexdigest()[:16]
                except:
                    return "0"*16

            link_key = df.get("link").map(_norm_str) if "link" in df.columns else ""
            rid_key  = df.get("rid").map(_norm_str)  if "rid"  in df.columns else ""
            aid_col  = df.get("author_id") if "author_id" in df.columns else ""
            ct_col   = df.get("created_time") if "created_time" in df.columns else ""
            txt_col  = df.get("content") if "content" in df.columns else ""

            fallback_key = (
                aid_col.map(lambda x: str(x) if x is not None else "") + "|" +
                ct_col.map(_safe_int).map(str) + "|" +
                txt_col.map(_hash_content)
            )

            df["__key"] = link_key
            df.loc[df["__key"] == "", "__key"] = rid_key
            df.loc[df["__key"] == "", "__key"] = fallback_key

            for col in ["like","love","haha","wow","sad","angry","care","comment","share"]:
                if col not in df.columns:
                    df[col] = 0
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

            df["__score"] = (
                df["like"] + df["love"] + df["haha"] + df["wow"] +
                df["sad"] + df["angry"] + df["care"] + df["comment"] + df["share"]
            )
            df["__ct"] = pd.to_numeric(df.get("created_time"), errors="coerce").fillna(0).astype(int)

            df = (
                df.sort_values(["__key", "__score", "__ct"], ascending=[True, False, False])
                  .drop_duplicates(subset=["__key"], keep="first")
            )
            df.drop(columns=["__key","__score","__ct"], inplace=True, errors="ignore")

            # 3) Chuẩn cột & xuất Excel
            want_cols = [
                "id","type","link","author_id","author","author_link","avatar",
                "created_time","content","image_url",
                "like","comment","haha","wow","sad","love","angry","care","share",
                "hashtag","video","source_id","is_share","link_share","type_share"
            ]
            for c in want_cols:
                if c not in df.columns:
                    df[c] = None

            if "rid" in df.columns:
                df["id"] = df["id"].fillna(df["rid"])

            def _as_json_list(x):
                if x is None or (isinstance(x, float) and pd.isna(x)):
                    return "[]"
                if isinstance(x, (list, tuple)):
                    return json.dumps(list(x), ensure_ascii=False)
                # nếu là chuỗi đã JSON → giữ nguyên nếu parse ra list được
                try:
                    obj = json.loads(x)
                    if isinstance(obj, (list, tuple)):
                        return json.dumps(list(obj), ensure_ascii=False)
                except:
                    pass
                if isinstance(x, str) and x.strip():
                    return json.dumps([x], ensure_ascii=False)
                return "[]"

            for col in ("image_url", "video", "hashtag"):
                df[col] = df[col].map(_as_json_list)

            def _as_int01(v):
                if isinstance(v, bool):
                    return 1 if v else 0
                if isinstance(v, (int, float)):
                    return int(v)
                return 0
            df["is_share"] = df["is_share"].map(_as_int01)

            df = df[want_cols]

            out_xlsx = Path(OUT_NDJSON).with_suffix(".xlsx")
            df.to_excel(out_xlsx, index=False)
            print(f"[EXPORT] Wrote Excel → {out_xlsx}")
        else:
            print("[EXPORT] NDJSON rỗng, bỏ qua xuất Excel.")
    except Exception as e:
        print(f"[EXPORT] Lỗi xuất Excel: {e}")
