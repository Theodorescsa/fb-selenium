# -*- coding: utf-8 -*-
# Facebook Group GraphQL crawler — V2 (robust cursor extract + rock-solid checkpoint)
# ⚠️ Chỉ crawl nơi bạn có quyền. Tôn trọng ToS.

import json, re, time, random, urllib.parse, subprocess, os, sys, datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# =========================
# CONFIG — nhớ sửa GROUP_URL
# =========================
CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"
PROFILE_NAME  = "Profile 5"
REMOTE_PORT   = 9222

GROUP_URL     = "https://web.facebook.com/groups/laptrinhvienit"  # <— ĐỔI Ở ĐÂY
KEEP_LAST     = 350
OUT_NDJSON    = "posts_all.ndjson"
RAW_DUMPS_DIR = "raw_dumps"

# Checkpoint tuyệt đối như bạn yêu cầu
CHECKPOINT    = r"E:\NCS\fb-selenium\checkpoint.json"

os.makedirs(RAW_DUMPS_DIR, exist_ok=True)

# =========================
# Boot
# =========================
def start_driver(chrome_path, user_data_dir, profile_name, port=REMOTE_PORT):
    # Mở Chrome ở chế độ headless + remote debugging
    subprocess.Popen([
        chrome_path,
        f'--remote-debugging-port={port}',
        f'--user-data-dir={user_data_dir}',
        f'--profile-directory={profile_name}',
        '--headless=new',  # 👈 thêm dấu phẩy bị thiếu ở đây
        '--no-sandbox',
        '--disable-gpu',
        '--disable-dev-shm-usage',
        '--window-size=1920,1080',
        '--disable-extensions',
        '--disable-blink-features=AutomationControlled',
    ])

    # Chờ Chrome khởi động
    time.sleep(2)

    # Kết nối với Chrome qua debuggerAddress
    options = Options()
    options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")

    driver = webdriver.Chrome(options=options)
    return driver
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
            if req and matcher(req):
                return (cur, req)
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
    # Friendly name ưu tiên
    if "fb_api_req_friendly_name=" in body:
        if re.search(r"(?:GroupComet|CometGroup|GroupsComet).*(?:Feed|Stories).*Pagination", body, re.I):
            return True
    # Fallback: variables chứa group + after/cursor
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
# Cursor extract (rất "tham lam")
# =========================
CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}

def deep_collect_cursors(obj):
    """Trả về list các cursor khả dĩ, ưu tiên page_info.end_cursor,
    nếu không có thì edges[-1].cursor, nếu không vẫn quét toàn bộ key 'cursor'."""
    found = []

    def dive(o):
        if isinstance(o, dict):
            # 1) page_info / pageInfo
            pi = o.get("page_info") or o.get("pageInfo")
            if isinstance(pi, dict):
                ec = pi.get("end_cursor") or pi.get("endCursor")
                if isinstance(ec, str) and len(ec) >= 10:
                    found.append(("page_info.end_cursor", ec))
                hn = pi.get("has_next_page") or pi.get("hasNextPage")
                # ghi nhận hasNext nếu cần (không dùng ở đây)

            # 2) edges[-1].cursor
            edges = o.get("edges")
            if isinstance(edges, list) and edges:
                last = edges[-1]
                if isinstance(last, dict):
                    cur = last.get("cursor")
                    if isinstance(cur, str) and len(cur) >= 10:
                        found.append(("edges[-1].cursor", cur))

            # 3) keys tên cursor khác
            for k, v in o.items():
                if k in CURSOR_KEYS and isinstance(v, str) and len(v) >= 10:
                    found.append((k, v))
                dive(v)
        elif isinstance(o, list):
            for v in o: dive(v)

    dive(obj)
    # Ưu tiên theo nguồn
    priority = {
        "page_info.end_cursor": 3,
        "end_cursor": 3, "endCursor": 3,
        "edges[-1].cursor": 2,
    }
    found.sort(key=lambda kv: (priority.get(kv[0], 1), len(kv[1])), reverse=True)
    # Trả ra unique theo giá trị
    uniq = []
    seenv = set()
    for k, v in found:
        if v not in seenv:
            uniq.append((k, v))
            seenv.add(v)
    return uniq  # [(source, cursor), ...]

def deep_find_has_next(obj):
    """Tìm has_next_page/hasNextPage nếu có."""
    res = []

    def dive(o):
        if isinstance(o, dict):
            pi = o.get("page_info") or o.get("pageInfo")
            if isinstance(pi, dict):
                hn = pi.get("has_next_page")
                if hn is None: hn = pi.get("hasNextPage")
                if isinstance(hn, bool):
                    res.append(hn)
            for v in o.values(): dive(v)
        elif isinstance(o, list):
            for v in o: dive(v)

    dive(obj)
    # ưu tiên True nếu có bất kỳ True
    if any(res): return True
    if res and not any(res): return False
    return None  # không chắc

# =========================
# Story collector (giữ nguyên logic nới lỏng)
# =========================
POST_URL_RE = re.compile(r"https?://(?:web\.)?facebook\.com/groups/[^/]+/(?:permalink|posts)/\d+/?$", re.I)

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

def collect_post_summaries(obj, out):
    if isinstance(obj, dict):
        if _looks_like_group_post(obj):
            post_id = obj.get("id") or obj.get("post_id")
            creation = obj.get("creation_time")
            text = _get_text_from_node(obj)
            url = obj.get("wwwURL") or obj.get("url")
            out.append({"id":post_id, "creation_time":creation, "text":text, "url":url})
        for v in obj.values(): collect_post_summaries(v, out)
    elif isinstance(obj, list):
        for v in obj: collect_post_summaries(v, out)

def filter_only_group_posts(items):
    keep = []
    for it in items:
        url = (it.get("url") or "").strip()
        pid = (it.get("id") or "").strip()
        if POST_URL_RE.match(url) or (isinstance(pid, str) and pid.startswith("Uzpf")) or it.get("post_id"):
            keep.append(it)
    return keep

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
# Update variables for next cursor
# =========================
def update_vars_for_next_cursor(form: dict, next_cursor: str):
    try:
        v = json.loads(form.get("variables", "{}"))
    except Exception:
        v = {}
    changed = False
    if "cursor" in v:
        v["cursor"] = next_cursor; changed = True
    if not changed:
        for key in ["after","endCursor","afterCursor","feedAfterCursor"]:
            if key in v:
                v[key] = next_cursor; changed = True
    if not changed:
        v["cursor"] = next_cursor
    # tối thiểu 10 để ít miss
    if "count" in v and isinstance(v["count"], int):
        v["count"] = max(v["count"], 10)
    form["variables"] = json.dumps(v, separators=(",", ":"))
    return form

# =========================
# Checkpoint / Output
# =========================
def load_checkpoint():
    if not os.path.exists(CHECKPOINT):
        return {"cursor": None, "seen_ids": [], "last_doc_id": None, "last_query_name": None, "ts": None}
    try:
        with open(CHECKPOINT, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"cursor": None, "seen_ids": [], "last_doc_id": None, "last_query_name": None, "ts": None}

def save_checkpoint(cursor, seen_ids, last_doc_id=None, last_query_name=None):
    data = {
        "cursor": cursor,
        "seen_ids": list(seen_ids)[:200000],
        "last_doc_id": last_doc_id,
        "last_query_name": last_query_name,
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

    nxt = wait_next_req(d, 0, is_group_feed_req, timeout=25, poll=0.25)
    if not nxt:
        raise RuntimeError("Không bắt được request feed của group. Hãy cuộn thêm / kiểm tra quyền vào group.")
    idx, first_req = nxt
    form = parse_form(first_req.get("body",""))
    # Lấy các metadata để log
    friendly = urllib.parse.parse_qs(first_req.get("body","")).get("fb_api_req_friendly_name", [""])[0]
    last_doc_id = form.get("doc_id")

    state = load_checkpoint()
    seen_ids = set(state.get("seen_ids", []))
    cursor   = state.get("cursor")
    total_written = 0

    # -------- Trang đầu (từ response hook)
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
    # fallback: nếu has_next None → suy diễn từ có cursor
    if has_next is None: has_next = bool(cursors)

    # chọn cursor tốt nhất (nếu có)
    end_cursor = cursors[0][1] if cursors else None
    if end_cursor: cursor = end_cursor

    print(f"[DEBUG] page1 stories={len(page_posts)} | cursors={len(cursors)} | has_next={has_next} | pick={str(end_cursor)[:24] if end_cursor else None}")
    print(f"[DEBUG] doc_id={form.get('doc_id')} | friendly={friendly}")

    fresh = [p for p in page_posts if p.get("id") and p["id"] not in seen_ids]
    append_ndjson(fresh)
    for p in fresh:
        if p.get("id"): seen_ids.add(p["id"])
    total_written += len(fresh)
    print(f"[PAGE#1] got {len(page_posts)} (new {len(fresh)}), next={bool(has_next)}")

    save_checkpoint(cursor, seen_ids, last_doc_id=form.get('doc_id'), last_query_name=friendly)

    # -------- Nếu có checkpoint.cursor (run lại), ép variables ngay trang sau
    page = 1
    no_progress_rounds = 0
    while has_next:
        page += 1
        if cursor:
            form = update_vars_for_next_cursor(form, cursor)
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
        if has_next is None: has_next = bool(cursors)

        new_cursor = cursors[0][1] if cursors else None
        if new_cursor: cursor = new_cursor

        fresh = [p for p in page_posts if p.get("id") and p["id"] not in seen_ids]
        if fresh:
            append_ndjson(fresh)
            for p in fresh:
                if p.get("id"): seen_ids.add(p["id"])
            total_written += len(fresh)
            no_progress_rounds = 0
        else:
            no_progress_rounds += 1

        print(f"[PAGE#{page}] got {len(page_posts)} (new {len(fresh)}), total={total_written}, next={bool(has_next)} | cursor={str(cursor)[:24] if cursor else None}")

        save_checkpoint(cursor, seen_ids, last_doc_id=form.get('doc_id'), last_query_name=friendly)

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
