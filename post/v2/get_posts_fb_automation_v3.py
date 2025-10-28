# -*- coding: utf-8 -*-
"""
Facebook GraphQL Feed Crawler — resume-first + time slicing + year-by-year + multi-doc fallback
- Attach Chrome qua remote debugging (xài profile thật của bạn).
- Hook sớm /api/graphql để "hứng" request và response.
- Resume bằng cursor + seen_ids trong checkpoint.
- Khi tiến độ chậm / next=False, thử soft-refetch (không đụng UI) hoặc hard-reload form.
- Hỗ trợ beforeTime/afterTime (epoch giây) để "đào lùi" theo mốc thời gian.
- Runner year-by-year: cắt theo năm để vét nhiều lịch sử hơn.

⚠️ Chỉ crawl nội dung bạn có quyền truy cập. Tôn trọng Điều khoản sử dụng của nền tảng.
"""

import os, re, json, time, random, datetime, urllib.parse, subprocess, socket
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlunparse

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# =========================
# CONFIG — chỉnh theo máy bạn
# =========================
CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"
PROFILE_NAME  = "Profile 5"
REMOTE_PORT   = 9222

# Page/Group/Profile gốc bạn muốn crawl
GROUP_URL     = "https://www.facebook.com/thoibao.de"

# (Optional) Nếu muốn nạp login thủ công từ file, set path 2 hằng dưới; nếu không, để None:
COOKIES_PATH       = None   # ví dụ: r".\cookies.json"
LOCALSTORAGE_PATH  = None   # ví dụ: r".\localstorage.json"

# Lưu trữ
KEEP_LAST     = 350
OUT_NDJSON    = "posts_all_v3.ndjson"
RAW_DUMPS_DIR = "raw_dumps_v3"
CHECKPOINT    = "checkpoint_v3.json"

# Giới hạn “đào lùi” theo năm (bao nhiêu năm về trước)
STOP_AT_YEAR  = 2015  # đổi tùy ý

os.makedirs(RAW_DUMPS_DIR, exist_ok=True)

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
        return (typeof h==='object')?h:{};}catch(e){return {}}
      }
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
          try{ rec.responseText = await res.clone().text(); }catch(e){ rec.responseText = null; }
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
# Chrome attach qua remote debugging
# =========================
def _wait_port(host: str, port: int, timeout: float = 20.0, poll: float = 0.1) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except Exception:
            time.sleep(poll)
    return False

def start_driver(chrome_path, user_data_dir, profile_name, port=REMOTE_PORT, headless=True):
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
    ]
    if headless:
        args += ['--headless=new', '--disable-gpu', '--no-sandbox',
                 '--disable-dev-shm-usage', '--window-size=1920,1080']

    proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ok = _wait_port('127.0.0.1', port, timeout=20.0)
    if not ok and headless:
        proc.kill()
        time.sleep(0.5)
        # thử non-headless
        args = [a for a in args if not a.startswith('--headless')]
        args += ['--window-size=1920,1080']
        proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ok = _wait_port('127.0.0.1', port, timeout=20.0)
    if not ok:
        proc.kill()
        raise RuntimeError(f"Không mở được remote debugging port {port}")

    options = Options()
    options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")
    driver = webdriver.Chrome(options=options)
    return driver

# =========================
# (Optional) bootstrap_auth — nạp cookies/localStorage nếu có
# =========================
def bootstrap_auth(d):
    if COOKIES_PATH and os.path.exists(COOKIES_PATH):
        try:
            d.get("https://www.facebook.com/")  # open first to set domain
            with open(COOKIES_PATH, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for c in cookies:
                # minimal cookie fields
                cookie = {k: c[k] for k in ("name","value","domain","path","secure","httpOnly","expiry") if k in c}
                # Selenium expects 'expires' not 'expiry' sometimes
                if "expiry" in cookie and "expires" not in cookie:
                    cookie["expires"] = cookie.pop("expiry")
                d.add_cookie(cookie)
            d.get("https://www.facebook.com/")
        except Exception as e:
            print("[WARN] bootstrap cookies:", e)

    if LOCALSTORAGE_PATH and os.path.exists(LOCALSTORAGE_PATH):
        try:
            d.get("https://www.facebook.com/")
            with open(LOCALSTORAGE_PATH, "r", encoding="utf-8") as f:
                kv = json.load(f)
            for k,v in kv.items():
                d.execute_script("localStorage.setItem(arguments[0], arguments[1]);", k, v)
            d.get("https://www.facebook.com/")
        except Exception as e:
            print("[WARN] bootstrap localStorage:", e)

# =========================
# Request matching / parsing
# =========================
def parse_form(body_str: str) -> Dict[str, str]:
    qs = urllib.parse.parse_qs(body_str, keep_blank_values=True)
    return {k: (v[0] if isinstance(v, list) else v) for k, v in qs.items()}

def is_group_feed_req(rec):
    if "/api/graphql/" not in (rec.get("url") or ""): return False
    if (rec.get("method") or "").upper() != "POST": return False
    body = rec.get("body") or ""
    if "fb_api_req_friendly_name=" in body:
        if re.search(r"(?:GroupComet|CometGroup|GroupsComet|ProfileComet|Comet).*?(?:Feed|Timeline|Stories).*?(?:Pagination|Refetch)", body, re.I):
            return True
    try:
        v = parse_form(body).get("variables","")
        vj = json.loads(urllib.parse.unquote_plus(v))
        if any(k in vj for k in ["groupID","groupIDV2","id","actorID","profileID","pageID"]):
            if any(k in vj for k in ["after","cursor","endCursor","afterCursor","feedAfterCursor","beforeTime","afterTime"]):
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
# Cursor / HasNext / Time helpers
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

def deep_collect_timestamps(obj) -> List[int]:
    keys_hint = {"creation_time","created_time","creationTime","createdTime"}
    out = []
    def as_epoch_s(x):
        try:
            v = int(x)
            if v > 10_000_000_000: v //= 1000
            if 1104537600 <= v <= 4102444800:  # 2005..2100
                return v
        except: pass
        return None
    def dive(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k in keys_hint:
                    vv = as_epoch_s(v)
                    if vv: out.append(vv)
                dive(v)
        elif isinstance(o, list):
            for v in o: dive(v)
    dive(obj)
    return out

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

def strip_cursors_from_vars(v: dict) -> dict:
    if not isinstance(v, dict): return {}
    return {k: v for k, v in v.items() if k not in CURSOR_KEYS}

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

def set_time_window_on_form(form: dict, t_from: Optional[int], t_to: Optional[int], vars_template: dict) -> dict:
    """
    Gắn beforeTime/afterTime nếu tồn tại trong template (hoặc cứ thêm, GraphQL sẽ bỏ qua nếu không hỗ trợ).
    - afterTime: epoch giây (>=)
    - beforeTime: epoch giây (<)
    """
    try:
        base = json.loads(form.get("variables", "{}"))
    except Exception:
        base = {}
    base = merge_vars(base, vars_template)
    known_keys = set(base.keys())
    cand_after = "afterTime"  if "afterTime"  in known_keys else "after_time"
    cand_before= "beforeTime" if "beforeTime" in known_keys else "before_time"
    if t_from is not None:  base[cand_after]  = int(t_from)
    if t_to   is not None:  base[cand_before] = int(t_to)
    if "count" in base and isinstance(base["count"], int):
        base["count"] = max(base["count"], 10)
    form2 = dict(form)
    form2["variables"] = json.dumps(base, separators=(",", ":"))
    return form2

# =========================
# Post collectors (tối giản, ưu tiên rid + link + created_time)
# =========================
POST_URL_RE = re.compile(
    r"""https?://(?:web\.)?facebook\.com/
        (?:
            groups/[^/]+/(?:permalink|posts)/\d+
          | [A-Za-z0-9.\-]+/posts/\d+
          | [A-Za-z0-9.\-]+/reel/\d+
          | photo(?:\.php)?\?(?:.*(?:fbid|story_fbid|video_id)=\d+)
          | .*?/pfbid[A-Za-z0-9]+
        )
    """, re.I | re.X
)

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

def _extract_url_digits(url: str) -> Optional[str]:
    if not url: return None
    try:
        path = urlparse(url).path.lower()
    except:
        path = url.lower()
    m = re.search(r"/(?:reel|posts|permalink)/(\d+)", path)
    if m: return m.group(1)
    qs = parse_qs(urlparse(url).query)
    for k in ("fbid","story_fbid","video_id","photo_id","id","v"):
        v = qs.get(k)
        if v and v[0] and v[0].isdigit():
            return v[0]
    return None

def collect_post_summaries(obj, out, group_url=GROUP_URL):
    if isinstance(obj, dict):
        if _looks_like_group_post(obj):
            post_id_api = obj.get("post_id")
            fb_id      = obj.get("id")
            url        = obj.get("wwwURL") or obj.get("url")
            url_digits = _extract_url_digits(url)
            rid        = post_id_api or url_digits or fb_id
            created_candidates = deep_collect_timestamps(obj)
            created = max(created_candidates) if created_candidates else None
            out.append({
                "id": fb_id,
                "rid": rid,
                "link": url,
                "created_time": created,
            })
        for v in obj.values():
            collect_post_summaries(v, out, group_url)
    elif isinstance(obj, list):
        for v in obj:
            collect_post_summaries(v, out, group_url)

def filter_only_feed_posts(items):
    keep = []
    for it in items or []:
        link = (it.get("link") or "").strip()
        fb_id = (it.get("id") or "").strip()
        rid = (it.get("rid") or "").strip()
        if rid or (link and POST_URL_RE.match(link)) or (fb_id and fb_id.startswith("Uzpf")):
            keep.append(it)
    return keep

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

def _best_primary_key(it: dict) -> Optional[str]:
    rid = it.get("rid"); link = it.get("link"); _id = it.get("id")
    digits = _extract_url_digits(link) if link else None
    norm   = _norm_link(link) if link else None
    for k in (rid, _id, digits, norm):
        if isinstance(k, str) and k.strip(): return k.strip()
    return None

def _all_join_keys(it: dict) -> List[str]:
    keys, seen = [], set()
    for k in (it.get("rid"), it.get("id"), _extract_url_digits(it.get("link") or ""), _norm_link(it.get("link") or "")):
        if isinstance(k, str) and k and (k not in seen):
            keys.append(k); seen.add(k)
    return keys

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
# JS fetch with page cookies
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
# Soft-refetch & reload
# =========================
def soft_refetch_form_and_cursor(driver, form, vars_template):
    try:
        base = json.loads(form.get("variables", "{}"))
    except Exception:
        base = {}
    base = merge_vars(base, vars_template)
    base = strip_cursors_from_vars(base)
    new_form = dict(form)
    new_form["variables"] = json.dumps(base, separators=(",", ":"))
    txt = js_fetch_in_page(driver, new_form, extra_headers={})
    obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
    if not obj: return None, None, None, None
    cursors = deep_collect_cursors(obj)
    new_has_next = deep_find_has_next(obj)
    if new_has_next is None: new_has_next = bool(cursors)
    new_cursor = cursors[0][1] if cursors else None
    return new_form, new_cursor, new_has_next, obj

def reload_and_refresh_form(d, group_url, cursor, vars_template, timeout=25, poll=0.25):
    d.get(group_url); time.sleep(1.5)
    for _ in range(4):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));"); time.sleep(0.5)
    nxt = wait_next_req(d, 0, is_group_feed_req, timeout=timeout, poll=poll)
    if not nxt: return None, None, None
    _, req = nxt
    new_form = parse_form(req.get("body", ""))
    friendly = urllib.parse.parse_qs(req.get("body","")).get("fb_api_req_friendly_name", [""])[0]
    new_doc_id = new_form.get("doc_id")
    if cursor: new_form = update_vars_for_next_cursor(new_form, cursor, vars_template)
    return new_form, friendly, new_doc_id

# =========================
# Checkpoint / Output
# =========================
def load_checkpoint():
    if not os.path.exists(CHECKPOINT):
        return {"cursor": None, "seen_ids": [], "vars_template": {}, "ts": None,
                "slice_to": None, "slice_from": None, "year_cursor": None}
    try:
        with open(CHECKPOINT, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"cursor": None, "seen_ids": [], "vars_template": {}, "ts": None,
                "slice_to": None, "slice_from": None, "year_cursor": None}

def save_checkpoint(**kw):
    data = load_checkpoint()
    data.update(kw)
    data["ts"] = datetime.datetime.now().isoformat(timespec="seconds")
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def append_ndjson(items):
    if not items: return
    with open(OUT_NDJSON, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

def normalize_seen_ids(seen_ids):
    return set(seen_ids or [])

# =========================
# Paginate 1 cửa sổ thời gian (optional t_from/t_to)
# =========================
def paginate_window(d, form, vars_template, seen_ids: set,
                    t_from: Optional[int]=None, t_to: Optional[int]=None,
                    page_limit: Optional[int]=None) -> Tuple[int, Optional[int], bool]:
    """
    Chạy paginate trong khoảng thời gian [t_from, t_to) nếu cung cấp.
    Trả về:
      - total_new viết ra
      - min_created_time bắt gặp (để dùng làm mốc beforeTime tiếp)
      - có còn has_next trong cửa sổ này không
    """
    total_new = 0
    min_created = None
    no_progress_rounds = 0

    if (t_from is not None) or (t_to is not None):
        form = set_time_window_on_form(form, t_from, t_to, vars_template)

    page = 0
    while True:
        page += 1
        txt = js_fetch_in_page(d, form, extra_headers={})
        obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
        with open(os.path.join(RAW_DUMPS_DIR, f"slice_{t_from or 'None'}_{t_to or 'None'}_p{page}.json"), "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

        if not obj:
            print(f"[SLICE {t_from}->{t_to}] parse fail → stop slice.")
            break

        page_posts = []
        collect_post_summaries(obj, page_posts)
        page_posts = coalesce_posts(filter_only_feed_posts(page_posts))

        # fresh by PK
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

        # cập nhật min_created_time
        for p in page_posts:
            ct = p.get("created_time")
            if isinstance(ct, int):
                if (min_created is None) or (ct < min_created):
                    min_created = ct

        # cursor / next
        cursors = deep_collect_cursors(obj)
        has_next = deep_find_has_next(obj)
        if has_next is None: has_next = bool(cursors)
        new_cursor = cursors[0][1] if cursors else None

        print(f"[SLICE {t_from or '-inf'}→{t_to or '+inf'}] p{page} got {len(page_posts)} (new {len(fresh)}), total_new={total_new}, next={has_next}")

        if not has_next or (page_limit and page >= page_limit):
            # hết trang trong slice hiện tại → thử soft-refetch để "nạp" doc khác
            if no_progress_rounds >= 2:
                new_form, boot_cursor, boot_has_next, _ = soft_refetch_form_and_cursor(d, form, vars_template)
                if new_form and (boot_cursor or boot_has_next):
                    form = set_time_window_on_form(new_form, t_from, t_to, vars_template)
                    no_progress_rounds = 0
                    continue
            break

        # có next: tiếp
        if new_cursor:
            form = update_vars_for_next_cursor(form, new_cursor, vars_template)

        time.sleep(random.uniform(0.7, 1.4))

    return total_new, min_created, bool(has_next)

# =========================
# Year-by-year runner
# =========================
def run_year_by_year(d, boot_form, vars_template, seen_ids):
    """
    Chạy vét theo từng năm:
    - Khởi điểm: slice warmup (không khóa thời gian) để lấy min_created.
    - Sau đó "đào lùi": đặt beforeTime = min_created hiện có (hoặc mốc cuối năm) và lặp.
    - Khi chạm năm < STOP_AT_YEAR thì dừng.
    """
    total_all = 0

    print("[YEAR] Warmup slice (no time window)...")
    new_warm, min_ct, _ = paginate_window(d, boot_form, vars_template, seen_ids, None, None)
    total_all += new_warm
    if not min_ct:
        print("[YEAR] Không nhặt được created_time — chuyển sang paginate bình thường.")
        return total_all

    cur_to = min_ct  # exclusive
    while True:
        year = datetime.datetime.utcfromtimestamp(cur_to).year
        if year < STOP_AT_YEAR:
            print(f"[YEAR] Hit STOP_AT_YEAR={STOP_AT_YEAR}. Done.")
            break

        start_of_year = int(datetime.datetime(year, 1, 1, 0, 0, 0).timestamp())
        print(f"[YEAR] Slice year {year}: {start_of_year} → {cur_to}")

        form2, friendly, docid = reload_and_refresh_form(d, GROUP_URL, None, vars_template)
        if not form2:
            print("[YEAR] reload form fail, dùng form cũ.")
            form2 = boot_form

        added, min_ct2, _ = paginate_window(d, form2, vars_template, seen_ids,
                                            t_from=start_of_year, t_to=cur_to)
        total_all += added

        if min_ct2 and min_ct2 < cur_to:
            cur_to = min_ct2
        else:
            cur_to = start_of_year

        time.sleep(random.uniform(1.0, 2.0))

    return total_all

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    d = start_driver(CHROME_PATH, USER_DATA_DIR, PROFILE_NAME, port=REMOTE_PORT, headless=False)
    bootstrap_auth(d)  # nếu có file cookies/localStorage thì nạp; còn không sẽ bỏ qua

    try:
        install_early_hook(d, keep_last=KEEP_LAST)
    except Exception as e:
        print("[WARN] install_early_hook:", e)

    d.get(GROUP_URL); time.sleep(1.2)
    for _ in range(6):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));"); time.sleep(0.6)

    nxt = wait_next_req(d, 0, is_group_feed_req, timeout=25, poll=0.25)
    if not nxt:
        raise RuntimeError("Không bắt được request feed. Hãy cuộn thêm/kiểm tra quyền.")
    _, first_req = nxt
    form         = parse_form(first_req.get("body", ""))
    friendly     = urllib.parse.parse_qs(first_req.get("body","")).get("fb_api_req_friendly_name", [""])[0]
    vars_now     = get_vars_from_form(form)
    template_now = make_vars_template(vars_now)

    state = load_checkpoint()
    seen_ids      = normalize_seen_ids(state.get("seen_ids"))
    cursor        = state.get("cursor")
    vars_template = state.get("vars_template") or template_now

    effective_template = vars_template or template_now

    # Nếu có cursor → resume thẳng
    if cursor:
        print(f"[RESUME] cursor={str(cursor)[:24]}..., friendly={friendly}")
        form = update_vars_for_next_cursor(form, cursor, vars_template=effective_template)

    total_got = run_year_by_year(d, form, effective_template, seen_ids)

    save_checkpoint(cursor=None, seen_ids=list(seen_ids), vars_template=effective_template)
    print(f"[DONE] total new written (year-by-year) = {total_got} → {OUT_NDJSON}")
