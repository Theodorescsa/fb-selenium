import json, re, time, random, urllib.parse, subprocess, os, sys, datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from get_info import *
import socket
CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}

def strip_cursors_from_vars(v: dict) -> dict:
    if not isinstance(v, dict): return {}
    return {k: v for k, v in v.items() if k not in CURSOR_KEYS}

def soft_refetch_form_and_cursor(driver, form, effective_template):
    try:
        base = json.loads(form.get("variables", "{}"))
    except Exception:
        base = {}
    # ghép lại template (không chứa cursor) rồi bỏ cursor hoàn toàn
    base = merge_vars(base, effective_template)
    base = strip_cursors_from_vars(base)

    new_form = dict(form)
    new_form["variables"] = json.dumps(base, separators=(",", ":"))

    txt = js_fetch_in_page(driver, new_form, extra_headers={})
    obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
    if not obj:
        return None, None, None, None

    cursors = deep_collect_cursors(obj)
    new_has_next = deep_find_has_next(obj)
    if new_has_next is None:
        new_has_next = bool(cursors)
    new_cursor = cursors[0][1] if cursors else None
    return new_form, new_cursor, new_has_next, obj

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

CHECKPOINT    = r"checkpoint.json"

os.makedirs(RAW_DUMPS_DIR, exist_ok=True)

# =========================
# Boot
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
                 headless: bool = True,
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
def filter_only_feed_posts(items):
    keep = []
    for it in items or []:
        link = (it.get("link") or "").strip()
        rid  = (it.get("rid")  or "").strip()
        fbid = (it.get("id")   or "").strip()

        ok_link = bool(link and POST_URL_RE.match(link))
        ok_fbid = bool(fbid and fbid.startswith("Uzpf"))  # classic story ID
        ok_rid  = bool(rid)

        if ok_link or ok_fbid or ok_rid:
            keep.append(it)
    return keep
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

            # # author & type label
            author_id, author_name, author_link, avatar, type_label = extract_author(obj)

            text = _get_text_from_node(obj)

            image_urls, video_urls = extract_media(obj)
            counts = extract_reactions_and_counts(obj)
            created = extract_created_time(obj)
            is_share, link_share, type_share, origin_id = extract_share_flags(obj)
            hashtags = extract_hashtags(text)
            # author & type label
            # author_id , author_name, author_link, avatar, type_label = None, None, None, None, None

            # text = None

            # image_urls, video_urls = None, None
            # counts = None
            # created = None
            # is_share, link_share, type_share, origin_id = None, None, None, None
            # hashtags = None
            # # source_id (group id/slug best-effort)
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

def _pick_text(a, b):
    """Ưu tiên b nếu b 'tốt hơn' (không None, dài hơn)."""
    if not b: 
        return a
    if not a:
        return b
    # ưu tiên chuỗi dài hơn (nhiều nội dung hơn)
    return b if isinstance(b, str) and isinstance(a, str) and len(b) > len(a) else (b or a)

def _pick_non_empty(a, b):
    """Ưu tiên b nếu b không rỗng/None."""
    return b if b not in (None, "", [], {}) else a

def _merge_arrays(a, b):
    out = []
    seen = set()
    for arr in (a or [], b or []):
        for x in arr:
            if x not in seen:
                out.append(x); seen.add(x)
    return out

def _merge_counts(a, b, keys):
    out = dict(a or {})
    for k in keys:
        out[k] = max((a or {}).get(k, 0), (b or {}).get(k, 0))
    return out

def _prefer_type(t1, t2):
    """Ưu tiên type cụ thể hơn: page/profile/group > story."""
    rank = {"facebook page": 3, "facebook profile": 3, "facebook group": 3, "story": 1, None: 0}
    return t2 if rank.get(t2,0) >= rank.get(t1,0) else t1

COUNT_KEYS = ["like","comment","haha","wow","sad","love","angry","care","share"]

def merge_two_posts(a: dict, b: dict) -> dict:
    if not a: return b or {}
    if not b: return a or {}
    m = dict(a)

    # id/rid: giữ nguyên
    m["id"]  = m.get("id")  or b.get("id")
    m["rid"] = m.get("rid") or b.get("rid")

    # type cụ thể hơn
    m["type"] = _prefer_type(m.get("type"), b.get("type"))

    # link/author fields: lấy cái có giá trị
    m["link"]        = _pick_non_empty(m.get("link"),        b.get("link"))
    m["author_id"]   = _pick_non_empty(m.get("author_id"),   b.get("author_id"))
    m["author"]      = _pick_non_empty(m.get("author"),      b.get("author"))
    m["author_link"] = _pick_non_empty(m.get("author_link"), b.get("author_link"))
    m["avatar"]      = _pick_non_empty(m.get("avatar"),      b.get("avatar"))

    # created_time: ưu tiên số lớn hơn (mới hơn)
    ct_a, ct_b = m.get("created_time"), b.get("created_time")
    try:
        m["created_time"] = max(int(ct_a) if ct_a is not None else 0, int(ct_b) if ct_b is not None else 0) or (ct_a or ct_b)
    except:
        m["created_time"] = ct_a or ct_b

    # content: ưu tiên nội dung dài hơn/đầy đủ hơn
    m["content"] = _pick_text(m.get("content"), b.get("content"))

    # media: union
    m["image_url"] = _merge_arrays(m.get("image_url"), b.get("image_url"))
    m["video"]     = _merge_arrays(m.get("video"),     b.get("video"))

    # hashtag: union + đã lowercase sẵn
    m["hashtag"] = _merge_arrays(m.get("hashtag"), b.get("hashtag"))

    # counts: lấy max
    counts_a = {k: m.get(k, 0) for k in COUNT_KEYS}
    counts_b = {k: b.get(k, 0) for k in COUNT_KEYS}
    counts   = _merge_counts(counts_a, counts_b, COUNT_KEYS)
    m.update(counts)

    # source_id: ưu tiên cái có giá trị
    m["source_id"] = _pick_non_empty(m.get("source_id"), b.get("source_id"))

    # share flags
    m["is_share"]   = bool(m.get("is_share")) or bool(b.get("is_share"))
    m["link_share"] = _pick_non_empty(m.get("link_share"), b.get("link_share"))
    m["type_share"] = _pick_non_empty(m.get("type_share"), b.get("type_share"))

    return m
from urllib.parse import urlparse, parse_qs, urlunparse

def _norm_link(u: str) -> str | None:
    """Chuẩn hóa link để so trùng: bỏ query/fragment, chuẩn host, lower path, bỏ trailing slash."""
    if not u or not isinstance(u, str): 
        return None
    try:
        p = urlparse(u)
        # chuẩn host: web.facebook.com / m.facebook.com / www.facebook.com -> facebook.com
        host = p.netloc.lower()
        if host.endswith("facebook.com"):
            host = "facebook.com"
        # bỏ query/fragment
        path = (p.path or "").rstrip("/")
        return urlunparse(("https", host, path.lower(), "", "", ""))
    except Exception:
        return u

def _extract_digits_from_fb_link(u: str) -> str | None:
    """Lấy chuỗi số trong link post (permalink/posts/reel/...) nếu có."""
    if not u: return None
    try:
        path = urlparse(u).path.lower()
    except:
        path = u.lower()
    # /reel/<digits>, /posts/<digits>, /permalink/<digits>
    m = re.search(r"/(?:reel|posts|permalink)/(\d+)", path)
    return m.group(1) if m else None

def _best_primary_key(it: dict) -> str | None:
    """
    Primary key để dedupe seen_ids:
    - Ưu tiên rid nếu có & là dạng Uzpf... hoặc digits
    - Nếu không, dùng id
    - Nếu không, dùng digits từ link
    - Nếu vẫn không, dùng normalized link
    """
    rid = it.get("rid")
    _id = it.get("id")
    link = it.get("link")
    norm = _norm_link(link) if link else None
    digits = _extract_digits_from_fb_link(link) if link else None

    for k in (rid, _id):
        if isinstance(k, str) and k.strip():
            return k.strip()
    return None
def _all_join_keys(it: dict) -> list[str]:
    """Tập khóa để GỘP: rid, id, digits từ link, normalized link."""
    keys = []
    rid = it.get("rid")
    _id = it.get("id")
    link = it.get("link")

    keys = []
    if isinstance(rid, str) and rid.strip(): keys.append(rid.strip())
    if isinstance(_id,  str) and _id.strip(): keys.append(_id.strip())
    return list(dict.fromkeys(keys))


def coalesce_posts(items: list[dict]) -> list[dict]:
    """
    Gộp các record cùng post theo TẬP KHÓA {rid, id, digits(link), normalized_link}.
    Nếu BẤT KỲ khóa nào trùng → merge vào cùng một group.
    """
    groups: dict[str, dict] = {}
    key2group: dict[str, str] = {}
    seq = 0

    def _new_group_id() -> str:
        nonlocal seq
        seq += 1
        return f"g{seq}"

    for it in (items or []):
        # Lấy toàn bộ khóa có thể join (rid, id, digits(link), qid, norm_link)
        keys = _all_join_keys(it)  # yêu cầu bạn đã có sẵn hàm này

        # tìm group hiện có nếu bất kỳ key nào đã thấy
        gid = None
        for k in keys:
            if k in key2group:
                gid = key2group[k]
                break

        # tạo mới hoặc merge
        if gid is None:
            gid = _new_group_id()
            groups[gid] = it
        else:
            groups[gid] = merge_two_posts(groups[gid], it)  # yêu cầu bạn đã có sẵn hàm này

        # cập nhật lại mapping cho TẤT CẢ key sau khi merge (để khóa mới sinh ra cũng map đúng)
        merged_keys = _all_join_keys(groups[gid])
        for k in merged_keys:
            key2group[k] = gid

    return list(groups.values())

def filter_only_group_posts(items):
    keep = []
    for it in items:
        url = (it.get("link") or "").strip()  # ĐỔI: dùng 'link' thay vì 'url'
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
def try_refetch_reel_ufi(driver, base_form: dict, video_id: str, timeout=8.0):
    """
    Dùng js_fetch_in_page để 'nhá' UFI cho video (reel) theo video_id.
    Không hardcode doc_id; rely vào app preloaded ops.
    Trả về dict counts hoặc {} nếu fail.
    """
    if not video_id: 
        return {}

    # 1) Tạo payload giống form hiện tại nhưng variables chỉ chứa feedback target
    vars_min = {"feedbackTargetID": video_id, "scale": 1}
    form2 = dict(base_form)
    form2["variables"] = json.dumps(vars_min, separators=(",", ":"))

    # 2) Gọi 1 phát để app khởi động UFI resolver (nhiều bản GraphQL sẽ tự bind)
    _ = js_fetch_in_page(driver, form2, extra_headers={})

    # 3) Chờ trong buffer __gqlReqs tìm response có UFI (top_reactions/reaction_count)
    start_idx = max(0, gql_count(driver) - 50)
    def _ufi_req(rec):
        if "/api/graphql/" not in (rec.get("url") or ""): return False
        body = rec.get("body") or ""
        # heuristics: có video_id + fields UFI
        if video_id not in body:
            return False
        txt = rec.get("responseText") or ""
        return ("top_reactions" in txt) or ("reaction_count" in txt) or ("total_comment_count" in txt)

    hit = wait_next_req(driver, start_idx, _ufi_req, timeout=timeout, poll=0.25)
    if not hit:
        return {}

    _, req = hit
    txt = req.get("responseText") or ""
    obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
    if not obj:
        return {}

    # 4) Dò counts từ obj này (tái dùng parser)
    counts = extract_reactions_and_counts(obj)
    return counts or {}

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
def reload_and_refresh_form(d, group_url, cursor, effective_template, timeout=25, poll=0.25):
    """Reload trang, bắt lại 1 request feed mới để lấy form/doc_id/tokens mới,
    rồi set lại variables theo cursor đang có và trả về form mới."""
    d.get(group_url)
    time.sleep(1.5)
    for _ in range(4):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
        time.sleep(0.5)

    nxt = wait_next_req(d, 0, is_group_feed_req, timeout=timeout, poll=poll)
    if not nxt:
        return None, None, None  # không bắt được → để caller tự xử lý
    _, req = nxt

    new_form = parse_form(req.get("body", ""))
    new_friendly = urllib.parse.parse_qs(req.get("body","")).get("fb_api_req_friendly_name", [""])[0]
    new_doc_id = new_form.get("doc_id")

    # ghép lại variables dựa trên template + cursor hiện tại
    new_form = update_vars_for_next_cursor(new_form, cursor, vars_template=effective_template)
    return new_form, new_friendly, new_doc_id

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
        page_posts = coalesce_posts(filter_only_feed_posts(page_posts))


        cursors = deep_collect_cursors(obj0)
        has_next = deep_find_has_next(obj0)
        if has_next is None:
            has_next = bool(cursors)

        end_cursor = cursors[0][1] if cursors else None
        if end_cursor:
            cursor = end_cursor

        print(f"[DEBUG] page1 posts={len(page_posts)} | cursors={len(cursors)} | has_next={has_next} | pick={str(end_cursor)[:24] if end_cursor else None}")
        print(f"[DEBUG] doc_id={form.get('doc_id')} | friendly={friendly}")

        # ✅ SAU KHI GỘP: lọc fresh theo primary key (không phụ thuộc rid)
        fresh = []
        for p in page_posts:
            pk = _best_primary_key(p)
            if pk and pk not in seen_ids:
                fresh.append(p)

        if fresh:
            append_ndjson(fresh)
            # ✅ thêm TẤT CẢ join-keys vào seen_ids để khóa mọi biến thể
            for p in fresh:
                for k in _all_join_keys(p):
                    seen_ids.add(k)
            total_written += len(fresh)

        print(f"[PAGE#1] got {len(page_posts)} (new {len(fresh)}), next={bool(has_next)}")

        save_checkpoint(cursor, seen_ids, last_doc_id=form.get('doc_id'), last_query_name=friendly, vars_template=template_now)
        page = 1

    # ======== PAGINATE (resume-first) ========
    no_progress_rounds = 0
    while True:
        page += 1
        # if page == 20:
        #     print(f"[PAGE#{page}] reached page limit 20, stop.")
        #     break
        if cursor:
            form = update_vars_for_next_cursor(form, cursor, vars_template=effective_template)

        txt = js_fetch_in_page(d, form, extra_headers={})
        obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))

        with open(os.path.join(RAW_DUMPS_DIR, f"page{page}_obj.json"), "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        if not obj:
            open(os.path.join(RAW_DUMPS_DIR, f"page{page}_raw.txt"), "w", encoding="utf-8").write(txt)
            print(f"[PAGE#{page}] parse fail → dumped raw, break.")
            break

        page_posts = []
        collect_post_summaries(obj, page_posts)
        page_posts = coalesce_posts(filter_only_feed_posts(page_posts))


        cursors = deep_collect_cursors(obj)
        has_next = deep_find_has_next(obj)
        if has_next is None:
            has_next = bool(cursors)

        new_cursor = cursors[0][1] if cursors else None
        if new_cursor:
            cursor = new_cursor

        # ✅ fresh theo primary key (pk), không chỉ rid
        fresh = []
        for p in page_posts:
            pk = _best_primary_key(p)
            if pk and pk not in seen_ids:
                fresh.append(p)

        # ✅ DEDUP trong vòng hiện tại theo pk
        written_this_round = set()
        fresh_dedup = []
        for p in fresh:
            pk = _best_primary_key(p)
            if pk and pk not in written_this_round:
                fresh_dedup.append(p)
                written_this_round.add(pk)

        if fresh_dedup:
            append_ndjson(fresh_dedup)
            # ✅ seen_ids: add toàn bộ join-keys
            for p in fresh_dedup:
                for k in _all_join_keys(p):
                    seen_ids.add(k)
            total_written += len(fresh_dedup)
            no_progress_rounds = 0
        else:
            no_progress_rounds += 1

        print(f"[PAGE#{page}] got {len(page_posts)} (new {len(fresh_dedup)}), total={total_written}, next={bool(has_next)} | cursor={str(cursor)[:24] if cursor else None}")

        save_checkpoint(cursor, seen_ids, last_doc_id=form.get('doc_id'), last_query_name=friendly, vars_template=effective_template)

        # === NEW: nếu next=False liên tiếp nhiều lần thì soft-refetch không cần UI ===
        MAX_NO_NEXT_ROUNDS = 3
        if not has_next and no_progress_rounds >= MAX_NO_NEXT_ROUNDS:
            print(f"[PAGE#{page}] next=False x{no_progress_rounds} → soft-refetch doc_id/variables (no UI)")
            # lưu checkpoint hiện tại để an toàn
            save_checkpoint(cursor, seen_ids, last_doc_id=form.get('doc_id'),
                            last_query_name=friendly, vars_template=effective_template)

            # thử soft-refresh vài lần
            refetch_ok = False
            for attempt in range(1, 3):  # thử tối đa 2 lần
                new_form, boot_cursor, boot_has_next, boot_obj = soft_refetch_form_and_cursor(d, form, effective_template)
                if new_form and (boot_cursor or boot_has_next):
                    # cập nhật form + cursor mới và reset bộ đếm
                    form = new_form
                    if boot_cursor:
                        cursor = boot_cursor
                    has_next = bool(boot_has_next)
                    no_progress_rounds = 0
                    refetch_ok = True
                    print(f"[PAGE#{page}] soft-refetch OK (attempt {attempt}) → has_next={has_next} | cursor={str(cursor)[:24] if cursor else None}")
                    break
                time.sleep(random.uniform(1.0, 2.0))

            if not refetch_ok:
                print(f"[PAGE#{page}] soft-refetch failed → stop pagination.")
                break  # hoặc: quay về cơ chế reload UI thay vì break

        time.sleep(random.uniform(0.7, 1.5))


    print(f"[DONE] wrote {total_written} posts → {OUT_NDJSON}")
    print(f"[INFO] resume later with checkpoint: {CHECKPOINT}")
