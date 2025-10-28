import os, re, json, time, random
from typing import Dict, Any, List, Optional
from pathlib import Path

from seleniumwire import webdriver                      # pip install selenium-wire
from selenium.webdriver.chrome.options import Options
from selenium_utils import *
from selenium_utils import _strip_xssi_prefix
from selenium_utils import _best_primary_key
from selenium_utils import _all_join_keys
# =============== Config ===============
HERE = Path(__file__).resolve().parent
PROXY_URL            = ""
GROUP_URL            = "https://www.facebook.com/thoibao.de"

KEEP_LAST            = 350
OUT_NDJSON           = "posts_all.ndjson"
RAW_DUMPS_DIR        = "raw_dumps"
CHECKPOINT           = "checkpoint.json"
POST_URL_RE = re.compile(
    r"""https?://(?:web\.)?facebook\.com/
        (?:
            groups/[^/]+/(?:permalink|posts)/(?P<gid>\d+)
            |
            [A-Za-z0-9.\-]+/posts/(?P<pid>\d+)
            |
            [A-Za-z0-9.\-]+/videos/(?P<vid>\d+)
            |
            photo(?:\.php)?\?fbid=(?P<fbid1>\d+)
            |
            .*?/pfbid(?P<pfbid>[A-Za-z0-9]+)  # permalink kiểu pfbid
        )
    """,
    re.I | re.X,
)


# ✅ FIX: point to authen folder under this script directory
COOKIES_PATH         = HERE / "authen" / "cookies.json"
LOCALSTORAGE_PATH    = HERE / "authen" / "localstorage.json"
SESSIONSTORAGE_PATH  = HERE / "authen" / "sessionstorage.json"

CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}
FB_ORIGINS = [
    "https://www.facebook.com",
    "https://web.facebook.com",
    "https://m.facebook.com",
]
os.makedirs(HERE / RAW_DUMPS_DIR, exist_ok=True)

# ====== CDP setter fallback (edit to your own if you already have one) ======
def _cdp_set_cookie_raw(driver, cookie: Dict[str, Any], url: Optional[str] = None) -> bool:
    """
    Sets a cookie via DevTools. Accepts fields like:
    name, value, domain, path, expires/expiry, httpOnly, secure, sameSite.
    If url is provided, DevTools requires it OR domain/path pair.
    """
    try:
        params = {"name": cookie["name"], "value": cookie.get("value", "")}
        # Prefer URL if passed (simplifies site-scoped cookies)
        if url:
            params["url"] = url
        else:
            if "domain" in cookie: params["domain"] = cookie["domain"]
            if "path" in cookie:   params["path"] = cookie["path"]

        # Expiry mapping
        exp = cookie.get("expiry", cookie.get("expires"))
        if isinstance(exp, (int, float)): params["expires"] = float(exp)

        if "httpOnly" in cookie: params["httpOnly"] = bool(cookie["httpOnly"])
        if "secure"   in cookie: params["secure"]   = bool(cookie["secure"])
        if "sameSite" in cookie:
            ss = str(cookie["sameSite"]).capitalize()
            if ss in {"Lax","Strict","None"}: params["sameSite"] = ss

        driver.execute_cdp_cmd("Network.setCookie", params)
        return True
    except Exception:
        return False

# ------------------ Loaders (robust to many formats) ------------------
def _read_text(p: Path) -> Optional[str]:
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        return None

def _load_json_file(p: Path) -> Optional[Any]:
    if not p or not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def _normalize_cookies(any_obj: Any) -> List[Dict[str, Any]]:
    """
    Supports:
    - EditThisCookie array: [{name,value,domain,path,expirationDate,...}]
    - Selenium cookies list: [{name,value,domain,path,expiry,...}]
    - Your format: {"cookies":[...]}
    - Netscape cookie text (fallback)
    """
    cookies: List[Dict[str, Any]] = []

    # Dict wrapper {"cookies":[...]}
    if isinstance(any_obj, dict) and "cookies" in any_obj and isinstance(any_obj["cookies"], list):
        any_obj = any_obj["cookies"]

    # Array JSON
    if isinstance(any_obj, list):
        for c in any_obj:
            if not isinstance(c, dict): continue
            name = c.get("name")
            value = c.get("value", c.get("content", ""))
            if not name: continue
            ck = {
                "name": name,
                "value": value,
                "domain": c.get("domain") or c.get("host", ".facebook.com"),
                "path": c.get("path", "/"),
                "secure": bool(c.get("secure", True)),
                "httpOnly": bool(c.get("httpOnly", False)),
            }
            # expiry / expirationDate
            if "expiry" in c: ck["expiry"] = c["expiry"]
            elif "expires" in c: ck["expiry"] = c["expires"]
            elif "expirationDate" in c: ck["expiry"] = c["expirationDate"]
            # sameSite variants
            ss = c.get("sameSite") or c.get("sameSiteAttribute")
            if ss: ck["sameSite"] = ss
            cookies.append(ck)
        return cookies

    # Netscape cookie.txt fallback
    if isinstance(any_obj, str):
        lines = any_obj.splitlines()
        for line in lines:
            if not line or line.startswith("#"): continue
            parts = line.split("\t")
            if len(parts) >= 7:
                domain, _flag, path, secure, expires, name, value = parts[:7]
                ck = {
                    "name": name,
                    "value": value.strip(),
                    "domain": domain.strip(),
                    "path": path.strip() if path else "/",
                    "secure": (secure.strip().upper()=="TRUE"),
                }
                try:
                    ck["expiry"] = int(expires)
                except Exception:
                    pass
                cookies.append(ck)
        return cookies

    return cookies

def _load_cookies_any_format(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        print(f"[AUTH] Cookie file not found: {path}")
        return []
    # Try JSON
    obj = _load_json_file(path)
    if obj is not None:
        return _normalize_cookies(obj)
    # Try Netscape text
    txt = _read_text(path)
    if txt:
        return _normalize_cookies(txt)
    return []

def _load_localstorage_any_format(path: Path) -> Dict[str, Any]:
    """
    Supports:
    - Dict: {"key1":"value1", ...}
    - Array: [{"key":"foo","value":"bar"}, ...]
    """
    js = _load_json_file(path)
    if js is None: return {}
    if isinstance(js, dict):
        return js
    if isinstance(js, list):
        out = {}
        for item in js:
            if isinstance(item, dict) and "key" in item:
                out[item["key"]] = item.get("value", "")
        return out
    return {}
def _cdp_set_cookie(driver,cookie: Dict[str, Any], url: Optional[str] = None) -> bool:
    assert driver is not None
    driver.execute_cdp_cmd("Network.enable", {})
    name  = cookie.get("name") or cookie.get("Name")
    value = cookie.get("value") or cookie.get("Value")
    domain = cookie.get("domain") or cookie.get("Domain")
    path = cookie.get("path") or cookie.get("Path") or "/"
    secure = bool(cookie.get("secure") or cookie.get("Secure") or False)
    httpOnly = bool(cookie.get("httpOnly") or cookie.get("HttpOnly") or False)
    same_site_raw = (cookie.get("sameSite") or cookie.get("SameSite") or "")
    if isinstance(same_site_raw, str):
        same_site_raw = same_site_raw.capitalize()
    sameSite = same_site_raw if same_site_raw in ("Lax","Strict","None") else None
    expires = cookie.get("expires") or cookie.get("expiry") or cookie.get("expirationDate")
    if isinstance(expires, str):
        try: expires = float(expires)
        except: expires = None
    if isinstance(expires, (int, float)) and expires <= 0:
        expires = None

    params: Dict[str, Any] = {
        "name": name, "value": value,
        "path": path,
        "secure": secure, "httpOnly": httpOnly,
    }
    if sameSite: params["sameSite"] = sameSite
    if isinstance(expires, (int, float)): params["expires"] = expires

    if url:
        params["url"] = url
    elif domain:
        params["domain"] = domain
    else:
        params["url"] = "https://www.facebook.com/"

    try:
        res = driver.execute_cdp_cmd("Network.setCookie", params)
        return bool(res.get("success"))
    except Exception:
        return False



# =============== Selenium boot (proxy-only) ===============
def start_driver_with_proxy(proxy_url: str, headless: bool = False) -> webdriver.Chrome:
    chrome_opts = Options()
    if headless:
        chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--window-size=1920,1080")
    chrome_opts.add_argument("--disable-extensions")
    chrome_opts.add_argument("--disable-background-networking")
    chrome_opts.add_argument("--disable-popup-blocking")
    chrome_opts.add_argument("--no-first-run")
    chrome_opts.add_argument("--no-default-browser-check")

    sw_options = None
    if proxy_url:
        sw_options = {
            "proxy": {
                "http":  proxy_url,
                "https": proxy_url,
                "no_proxy": "localhost,127.0.0.1",
            },
            # "verify_ssl": False,  # nếu proxy self-signed
        }

    driver = webdriver.Chrome(options=chrome_opts, seleniumwire_options=sw_options)
    driver.scopes = ['.*']  # hook all; có thể thu hẹp sau
    return driver

# =============== Session bootstrap (cookies + storage) ===============
def _coerce_expiry(ts) -> Optional[int]:
    try:
        if ts is None: return None
        return int(ts)
    except Exception:
        return None

def apply_cookies(driver, cookies: List[Dict[str, Any]], base_url="https://www.facebook.com/"):
    driver.get(base_url); time.sleep(0.5)
    driver.delete_all_cookies()
    for c in (cookies or []):
        try:
            ck = {
                "name":   c.get("name"),
                "value":  c.get("value", ""),
                "domain": c.get("domain", ".facebook.com"),
                "path":   c.get("path", "/"),
                "secure": bool(c.get("secure", True)),
            }
            exp = _coerce_expiry(c.get("expires") or c.get("expiry"))
            if exp: ck["expiry"] = exp
            if "sameSite" in c: ck["sameSite"] = c["sameSite"]
            if "httpOnly" in c: ck["httpOnly"] = bool(c["httpOnly"])
            try:
                driver.add_cookie(ck)
            except Exception:
                ck.pop("sameSite", None); ck.pop("httpOnly", None)
                driver.add_cookie(ck)
        except Exception as e:
            print("[WARN] add_cookie failed:", e)
    driver.get(base_url); time.sleep(0.7)

def apply_storage(driver, local_kv: Dict[str, str] = None, session_kv: Dict[str, str] = None, origin="https://www.facebook.com/"):
    driver.get(origin); time.sleep(0.4)
    if local_kv:
        driver.execute_script("""
            const data = arguments[0] || {};
            Object.entries(data).forEach(([k,v]) => { try{ localStorage.setItem(k, String(v)); }catch(e){} });
        """, local_kv)
    if session_kv:
        driver.execute_script("""
            const data = arguments[0] || {};
            Object.entries(data).forEach(([k,v]) => { try{ sessionStorage.setItem(k, String(v)); }catch(e){} });
        """, session_kv)
    driver.get(origin); time.sleep(0.4)

def bootstrap_auth(driver):
    print(f"[AUTH] Using:\n - COOKIES_PATH={COOKIES_PATH}\n - LOCALSTORAGE_PATH={LOCALSTORAGE_PATH}")
    cookies_list = _load_cookies_any_format(COOKIES_PATH)
    print(f"[AUTH] Loaded {len(cookies_list)} cookies from file.")

    # 1) Hit the domain so LS is available, also enables add_cookie fallback if needed
    driver.get("https://www.facebook.com/")
    time.sleep(1.0)

    # 2) Set cookies via CDP; mirror across origins to be safe
    ok = 0
    for c in cookies_list:
        if _cdp_set_cookie_raw(driver, c):
            ok += 1
    ok_host = 0
    for origin in FB_ORIGINS:
        for c in cookies_list:
            if _cdp_set_cookie_raw(driver, c, url=origin + "/"):
                ok_host += 1
    print(f"[AUTH] cookies set: domain_ok={ok}/{len(cookies_list)} | host_mirrors={ok_host}")

    # 3) LocalStorage
    ls_dict = _load_localstorage_any_format(LOCALSTORAGE_PATH)
    if ls_dict:
        driver.execute_script("window.localStorage.clear();")
        script = """
            const data = arguments[0];
            for (const [k,v] of Object.entries(data)) {
                try { localStorage.setItem(k, (typeof v === 'string') ? v : JSON.stringify(v)); } catch(e) {}
            }
            return Object.keys(data).length;
        """
        set_count = driver.execute_script(script, ls_dict)
        print(f"[AUTH] Injected {set_count} localStorage keys.")
        driver.refresh()
        time.sleep(0.8)
    else:
        print("[AUTH] No localStorage keys loaded (file empty or wrong format).")

    print(f"[AUTH] Ready at {driver.execute_script('return location.href;')}")


# =============== Main ===============
if __name__ == "__main__":
    d = start_driver_with_proxy(PROXY_URL, headless=False)

    bootstrap_auth(d)

    try:
        install_early_hook(d, keep_last=KEEP_LAST)
    except Exception as e:
        print("[WARN] install_early_hook:", e)

    d.get(GROUP_URL); time.sleep(1.2)
    for _ in range(6):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
        time.sleep(0.6)

    # Boot request
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
        page_posts = coalesce_posts(page_posts)  # ✅ gộp đa-khóa

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
        page_posts = filter_only_group_posts(page_posts)
        page_posts = coalesce_posts(page_posts)  # ✅ gộp trước

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
