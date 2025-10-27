# -*- coding: utf-8 -*-
"""
Facebook Group GraphQL crawler — Minimal (proxy-only, no NDJSON)
- Dùng proxy (--proxy-server)
- KHÔNG dùng profile thật: tạo user-data-dir tạm thời
- Nạp cookies + localStorage để đăng nhập
- Hook GraphQL, dump response JSON theo trang (raw + clean)
- Paginate bằng cursor (resume trong phiên hiện tại, không lưu NDJSON)

⚠️ Chỉ crawl nơi bạn có quyền. Tôn trọng ToS.
"""

import os, re, json, time, random, urllib.parse, datetime, shutil, tempfile
from typing import Any, Dict, Optional, List

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# =========================
# Config nhanh (sửa cho phù hợp)
# =========================
GROUP_URL          = "https://www.facebook.com/groups/<your-group-id-or-slug>"
COOKIES_PATH       = "./cookies.json"        # file cookies bạn export
LOCALSTORAGE_PATH  = "./localstorage.json"   # file localstorage bạn export
RAW_DUMPS_DIR      = "raw_dumps"
PROXY_URL          = "http://123.45.67.89:8080"   # proxy của bạn (http/socks5 đều được)
HEADLESS           = False
KEEP_LAST          = 350  # buffer hook

FB_ORIGINS = [
    "https://www.facebook.com",
    "https://web.facebook.com",
    "https://m.facebook.com",
]

CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}

os.makedirs(RAW_DUMPS_DIR, exist_ok=True)

# =========================
# Helpers nho nhỏ (self-contained)
# =========================
def _ts() -> str:
    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f")

def _strip_xssi_prefix(s: str) -> str:
    # Facebook thường trả về 'for(;;);' ở đầu
    if not isinstance(s, str):
        return s
    return re.sub(r'^\s*for\s*\(\s*;\s*;\s*\)\s*;\s*', '', s)

def _iter_json_values(s: str):
    """Cố gắng parse thành JSON; nếu là 1 object/array -> yield; nếu fail -> không yield."""
    if not s:
        return
    try:
        obj = json.loads(s)
        # có thể là list nhiều payload; cứ yield từng cái (hoặc yield nguyên)
        if isinstance(obj, list):
            for it in obj:
                yield it
        else:
            yield obj
    except Exception:
        return

def _safe_write_text(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def _dump_graphql_text(raw_text: str, tag: str, page_idx: int, seq: int = 0):
    if not isinstance(raw_text, str):
        raw_text = str(raw_text)
    base = f"{tag}_p{page_idx:03d}_{seq:02d}_{_ts()}"
    raw_path   = os.path.join(RAW_DUMPS_DIR, f"{base}.raw.txt")
    clean_path = os.path.join(RAW_DUMPS_DIR, f"{base}.json")
    _safe_write_text(raw_path, raw_text)

    cleaned = _strip_xssi_prefix(raw_text)
    objs = list(_iter_json_values(cleaned))
    try:
        if len(objs) == 1:
            _safe_write_text(clean_path, json.dumps(objs[0], ensure_ascii=False, indent=2))
        elif len(objs) > 1:
            _safe_write_text(clean_path, json.dumps(objs, ensure_ascii=False, indent=2))
        else:
            _safe_write_text(clean_path, cleaned)
    except Exception:
        _safe_write_text(clean_path, cleaned)

def _parse_form(body_str: str) -> Dict[str, str]:
    qs = urllib.parse.parse_qs(body_str, keep_blank_values=True)
    return {k: (v[0] if isinstance(v, list) else v) for k, v in qs.items()}

def _is_group_feed_req(rec) -> bool:
    if "/api/graphql/" not in (rec.get("url") or ""):
        return False
    if (rec.get("method") or "").upper() != "POST":
        return False
    body = rec.get("body") or ""
    if "fb_api_req_friendly_name=" in body:
        if re.search(r"(?:GroupComet|CometGroup|GroupsComet).*(?:Feed|Stories).*Pagination", body, re.I):
            return True
    try:
        v = _parse_form(body).get("variables", "")
        vj = json.loads(urllib.parse.unquote_plus(v))
        if any(k in vj for k in ["groupID", "groupIDV2", "id"]) and any(
            k in vj for k in ["after", "cursor", "endCursor", "afterCursor", "feedAfterCursor"]
        ):
            return True
    except Exception:
        pass
    return False

def _deep_collect_cursors(obj):
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

def _deep_find_has_next(obj):
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
# Class tối giản
# =========================
class GroupGraphQLCrawler:
    def __init__(self,
                 group_url: str = GROUP_URL,
                 cookies_path: Optional[str] = COOKIES_PATH,
                 localstorage_path: Optional[str] = LOCALSTORAGE_PATH,
                 proxy_url: Optional[str] = PROXY_URL,
                 headless: bool = HEADLESS):
        self.group_url = group_url
        self.cookies_path = cookies_path
        self.localstorage_path = localstorage_path
        self.proxy_url = proxy_url
        self.headless = headless

        self.driver: Optional[webdriver.Chrome] = None
        self.cursor: Optional[str] = None
        self.friendly: Optional[str] = None
        self.last_doc_id: Optional[str] = None

    # ---------- Boot ----------
    def start_driver(self) -> webdriver.Chrome:
        options = Options()

        # Tìm nhanh Chrome/Chromium (Linux)
        for cand in ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser"]:
            p = shutil.which(cand)
            if p:
                options.binary_location = p
                break

        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1400,920")
        options.add_argument("--disable-blink-features=AutomationControlled")

        # user-data-dir tạm thời (tránh lock profile thật)
        tmp_ud = tempfile.mkdtemp(prefix="fbcrawl_ud_")
        options.add_argument(f"--user-data-dir={tmp_ud}")

        # Proxy duy nhất
        if self.proxy_url:
            options.add_argument(f"--proxy-server={self.proxy_url}")
            options.add_argument("--proxy-bypass-list=<-loopback>")

        if self.headless:
            options.add_argument("--headless=new")

        self.driver = webdriver.Chrome(options=options)
        print(f"[PROXY] Using proxy: {self.proxy_url}")
        print(f"[PROFILE] Ephemeral user-data-dir: {tmp_ud}")
        return self.driver

    # ---------- Hook ----------
    def install_early_hook(self):
        HOOK_SRC = r"""
        (function(){
          if (window.__gqlHooked) return;
          window.__gqlHooked = true;
          window.__gqlReqs = [];
          function headersToObj(h){try{
            if (!h) return {};
            if (h instanceof Headers){const o={}; h.forEach((v,k)=>o[k]=v); return o;}
            if (Array.isArray(h)){const o={}; for (const [k,v] of h) o[k]=v; return o;}
            return (typeof h==='object')?h:{}; }catch(e){return {}}}
          function pushRec(rec){try{
            const q = window.__gqlReqs; q.push(rec);
            if (q.length > __KEEP_LAST__) q.splice(0, q.length - __KEEP_LAST__); }catch(e){}}
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
        """.replace("__KEEP_LAST__", str(KEEP_LAST))
        assert self.driver is not None
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": HOOK_SRC})
        self.driver.execute_script(HOOK_SRC)

    # ---------- GQL buffer ----------
    @staticmethod
    def gql_count(d): return d.execute_script("return (window.__gqlReqs||[]).length")
    @staticmethod
    def get_gql_at(d, i): return d.execute_script("return (window.__gqlReqs||[])[arguments[0]]", i)

    def wait_next_req(self, d, start_idx, matcher, timeout=25, poll=0.25):
        end = time.time() + timeout
        cur = start_idx
        while time.time() < end:
            n = self.gql_count(d)
            while cur < n:
                req = self.get_gql_at(d, cur)
                if req and matcher(req): return (cur, req)
                cur += 1
            time.sleep(poll)
        return None

    # ---------- Cookie & localStorage ----------
    def _load_json_file(self, path: Optional[str]):
        if not path or not os.path.exists(path): return None
        with open(path, "r", encoding="utf-8") as f:
            try: return json.load(f)
            except Exception: return None

    def _cdp_set_cookie(self, cookie: Dict[str, Any]) -> bool:
        assert self.driver is not None
        self.driver.execute_cdp_cmd("Network.enable", {})
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
            "domain": domain, "path": path,
            "secure": secure, "httpOnly": httpOnly,
        }
        if sameSite: params["sameSite"] = sameSite
        if isinstance(expires, (int, float)): params["expires"] = expires
        if not domain:
            params["url"] = "https://www.facebook.com/"

        try:
            res = self.driver.execute_cdp_cmd("Network.setCookie", params)
            return bool(res.get("success"))
        except Exception:
            return False

    def _cdp_set_cookie_for_url(self, cookie: Dict[str, Any], url: str) -> bool:
        assert self.driver is not None
        self.driver.execute_cdp_cmd("Network.enable", {})
        name  = cookie.get("name") or cookie.get("Name")
        value = cookie.get("value") or cookie.get("Value")
        path      = cookie.get("path") or cookie.get("Path") or "/"
        secure    = bool(cookie.get("secure") or cookie.get("Secure") or False)
        httpOnly  = bool(cookie.get("httpOnly") or cookie.get("HttpOnly") or False)
        same_site = cookie.get("sameSite") or cookie.get("SameSite")
        if isinstance(same_site, str):
            s = same_site.capitalize()
            same_site = s if s in ("Lax", "Strict", "None") else None
        expires = cookie.get("expires") or cookie.get("expiry") or cookie.get("expirationDate")
        if isinstance(expires, str):
            try: expires = float(expires)
            except: expires = None
        if isinstance(expires, (int, float)) and expires <= 0:
            expires = None

        params = {
            "name": name, "value": value,
            "url": url, "path": path,
            "secure": secure, "httpOnly": httpOnly
        }
        if same_site: params["sameSite"] = same_site
        if isinstance(expires, (int, float)): params["expires"] = expires
        try:
            res = self.driver.execute_cdp_cmd("Network.setCookie", params)
            return bool(res.get("success"))
        except Exception:
            return False

    def bootstrap_auth(self):
        assert self.driver is not None

        cookies_data = self._load_json_file(self.cookies_path)
        if isinstance(cookies_data, dict) and "cookies" in cookies_data:
            cookies_list = cookies_data["cookies"]
        elif isinstance(cookies_data, list):
            cookies_list = cookies_data
        else:
            cookies_list = []

        ok_domain = sum(1 for c in cookies_list if self._cdp_set_cookie(c))
        print(f"[AUTH] Domain-scoped cookies: {ok_domain}/{len(cookies_list)}")

        ok_host = 0
        for origin in FB_ORIGINS:
            for c in cookies_list:
                if self._cdp_set_cookie_for_url(c, origin):
                    ok_host += 1
        print(f"[AUTH] Host-only mirrors set: {ok_host} (over {len(cookies_list)*len(FB_ORIGINS)} attempts)")

        # Inject localStorage
        ls_dict = self._load_json_file(self.localstorage_path)
        self.driver.get("https://www.facebook.com/")
        time.sleep(1.2)
        if isinstance(ls_dict, dict) and ls_dict:
            self.driver.execute_script("window.localStorage.clear();")
            script = """
                const data = arguments[0];
                for (const [k,v] of Object.entries(data)) {
                    try { localStorage.setItem(k, (typeof v === 'string') ? v : JSON.stringify(v)); } catch(e) {}
                }
                return Object.keys(data).length;
            """
            set_count = self.driver.execute_script(script, ls_dict)
            print(f"[AUTH] Injected {set_count} localStorage keys.")
            self.driver.refresh()
            time.sleep(1.0)

        # Verify nhanh theo từng origin
        self.driver.execute_cdp_cmd("Network.enable", {})
        for origin in FB_ORIGINS:
            self.driver.get(origin + "/")
            time.sleep(0.8)
            has_cuser = self.driver.execute_script("return document.cookie.includes('c_user');")
            got = self.driver.execute_cdp_cmd("Network.getCookies", {"urls": [origin + "/"]})
            names = sorted([c.get("name","") for c in (got.get("cookies") or [])])
            print(f"[VERIFY] {origin} | document.has(c_user)={has_cuser} | cookies={names}")

        href = self.driver.execute_script("return location.href;")
        print(f"[AUTH] Final URL: {href}")

    # ---------- JS fetch ----------
    @staticmethod
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

    # ---------- Vars helpers ----------
    @staticmethod
    def update_vars_for_next_cursor(form: dict, next_cursor: str):
        try:
            base = json.loads(form.get("variables", "{}"))
        except Exception:
            base = {}
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

    # ---------- Main ----------
    def run(self):
        d = self.start_driver()
        self.install_early_hook()

        # 0) Auth bootstrap
        self.bootstrap_auth()

        # 1) Mở group & chờ request GraphQL đầu tiên
        d.get(self.group_url)
        time.sleep(1.2)
        for _ in range(6):
            d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
            time.sleep(0.6)

        nxt = self.wait_next_req(d, 0, _is_group_feed_req, timeout=25, poll=0.25)
        if not nxt:
            raise RuntimeError("Không bắt được request feed của group. Hãy cuộn thêm / kiểm tra quyền vào group.")
        _, first_req = nxt

        form = _parse_form(first_req.get("body", ""))
        self.friendly = urllib.parse.parse_qs(first_req.get("body", "")).get("fb_api_req_friendly_name", [""])[0]
        self.last_doc_id = form.get("doc_id")

        raw0 = first_req.get("responseText") or ""
        _dump_graphql_text(raw0, tag="page1", page_idx=1, seq=0)

        # parse cursor/has_next
        cleaned0 = _strip_xssi_prefix(raw0)
        objs0 = list(_iter_json_values(cleaned0))
        obj0 = objs0[0] if objs0 else None

        cursors = _deep_collect_cursors(obj0) if obj0 else []
        has_next = _deep_find_has_next(obj0) if obj0 else None
        if has_next is None:
            has_next = bool(cursors)
        end_cursor = cursors[0][1] if cursors else None
        if end_cursor:
            self.cursor = end_cursor

        print(f"[DEBUG] page1 cursors={len(cursors)} | has_next={has_next} | pick={str(end_cursor)[:24] if end_cursor else None}")
        print(f"[DEBUG] doc_id={self.last_doc_id} | friendly={self.friendly}")

        page = 1
        no_progress_rounds = 0

        # 2) Paginate + dump
        while True:
            page += 1
            if self.cursor:
                form = self.update_vars_for_next_cursor(form, self.cursor)

            txt = self.js_fetch_in_page(d, form, extra_headers={})
            _dump_graphql_text(txt, tag="paginate", page_idx=page, seq=0)

            cleaned = _strip_xssi_prefix(txt)
            objs = list(_iter_json_values(cleaned))
            obj = objs[0] if objs else None
            if not obj:
                _safe_write_text(os.path.join(RAW_DUMPS_DIR, f"page{page:03d}_raw.txt"), txt)
                print(f"[PAGE#{page}] parse fail → dumped raw, stop.")
                break

            cursors = _deep_collect_cursors(obj)
            has_next = _deep_find_has_next(obj)
            if has_next is None:
                has_next = bool(cursors)
            new_cursor = cursors[0][1] if cursors else None
            if new_cursor:
                self.cursor = new_cursor
                no_progress_rounds = 0
            else:
                no_progress_rounds += 1

            print(f"[PAGE#{page}] next={bool(has_next)} | cursor={str(self.cursor)[:24] if self.cursor else None}")

            MAX_NO_NEXT_ROUNDS = 3
            if not has_next and no_progress_rounds >= MAX_NO_NEXT_ROUNDS:
                print(f"[PAGE#{page}] next=False x{no_progress_rounds} → stop.")
                break

            time.sleep(random.uniform(0.7, 1.5))

        print("[DONE] Dump xong toàn bộ trang/response (raw + clean) trong folder:", RAW_DUMPS_DIR)


if __name__ == "__main__":
    crawler = GroupGraphQLCrawler(
        group_url=GROUP_URL,
        cookies_path=COOKIES_PATH,
        localstorage_path=LOCALSTORAGE_PATH,
        proxy_url=PROXY_URL,
        headless=HEADLESS,
    )
    crawler.run()
