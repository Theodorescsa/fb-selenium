# -*- coding: utf-8 -*-
"""
Facebook Group GraphQL crawler — Class-based (resume-first)
- Không dùng profile thật; nạp cookies + localStorage trước khi crawl
- Ưu tiên: dump response JSON của từng request GraphQL ra file riêng (raw + clean)
- Resume-first bằng cursor trong checkpoint

⚠️ Chỉ crawl nơi bạn có quyền. Tôn trọng ToS.
"""

import os, re, json, time, random, socket, subprocess, urllib.parse, datetime
import shutil, tempfile
from typing import Any, Dict, List, Optional, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ===== Bạn có thể giữ utils/configs cũ nếu muốn =====
from utils import (
    _strip_xssi_prefix, iter_json_values, choose_best_graphql_obj,
    collect_post_summaries, filter_only_group_posts)

# === Nếu bạn đã có configs.py, thêm 2 biến COOKIES_PATH, LOCALSTORAGE_PATH trong đó
#     Còn không, block dưới sẽ tự fallback giá trị mặc định.
try:
    from configs import (
        CHROME_PATH, USER_DATA_DIR, PROFILE_NAME, REMOTE_PORT,
        GROUP_URL, KEEP_LAST, OUT_NDJSON, RAW_DUMPS_DIR, CHECKPOINT,
        ENABLE_PARSE, CURSOR_KEYS, COOKIES_PATH, LOCALSTORAGE_PATH, FB_ORIGINS
    )
except Exception:
    # ---- fallback nhanh nếu bạn không import configs ----
    CHROME_PATH = r""
    USER_DATA_DIR = r""      # nên trỏ thư mục TRỐNG, không phải profile thật
    PROFILE_NAME  = ""
    REMOTE_PORT   = 9222
    GROUP_URL     = "https://www.facebook.com/groups/<your-group-id-or-slug>"
    KEEP_LAST     = 350
    OUT_NDJSON    = "posts_all.ndjson"
    RAW_DUMPS_DIR = "raw_dumps"
    CHECKPOINT    = "checkpoint.json"
    ENABLE_PARSE  = False
    CURSOR_KEYS   = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}
    COOKIES_PATH      = "./cookies.json"        # <-- file bạn vừa export
    LOCALSTORAGE_PATH = "./localstorage.json"   # <-- file bạn vừa export

os.makedirs(RAW_DUMPS_DIR, exist_ok=True)


class GroupGraphQLCrawler:
    def __init__(self,
                 group_url: str = GROUP_URL,
                 chrome_path: str = CHROME_PATH,
                 user_data_dir: str = USER_DATA_DIR,
                 profile_name: str = PROFILE_NAME,
                 remote_port: int = REMOTE_PORT,
                 raw_dir: str = RAW_DUMPS_DIR,
                 checkpoint_path: str = CHECKPOINT,
                 out_ndjson: str = OUT_NDJSON,
                 keep_last: int = KEEP_LAST,
                 headless: bool = True,  # <-- mặc định KHÔNG headless để bạn kiểm tra login
                 cookies_path: Optional[str] = COOKIES_PATH,
                 localstorage_path: Optional[str] = LOCALSTORAGE_PATH):
        self.group_url = group_url
        self.chrome_path = chrome_path
        self.user_data_dir = user_data_dir
        self.profile_name = profile_name
        self.remote_port = remote_port
        self.raw_dir = raw_dir
        self.checkpoint_path = checkpoint_path
        self.out_ndjson = out_ndjson
        self.keep_last = keep_last
        self.headless = headless
        self.cookies_path = cookies_path
        self.localstorage_path = localstorage_path

        self.driver: Optional[webdriver.Chrome] = None
        self.friendly: Optional[str] = None
        self.last_doc_id: Optional[str] = None
        self.vars_template: Dict[str, Any] = {}
        self.seen_ids = set()
        self.cursor: Optional[str] = None

    # ---------- Boot ----------
    def _wait_port(self, host: str, port: int, timeout: float = 15.0, poll: float = 0.1) -> bool:
        end = time.time() + timeout
        while time.time() < end:
            try:
                with socket.create_connection((host, port), timeout=1):
                    return True
            except Exception:
                time.sleep(poll)
        return False

    def start_driver(self) -> webdriver.Chrome:
        options = Options()

        # Optional: nếu bạn biết rõ chrome_path
        if self.chrome_path:
            options.binary_location = self.chrome_path
        else:
            # Thử tự tìm binary; nếu không thấy, Selenium Manager vẫn tự handle
            for cand in ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser"]:
                if shutil.which(cand):
                    options.binary_location = shutil.which(cand)
                    break

        # Linux flags
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1400,920")

        # Headless hay không tuỳ bạn
        if self.headless:
            options.add_argument("--headless=new")

        # user-data-dir: nếu trống, tạo tạm để ngăn Chrome đè vào profile mặc định
        if not self.user_data_dir:
            self.user_data_dir = tempfile.mkdtemp(prefix="fbcrawl_ud_")
        options.add_argument(f"--user-data-dir={self.user_data_dir}")

        # KHÔNG cần remote debugging port / subprocess
        self.driver = webdriver.Chrome(options=options)
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
        """.replace("__KEEP_LAST__", str(self.keep_last))
        assert self.driver is not None
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": HOOK_SRC})
        self.driver.execute_script(HOOK_SRC)

    # ---------- GQL buffer helpers ----------
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

    # ---------- Matching & parsing ----------
    @staticmethod
    def parse_form(body_str: str) -> Dict[str, str]:
        qs = urllib.parse.parse_qs(body_str, keep_blank_values=True)
        return {k:(v[0] if isinstance(v, list) else v) for k,v in qs.items()}

    @staticmethod
    def is_group_feed_req(rec) -> bool:
        if "/api/graphql/" not in (rec.get("url") or ""): return False
        if (rec.get("method") or "").upper() != "POST": return False
        body = rec.get("body") or ""
        if "fb_api_req_friendly_name=" in body:
            if re.search(r"(?:GroupComet|CometGroup|GroupsComet).*(?:Feed|Stories).*Pagination", body, re.I):
                return True
        try:
            v = GroupGraphQLCrawler.parse_form(body).get("variables","")
            vj = json.loads(urllib.parse.unquote_plus(v))
            if any(k in vj for k in ["groupID","groupIDV2","id"]) and any(
                k in vj for k in ["after","cursor","endCursor","afterCursor","feedAfterCursor"]
            ):
                return True
        except:
            pass
        return False

    # ---------- Vars helpers ----------
    @staticmethod
    def get_vars_from_form(form_dict):
        try:
            return json.loads(form_dict.get("variables", "{}")) if form_dict else {}
        except:
            return {}

    @staticmethod
    def make_vars_template(vars_dict):
        if not isinstance(vars_dict, dict): return {}
        t = dict(vars_dict)
        for k in list(t.keys()):
            if k in CURSOR_KEYS: del t[k]
        return t

    @staticmethod
    def merge_vars(base_vars, template_vars):
        if not isinstance(base_vars, dict): base_vars = {}
        if not isinstance(template_vars, dict): template_vars = {}
        out = dict(base_vars)
        for k, v in template_vars.items():
            if k in CURSOR_KEYS: continue
            out[k] = v
        return out

    @staticmethod
    def update_vars_for_next_cursor(form: dict, next_cursor: str, vars_template: dict = None):
        try:
            base = json.loads(form.get("variables", "{}"))
        except Exception:
            base = {}
        if vars_template:
            base = GroupGraphQLCrawler.merge_vars(base, vars_template)
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

    # ---------- Cursor discovery ----------
    @staticmethod
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

    @staticmethod
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

    # ---------- Dump helpers ----------
    @staticmethod
    def _ts() -> str:
        return datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f")

    @staticmethod
    def _safe_write(path, text, mode="w", encoding="utf-8"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, mode, encoding=encoding) as f:
            f.write(text)

    def dump_graphql_text(self, raw_text: str, tag: str, page_idx: int, seq: int = 0):
        if not isinstance(raw_text, str):
            raw_text = str(raw_text)
        base = f"{tag}_p{page_idx:03d}_{seq:02d}_{self._ts()}"
        raw_path   = os.path.join(self.raw_dir, f"{base}.raw.txt")
        clean_path = os.path.join(self.raw_dir, f"{base}.json")
        self._safe_write(raw_path, raw_text)
        cleaned = _strip_xssi_prefix(raw_text)
        objs = list(iter_json_values(cleaned))
        try:
            if len(objs) == 1:
                self._safe_write(clean_path, json.dumps(objs[0], ensure_ascii=False, indent=2))
            elif len(objs) > 1:
                self._safe_write(clean_path, json.dumps(objs, ensure_ascii=False, indent=2))
            else:
                self._safe_write(clean_path, cleaned)
        except Exception:
            self._safe_write(clean_path, cleaned)

    # ---------- Cookie & localStorage bootstrap ----------
    def _load_json_file(self, path: Optional[str]):
        if not path: return None
        if not os.path.exists(path): return None
        with open(path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except Exception:
                return None

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
        """
        Tạo cookie host-only cho 1 origin cụ thể bằng cách truyền 'url' (KHÔNG truyền domain).
        Điều này giúp đảm bảo cookie gắn chặt vào www/web/m host.
        """
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
            "url": url,                 # <— host-only theo origin
            "path": path,
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

        # 1) Load cookies từ file
        cookies_data = self._load_json_file(self.cookies_path)
        if isinstance(cookies_data, dict) and "cookies" in cookies_data:
            cookies_list = cookies_data["cookies"]
        elif isinstance(cookies_data, list):
            cookies_list = cookies_data
        else:
            cookies_list = []

        # 1a) Set theo domain (nếu có) — cho đủ bộ
        ok_domain = 0
        for c in cookies_list:
            if self._cdp_set_cookie(c):
                ok_domain += 1
        print(f"[AUTH] Domain-scoped cookies: {ok_domain}/{len(cookies_list)}")

        # 1b) Mirror host-only cho từng origin www/web/m — đặc biệt quan trọng cho c_user/xs/fr
        ok_host = 0
        for origin in FB_ORIGINS:
            for c in cookies_list:
                if self._cdp_set_cookie_for_url(c, origin):
                    ok_host += 1
        print(f"[AUTH] Host-only mirrors set: {ok_host} (over {len(cookies_list)*len(FB_ORIGINS)} attempts)")

        # 2) Inject localStorage sau khi vào www
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

        # 3) Verify theo từng origin: mở URL, check cookie ở document & qua CDP
        self.driver.execute_cdp_cmd("Network.enable", {})
        for origin in FB_ORIGINS:
            self.driver.get(origin + "/")
            time.sleep(0.8)
            has_cuser = self.driver.execute_script("return document.cookie.includes('c_user');")
            # đọc cookies gửi cho URL này
            got = self.driver.execute_cdp_cmd("Network.getCookies", {"urls": [origin + "/"]})
            names = sorted([c.get("name","") for c in (got.get("cookies") or [])])
            print(f"[VERIFY] {origin} | document.has(c_user)={has_cuser} | cookies={names}")

        # 4) In location cuối
        href = self.driver.execute_script("return location.href;")
        print(f"[AUTH] Final URL: {href}")

    # ---------- Checkpoint ----------
    def load_checkpoint(self):
        if not os.path.exists(self.checkpoint_path):
            return {"cursor": None, "seen_ids": [], "last_doc_id": None, "last_query_name": None, "vars_template": {}, "ts": None}
        try:
            with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {"cursor": None, "seen_ids": [], "last_doc_id": None, "last_query_name": None, "vars_template": {}, "ts": None}

    def save_checkpoint(self, cursor, seen_ids, last_doc_id=None, last_query_name=None, vars_template=None):
        data = {
            "cursor": cursor,
            "seen_ids": list(seen_ids)[:200000],
            "last_doc_id": last_doc_id,
            "last_query_name": last_query_name,
            "vars_template": vars_template or {},
            "ts": datetime.datetime.now().isoformat(timespec="seconds")
        }
        with open(self.checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    @staticmethod
    def normalize_seen_ids(seen_ids):
        return set(seen_ids or [])

    # ---------- Reload + soft refetch ----------
    def reload_and_refresh_form(self, d, cursor, effective_template, timeout=25, poll=0.25):
        d.get(self.group_url)
        time.sleep(1.5)
        for _ in range(4):
            d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
            time.sleep(0.5)
        nxt = self.wait_next_req(d, 0, self.is_group_feed_req, timeout=timeout, poll=poll)
        if not nxt: return None, None, None
        _, req = nxt
        new_form = self.parse_form(req.get("body", ""))
        new_friendly = urllib.parse.parse_qs(req.get("body","")).get("fb_api_req_friendly_name", [""])[0]
        new_doc_id = new_form.get("doc_id")
        new_form = self.update_vars_for_next_cursor(new_form, cursor, vars_template=effective_template)
        return new_form, new_friendly, new_doc_id

    def soft_refetch_form_and_cursor(self, driver, form, effective_template):
        try:
            base = json.loads(form.get("variables", "{}"))
        except Exception:
            base = {}
        base = self.merge_vars(base, effective_template)
        base = {k: v for k, v in base.items() if k not in CURSOR_KEYS}
        new_form = dict(form)
        new_form["variables"] = json.dumps(base, separators=(",", ":"))
        txt = self.js_fetch_in_page(driver, new_form, extra_headers={})
        self.dump_graphql_text(txt, tag="softrefetch_boot", page_idx=0, seq=0)
        obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
        if not obj:
            return None, None, None, None
        cursors = self.deep_collect_cursors(obj)
        new_has_next = self.deep_find_has_next(obj)
        if new_has_next is None:
            new_has_next = bool(cursors)
        new_cursor = cursors[0][1] if cursors else None
        return new_form, new_cursor, new_has_next, obj

    # ---------- Append NDJSON ----------
    def append_ndjson(self, items):
        if not items: return
        with open(self.out_ndjson, "a", encoding="utf-8") as f:
            for it in items:
                f.write(json.dumps(it, ensure_ascii=False) + "\n")

    # ---------- Main ----------
    def run(self):
        d = self.start_driver()

        # 0) NẠP COOKIES + LOCALSTORAGE (không headless để bạn nhìn)
        self.bootstrap_auth()

        # 1) Cài hook
        self.install_early_hook()

        # 2) Vào group
        d.get(self.group_url)
        time.sleep(1.2)
        for _ in range(6):
            d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
            time.sleep(0.6)

        nxt = self.wait_next_req(d, 0, self.is_group_feed_req, timeout=25, poll=0.25)
        if not nxt:
            raise RuntimeError("Không bắt được request feed của group. Hãy cuộn thêm / kiểm tra quyền vào group.")
        idx, first_req = nxt

        form = self.parse_form(first_req.get("body", ""))
        self.friendly = urllib.parse.parse_qs(first_req.get("body", "")).get("fb_api_req_friendly_name", [""])[0]
        self.last_doc_id = form.get("doc_id")
        vars_now = self.get_vars_from_form(form)
        template_now = self.make_vars_template(vars_now)

        raw0 = first_req.get("responseText") or ""
        self.dump_graphql_text(raw0, tag="page1", page_idx=1, seq=0)

        # state
        state = self.load_checkpoint()
        self.seen_ids = self.normalize_seen_ids(state.get("seen_ids", []))
        self.cursor = state.get("cursor")
        self.vars_template = state.get("vars_template") or {}
        total_written = 0

        effective_template = self.vars_template or template_now

        if self.cursor:
            print(f"[RESUME] Using saved cursor → jump directly. cursor={str(self.cursor)[:24]}..., friendly={self.friendly}")
            has_next = True
            page = 0
        else:
            obj0 = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(raw0)))
            if not obj0:
                open(os.path.join(self.raw_dir, "page1_raw.txt"), "w", encoding="utf-8").write(raw0)
                raise RuntimeError("Không parse được trang đầu; đã dump raw_dumps/page1_raw.txt")

            if ENABLE_PARSE:
                page_posts = []
                collect_post_summaries(obj0, page_posts)
                page_posts = filter_only_group_posts(page_posts)

            cursors = self.deep_collect_cursors(obj0)
            has_next = self.deep_find_has_next(obj0)
            if has_next is None:
                has_next = bool(cursors)
            end_cursor = cursors[0][1] if cursors else None
            if end_cursor:
                self.cursor = end_cursor

            print(f"[DEBUG] page1 posts={(len(page_posts) if ENABLE_PARSE else '—dump-only—')} | cursors={len(cursors)} | has_next={has_next} | pick={str(end_cursor)[:24] if end_cursor else None}")
            print(f"[DEBUG] doc_id={form.get('doc_id')} | friendly={self.friendly}")

            if ENABLE_PARSE:
                fresh = [p for p in page_posts if p.get("rid") and p["rid"] not in self.seen_ids]
                self.append_ndjson(fresh)
                for p in fresh:
                    if p.get("rid"): self.seen_ids.add(p["rid"])
                total_written += len(fresh)
                print(f"[PAGE#1] got {len(page_posts)} (new {len(fresh)}), next={bool(has_next)}")
            else:
                print(f"[PAGE#1] dump-only mode. next={bool(has_next)}")

            self.save_checkpoint(self.cursor, self.seen_ids, last_doc_id=form.get('doc_id'),
                                 last_query_name=self.friendly, vars_template=template_now)
            page = 1

        # paginate
        no_progress_rounds = 0
        while True:
            page += 1
            if self.cursor:
                form = self.update_vars_for_next_cursor(form, self.cursor, vars_template=effective_template)

            txt = self.js_fetch_in_page(d, form, extra_headers={})
            self.dump_graphql_text(txt, tag="paginate", page_idx=page, seq=0)

            obj = choose_best_graphql_obj(iter_json_values(_strip_xssi_prefix(txt)))
            if not obj:
                open(os.path.join(self.raw_dir, f"page{page}_raw.txt"), "w", encoding="utf-8").write(txt)
                print(f"[PAGE#{page}] parse fail → dumped raw, break.")
                break

            if ENABLE_PARSE:
                page_posts = []
                collect_post_summaries(obj, page_posts)
                page_posts = filter_only_group_posts(page_posts)

            cursors = self.deep_collect_cursors(obj)
            has_next = self.deep_find_has_next(obj)
            if has_next is None:
                has_next = bool(cursors)
            new_cursor = cursors[0][1] if cursors else None
            if new_cursor:
                self.cursor = new_cursor

            if ENABLE_PARSE:
                fresh = [p for p in page_posts if p.get("rid") and p["rid"] not in self.seen_ids]
                if fresh:
                    self.append_ndjson(fresh)
                    for p in fresh:
                        if p.get("rid"): self.seen_ids.add(p["rid"])
                    total_written += len(fresh)
                    no_progress_rounds = 0
                else:
                    no_progress_rounds += 1
            else:
                no_progress_rounds = 0 if new_cursor else (no_progress_rounds + 1)

            print(f"[PAGE#{page}] {(f'got {len(page_posts)} (new {len(fresh)}), ' if ENABLE_PARSE else '')}total={total_written}, next={bool(has_next)} | cursor={str(self.cursor)[:24] if self.cursor else None}")

            self.save_checkpoint(self.cursor, self.seen_ids, last_doc_id=form.get('doc_id'),
                                 last_query_name=self.friendly, vars_template=effective_template)

            MAX_NO_NEXT_ROUNDS = 3
            if not has_next and no_progress_rounds >= MAX_NO_NEXT_ROUNDS:
                print(f"[PAGE#{page}] next=False x{no_progress_rounds} → soft-refetch doc_id/variables (no UI)")
                self.save_checkpoint(self.cursor, self.seen_ids, last_doc_id=form.get('doc_id'),
                                     last_query_name=self.friendly, vars_template=effective_template)

                refetch_ok = False
                for attempt in range(1, 3):
                    new_form, boot_cursor, boot_has_next, boot_obj = self.soft_refetch_form_and_cursor(d, form, effective_template)
                    if new_form and (boot_cursor or boot_has_next):
                        form = new_form
                        if boot_cursor: self.cursor = boot_cursor
                        has_next = bool(boot_has_next)
                        no_progress_rounds = 0
                        refetch_ok = True
                        print(f"[PAGE#{page}] soft-refetch OK (attempt {attempt}) → has_next={has_next} | cursor={str(self.cursor)[:24] if self.cursor else None}")
                        break
                    time.sleep(random.uniform(1.0, 2.0))

                if not refetch_ok:
                    print(f"[PAGE#{page}] soft-refetch failed → stop pagination.")
                    break

            time.sleep(random.uniform(0.7, 1.5))

        print(f"[DONE] wrote {total_written} posts → {self.out_ndjson}")
        print(f"[INFO] resume later with checkpoint: {self.checkpoint_path}")


if __name__ == "__main__":
    crawler = GroupGraphQLCrawler(
        group_url=GROUP_URL,
        chrome_path=CHROME_PATH,
        user_data_dir=USER_DATA_DIR,   # Nên là THƯ MỤC TRỐNG (không phải profile thật)
        profile_name="Default",
        remote_port=REMOTE_PORT,
        raw_dir=RAW_DUMPS_DIR,
        checkpoint_path=CHECKPOINT,
        out_ndjson=OUT_NDJSON,
        keep_last=KEEP_LAST,
        headless=True,               # để bạn xem đã login hay chưa
        cookies_path=COOKIES_PATH,
        localstorage_path=LOCALSTORAGE_PATH,
    )
    crawler.run()
