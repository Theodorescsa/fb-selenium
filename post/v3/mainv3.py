# -*- coding: utf-8 -*-
"""
Facebook GraphQL Recorder — record-all mode
- Captures EVERY /api/graphql/ POST response (fetch + XHR), no filtering.
- Saves 2 files per response: <tag>_<seq>_<ts>.raw.txt and <tag>_<seq>_<ts>.json
  * .raw.txt  : server text as-is (may include XSSI like "for (;;);")
  * .json     : cleaned text with XSSI stripped; if valid JSON, pretty-printed; else saved as cleaned text.
- Boots auth from cookies.json & localstorage.json (optional).
- Optional SOCKS5 proxy via selenium-wire (socks5h-like DNS through proxy).

⚠️ Only crawl places you have rights to access. Respect ToS.

Quick start:
  pip install selenium-wire webdriver-manager
  # Put cookies/localstorage exports next to this file (optional)
  python graphql_recorder.py --url "https://www.facebook.com/groups/<your-group>"

Tested with Chrome 120+ / selenium-wire 5.x

Author: you :)
"""

import os, re, json, time, datetime, random, urllib.parse, tempfile, hashlib, sys, argparse
from typing import Any, Dict, List, Optional, Tuple

# -----------------------------
# Config defaults (overridable via CLI or env)
# -----------------------------
GROUP_URL          = os.getenv("GROUP_URL", "https://www.facebook.com/thoibao.de")
RAW_DUMPS_DIR      = os.getenv("RAW_DUMPS_DIR", "raw_dumps")
COOKIES_PATH       = os.getenv("COOKIES_PATH", "./cookies.json")
LOCALSTORAGE_PATH  = os.getenv("LOCALSTORAGE_PATH", "./localstorage.json")
HEADLESS_DEFAULT   = (os.getenv("HEADLESS", "0") == "1")
KEEP_LAST          = int(os.getenv("KEEP_LAST", "400"))  # in-memory buffer in page
SCROLL_ROUNDS      = int(os.getenv("SCROLL_ROUNDS", "6"))

# proxy (optional) — if you want full-tunnel via SOCKS5 auth
SOCKS_ENABLED      = os.getenv("SOCKS_ENABLED", "0") == "1"
SOCKS_HOST         = os.getenv("SOCKS_HOST", "127.0.0.1")
SOCKS_PORT         = int(os.getenv("SOCKS_PORT", "1080"))
SOCKS_USER         = os.getenv("SOCKS_USER", "")
SOCKS_PASS         = os.getenv("SOCKS_PASS", "")

# generic HTTP proxy alternative (e.g., "http://user:pass@host:8080")
HTTP_PROXY_URL     = os.getenv("HTTP_PROXY_URL", "")  # left blank by default

# Facebook origins we may mirror cookies to:
FB_ORIGINS = [
    "https://www.facebook.com",
    "https://web.facebook.com",
    "https://m.facebook.com",
]

CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}  # not used here, but kept for completeness

os.makedirs(RAW_DUMPS_DIR, exist_ok=True)

# -----------------------------
# tiny JSON helpers
# -----------------------------
def _strip_xssi_prefix(s: str) -> str:
    if not isinstance(s, str):
        return s
    # common XSSI forms: "for (;;);" or "for(;;);"
    return re.sub(r'^\s*for\s*\(\s*;\s*;\s*\)\s*;\s*', '', s)

def iter_json_values(text: str):
    """Yield JSON root(s) from a text that might include XSSI or concatenated JSON."""
    if not text or not isinstance(text, str):
        return
    cleaned = text.strip()
    if not cleaned:
        return
    # Most GraphQL responses are single JSON object/array. Try fast path:
    try:
        yield json.loads(cleaned)
        return
    except Exception:
        pass
    # Fallback: very naive scan for JSON objects/arrays — robust enough for our use
    # (kept simple on purpose; we already save raw if this fails)
    stack = []
    start = None
    for i, ch in enumerate(cleaned):
        if ch in "{[":
            if not stack:
                start = i
            stack.append(ch)
        elif ch in "}]":
            if stack:
                open_ch = stack.pop()
                if ((open_ch == "{" and ch == "}") or (open_ch == "[" and ch == "]")) and not stack and start is not None:
                    chunk = cleaned[start:i+1]
                    try:
                        yield json.loads(chunk)
                    except Exception:
                        pass
                    start = None
    # if nothing yielded, caller can handle fallback

# -----------------------------
# Selenium-wire setup
# -----------------------------
from seleniumwire import webdriver  # pip install selenium-wire
from selenium.webdriver.chrome.options import Options

class GraphQLRecorder:
    def __init__(self,
                 url: str = GROUP_URL,
                 raw_dir: str = RAW_DUMPS_DIR,
                 cookies_path: Optional[str] = COOKIES_PATH,
                 localstorage_path: Optional[str] = LOCALSTORAGE_PATH,
                 headless: bool = HEADLESS_DEFAULT,
                 keep_last: int = KEEP_LAST,
                 socks_enabled: bool = SOCKS_ENABLED,
                 socks_host: str = SOCKS_HOST,
                 socks_port: int = SOCKS_PORT,
                 socks_user: str = SOCKS_USER,
                 socks_pass: str = SOCKS_PASS,
                 http_proxy_url: str = HTTP_PROXY_URL):
        self.url = url
        self.raw_dir = raw_dir
        self.cookies_path = cookies_path
        self.localstorage_path = localstorage_path
        self.headless = headless
        self.keep_last = keep_last

        self.socks_enabled = socks_enabled
        self.socks_host = socks_host
        self.socks_port = socks_port
        self.socks_user = socks_user
        self.socks_pass = socks_pass
        self.http_proxy_url = http_proxy_url

        self.driver: Optional[webdriver.Chrome] = None
        self._dumped_ids = set()
        self._seq_auto = 0

    # ---------- utils ----------
    @staticmethod
    def _ts() -> str:
        return datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f")

    @staticmethod
    def _safe_write(path, text, mode="w", encoding="utf-8"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, mode, encoding=encoding) as f:
            f.write(text)

    # ---------- driver ----------
    def start_driver(self) -> webdriver.Chrome:
        # proxy wiring
        wire_opts: Dict[str, Any] = {
            'connection_timeout': 30,
            'verify_ssl': True,
        }

        if self.http_proxy_url.strip():
            # Use explicit HTTP(S) upstream proxy
            wire_opts['proxy'] = {
                'http':  self.http_proxy_url,
                'https': self.http_proxy_url,
                'no_proxy': 'localhost,127.0.0.1'
            }
        elif self.socks_enabled:
            # SOCKS5 (auth optional)
            cred = ""
            if self.socks_user or self.socks_pass:
                cred = f"{self.socks_user}:{self.socks_pass}@"
            wire_opts['proxy'] = {
                'http':  f'socks5://{cred}{self.socks_host}:{self.socks_port}',
                'https': f'socks5://{cred}{self.socks_host}:{self.socks_port}',
                'no_proxy': 'localhost,127.0.0.1'
            }

        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1400,920")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-quic")
        # Force DNS through proxy (like socks5h) by disabling host resolver
        options.add_argument(r'--host-resolver-rules=MAP * ~NOTFOUND , EXCLUDE 127.0.0.1')

        # Ephemeral user data dir (don't use your real profile)
        tmp_ud = tempfile.mkdtemp(prefix="fbrecord_ud_")
        options.add_argument(f"--user-data-dir={tmp_ud}")

        driver = webdriver.Chrome(
            options=options,
            seleniumwire_options=wire_opts
        )
        self.driver = driver
        print(f"[DRIVER] Chrome started. Headless={self.headless}. Ephemeral profile: {tmp_ud}")
        if 'proxy' in wire_opts:
            print(f"[PROXY] selenium-wire proxy configured: {wire_opts['proxy']}")
        return driver

    # ---------- page hook ----------
    def install_early_hook(self):
        assert self.driver is not None
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
            this.__b = (typeof b==='string')?b:''; const self=this;
            this.addEventListener('load', ()=>{ try{
              if ((self.__u||'').includes('/api/graphql/') && (self.__m||'')==='POST'){
                pushRec({kind:'xhr', url:self.__u, method:self.__m, headers:{}, body:String(self.__b),
                         responseText:(typeof self.responseText==='string'?self.responseText:null)});
              }}catch(e){} });
            return XS.apply(this, arguments);
          };
        })();
        """.replace("__KEEP_LAST__", str(self.keep_last))
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": HOOK_SRC})
        self.driver.execute_script(HOOK_SRC)

    # ---------- record buffer accessors ----------
    @staticmethod
    def gql_count(d) -> int:
        try:
            return int(d.execute_script("return (window.__gqlReqs||[]).length"))
        except Exception:
            return 0

    @staticmethod
    def get_gql_at(d, i: int) -> Optional[dict]:
        try:
            return d.execute_script("return (window.__gqlReqs||[])[arguments[0]]", i)
        except Exception:
            return None

    # ---------- auth bootstrap ----------
    def _load_json_file(self, path: Optional[str]):
        if not path: return None
        if not os.path.exists(path): return None
        with open(path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except Exception:
                return None

    def _cdp_set_cookie(self, cookie: Dict[str, Any], url: Optional[str] = None) -> bool:
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
            res = self.driver.execute_cdp_cmd("Network.setCookie", params)
            return bool(res.get("success"))
        except Exception:
            return False

    def bootstrap_auth(self):
        assert self.driver is not None
        # Attempt to set cookies for domain + per-origin mirrors
        cookies_data = self._load_json_file(self.cookies_path)
        if isinstance(cookies_data, dict) and "cookies" in cookies_data:
            cookies_list = cookies_data["cookies"]
        elif isinstance(cookies_data, list):
            cookies_list = cookies_data
        else:
            cookies_list = []

        ok = 0
        for c in cookies_list:
            if self._cdp_set_cookie(c):
                ok += 1
        # Mirror to origins to be safe
        ok_host = 0
        for origin in FB_ORIGINS:
            for c in cookies_list:
                if self._cdp_set_cookie(c, url=origin + "/"):
                    ok_host += 1
        print(f"[AUTH] cookies set: domain_ok={ok}/{len(cookies_list)} | host_mirrors={ok_host}")

        # localStorage (best-effort)
        d = self.driver
        d.get("https://www.facebook.com/")
        time.sleep(1.0)
        ls_dict = self._load_json_file(self.localstorage_path)
        if isinstance(ls_dict, dict) and ls_dict:
            d.execute_script("window.localStorage.clear();")
            script = """
                const data = arguments[0];
                for (const [k,v] of Object.entries(data)) {
                    try { localStorage.setItem(k, (typeof v === 'string') ? v : JSON.stringify(v)); } catch(e) {}
                }
                return Object.keys(data).length;
            """
            set_count = d.execute_script(script, ls_dict)
            print(f"[AUTH] Injected {set_count} localStorage keys.")
            d.refresh()
            time.sleep(0.8)
        print(f"[AUTH] Ready at {d.execute_script('return location.href;')}")

    # ---------- identify + dump ----------
    @staticmethod
    def _rec_is_graphql(rec: dict) -> bool:
        try:
            return ("/api/graphql/" in (rec.get("url") or "")) and ((rec.get("method") or "").upper() == "POST")
        except Exception:
            return False

    @staticmethod
    def _rec_friendly_name(rec: dict) -> str:
        body = rec.get("body") or ""
        try:
            q = urllib.parse.parse_qs(body)
            friendly = q.get("fb_api_req_friendly_name", [""])[0]
            return re.sub(r"[^A-Za-z0-9._-]+", "_", friendly.strip())[:120] or "graphql"
        except Exception:
            return "graphql"

    @staticmethod
    def _rec_id(rec: dict) -> str:
        raw = f"{rec.get('url','')}|{rec.get('method','')}|{rec.get('body','')}|{len(rec.get('responseText') or '')}"
        return hashlib.md5(raw.encode("utf-8", errors="ignore")).hexdigest()

    def dump_rec_to_files(self, rec: dict, tag: Optional[str] = None):
        self._seq_auto += 1
        base_tag = tag or self._rec_friendly_name(rec)
        ts = self._ts()
        base = f"{base_tag}_{self._seq_auto:06d}_{ts}"
        raw_path   = os.path.join(self.raw_dir, f"{base}.raw.txt")
        clean_path = os.path.join(self.raw_dir, f"{base}.json")

        raw_text = rec.get("responseText") or ""
        self._safe_write(raw_path, raw_text)

        cleaned = _strip_xssi_prefix(raw_text)
        objs = list(iter_json_values(cleaned))
        try:
            if len(objs) == 1:
                self._safe_write(clean_path, json.dumps(objs[0], ensure_ascii=False, indent=2))
            elif len(objs) > 1:
                self._safe_write(clean_path, json.dumps(objs, ensure_ascii=False, indent=2))
            else:
                # fallback: save cleaned text; consumer can parse later
                self._safe_write(clean_path, cleaned)
        except Exception:
            self._safe_write(clean_path, cleaned)

    def flush_all_graphql_reqs(self, tag: Optional[str] = None, verbose: bool = True) -> int:
        assert self.driver is not None
        n = self.gql_count(self.driver)
        saved = 0
        for i in range(n):
            rec = self.get_gql_at(self.driver, i)
            if not rec or not self._rec_is_graphql(rec):
                continue
            rid = self._rec_id(rec)
            if rid in self._dumped_ids:
                continue
            self.dump_rec_to_files(rec, tag=tag)
            self._dumped_ids.add(rid)
            saved += 1
        if verbose and saved:
            print(f"[RECORD] dumped {saved} new GraphQL responses.")
        return saved

    # ---------- run (record-all loop) ----------
    def run(self):
        d = self.start_driver()
        self.install_early_hook()

        # Auth (optional but recommended)
        self.bootstrap_auth()

        # Open target URL & gently scroll to trigger requests
        d.get(self.url)
        time.sleep(1.2)
        for _ in range(SCROLL_ROUNDS):
            d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
            time.sleep(0.6)

        print("[MODE] RECORD-ALL: will save EVERY /api/graphql/ response as .raw.txt + .json")
        print("[HINT] Scroll/click/navigate in the tab to make more GraphQL requests...")

        idle_rounds = 0
        MAX_IDLE = 15  # ~30s without new responses (sleep=2s per loop)

        while True:
            # light scroll to trigger lazy loads
            try:
                d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.5));")
            except Exception:
                pass

            new_saved = self.flush_all_graphql_reqs(tag="graphql")
            idle_rounds = 0 if new_saved > 0 else (idle_rounds + 1)
            if idle_rounds >= MAX_IDLE:
                print("[RECORD] No new GraphQL responses for a while → stopping.")
                break

            time.sleep(2.0)

        print("[DONE] Recording finished. Check folder:", self.raw_dir)


# -----------------------------
# CLI
# -----------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Record all Facebook /api/graphql/ responses to files.")
    p.add_argument("--url", type=str, default=GROUP_URL, help="Start URL (group/feed/profile...)")
    p.add_argument("--raw-dir", type=str, default=RAW_DUMPS_DIR, help="Output directory")
    p.add_argument("--cookies", type=str, default=COOKIES_PATH, help="cookies.json path (exported)")
    p.add_argument("--localstorage", type=str, default=LOCALSTORAGE_PATH, help="localstorage.json path (exported)")
    p.add_argument("--headless", action="store_true", help="Run headless")
    p.add_argument("--no-headless", action="store_true", help="Force not headless")
    p.add_argument("--keep-last", type=int, default=KEEP_LAST, help="In-page buffer size for captured requests")
    p.add_argument("--socks", action="store_true", help="Enable SOCKS5 proxy from env vars")
    p.add_argument("--http-proxy", type=str, default=HTTP_PROXY_URL, help="HTTP proxy URL (e.g., http://user:pass@host:8080)")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    headless = HEADLESS_DEFAULT
    if args.headless:
        headless = True
    if args.no_headless:
        headless = False

    rec = GraphQLRecorder(
        url=args.url,
        raw_dir=args.raw_dir,
        cookies_path=args.cookies,
        localstorage_path=args.localstorage,
        headless=headless,
        keep_last=args.keep_last,
        socks_enabled=args.socks or SOCKS_ENABLED,
        socks_host=SOCKS_HOST,
        socks_port=SOCKS_PORT,
        socks_user=SOCKS_USER,
        socks_pass=SOCKS_PASS,
        http_proxy_url=args.http_proxy or HTTP_PROXY_URL
    )
    rec.run()
