from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from utils import Utils
from config import *
import time, os, re, json, random, urllib.parse, subprocess
from pathlib import Path

RAW_OUT = getattr(globals(), "RAW_OUT", None) or "graphql_raw.jsonl"  # file raw duy nhất
import json, re, hashlib

POST_URL_RE = re.compile(r"https?://(?:web\.)?facebook\.com/groups/[^/]+/(?:permalink|posts)/(\d+)/?$", re.I)

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

def _deep_iter(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k, v
            if isinstance(v, (dict, list)):
                yield from _deep_iter(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _deep_iter(v)

def _is_story_node(n: dict) -> bool:
    if not isinstance(n, dict): return False
    if n.get("__typename") == "Story": return True
    if n.get("__isFeedUnit") == "Story": return True
    if "post_id" in n or "comet_sections" in n: return True
    return False

def _extract_url_digits(url: str):
    if not url: return None
    m = POST_URL_RE.match(url)
    return m.group(1) if m else None

def _rid_for(n: dict):
    post_id_api = n.get("post_id")
    fb_id      = n.get("id")
    url        = n.get("wwwURL") or n.get("url")
    url_digits = _extract_url_digits(url)
    base = post_id_api or url_digits or fb_id
    if base: return base
    raw = json.dumps(n, ensure_ascii=False, sort_keys=True)
    return "hash_" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]

def extract_posts(obj):
    """yield từng node post (giữ nguyên node gốc)"""
    if isinstance(obj, dict):
        if _is_story_node(obj):
            yield obj
        for v in obj.values():
            yield from extract_posts(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from extract_posts(v)

class Crawler:
    def __init__(self,
                 chrome_path=CHROME_PATH,
                 user_data_dir=USER_DATA_DIR,
                 profile_name=PROFILE_NAME,
                 remote_port=REMOTE_PORT,
                 group_url=GROUP_URL,
                 checkpoint=CHECKPOINT,
                 keep_last=KEEP_LAST,
                 raw_out=RAW_OUT,
                 posts_dir=POSTS_DIR):
        self.chrome_path = chrome_path
        self.user_data_dir = user_data_dir
        self.profile_name = profile_name
        self.remote_port = remote_port
        self.group_url = group_url
        self.checkpoint = checkpoint
        self.keep_last = keep_last
        self.raw_out = raw_out
        self.driver = None
        self.posts_dir = posts_dir
        Path(self.posts_dir).mkdir(parents=True, exist_ok=True)
        # ensure out dir exists
        Path(self.raw_out).parent.mkdir(parents=True, exist_ok=True)

    # ---------- selenium boot / hook ----------
    def start_driver(self):
        subprocess.Popen([
            self.chrome_path,
            f'--remote-debugging-port={self.remote_port}',
            f'--user-data-dir={self.user_data_dir}',
            f'--profile-directory={self.profile_name}'
        ])
        time.sleep(2)
        options = Options()
        options.add_experimental_option("debuggerAddress", f"127.0.0.1:{self.remote_port}")
        self.driver = webdriver.Chrome(options=options)

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
              rec.ts = Date.now();
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
                  const rec = {kind:'xhr', url:this.__u, method:this.__m, headers:{}, body:String(this.__b),
                               ts: Date.now(),
                               responseText:(typeof this.responseText==='string'?this.responseText:null)};
                  pushRec(rec);
                }
              }catch(e){}
            });
            return XS.apply(this, arguments);
          };
        })();
        """.replace("__KEEP_LAST__", str(self.keep_last))
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": HOOK_SRC})
        self.driver.execute_script(HOOK_SRC)
    def emit_posts_from_response(self, txt: str):
        """
        Từ response_text (GraphQL), tìm các node 'Story' và ghi mỗi post → 1 file JSON trong self.posts_dir.
        Dedupe theo rid bằng cách bỏ qua file đã tồn tại.
        """
        if not isinstance(txt, str) or not txt.strip():
            return 0, 0
        found = 0
        written = 0
        for obj in iter_json_values(_strip_xssi_prefix(txt)):
            for post in extract_posts(obj):
                found += 1
                rid = _rid_for(post)
                out_file = Path(self.posts_dir) / f"{rid}.json"
                if out_file.exists():
                    continue  # đã có → bỏ
                with open(out_file, "w", encoding="utf-8") as fo:
                    json.dump(post, fo, ensure_ascii=False, indent=2)
                written += 1
        return found, written
    # ---------- low-level buffer ----------
    @staticmethod
    def gql_count(d):
        return d.execute_script("return (window.__gqlReqs||[]).length")

    @staticmethod
    def get_gql_at(d, i):
        return d.execute_script("return (window.__gqlReqs||[])[arguments[0]]", i)

    @staticmethod
    def wait_next_req(d, start_idx, matcher, timeout=25, poll=0.25):
        end = time.time() + timeout
        cur = start_idx
        while time.time() < end:
            n = Crawler.gql_count(d)
            while cur < n:
                req = Crawler.get_gql_at(d, cur)
                if req and matcher(req):
                    return (cur, req)
                cur += 1
            time.sleep(poll)
        return None

    # ---------- matchers ----------
    @staticmethod
    def is_group_feed_req(rec):
        if "/api/graphql/" not in (rec.get("url") or ""): return False
        if (rec.get("method") or "").upper() != "POST": return False
        body = rec.get("body") or ""
        if "fb_api_req_friendly_name=" in body:
            if re.search(r"(?:GroupComet|CometGroup|GroupsComet).*(?:Feed|Stories).*Pagination", body, re.I):
                return True
        try:
            v = Utils.parse_form(body).get("variables","")
            vj = json.loads(urllib.parse.unquote_plus(v))
            if any(k in vj for k in ["groupID","groupIDV2","id"]) and any(
                k in vj for k in ["after","cursor","endCursor","afterCursor","feedAfterCursor"]
            ):
                return True
        except:
            pass
        return False

    # ---------- fetch with page cookies ----------
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

    @staticmethod
    def update_vars_for_next_cursor(form: dict, next_cursor: str, vars_template: dict = None):
        try:
            base = json.loads(form.get("variables", "{}"))
        except Exception:
            base = {}
        if vars_template:
            base = Utils.merge_vars(base, vars_template)
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

    # ---------- writer ----------
    def _append_raw(self, payload: dict):
        """
        Ghi một bản ghi JSON ra file self.raw_out (JSON lines).
        payload nên có các field:
          ts, friendly, doc_id, variables, cursor, response_text, kind
        """
        with open(self.raw_out, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    # ---------- run (raw only) ----------
    def run(self):
        self.start_driver()
        self.install_early_hook()

        d = self.driver
        d.get(self.group_url)
        time.sleep(1.2)
        for _ in range(6):
            d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
            time.sleep(0.6)

        nxt = Crawler.wait_next_req(d, 0, Crawler.is_group_feed_req, timeout=25, poll=0.25)
        if not nxt:
            raise RuntimeError("Không bắt được request feed. Hãy cuộn thêm / kiểm tra quyền.")
        idx, first_req = nxt
        form = Utils.parse_form(first_req.get("body", ""))
        friendly    = urllib.parse.parse_qs(first_req.get("body", "")).get("fb_api_req_friendly_name", [""])[0]
        doc_id      = form.get("doc_id")
        vars_now    = Utils.get_vars_from_form(form)
        template_now= Utils.make_vars_template(vars_now)

        state = Utils.load_checkpoint(CHECKPOINT)
        cursor        = state.get("cursor")
        vars_template = state.get("vars_template") or {}
        effective_template = vars_template or template_now

        # --- PAGE 1: parse & emit posts ---
        raw0 = first_req.get("responseText") or ""
        # Nếu vẫn muốn lưu raw, bỏ comment dòng dưới:
        # self._append_raw({"ts": int(time.time()*1000),"kind": first_req.get("kind"),"friendly": friendly,"doc_id": doc_id,"variables": vars_now,"cursor": None,"response_text": raw0})

        # Lưu mỗi post -> 1 file
        found, written = self.emit_posts_from_response(raw0)
        print(f"[PAGE#1 POSTS] found={found}, written={written} → {self.posts_dir}")

        # Tính cursor/has_next từ nội dung đã parse
        obj0 = Utils.choose_best_graphql_obj(Utils.iter_json_values(Utils._strip_xssi_prefix(raw0)))
        cursors = Utils.deep_collect_cursors(obj0) if obj0 else []
        has_next = Utils.deep_find_has_next(obj0) if obj0 else None
        if has_next is None:
            has_next = bool(cursors)
        if cursors:
            cursor = cursors[0][1]

        Utils.save_checkpoint(self.checkpoint, cursor, seen_ids=[], last_doc_id=doc_id,
                            last_query_name=friendly, vars_template=effective_template)
        print(f"[PAGE#1] next={bool(has_next)} | cursor={str(cursor)[:24] if cursor else None}")

        # --- PAGINATE ---
        page = 1
        while has_next:
            page += 1
            if cursor:
                form = Crawler.update_vars_for_next_cursor(form, cursor, vars_template=effective_template)

            txt = Crawler.js_fetch_in_page(d, form, extra_headers={})

            # Nếu vẫn muốn lưu raw, bỏ comment:
            # self._append_raw({"ts": int(time.time()*1000),"kind": "fetch","friendly": friendly,"doc_id": doc_id,"variables": Utils.get_vars_from_form(form),"cursor": cursor,"response_text": txt})

            # Lưu mỗi post -> 1 file
            found, written = self.emit_posts_from_response(txt)
            print(f"[PAGE#{page} POSTS] found={found}, written={written} → {self.posts_dir}")

            obj = Utils.choose_best_graphql_obj(Utils.iter_json_values(Utils._strip_xssi_prefix(txt)))
            if not obj:
                print(f"[PAGE#{page}] parse fail (đã lưu bài nếu có), dừng.")
                break

            cursors = Utils.deep_collect_cursors(obj)
            has_next = Utils.deep_find_has_next(obj)
            if has_next is None:
                has_next = bool(cursors)
            new_cursor = cursors[0][1] if cursors else None
            if new_cursor:
                cursor = new_cursor

            Utils.save_checkpoint(self.checkpoint, cursor, seen_ids=[], last_doc_id=doc_id,
                                last_query_name=friendly, vars_template=effective_template)
            print(f"[PAGE#{page}] next={bool(has_next)} | cursor={str(cursor)[:24] if cursor else None}")

            time.sleep(random.uniform(0.7, 1.5))

        print(f"[DONE] All posts saved as JSON files → {self.posts_dir}")
        print(f"[INFO] resume later with checkpoint: {self.checkpoint}")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    crawler = Crawler()
    crawler.run()
