# crawler.py
# -*- coding: utf-8 -*-
import os, re, json, time, random, urllib.parse, subprocess, socket
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from parser import Parser
from utils import Utils
from config import (
    CHROME_PATH, USER_DATA_DIR, PROFILE_NAME, REMOTE_PORT,
    GROUP_URL, OUT_NDJSON, RAW_DUMPS_DIR, CHECKPOINT, KEEP_LAST
)

class Crawler:
    def __init__(self,
                 chrome_path=CHROME_PATH,
                 user_data_dir=USER_DATA_DIR,
                 profile_name=PROFILE_NAME,
                 remote_port=REMOTE_PORT,
                 group_url=GROUP_URL,
                 out_ndjson=OUT_NDJSON,
                 raw_dir=RAW_DUMPS_DIR,
                 checkpoint=CHECKPOINT,
                 keep_last=KEEP_LAST,
                 headless=True,
                 wait_port_timeout=20.0):
        self.chrome_path = chrome_path
        self.user_data_dir = user_data_dir
        self.profile_name = profile_name
        self.remote_port = remote_port
        self.group_url = group_url
        self.out_ndjson = out_ndjson
        self.raw_dir = raw_dir
        self.checkpoint = checkpoint
        self.keep_last = keep_last
        self.headless = headless
        self.wait_port_timeout = wait_port_timeout

        self.driver = None
        self.parser = Parser(group_url)

        os.makedirs(self.raw_dir, exist_ok=True)
        Path(self.out_ndjson).parent.mkdir(parents=True, exist_ok=True)

    # --------------------- boot headless / normal ---------------------
    @staticmethod
    def _wait_port(host: str, port: int, timeout: float = 15.0, poll: float = 0.1) -> bool:
        end = time.time() + timeout
        while time.time() < end:
            try:
                with socket.create_connection((host, port), timeout=1):
                    return True
            except Exception:
                time.sleep(poll)
        return False

    def start_driver(self):
        args = [
            self.chrome_path,
            f'--remote-debugging-port={self.remote_port}',
            f'--user-data-dir={self.user_data_dir}',
            f'--profile-directory={self.profile_name}',
            '--no-first-run',
            '--no-default-browser-check',
            '--disable-extensions',
            '--disable-background-networking',
            '--disable-popup-blocking',
            '--disable-default-apps',
            '--disable-infobars',
        ]
        if self.headless:
            args += [
                '--headless=new',
                '--disable-gpu',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--window-size=1920,1080'
            ]

        proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ok = self._wait_port('127.0.0.1', self.remote_port, timeout=self.wait_port_timeout)
        if not ok and self.headless:
            # fallback: non-headless
            proc.kill(); time.sleep(0.4)
            args = [a for a in args if not a.startswith('--headless')]
            proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            ok = self._wait_port('127.0.0.1', self.remote_port, timeout=self.wait_port_timeout)
            if not ok:
                proc.kill()
                raise RuntimeError(f"Chrome remote debugging port {self.remote_port} not ready (fallback failed).")
        elif not ok:
            proc.kill()
            raise RuntimeError(f"Chrome remote debugging port {self.remote_port} not ready.")

        options = Options()
        options.add_experimental_option("debuggerAddress", f"127.0.0.1:{self.remote_port}")
        self.driver = webdriver.Chrome(options=options)

    # --------------------- hook GraphQL ---------------------
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
            if (q.length > __KEEP_LAST__) q.splice(0, q.length - __KEEP_LAST__);}catch(e){}}
          const origFetch = window.fetch;
          window.fetch = async function(input, init){
            const url = (typeof input==='string') ? input : (input&&input.url)||'';
            const method = (init&&init.method)||'GET';
            const body = (init && typeof init.body==='string') ? init.body : '';
            const hdrs = headersToObj(init && init.headers);
            let rec = null;
            if (url.includes('/api/graphql/') && method==='POST'){
              rec = {kind:'fetch', url, method, headers:hdrs, body:String(body)};}
            const res = await origFetch(input, init);
            if (rec){ try{ rec.responseText = await res.clone().text(); }catch(e){ rec.responseText = null; }
              pushRec(rec); }
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
                }}catch(e){}});
            return XS.apply(this, arguments);
          };
        })();
        """.replace("__KEEP_LAST__", str(self.keep_last))
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": HOOK_SRC})
        self.driver.execute_script(HOOK_SRC)

    # --------------------- buffer helpers ---------------------
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
                if req and matcher(req): return (cur, req)
                cur += 1
            time.sleep(poll)
        return None

    # --------------------- matchers / fetch ---------------------
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
                k in vj for k in ["after","cursor","endCursor","afterCursor","feedAfterCursor"]):
                return True
        except:
            pass
        return False

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
                if key in base: base[key] = next_cursor; changed = True
        if not changed:
            base["cursor"] = next_cursor
        if "count" in base and isinstance(base["count"], int):
            base["count"] = max(base["count"], 10)  # đảm bảo không về 1
        form["variables"] = json.dumps(base, separators=(",", ":"))
        return form

    # --------------------- reload & refresh form ---------------------
    def reload_and_refresh_form(self, cursor, effective_template, timeout=25, poll=0.25):
        d = self.driver
        d.get(self.group_url)
        time.sleep(1.5)
        for _ in range(4):
            d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
            time.sleep(0.5)

        nxt = Crawler.wait_next_req(d, 0, Crawler.is_group_feed_req, timeout=timeout, poll=poll)
        if not nxt:
            return None, None, None
        _, req = nxt
        new_form = Utils.parse_form(req.get("body", ""))
        new_friendly = urllib.parse.parse_qs(req.get("body", "")).get("fb_api_req_friendly_name", [""])[0]
        new_doc_id = new_form.get("doc_id")
        new_form = Crawler.update_vars_for_next_cursor(new_form, cursor, vars_template=effective_template)
        return new_form, new_friendly, new_doc_id

    # --------------------- run ---------------------
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
        _, first_req = nxt
        form = Utils.parse_form(first_req.get("body", ""))
        friendly = urllib.parse.parse_qs(first_req.get("body", "")).get("fb_api_req_friendly_name", [""])[0]
        vars_now = Utils.get_vars_from_form(form)
        template_now = Utils.make_vars_template(vars_now)

        state = Utils.load_checkpoint(self.checkpoint)
        seen_ids = Utils.normalize_seen_ids(state.get("seen_ids", []))
        cursor = state.get("cursor")
        vars_template = state.get("vars_template") or {}
        effective_template = vars_template or template_now

        total_written = 0

        # First page
        if cursor:
            print(f"[RESUME] Using saved cursor → jump directly. cursor={str(cursor)[:24]}..., friendly={friendly}")
            has_next = True
            page = 0
        else:
            raw0 = first_req.get("responseText") or ""
            obj0 = Utils.choose_best_graphql_obj(Utils.iter_json_values(Utils._strip_xssi_prefix(raw0)))
            if not obj0:
                open(os.path.join(self.raw_dir, "page1_raw.txt"), "w", encoding="utf-8").write(raw0)
                raise RuntimeError("Không parse được trang đầu; đã dump raw_dumps/page1_raw.txt")

            page_posts = []
            self.parser.collect_post_summaries(obj0, page_posts)
            page_posts = self.parser.filter_only_group_posts(page_posts)

            cursors = Utils.deep_collect_cursors(obj0)
            has_next = Utils.deep_find_has_next(obj0)
            if has_next is None: has_next = bool(cursors)
            if cursors: cursor = cursors[0][1]

            fresh = [p for p in page_posts if p.get("rid") and p["rid"] not in seen_ids]
            if fresh:
                Utils.append_ndjson(self.out_ndjson, fresh)
                for p in fresh:
                    if p.get("rid"): seen_ids.add(p["rid"])
                total_written += len(fresh)
            print(f"[PAGE#1] got {len(page_posts)} (new {len(fresh)}), next={bool(has_next)}")

            Utils.save_checkpoint(self.checkpoint, cursor, seen_ids,
                                  last_doc_id=form.get('doc_id'),
                                  last_query_name=friendly,
                                  vars_template=template_now)
            page = 1

        # Pagination
        no_progress_rounds = 0
        while has_next:
            page += 1
            if cursor:
                form = Crawler.update_vars_for_next_cursor(form, cursor, vars_template=effective_template)

            txt = Crawler.js_fetch_in_page(d, form, extra_headers={})
            obj = Utils.choose_best_graphql_obj(Utils.iter_json_values(Utils._strip_xssi_prefix(txt)))
            if not obj:
                open(os.path.join(self.raw_dir, f"page{page}_raw.txt"), "w", encoding="utf-8").write(txt)
                print(f"[PAGE#{page}] parse fail → dumped raw, break.")
                break

            page_posts = []
            self.parser.collect_post_summaries(obj, page_posts)
            page_posts = self.parser.filter_only_group_posts(page_posts)

            cursors = Utils.deep_collect_cursors(obj)
            has_next = Utils.deep_find_has_next(obj)
            if has_next is None: has_next = bool(cursors)
            if cursors: cursor = cursors[0][1]

            fresh = [p for p in page_posts if p.get("rid") and p["rid"] not in seen_ids]
            if fresh:
                Utils.append_ndjson(self.out_ndjson, fresh)
                for p in fresh:
                    if p.get("rid"): seen_ids.add(p["rid"])
                total_written += len(fresh)
                no_progress_rounds = 0
            else:
                no_progress_rounds += 1

            print(f"[PAGE#{page}] got {len(page_posts)} (new {len(fresh)}), total={total_written}, next={bool(has_next)} | cursor={str(cursor)[:24] if cursor else None}")
            Utils.save_checkpoint(self.checkpoint, cursor, seen_ids,
                                  last_doc_id=form.get('doc_id'),
                                  last_query_name=friendly,
                                  vars_template=effective_template)

            if no_progress_rounds >= 3:
                print(f"[PAGE#{page}] no new items 3 rounds → reload & resume from checkpoint cursor")
                Utils.save_checkpoint(self.checkpoint, cursor, seen_ids,
                                      last_doc_id=form.get('doc_id'),
                                      last_query_name=friendly,
                                      vars_template=effective_template)
                time.sleep(random.uniform(2.0, 4.0))

                new_form, new_friendly, _ = self.reload_and_refresh_form(cursor, effective_template, timeout=25, poll=0.25)
                if new_form:
                    form = new_form
                    friendly = new_friendly or friendly
                    no_progress_rounds = 0
                    continue
                else:
                    # fallback nudge UI
                    for _ in range(2):
                        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.8));")
                        time.sleep(0.5)
                    time.sleep(random.uniform(2.0, 4.0))
                    no_progress_rounds = 0

            time.sleep(random.uniform(0.7, 1.5))

        print(f"[DONE] wrote {total_written} posts → {self.out_ndjson}")
        print(f"[INFO] resume later with checkpoint: {self.checkpoint}")

    # --------------------- post process ---------------------
    def post_process_and_export(self):
        try:
            out_clean = Path(self.out_ndjson).with_name(Path(self.out_ndjson).stem + "_clean.ndjson")
            if os.path.exists(self.out_ndjson) and os.path.getsize(self.out_ndjson) > 0:
                stats = Utils.filter_and_dedupe_ndjson(self.out_ndjson, str(out_clean))
                print(f"[CLEAN] in={stats['total_in']} → removed≈{stats['removed']} → kept={stats['written']}")
                print(f"[CLEAN] NDJSON → {stats['out_path']}")
            else:
                print("[CLEAN] NDJSON rỗng, bỏ qua lọc & dedupe.")
                out_clean = None

            rows = []
            if out_clean and os.path.exists(out_clean):
                with open(out_clean, "r", encoding="utf-8") as f:
                    for line in f:
                        try: rows.append(json.loads(line))
                        except: pass

            if rows:
                df = pd.DataFrame(rows)
                if "type" in df.columns:
                    df = df[df["type"].astype(str).str.lower().eq("story")].copy()

                for col in ["like","love","haha","wow","sad","angry","care","comment","share"]:
                    if col not in df.columns: df[col] = 0
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

                df["__score"] = (df["like"] + df["love"] + df["haha"] + df["wow"] +
                                 df["sad"] + df["angry"] + df["care"] + df["comment"] + df["share"])
                df["__ct"] = pd.to_numeric(df.get("created_time"), errors="coerce").fillna(0).astype(int)

                want_cols = [
                    "id","type","link","author_id","author","author_link","avatar",
                    "created_time","content","image_url",
                    "like","comment","haha","wow","sad","love","angry","care","share",
                    "hashtag","video","source_id","is_share","link_share","type_share"
                ]
                for c in want_cols:
                    if c not in df.columns: df[c] = None

                if "rid" in df.columns:
                    df["id"] = df["id"].fillna(df["rid"])

                for col in ("image_url","video","hashtag"):
                    df[col] = df[col].map(Utils.as_json_list)

                if "is_share" in df.columns:
                    df["is_share"] = df["is_share"].map(Utils.as_int01)
                else:
                    df["is_share"] = 0

                df = df.sort_values(["__score","__ct"], ascending=[False, False])
                df = df[want_cols]

                out_xlsx = Path(out_clean).with_suffix(".xlsx") if out_clean else Path(self.out_ndjson).with_suffix(".xlsx")
                df.to_excel(out_xlsx, index=False)
                print(f"[EXPORT] Wrote Excel → {out_xlsx}")
            else:
                print("[EXPORT] Không có dòng hợp lệ sau khi lọc & dedupe, bỏ qua xuất Excel.")
        except Exception as e:
            print(f"[EXPORT] Lỗi xuất Excel: {e}")


# =========================
# MAIN (chạy như code thường)
# =========================
if __name__ == "__main__":
    crawler = Crawler(headless=True)   # đổi headless=False nếu feed rỗng khi chạy nền
    crawler.run()
    crawler.post_process_and_export()
