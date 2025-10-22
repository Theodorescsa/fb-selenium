# -*- coding: utf-8 -*-
import json, re, time, urllib.parse, subprocess, os, uuid
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"
PROFILE_NAME  = "Profile 5"
REMOTE_PORT   = 9222
GROUP_URL     = "https://web.facebook.com/groups/laptrinhvienit"
GROUP_URL     = "https://www.facebook.com/ThuToiConfession"  # <— ĐỔI Ở ĐÂY

CHECKPOINT    = r"E:\NCS\fb-selenium\checkpoint.json"
RAW_DUMPS_DIR = "raw_dumps"
os.makedirs(RAW_DUMPS_DIR, exist_ok=True)

def install_early_hook(driver, keep_last=350):
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

def start_driver():
    subprocess.Popen([
        CHROME_PATH,
        f'--remote-debugging-port={REMOTE_PORT}',
        f'--user-data-dir={USER_DATA_DIR}',
        f'--profile-directory={PROFILE_NAME}',
    ])
    time.sleep(2)
    options = Options()
    options.add_experimental_option("debuggerAddress", f"127.0.0.1:{REMOTE_PORT}")
    return webdriver.Chrome(options=options)

def is_group_feed_req_body(body: str) -> bool:
    if not body: return False
    if "fb_api_req_friendly_name=" in body:
        if re.search(r"(?:GroupComet|CometGroup|GroupsComet).*(?:Feed|Stories).*Pagination", body, re.I):
            return True
    qs = urllib.parse.parse_qs(body, keep_blank_values=True)
    v = qs.get("variables", [""])[0]
    try:
        vj = json.loads(urllib.parse.unquote_plus(v))
        if any(k in vj for k in ["groupID","groupIDV2","id"]) and any(
            k in vj for k in ["after","cursor","endCursor","afterCursor","feedAfterCursor"]
        ):
            return True
    except: pass
    return False

def parse_form(body: str):
    qs = urllib.parse.parse_qs(body, keep_blank_values=True)
    return {k:(v[0] if isinstance(v, list) else v) for k,v in qs.items()}

# === robust JSON parsing ===
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
            if chunk == s[j:]:
                # nếu vẫn fail, cắt đoạn đến ký tự kết thúc gần nhất để tránh kẹt
                end = s.rfind('}', j)
                if end == -1: break
                try:
                    obj, k2 = dec.raw_decode(s[j:end+1], 0); yield obj; i = end+1
                except json.JSONDecodeError:
                    break
            else:
                try:
                    obj, k_rel = dec.raw_decode(chunk, 0); yield obj; i = j + k_rel
                except json.JSONDecodeError:
                    break

def choose_best_graphql_obj(objs):
    objs = list(objs)
    if not objs: return None
    with_data = [o for o in objs if isinstance(o, dict) and 'data' in o]
    pick = with_data or [o for o in objs if isinstance(o, dict)] or objs
    return max(pick, key=lambda o: len(json.dumps(o, ensure_ascii=False)))

def extract_first_story_text(obj: dict):
    def find_story(n):
        if isinstance(n, dict):
            if n.get("__typename") == "Story":
                if isinstance(n.get("message"), dict):
                    t = n["message"].get("text")
                    if t:
                        return t
                if isinstance(n.get("body"), dict):
                    t = n["body"].get("text")
                    if t:
                        return t
            for v in n.values():
                t = find_story(v)
                if t:
                    return t
        elif isinstance(n, list):
            for v in n:
                t = find_story(v)
                if t: return t
        return None
    return find_story(obj)

def choose_best_cursor(obj: dict):
    def deep(o):
        curs = []
        if isinstance(o, dict):
            pi = o.get("page_info") or o.get("pageInfo")
            if isinstance(pi, dict):
                ec = pi.get("end_cursor") or pi.get("endCursor")
                if isinstance(ec, str) and len(ec) > 10: curs.append(("page_info.end_cursor", ec))
            edges = o.get("edges")
            if isinstance(edges, list) and edges:
                last = edges[-1]
                if isinstance(last, dict):
                    c = last.get("cursor")
                    if isinstance(c, str) and len(c) > 10: curs.append(("edges[-1].cursor", c))
            for k, v in o.items():
                if k in {"cursor","after","endCursor","afterCursor","feedAfterCursor"} and isinstance(v, str) and len(v) > 10:
                    curs.append((k, v))
                curs.extend(deep(v))
        elif isinstance(o, list):
            for v in o: curs.extend(deep(v))
        return curs
    cand = deep(obj)
    if not cand: return None
    prio = {"page_info.end_cursor":3, "end_cursor":3, "endCursor":3, "edges[-1].cursor":2}
    cand.sort(key=lambda kv: (prio.get(kv[0],1), len(kv[1])), reverse=True)
    return cand[0][1]
def sanitize_headers(h: dict) -> dict:
    """Giữ lại các header cần thiết cho GraphQL, bỏ những cái trình duyệt tự set."""
    if not isinstance(h, dict): return {}
    drop = {
        "cookie","Cookie","host","Host","authority","Authority",
        "content-length","Content-Length","accept-encoding","Accept-Encoding",
        "connection","Connection","origin","Origin","referer","Referer",  # Referer/Origin để browser tự gắn
        # thêm các header đặc thù có thể gây reject nếu sai:
        "sec-ch-ua","Sec-CH-UA","sec-ch-ua-mobile","Sec-CH-UA-Mobile","sec-ch-ua-platform","Sec-CH-UA-Platform",
        "user-agent","User-Agent"
    }
    out = {}
    for k,v in h.items():
        if k in drop: 
            continue
        out[k] = v
    # đảm bảo 2 header quan trọng tồn tại nếu có trong body:
    # (nhiều build của FB yêu cầu x-fb-friendly-name + x-fb-lsd)
    return out
# --- put these utilities next to your current helpers ---

LIKELY_MSG_KEYS = {"message", "body", "title", "content", "textWithEntities"}

def _pick_first_nonempty(*vals):
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def _get(d, *path):
    cur = d
    for k in path:
        if not isinstance(cur, dict): return None
        cur = cur.get(k)
    return cur

def extract_text_from_story_node(n: dict):
    """
    Try common comet paths first, then recursive fallback for any {..., "text": "..."} 
    under keys that look like message/title/body.
    """
    if not isinstance(n, dict): 
        return None

    # 1) "simple" paths
    t = _pick_first_nonempty(
        _get(n, "message", "text"),
        _get(n, "body", "text"),
        _get(n, "title", "text"),
        _get(n, "message", "textWithEntities", "text"),
    )
    if t: return t

    # 2) comet_sections variations (FB hay nhét text ở đây)
    cs = n.get("comet_sections") or {}
    if isinstance(cs, dict):
        t = _pick_first_nonempty(
            _get(cs, "message", "message", "text"),
            _get(cs, "message", "story", "message", "text"),
            _get(cs, "content", "story", "message", "text"),
            _get(cs, "content", "message", "text"),
            _get(cs, "context_layout", "story", "message", "text"),
            _get(cs, "sub_message", "message", "text"),
            _get(cs, "title", "story", "message", "text"),
        )
        if t: return t

    # 3) attached / aggregated (share bài khác)
    t = _pick_first_nonempty(
        _get(n, "attached_story", "message", "text"),
        _get(n, "attached_story", "comet_sections", "message", "message", "text"),
        _get(n, "aggregated_story", "message", "text"),
        _get(n, "aggregated_story", "comet_sections", "message", "message", "text"),
    )
    if t: return t

    # 4) fallback: scan recursively for any dict under likely keys that has "text"
    def deep(n, parent_key=None):
        if isinstance(n, dict):
            # if this node looks like {text: "..."} and parent_key signals content
            if "text" in n and isinstance(n["text"], str) and parent_key in LIKELY_MSG_KEYS:
                txt = n["text"].strip()
                if txt:
                    return txt
            for k, v in n.items():
                r = deep(v, k)
                if r: return r
        elif isinstance(n, list):
            for v in n:
                r = deep(v, parent_key)
                if r: return r
        return None

    return deep(n)

def extract_first_k_posts(obj: dict, k=1):
    """Find the first K Story nodes; return list of dicts: {id, url, text}"""
    out = []
    def visit(n):
        if len(out) >= k: 
            return
        if isinstance(n, dict):
            if n.get("__typename") == "Story" or n.get("__isFeedUnit") == "Story":
                fb_id = n.get("id")
                url = n.get("wwwURL") or n.get("url")
                text = extract_text_from_story_node(n)
                out.append({"id": fb_id, "url": url, "text": text})
            for v in n.values():
                if len(out) >= k: break
                visit(v)
        elif isinstance(n, list):
            for v in n:
                if len(out) >= k: break
                visit(v)
    visit(obj)
    return out

def demo_resume_fetch_v3():
    # 0) read checkpoint
    with open(CHECKPOINT, "r", encoding="utf-8") as f:
        state = json.load(f)
    cursor = state.get("cursor")
    vars_template = state.get("vars_template") or {}
    if not cursor:
        print("❌ Checkpoint không có cursor. Crawl 1 lần trước đã nha.")
        return
    print(f"[DEBUG] resume cursor={cursor[:28]}…")

    # 1) start & hook
    d = start_driver()
    install_early_hook(d, keep_last=350)

    # 2) open & scroll to trigger graphql
    d.get(GROUP_URL)
    time.sleep(1.2)
    for _ in range(10):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
        time.sleep(0.5)

    # 3) capture 1 fresh feed request
    req = None
    t_end = time.time() + 25
    while time.time() < t_end and not req:
        arr = d.execute_script("return (window.__gqlReqs||[])") or []
        for r in reversed(arr):
            if "/api/graphql/" in (r.get("url") or "") and is_group_feed_req_body(r.get("body") or ""):
                req = r; break
        time.sleep(0.3)
    if not req:
        print("❌ Không bắt được request feed mới (login/quyền/scroll?)."); return
    req_headers = sanitize_headers(req.get("headers") or {})
    print(f"[DEBUG] friendly-name hdr = {req_headers.get('x-fb-friendly-name')}")
    # 4) parse fresh form
    form = parse_form(req.get("body",""))
    try:
        vars_now = json.loads(urllib.parse.unquote_plus(form.get("variables","{}")))
    except Exception:
        vars_now = {}

    # 5) merge template + inject cursor
    eff = dict(vars_now)
    for k,v in vars_template.items():
        if k not in {"cursor","after","endCursor","afterCursor","feedAfterCursor"}:
            eff[k] = v
    placed = False
    for key in ["cursor","after","endCursor","afterCursor","feedAfterCursor"]:
        if key in eff:
            eff[key] = cursor; placed = True; break
    if not placed:
        eff["cursor"] = cursor
    form["variables"] = json.dumps(eff, separators=(",",":"))

    # util: post & robust parse
    def post_and_parse(fd, extra_headers):
        script = """
        const url = "/api/graphql/";
        const form = arguments[0];
        const extra = arguments[1] || {};
        const headers = Object.assign({"Content-Type":"application/x-www-form-urlencoded"}, extra);
        const body = new URLSearchParams(form).toString();
        return fetch(url, {method:"POST", headers, body, credentials:"include"}).then(r=>r.text());
        """
        txt = d.execute_script(script, fd, extra_headers) or ""
        preview = txt[:300].replace("\n"," ")
        if "<!DOCTYPE html" in txt[:200]:
            print("❌ FB trả HTML (có thể chưa login / bị interstitial).")
        print(f"[DEBUG] resp preview: {preview!r}")
        stripped = _strip_xssi_prefix(txt)
        objs = list(iter_json_values(stripped))
        if not objs:
            path = os.path.join(RAW_DUMPS_DIR, f"demo_dump_{uuid.uuid4().hex[:8]}.txt")
            with open(path, "w", encoding="utf-8") as f: f.write(txt)
            print(f"⚠️ Không parse được JSON. Đã dump -> {path}")
            return None
        return choose_best_graphql_obj(objs)

    # 6) current page (cursor cũ)
    obj1 = post_and_parse(form, req_headers)
    if not obj1:
        return
    posts1 = extract_first_k_posts(obj1, k=3)   # lấy thử 3 bài đầu
    print("\n✅ Trang tại cursor cũ — preview:")
    for i, p in enumerate(posts1, 1):
        print(f"#{i} id={p['id']} url={p['url']}\n{(p['text'] or '<no text>').strip()}\n")

    # 7) next page
    next_cursor = choose_best_cursor(obj1)
    if not next_cursor:
        print("\n⚠️ Không tìm thấy end_cursor cho trang tiếp theo.")
        return
    eff2 = dict(eff)
    placed = False
    for key in ["cursor","after","endCursor","afterCursor","feedAfterCursor"]:
        if key in eff2:
            eff2[key] = next_cursor; placed = True; break
    if not placed:
        eff2["cursor"] = next_cursor
    form2 = dict(form)
    form2["variables"] = json.dumps(eff2, separators=(",",":"))
    obj2 = post_and_parse(form2, req_headers)
    if not obj2:
        return
    posts2 = extract_first_k_posts(obj2, k=3)
    print("\n➡️ Trang kế tiếp — preview:")
    for i, p in enumerate(posts2, 1):
        print(f"#{i} id={p['id']} url={p['url']}\n{(p['text'] or '<no text>').strip()}\n")
    t2 = extract_first_story_text(obj2)
    print("\n➡️ Trang kế tiếp — bài đầu tiên:")
    print(t2 or "<không có text>")

if __name__ == "__main__":
    demo_resume_fetch_v3()
