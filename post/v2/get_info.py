# === replace / thêm các helper sau ===
import pandas as pd
from pathlib import Path
import re
REACTION_KEYS = {
    "LIKE": "like", "LOVE": "love", "HAHA": "haha", "WOW": "wow",
    "SAD": "sad", "ANGRY": "angry", "CARE": "care"
}

def _deep_iter(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k, v
            if isinstance(v, (dict, list)):
                yield from _deep_iter(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _deep_iter(v)

def deep_get_first(obj, want_keys):
    want = {k.lower() for k in want_keys}
    for k, v in _deep_iter(obj):
        if isinstance(k, str) and k.lower() in want:
            return k, v
    return None, None

def extract_author(n):
    actor = None
    if isinstance(n.get("actors"), list) and n["actors"]:
        actor = n["actors"][0]
    elif isinstance(n.get("author"), dict):
        actor = n["author"]
    if not isinstance(actor, dict):
        actor = {}

    name = actor.get("name") or actor.get("title") or actor.get("text")
    aid  = actor.get("id")
    link = actor.get("url") or actor.get("wwwURL") or actor.get("profile_url")

    # avatar
    avatar = None
    try:
        avatar = actor.get("profile_picture", {}).get("uri")
    except:
        pass
    if not avatar:
        for k, v in _deep_iter(actor):
            if k in ("uri", "url") and isinstance(v, str) and v.startswith("http"):
                avatar = v; break

    # entity type -> label "facebook page/profile/group"
    raw_t = actor.get("__typename") or actor.get("typename") or ""
    raw_t = (raw_t or "").lower()
    if "page" in raw_t:
        etype = "facebook page"
    elif "user" in raw_t or "profile" in raw_t:
        etype = "facebook profile"
    elif "group" in raw_t:
        etype = "facebook group"
    else:
        etype = "story"

    return aid, name, link, avatar, etype

def extract_media(n):
    """Trả về (image_urls[], video_urls[])"""
    image_urls, video_urls = [], []

    # image candidates
    for k, v in _deep_iter(n):
        if k in ("image", "previewImage", "photo_image", "preferred_thumbnail"):
            if isinstance(v, dict):
                uri = v.get("uri") or v.get("url")
                if isinstance(uri, str) and uri.startswith("http"):
                    if uri not in image_urls:
                        image_urls.append(uri)

    # video candidates
    for k, v in _deep_iter(n):
        if k in ("playable_url_quality_hd", "playable_url", "browser_native_hd_url", "browser_native_sd_url"):
            if isinstance(v, str) and v.startswith("http"):
                if v not in video_urls:
                    video_urls.append(v)

    return image_urls, video_urls


# --- [1] BỔ SUNG: map id → reaction key (hay gặp trên Comet UFI)
REACTION_ID_MAP = {
    # core set
    "1635855486666999": "like",     # Thích / Like
    "1678524932434102": "love",     # Yêu thích / Love
    "115940658764963":  "haha",     # Haha
    "478547315650144":  "wow",      # Wow
    "908563459236466":  "sad",      # Buồn / Sad
    "444813342392137":  "angry",    # Phẫn nộ / Angry
    # (CARE ít xuất hiện trong top_reactions mới — FB thay đổi theo thời điểm)
}

# --- [2] BỔ SUNG: chuẩn hoá localized_name → reaction key
def _norm_reaction_name(name: str) -> str | None:
    if not isinstance(name, str): 
        return None
    s = name.strip().lower()
    mapping = {
        "thích": "like", "like": "like",
        "yêu thích": "love", "yêu": "love", "love": "love",
        "haha": "haha",
        "wow": "wow",
        "buồn": "sad", "sad": "sad",
        "phẫn nộ": "angry", "giận dữ": "angry", "angry": "angry",
        "care": "care", "quan tâm": "care",
    }
    return mapping.get(s)
# --- [3] REPLACE: extract_reactions_and_counts với hỗ trợ đầy đủ UFI (new + old)
def extract_reactions_and_counts(n):
    """
    Trích xuất reactions / comment / share từ cả kiểu cũ (UFI cũ) lẫn kiểu mới (Comet):
      - feedback.reaction_count.count
      - feedback.top_reactions.edges[].node.{id|localized_name} + reaction_count
      - share_count.count hoặc i18n_share_count (string)
      - comment_rendering_instance.comments.total_count
      - (fallback) total_comment_count, comment_count, display_comments_count, ...
    Trả về dict: {"like","love","haha","wow","sad","angry","care","comment","share"}
    """
    counts = {v: 0 for v in REACTION_KEYS.values()}
    counts.update({"comment": 0, "share": 0})

    # ---- A) SHARE (new style + fallback string)
    for k, v in _deep_iter(n):
        if k == "share_count" and isinstance(v, dict):
            c = v.get("count")
            if isinstance(c, int):
                counts["share"] = max(counts["share"], c)
        if k == "i18n_share_count":  # "1.2K", "5"...
            try:
                s = str(v).replace(".", "").replace(",", "")
                # FB VN thường dùng "." làm thousand sep trong i18n — ta gỡ hết
                c = int(s)
                counts["share"] = max(counts["share"], c)
            except:
                pass
        # Kiểu cũ (đôi khi có nguyên int):
        if k in ("sharecount", "resharesCount") and isinstance(v, int):
            counts["share"] = max(counts["share"], v)

    # ---- B) COMMENT (đủ pattern)
    # 1) Comet summary renderer (mới)
    #    comet_ufi_summary_and_actions_renderer.feedback.comment_rendering_instance.comments.total_count
    total_cmt = 0
    for k, v in _deep_iter(n):
        if k == "comments_count_summary_renderer" and isinstance(v, dict):
            fb = v.get("feedback") or {}
            # path 1: như JSON bạn gửi
            cri = fb.get("comment_rendering_instance") or {}
            comments = cri.get("comments") or {}
            tc = comments.get("total_count")
            if isinstance(tc, int):
                total_cmt = max(total_cmt, tc)
            # path 2: đôi khi đặt tên khác
            tlc = cri.get("top_level_comments") or {}
            tc2 = tlc.get("count")
            if isinstance(tc2, int):
                total_cmt = max(total_cmt, tc2)

    # 2) Rải rác ở các field khác (fallback)
    for k, v in _deep_iter(n):
        if k in ("total_comment_count", "comment_count", "commentsCount", "display_comments_count"):
            if isinstance(v, int):
                total_cmt = max(total_cmt, v)
        # Có nơi wrap thành dict {count: <int>}
        if k == "comment_count" and isinstance(v, dict):
            c = v.get("count")
            if isinstance(c, int):
                total_cmt = max(total_cmt, c)
        if k == "i18n_comment_count":
            try:
                c = int(str(v).replace(".", "").replace(",", ""))
                total_cmt = max(total_cmt, c)
            except:
                pass
    counts["comment"] = max(counts["comment"], total_cmt)

    # ---- C) REACTIONS (new style breakdown + total)
    found_breakdown = False
    # 1) breakdown mới: top_reactions.edges[].node.{id|localized_name} + reaction_count
    for k, v in _deep_iter(n):
        if k == "top_reactions" and isinstance(v, dict):
            edges = v.get("edges") or []
            for e in edges:
                if not isinstance(e, dict): 
                    continue
                node = (e.get("node") or {})
                rid  = node.get("id")
                rname= node.get("localized_name")
                rkey = None
                if isinstance(rid, str) and rid in REACTION_ID_MAP:
                    rkey = REACTION_ID_MAP[rid]
                if not rkey and rname:
                    rkey = _norm_reaction_name(rname)
                rc = e.get("reaction_count")
                if rkey in REACTION_KEYS.values() and isinstance(rc, int):
                    counts[rkey] = max(counts[rkey], rc)
                    found_breakdown = True

    # 2) tổng (new style): reaction_count.count
    total_any = 0
    for k, v in _deep_iter(n):
        if k == "reaction_count" and isinstance(v, dict):
            c = v.get("count")
            if isinstance(c, int):
                total_any = max(total_any, c)
    if total_any and not found_breakdown:
        # fallback: nếu không có breakdown thì dồn vào 'like' (giữ hành vi cũ)
        counts["like"] = max(counts["like"], total_any)

    # ---- D) REACTIONS (kiểu cũ list [reactionType/key]:count/total_count)
    for _k, v in _deep_iter(n):
        if isinstance(v, list) and v and isinstance(v[0], dict) and (
            ("reactionType" in v[0] and "count" in v[0]) or
            ("key" in v[0] and "total_count" in v[0])
        ):
            for it in v:
                rtype = (it.get("reactionType") or it.get("key") or "")
                cnt = it.get("count") if "count" in it else it.get("total_count")
                if isinstance(rtype, str):
                    rtype = rtype.upper()
                if rtype in REACTION_KEYS and isinstance(cnt, int):
                    counts[REACTION_KEYS[rtype]] = max(counts[REACTION_KEYS[rtype]], cnt)

    return counts


def extract_created_time(n):
    t = n.get("creation_time") or n.get("created_time") or n.get("creationTime")
    if not t:
        for k, v in _deep_iter(n):
            if k in ("creation_time", "created_time", "creationTime") and isinstance(v, (int, float, str)):
                t = v; break
    try:
        return int(t)
    except:
        return t

def extract_share_flags(n):
    is_share = False
    link_share = None
    type_share = None
    source_id = None

    # v1: attached_story trực tiếp
    attached = None
    for k, v in _deep_iter(n):
        if k in ("attached_story", "attachedStory", "attached_share_story") and isinstance(v, dict):
            attached = v
            break

    # v2: Comet sections
    if not attached and isinstance(n.get("comet_sections"), dict):
        sec = n["comet_sections"]
        for kk in ("attached_story", "context_layout", "content"):
            node = sec.get(kk)
            if isinstance(node, dict) and isinstance(node.get("story"), dict):
                attached = node["story"]; break

    if attached:
        is_share = True
        link_share = attached.get("wwwURL") or attached.get("url")
        type_share = attached.get("__typename")
        source_id = attached.get("id")

    return is_share, link_share, type_share, source_id


HASHTAG_RE = re.compile(r"(#\w+)", re.UNICODE)
def extract_hashtags(text):
    if not isinstance(text, str): return []
    tags = [t.lower() for t in HASHTAG_RE.findall(text)]
    # unique, stable order by lowercase
    seen, out = set(), []
    for t in tags:
        if t not in seen:
            out.append(t); seen.add(t)
    return out

from urllib.parse import urlparse
URL_RE = re.compile(r'https?://[^\s)>\]"]+', re.I)

FB_HOSTS = {
    "facebook.com","www.facebook.com","m.facebook.com","web.facebook.com",
    "fb.watch","fb.me","fb.com"
}
def _clean_url(u:str)->str:
    if not isinstance(u,str): return ""
    u=u.strip()
    # bỏ dấu ')' hoặc '…' dư ở cuối
    return u.rstrip(').,]»›…')

def _is_fb(u:str)->bool:
    try:
        host = urlparse(u).netloc.lower()
        host = host.split(":")[0]
        return any(host==h or host.endswith("."+h) for h in FB_HOSTS)
    except: 
        return False

def _all_urls_from_text(s: str):
    if not isinstance(s,str) or not s: return []
    urls = [ _clean_url(m.group(0)) for m in URL_RE.finditer(s) ]
    # unique, giữ thứ tự
    seen=set(); out=[]
    for u in urls:
        if u not in seen:
            out.append(u); seen.add(u)
    return out

def _dig_attachment_urls(n:dict):
    """
    Lục các URL trong attachments/shareable để lấy OG meta.
    Trả về (urls, meta) với meta có og_title/og_desc/site_name nếu có.
    """
    urls, meta = [], {}
    def take(u):
        u=_clean_url(u)
        if u and u not in urls:
            urls.append(u)

    def dive(x):
        if isinstance(x, dict):
            # các key hay chứa URL
            for k in ("url","canonical_url","source","href","permalink_url","external_url"):
                v=x.get(k)
                if isinstance(v,str): take(v)
            # OG-esque meta
            for (k1,k2) in (("title","og_title"),("subtitle","og_desc"),("site_name","og_site_name"),("publisher","og_site_name")):
                if isinstance(x.get(k1), dict) and isinstance(x[k1].get("text"), str):
                    meta.setdefault(k2, x[k1]["text"].strip())
                elif isinstance(x.get(k1), str):
                    meta.setdefault(k2, x[k1].strip())

            for v in x.values(): dive(v)
        elif isinstance(x, list):
            for v in x: dive(v)
    dive(n)
    return urls, meta

def extract_share_flags_smart(n: dict, actor_text: str = None):
    """
    Trả về: (is_share, link_share, type_share, origin_id, share_meta)
    - link_share: ưu tiên URL 'ngoài FB'. Nếu không có → nếu share bài FB thì trả permalink FB.
    - type_share: 'link' nếu out-domain, 'post' nếu là share nội bộ FB.
    - origin_id: id bài gốc nếu tóm được
    - share_meta: {og_title, og_desc, og_site_name} nếu có
    """
    is_share, link_share, type_share, origin_id = False, None, None, None
    share_meta = {}

    cs = n.get("comet_sections") or {}
    # 1) cố nhìn attached/content story (nếu share)
    cand_nodes = []
    if isinstance(cs, dict):
        for k in ("attached_story","content","context_layout"):
            v = cs.get(k)
            if isinstance(v, dict):
                cand_nodes.append(v)
                if isinstance(v.get("story"), dict):
                    cand_nodes.append(v["story"])

    # 2) attachments các kiểu
    if isinstance(n.get("attachments"), (list,dict)):
        cand_nodes.append(n["attachments"])
    if isinstance(n.get("story_attachment"), dict):
        cand_nodes.append(n["story_attachment"])

    # gom URL + meta trong attachments
    att_urls = []
    for node in cand_nodes:
        u, meta = _dig_attachment_urls(node)
        att_urls.extend(u)
        share_meta.update({k:v for k,v in meta.items() if v})

    # 3) URL trong caption
    text_urls = _all_urls_from_text(actor_text or "")

    # 4) hợp nhất ứng viên URL
    all_urls = []
    for arr in (text_urls, att_urls):
        for u in arr:
            if u not in all_urls:
                all_urls.append(u)

    # 5) quyết định link_share/type_share
    # ưu tiên link ngoài FB
    ext = [u for u in all_urls if not _is_fb(u)]
    if ext:
        link_share = ext[0]
        type_share = "link"
        is_share = True
    else:
        # fallback: nếu có permalink/canonical FB của bài đính kèm → share post
        fb_urls = [u for u in all_urls if _is_fb(u)]
        if fb_urls:
            link_share = fb_urls[0]
            type_share = "post"
            is_share = True

    # 6) origin_id (nếu mò thấy)
    for node in cand_nodes:
        if isinstance(node, dict):
            for key in ("id","post_id","legacy_api_post_id","shareable_id","target_id"):
                val = node.get(key)
                if isinstance(val, str) and val.isdigit():
                    origin_id = val; break
        if origin_id: break

    return is_share, link_share, type_share, origin_id, share_meta
