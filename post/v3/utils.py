import re, json
from configs import (GROUP_URL, REACTION_KEYS, POST_URL_RE)
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
    avatar = None
    try:
        avatar = actor.get("profile_picture", {}).get("uri")
    except:
        pass
    if not avatar:
        for k, v in _deep_iter(actor):
            if k in ("uri", "url") and isinstance(v, str) and v.startswith("http"):
                avatar = v; break
    raw_t = (actor.get("__typename") or actor.get("typename") or "").lower()
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
    image_urls, video_urls = [], []
    for k, v in _deep_iter(n):
        if k in ("image", "previewImage", "photo_image", "preferred_thumbnail"):
            if isinstance(v, dict):
                uri = v.get("uri") or v.get("url")
                if isinstance(uri, str) and uri.startswith("http"):
                    if uri not in image_urls:
                        image_urls.append(uri)
    for k, v in _deep_iter(n):
        if k in ("playable_url_quality_hd", "playable_url", "browser_native_hd_url", "browser_native_sd_url"):
            if isinstance(v, str) and v.startswith("http"):
                if v not in video_urls:
                    video_urls.append(v)
    return image_urls, video_urls

def extract_reactions_and_counts(n):
    counts = {v: 0 for v in REACTION_KEYS.values()}
    counts.update({"comment": 0, "share": 0})
    for k, v in _deep_iter(n):
        if k in ("comment_count", "total_comment_count", "commentsCount", "commentcount"):
            if isinstance(v, int): counts["comment"] = max(counts["comment"], v)
        if k in ("share_count", "sharecount", "resharesCount", "shareCount"):
            if isinstance(v, int): counts["share"] = max(counts["share"], v)
    for _k, v in _deep_iter(n):
        if isinstance(v, list) and v and isinstance(v[0], dict) and (
            ("reactionType" in v[0] and "count" in v[0]) or
            ("key" in v[0] and "total_count" in v[0])
        ):
            for it in v:
                rtype = (it.get("reactionType") or it.get("key") or "").upper()
                cnt = it.get("count") if "count" in it else it.get("total_count")
                if rtype in REACTION_KEYS and isinstance(cnt, int):
                    counts[REACTION_KEYS[rtype]] = max(counts[REACTION_KEYS[rtype]], cnt)
    for k, v in _deep_iter(n):
        if k in ("reaction_count", "reactionsCount", "top_reactions_total_count") and isinstance(v, int):
            if sum(counts[t] for t in REACTION_KEYS.values()) == 0:
                counts["like"] = v
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
    attached = None
    for k, v in _deep_iter(n):
        if k in ("attached_story", "attachedStory", "attached_share_story") and isinstance(v, dict):
            attached = v; break
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
    seen, out = set(), []
    for t in tags:
        if t not in seen:
            out.append(t); seen.add(t)
    return out

def _is_story_node(n: dict) -> bool:
    if n.get("__typename") == "Story": return True
    if n.get("__isFeedUnit") == "Story": return True
    if "post_id" in n or "comet_sections" in n: return True
    return False

def _get_text_from_node(n: dict):
    if isinstance(n.get("message"), dict):
        t = n["message"].get("text")
        if t: return t
    if isinstance(n.get("body"), dict):
        t = n["body"].get("text")
        if t: return t
    return None

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

def collect_post_summaries(obj, out, group_url=GROUP_URL):
    if isinstance(obj, dict):
        if _looks_like_group_post(obj):
            post_id_api = obj.get("post_id")
            fb_id      = obj.get("id")
            url        = obj.get("wwwURL") or obj.get("url")
            url_digits = _extract_url_digits(url)
            rid        = post_id_api or url_digits or fb_id
            author_id, author_name, author_link, avatar, type_label = extract_author(obj)
            text = _get_text_from_node(obj)
            image_urls, video_urls = extract_media(obj)
            counts = extract_reactions_and_counts(obj)
            created = extract_created_time(obj)
            is_share, link_share, type_share, origin_id = extract_share_flags(obj)
            hashtags = extract_hashtags(text)
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
                "id": fb_id,
                "rid": rid,
                "type": type_label,
                "link": url,
                "author_id": author_id,
                "author": author_name,
                "author_link": author_link,
                "avatar": avatar,
                "created_time": created,
                "content": text,
                "image_url": image_urls,
                "like": counts["like"],
                "comment": counts["comment"],
                "haha": counts["haha"],
                "wow": counts["wow"],
                "sad": counts["sad"],
                "love": counts["love"],
                "angry": counts["angry"],
                "care": counts["care"],
                "share": counts["share"],
                "hashtag": hashtags,
                "video": video_urls,
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

def filter_only_group_posts(items):
    keep = []
    for it in items:
        url = (it.get("url") or "").strip()
        fb_id = (it.get("id") or "").strip()
        if POST_URL_RE.match(url) or (isinstance(fb_id, str) and fb_id.startswith("Uzpf")) or it.get("post_id"):
            keep.append(it)
    return keep

# =========================
# Core JSON utils
# =========================
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