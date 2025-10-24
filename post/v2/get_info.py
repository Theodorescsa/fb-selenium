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
    # unique, stable order by lowercase
    seen, out = set(), []
    for t in tags:
        if t not in seen:
            out.append(t); seen.add(t)
    return out
