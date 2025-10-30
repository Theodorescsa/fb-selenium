# ========= Full-field extractors (NON-BREAKING: only adds new helpers) =========
import datetime
import json
import re
from get_comment_fb_utils import find_pageinfo_any

_HASHTAG_RE = re.compile(r"(?:#|＃)([A-Za-z0-9_]+)", re.UNICODE)


# 1) Generic safe getters -------------------------------------------------------
def _as_list(x):
    """Return x as list, handling None gracefully."""
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def _first(*vals):
    """Return first non-empty value among vals."""
    for v in vals:
        if v not in (None, "", [], {}):
            return v
    return None


def _get_in(d, path, default=None):
    """
    Safe nested get.

    Args:
        d: dict/list root
        path: list[str|int]
        default: fallback

    Returns:
        Nested value or default.
    """
    cur = d
    for p in path:
        try:
            if isinstance(cur, dict):
                cur = cur.get(p)
            elif isinstance(cur, list) and isinstance(p, int):
                cur = cur[p]
            else:
                return default
        except Exception:
            return default
    return default if cur in (None, "", []) else cur


def _epoch_to_iso(ts):
    """Facebook epoch seconds -> ISO string."""
    try:
        ts = int(ts)
        return datetime.datetime.utcfromtimestamp(ts).isoformat() + "Z"
    except Exception:
        return None


def _pick_url(*cands):
    """Prefer http* urls among candidates, else first non-empty."""
    for c in cands:
        if isinstance(c, str) and c.startswith("http"):
            return c
    return _first(*cands)


def _pick_author(node):
    """
    Trả về (author_id, author_name, author_link, avatar).
    Tìm lần lượt trong owning_profile, actors[0], author/creator…
    """
    owning = _get_in(node, ["owning_profile"]) or _get_in(node, ["page", "profile"]) or {}
    actors0 = _get_in(node, ["actors", 0]) or {}
    author = _get_in(node, ["author"]) or _get_in(node, ["creator"]) or {}

    cand_objs = [owning, actors0, author]

    for obj in cand_objs:
        if not isinstance(obj, dict):
            continue
        aid = _first(obj.get("id"), obj.get("actor_id"), obj.get("profile_id"))
        name = _first(obj.get("name"), obj.get("display_name"))
        link = _pick_url(
            _get_in(obj, ["url"]),
            _get_in(obj, ["wwwURL"]),
            _get_in(obj, ["timeline_url"]),
            _get_in(obj, ["profile_url"]),
        )
        avatar = _first(
            _get_in(obj, ["profile_picture", "uri"]),
            _get_in(obj, ["profile_picture", "url"]),
            _get_in(obj, ["profilePicLarge", "uri"]),
            _get_in(obj, ["profilePicLarge", "url"]),
        )
        if any([aid, name, link, avatar]):
            return aid, name, link, avatar
    return None, None, None, None


def _pick_content(node):
    """Lấy nội dung text của post/comment theo nhiều khả năng."""
    return _first(
        _get_in(node, ["message", "text"]),
        _get_in(node, ["body", "text"]),
        _get_in(node, ["comet_sections", "content", "story", "message", "text"]),
        _get_in(node, ["comet_sections", "message", "text"]),
        _get_in(node, ["content", "text"]),
        _get_in(node, ["title", "text"]),
    )


def _pick_type(node):
    """Ưu tiên __typename; nếu không có thì suy từ attachments/media."""
    t = node.get("__typename")
    if t:
        return t
    attach = (
        _get_in(node, ["attachments", 0, "media"])
        or _get_in(node, ["comet_sections", "attachments", 0, "media"])
        or {}
    )
    if isinstance(attach, dict):
        if attach.get("__typename") in ("Photo", "Image"):
            return "photo"
        if attach.get("__typename") in ("Video", "PlayableVideo"):
            return "video"
    return "status"


def _pick_images(node):
    """Lấy ảnh đại diện của bài (nếu có)."""
    paths = [
        ["attachments", 0, "media", "image", "uri"],
        ["attachments", 0, "media", "photo_image", "uri"],
        ["comet_sections", "attachments", 0, "media", "image", "uri"],
        ["comet_sections", "attachments", 0, "media", "photo_image", "uri"],
    ]
    for p in paths:
        u = _get_in(node, p)
        if isinstance(u, str) and u.startswith("http"):
            return u
    return None


def _pick_video(node):
    """Pick playable_url / browser_native_hd_url nếu có."""
    return _first(
        _get_in(node, ["attachments", 0, "media", "playable_url"]),
        _get_in(node, ["attachments", 0, "media", "browser_native_hd_url"]),
        _get_in(node, ["comet_sections", "attachments", 0, "media", "playable_url"]),
        _get_in(node, ["video", "playable_url"]),
    )


def _pick_link(node):
    """Pick permalink/link của node."""
    return _pick_url(
        _get_in(node, ["wwwURL"]),
        _get_in(node, ["url"]),
        _get_in(node, ["permalink_url"]),
        _get_in(node, ["feedback", "wwwURL"]),
    )


def _pick_source_id(node):
    """group/page/source id nếu có."""
    return _first(
        _get_in(node, ["owning_profile", "id"]),
        _get_in(node, ["page", "id"]),
        _get_in(node, ["group", "id"]),
        _get_in(node, ["source", "id"]),
    )


def _pick_is_share_and_link(node):
    """Nếu là shared story: trả (True, link_share); ngược lại (False, None)."""
    attached = _get_in(node, ["attached_story"]) or _get_in(node, ["share", "attached_story"]) or {}
    if isinstance(attached, dict) and attached:
        return True, _pick_link(attached)
    return False, None


# --- Put near the top of your helpers (or keep your existing map and extend) ---
REACTION_ID_MAP = {
    # Facebook canonical IDs for reactions
    "1635855486666999": "like",
    "1678524932434102": "love",
    "613557422527858": "haha",
    "115940658764963": "wow",
    "478547315650144": "sad",
    "908563459236466": "angry",
    "444813342392137": "care",
}


def _norm_reaction_name(rtype_or_id: str | None) -> str | None:
    """Normalize reaction name or map id -> canonical name."""
    if not rtype_or_id:
        return None
    s = str(rtype_or_id).lower()
    alias = {
        "like": "like",
        "likes": "like",
        "love": "love",
        "haha": "haha",
        "wow": "wow",
        "sad": "sad",
        "angry": "angry",
        "care": "care",
        "thankful": "care",  # historical
    }.get(s)
    if alias:
        return alias
    return REACTION_ID_MAP.get(str(rtype_or_id))


def _reaction_breakdown_from_top_edges(fb: dict) -> dict:
    """
    Support ALL seen variants:
      A) lightweight_reactions: [{'type': 'LIKE', 'count': N}, ...]
      B) top_reactions.edges:   [{'count': N, 'node': {'reaction_type': 'LIKE'}}]
      C) top_reactions.edges:   [{'reaction_count': N, 'node': {'id': '1635…'}}]
    """
    out = {"like": 0, "love": 0, "haha": 0, "wow": 0, "sad": 0, "angry": 0, "care": 0}

    # Variant A: lightweight_reactions
    lw = ((fb or {}).get("reaction_summary", {}).get("lightweight_reactions")) or []
    for r in lw if isinstance(lw, list) else []:
        rname = _norm_reaction_name(r.get("type"))
        cnt = r.get("count") or 0
        if rname:
            try:
                out[rname] += int(cnt)
            except Exception:
                pass

    # Variants B & C: top_reactions.edges
    edges = ((fb or {}).get("top_reactions", {}) or {}).get("edges") or []
    for e in edges if isinstance(edges, list) else []:
        node = e.get("node") or {}
        cnt = e.get("count")
        if cnt is None:
            cnt = e.get("reaction_count")
        rname = _norm_reaction_name(node.get("reaction_type") or node.get("key") or node.get("id"))
        if rname:
            try:
                out[rname] += int(cnt or 0)
            except Exception:
                pass

    return out


def _reaction_totals(fb: dict) -> tuple[int, dict]:
    """Return (total_reactors, breakdown)."""
    total = (
        ((fb or {}).get("unified_reactors") or {}).get("count")
        or ((fb or {}).get("reactors") or {}).get("count")
        or 0
    )
    try:
        total = int(total)
    except Exception:
        total = 0
    breakdown = _reaction_breakdown_from_top_edges(fb)
    return total, breakdown


def _pick_reaction_counts(node: dict):
    """
    Post/story path (giữ nguyên signature cũ của bạn):
      -> returns (breakdown_dict, comment_count, share_count)
    """
    out = {"like": 0, "haha": 0, "wow": 0, "sad": 0, "love": 0, "angry": 0, "care": 0}

    fb = (node or {}).get("feedback") or {}

    comment_count = (
        _first(
            _get_in(node, ["feedback", "total_comment_count"]),
            _get_in(node, ["feedback", "comment_count"]),
            _get_in(node, ["comments", "count"]),
        )
        or 0
    )

    share_count = _first(
        _get_in(node, ["feedback", "share_count"]),
        _get_in(node, ["share_count"]),
    ) or 0

    _, bd = _reaction_totals(fb)
    out.update(bd)

    return out, int(comment_count), int(share_count)


def _pick_created_time(node):
    """Pick created time (ISO or raw)."""
    return _first(
        _epoch_to_iso(_get_in(node, ["creation_time"])),
        _get_in(node, ["created_time"]),
        _get_in(node, ["creation_time_string"]),
    )


def _extract_hashtags_from_text(text):
    """Extract hashtags from plain text; return CSV string or None."""
    if not text:
        return None
    tags = _HASHTAG_RE.findall(text)
    return ",".join(sorted(set(["#" + t for t in tags]))) if tags else None


# 2) Find candidate "post/story" nodes -----------------------------------------
def _iter_candidate_posts(obj):
    """
    Dò toàn bộ payload để tìm các node "bài viết" có id + feedback/message/attachments…
    Trả ra các dict gốc của node.
    """
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            if cur.get("id") and any(
                k in cur for k in ("feedback", "message", "attachments", "actors", "owning_profile")
            ):
                yield cur
            for v in cur.values():
                stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)


# 3) Public: extract full rows --------------------------------------------------
# ===== Comment-only extractor with de-dup & merge (non-breaking) =====

def _nz(*vals):
    """First non-empty (like _first) but kept separate for clarity in merge step."""
    for v in vals:
        if v not in (None, "", [], {}):
            return v
    return None


def _merge_counts(dst_val, src_val):
    """Merge numeric counters using max; keep original if cast fails."""
    try:
        return max(int(dst_val or 0), int(src_val or 0))
    except Exception:
        return dst_val or src_val or 0


def _get_comment_permalink(n: dict) -> str | None:
    """Get comment permalink from timestamp action link or feedback.url."""
    for al in n.get("comment_action_links", []) or []:
        if al.get("__typename") == "XFBCommentTimeStampActionLink":
            u = (((al.get("comment") or {}).get("url")) or "").strip()
            if u:
                return u
    fb = n.get("feedback") or {}
    u = (fb.get("url") or "").strip()
    return u or None


def _get_comment_created_time(n: dict) -> int | None:
    """Created time from action link; fallback to field on node."""
    for al in n.get("comment_action_links", []) or []:
        if al.get("__typename") == "XFBCommentTimeStampActionLink":
            ct = ((al.get("comment") or {}).get("created_time"))
            if isinstance(ct, int):
                return ct
    ct = n.get("created_time")
    return ct if isinstance(ct, int) else None


def _pick_comment_text(n: dict) -> str | None:
    """Prefer preferred_body > body_renderer > body."""
    for k in ("preferred_body", "body_renderer", "body"):
        t = ((n.get(k) or {}).get("text"))
        if isinstance(t, str) and t.strip():
            return t
    return None


def _author_block(n: dict):
    """Return author basic info tuple."""
    a = n.get("author") or {}
    return a.get("id"), a.get("name"), a.get("url") or a.get("profile_url")


def _get_image_url_if_any(n: dict) -> str | None:
    """Pick media image/thumb from attachments if available."""
    for att in n.get("attachments", []) or []:
        media = att.get("media") or {}
        uri = (
            (media.get("image") or {}).get("uri")
            or (media.get("photo_image") or {}).get("uri")
        )
        if isinstance(uri, str) and uri.startswith("http"):
            return uri
        thumb = (media.get("previewImage") or {}).get("uri")
        if isinstance(thumb, str) and thumb.startswith("http"):
            return thumb
    return None


def _reply_count(fb: dict, n: dict) -> int:
    """Count replies from replies_fields or edges length."""
    rf = (fb or {}).get("replies_fields") or {}
    if isinstance(rf.get("total_count"), int):
        return rf["total_count"]
    rc = (fb or {}).get("replies_connection") or (n.get("replies_connection") or {})
    edges = (rc or {}).get("edges") or []
    return len(edges) if isinstance(edges, list) else 0


def _iter_comment_nodes(root):
    """
    Yield top-level Comment nodes:
      - data.node.comment_rendering_instance_for_feed_location.comments.edges[].node
      - fallback: data.node.feedback.comment_rendering_instance.comments.edges[].node
    Cuối cùng mới fallback quét toàn bộ.
    """
    # 1) Schema mới (Comet)
    try:
        cr = (root["data"]["node"]
                  ["comment_rendering_instance_for_feed_location"]["comments"])
        for edge in cr.get("edges", []):
            n = edge.get("node")
            if isinstance(n, dict) and n.get("__typename") == "Comment":
                yield n
        return
    except Exception:
        pass

    # 2) Fallback schema cũ qua feedback
    try:
        tl = (root["data"]["node"]["feedback"]
                   ["comment_rendering_instance"]["comments"])
        for edge in tl.get("edges", []):
            n = edge.get("node")
            if isinstance(n, dict) and n.get("__typename") == "Comment":
                yield n
        return
    except Exception:
        pass

    # 3) Fallback cuối: quét toàn bộ (ít ưu tiên)
    stack = [root]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            if cur.get("__typename") == "Comment":
                yield cur
            for v in cur.values():
                stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)

def extract_full_posts_from_resptext(resp_text: str):
    try:
        obj = json.loads(resp_text)
    except Exception:
        return [], None, None, None

    payloads = obj if isinstance(obj, list) else [obj]
    end_cursor, has_next, total = None, None, None
    by_id = {}  # id -> row (merged)

    for pay in payloads:
        # page/cursor
        try:
            ec, hn = find_pageinfo_any(pay)
            if ec:
                end_cursor = ec
            if hn is not None:
                has_next = hn
        except Exception:
            pass

        # thread total (nếu có)
        try:
            c = pay["data"]["node"]["comment_rendering_instance_for_feed_location"]["comments"]
            total = c.get("count") or c.get("total_count") or total
        except Exception:
            pass

        # collect comment nodes & reduce
        for n in _iter_comment_nodes(pay):
            cid = n.get("id") or n.get("legacy_fbid")
            if not cid:
                continue

            fb = n.get("feedback") or {}

            author_id, author_name, author_link = _author_block(n)
            row_new = {
                "id": cid,
                "type": "Comment",
                "link": _get_comment_permalink(n),
                "author_id": author_id,
                "author": author_name,
                "author_link": author_link,
                "avatar": None,  # có thể thêm nếu payload có
                "created_time": _get_comment_created_time(n),
                "content": _pick_comment_text(n),
                "image_url": _get_image_url_if_any(n),
                # reactions breakdown
                "like": 0,
                "haha": 0,
                "wow": 0,
                "sad": 0,
                "love": 0,
                "angry": 0,
                "care": 0,
                "comment": _reply_count(fb, n),
                "share": 0,
                "hashtag": None,
                "video": None,
                "source_id": None,
                "is_share": False,
                "link_share": None,
                "type_share": "shared_none",
            }

            # breakdown fill
            bd = _reaction_breakdown_from_top_edges(fb)
            for k in ("like", "haha", "wow", "sad", "love", "angry", "care"):
                row_new[k] = int(bd.get(k, 0))

            # merge into by_id
            prev = by_id.get(cid)
            if not prev:
                by_id[cid] = row_new
            else:
                # prefer non-empty values; counts take max
                for k in (
                    "link",
                    "author_id",
                    "author",
                    "author_link",
                    "avatar",
                    "created_time",
                    "content",
                    "image_url",
                    "hashtag",
                    "video",
                    "source_id",
                    "link_share",
                ):
                    prev[k] = _nz(prev.get(k), row_new.get(k))
                for k in ("like", "haha", "wow", "sad", "love", "angry", "care", "comment", "share"):
                    prev[k] = _merge_counts(prev.get(k), row_new.get(k))
                # fixed flags
                prev["is_share"] = prev.get("is_share", False) or row_new.get("is_share", False)
                prev["type_share"] = prev.get("type_share") or row_new.get("type_share")

    rows = list(by_id.values())
    return rows, end_cursor, total, obj
