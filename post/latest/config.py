# =========================
# CONFIG — nhớ sửa GROUP_URL
# =========================
import os, re
CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"
PROFILE_NAME  = "Profile 5"
REMOTE_PORT   = 9222

# GROUP_URL     = "https://web.facebook.com/groups/laptrinhvienit"   # <— ĐỔI Ở ĐÂY
GROUP_URL     = "https://www.facebook.com/thoibao.de"               # <— ĐỔI Ở ĐÂY
KEEP_LAST     = 350
OUT_NDJSON    = "posts_all.ndjson"
RAW_DUMPS_DIR = "raw_dumps"
CHECKPOINT    = r"E:\NCS\fb-selenium\checkpoint.json"

os.makedirs(RAW_DUMPS_DIR, exist_ok=True)

REACTION_KEYS = {
    "LIKE": "like", "LOVE": "love", "HAHA": "haha", "WOW": "wow",
    "SAD": "sad", "ANGRY": "angry", "CARE": "care"
}
CURSOR_KEYS   = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}
POST_URL_RE   = re.compile(r"https?://(?:web\.)?facebook\.com/groups/[^/]+/(?:permalink|posts)/(\d+)/?$", re.I)
HASHTAG_RE    = re.compile(r"(#\w+)", re.UNICODE)

POSTS_DIR = getattr(globals(), "POSTS_DIR", None) or "posts"  # thư mục mỗi post 1 file
