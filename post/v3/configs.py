# =========================
# CONFIG — nhớ sửa GROUP_URL
# =========================
import re
CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"
PROFILE_NAME  = "Profile 5"
REMOTE_PORT   = 9222

# GROUP_URL     = "https://web.facebook.com/groups/laptrinhvienit"  # <— ĐỔI Ở ĐÂY
GROUP_URL     = "https://www.facebook.com/thoibao.de"              # <— ĐỔI Ở ĐÂY
KEEP_LAST     = 350
OUT_NDJSON    = "posts_all.ndjson"
RAW_DUMPS_DIR = "raw_dumps"
CHECKPOINT    = r"E:\NCS\fb-selenium\checkpoint.json"

# Bật nếu muốn parse & ghi NDJSON (mặc định dump-only)
ENABLE_PARSE = False

# =========================
# Regex & constants
# =========================
CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}
POST_URL_RE = re.compile(
    r"https?://(?:web\.)?facebook\.com/groups/[^/]+/(?:permalink|posts)/(\d+)/?$", re.I
)

# =========================
# Optional parse helpers (để sẵn nếu cần bật ENABLE_PARSE)
# =========================
REACTION_KEYS = {
    "LIKE": "like", "LOVE": "love", "HAHA": "haha", "WOW": "wow",
    "SAD": "sad", "ANGRY": "angry", "CARE": "care"
}

FB_ORIGINS = [
    "https://www.facebook.com",
    "https://web.facebook.com",
    "https://m.facebook.com",
]
COOKIES_PATH      = "./cookies.json"        # <-- file bạn vừa export
LOCALSTORAGE_PATH = "./localstorage.json"   # <-- file bạn vừa export