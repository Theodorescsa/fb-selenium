# =========================
# CONFIG — chỉnh theo máy bạn
# =========================
from pathlib import Path
import re
HERE = Path(__file__).resolve().parent

# Page/Group/Profile gốc bạn muốn crawl
GROUP_URL     = "https://www.facebook.com/thoibao.de"

# (Optional) Nếu muốn nạp login thủ công từ file, set path 2 hằng dưới; nếu không, để None:
COOKIES_PATH         = HERE / "authen" / "cookies.json"
LOCALSTORAGE_PATH    = HERE / "authen" / "localstorage.json"
SESSIONSTORAGE_PATH  = HERE / "authen" / "sessionstorage.json"

# Proxy tuỳ chọn cho selenium-wire (để trống nếu không dùng)
PROXY_URL = ""
# Cookie
ALLOWED_COOKIE_DOMAINS = {".facebook.com", "facebook.com", "m.facebook.com", "web.facebook.com"}

# Lưu trữ
KEEP_LAST     = 350
OUT_NDJSON    = HERE / "database" / "posts_all.ndjson"
RAW_DUMPS_DIR = HERE / "database" / "raw_dumps"
CHECKPOINT    = HERE / "database" / "checkpoint.json"

# Cursor
CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}

POST_URL_RE = re.compile(
    r"""https?://(?:web\.)?facebook\.com/
        (?:
            groups/[^/]+/(?:permalink|posts)/\d+
          | [A-Za-z0-9.\-]+/posts/\d+
          | [A-Za-z0-9.\-]+/reel/\d+
          | photo(?:\.php)?\?(?:.*(?:fbid|story_fbid|video_id)=\d+)
          | .*?/pfbid[A-Za-z0-9]+
        )
    """, re.I | re.X
)