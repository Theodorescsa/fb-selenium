import time
import json
import os
import pandas as pd
from openpyxl import Workbook, load_workbook
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options

# ====== import từ code của ông ======
from get_comment_fb_utils import (
    open_reel_comments_if_present,
    set_sort_to_all_comments_unified,
)
from get_comment_fb_automation import (
    bootstrap_auth,
    hook_graphql,
    install_early_hook,
)
from main import crawl_comments  # nếu file ông tên khác thì đổi lại
from configs import *

# ====== cấu hình ======
PROXY_URL = ""
INPUT_EXCEL = r"E:\NCS\fb-selenium\thoibao-de-last.xlsx"          # file excel nguồn (có cột link)
OUTPUT_EXCEL = r"E:\NCS\fb-selenium\thoibaode-comments.xlsx"      # file excel đích
ERROR_EXCEL = r"E:\NCS\fb-selenium\crawl_errors.xlsx"             # file ghi những post crawl fail
MAX_RETRIES = 2                                                   # retry tối đa

# cột mong muốn cho file output
OUTPUT_COLUMNS = [
    "id",
    "type",
    "postlink",
    "commentlink",
    "author_id",
    "author",
    "author_link",
    "avatar",
    "created_time",
    "content",
    "image_url",
    "like",
    "comment",
    "haha",
    "wow",
    "sad",
    "love",
    "angry",
    "care",
    "video",
    "source_id",
    "is_share",
    "link_share",
    "type_share",
]


def start_driver_with_proxy(proxy_url: str, headless: bool = False) -> webdriver.Chrome:
    chrome_opts = Options()
    if headless:
        chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--window-size=1920,1080")
    chrome_opts.add_argument("--disable-extensions")
    chrome_opts.add_argument("--disable-background-networking")
    chrome_opts.add_argument("--disable-popup-blocking")
    chrome_opts.add_argument("--no-first-run")
    chrome_opts.add_argument("--no-default-browser-check")
    chrome_opts.add_argument("--disable-background-timer-throttling")
    chrome_opts.add_argument("--disable-backgrounding-occluded-windows")
    chrome_opts.add_argument("--disable-renderer-backgrounding")

    sw_options = None
    if proxy_url:
        sw_options = {
            "proxy": {
                "http": proxy_url,
                "https": proxy_url,
                "no_proxy": "localhost,127.0.0.1",
            },
            # "verify_ssl": False,
        }

    driver = webdriver.Chrome(options=chrome_opts, seleniumwire_options=sw_options)
    driver.scopes = [r".*"]
    return driver


def _to_cell(v):
    # None thì để rỗng
    if v is None:
        return ""
    # nếu là dict kiểu {"uri": "..."} thì lấy uri
    if isinstance(v, dict):
        return v.get("uri") or v.get("url") or json.dumps(v, ensure_ascii=False)
    # nếu là list thì join
    if isinstance(v, list):
        return ",".join(str(x) for x in v)
    # còn lại convert sang str
    return v


def ensure_excel_with_header(path: str, columns: list[str]):
    """Tạo file excel nếu chưa có và ghi header."""
    if not os.path.exists(path):
        wb = Workbook()
        ws = wb.active
        ws.append(columns)
        wb.save(path)


def append_row_to_excel(path: str, columns: list[str], row_dict: dict):
    wb = load_workbook(path)
    ws = wb.active
    row = []
    for col in columns:
        v = row_dict.get(col)
        if isinstance(v, dict):
            v = v.get("uri") or v.get("url") or json.dumps(v, ensure_ascii=False)
        elif isinstance(v, list):
            v = ",".join(str(x) for x in v)
        row.append(v)
    ws.append(row)
    wb.save(path)


def ensure_error_excel(path: str):
    if not os.path.exists(path):
        wb = Workbook()
        ws = wb.active
        ws.append(["link", "error"])
        wb.save(path)


def append_error(path: str, link: str, error: str):
    wb = load_workbook(path)
    ws = wb.active
    ws.append([link, error])
    wb.save(path)


def prepare_fb_page(driver, url: str):
    """Mở post và chuẩn bị để replay GraphQL."""
    driver.get(url)
    time.sleep(2)
    hook_graphql(driver)
    time.sleep(0.5)
    if "reel" in url:
        open_reel_comments_if_present(driver)
    set_sort_to_all_comments_unified(driver)
    time.sleep(0.8)


def crawl_one_post(driver, url: str, max_pages=None):
    """
    Crawl comment cho 1 post DUY NHẤT, trả về list[dict] đã chuẩn schema.
    NOTE: phải xoá file tạm trước khi crawl post mới để tránh đọc dính post trước.
    """
    out_json = "comments_tmp.ndjson"
    ckpt = "checkpoint_tmp.json"

    # 💥 xoá file của lần crawl trước
    if os.path.exists(out_json):
        os.remove(out_json)
    if os.path.exists(ckpt):
        os.remove(ckpt)

    # ép FB tạo UFI
    prepare_fb_page(driver, url)

    # gọi crawler gốc (của ông) – nó sẽ ghi vào out_json
    _ = crawl_comments(
        driver,
        out_json=out_json,
        checkpoint_path=ckpt,
        max_pages=max_pages,
    )

    comments = []
    if os.path.exists(out_json):
        with open(out_json, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    comments.append(rec)
                except Exception:
                    pass

    # 🧹 optional: dọn sau khi đọc xong để folder đỡ đầy
    # os.remove(out_json)
    # if os.path.exists(ckpt):
    #     os.remove(ckpt)

    return comments


def normalize_comment_row(c: dict, postlink: str) -> dict:
    """
    Convert 1 comment dict của ông → đúng format output.
    Hỗ trợ luôn cả dòng reply (is_reply=True).
    """
    # Nếu là reply thì mình đặt type = "Reply" cho dễ nhìn
    _type = c.get("type") or ("Reply" if c.get("is_reply") else "Comment")

    # link comment: top-level có sẵn, reply thì thường không có
    comment_link = c.get("link")

    return {
        "id": c.get("id") or c.get("reply_id"),
        "type": _type,
        "postlink": postlink,
        "commentlink": comment_link,
        "author_id": c.get("author_id"),
        "author": c.get("author"),
        "author_link": c.get("author_link"),
        "avatar": c.get("avatar"),
        "created_time": c.get("created_time"),
        "content": c.get("content") or c.get("text"),
        "image_url": ",".join(c.get("image_url") or []) if isinstance(c.get("image_url"), list) else c.get("image_url"),
        "like": c.get("like", 0),
        "comment": c.get("comment", 0),
        "haha": c.get("haha", 0),
        "wow": c.get("wow", 0),
        "sad": c.get("sad", 0),
        "love": c.get("love", 0),
        "angry": c.get("angry", 0),
        "care": c.get("care", 0),
        "video": ",".join(c.get("video") or []) if isinstance(c.get("video"), list) else c.get("video"),
        "source_id": c.get("source_id"),
        "is_share": c.get("is_share"),
        "link_share": c.get("link_share"),
        "type_share": c.get("type_share"),
    }


def crawl_from_excel_stream(
    input_path: str,
    output_path: str,
    error_path: str,
    driver,
    max_retries: int = 2,
):
    # chuẩn bị file output + file error trước
    ensure_excel_with_header(output_path, OUTPUT_COLUMNS)
    ensure_error_excel(error_path)

    df = pd.read_excel(input_path)

    for idx, row in df.iterrows():
        postlink = str(row.get("link") or "").strip()
        if not postlink:
            continue

        print(f"=== [{idx+1}/{len(df)}] Crawl: {postlink}")

        success = False
        last_error = None

        for attempt in range(max_retries + 1):
            try:
                comments = crawl_one_post(driver, postlink, max_pages=None)
                for c in comments:
                    norm = normalize_comment_row(c, postlink)
                    # 👇 GHI LUÔN TỪNG DÒNG
                    append_row_to_excel(output_path, OUTPUT_COLUMNS, norm)
                success = True
                break
            except Exception as e:
                last_error = str(e)
                print(f"[WARN] crawl fail {postlink} (attempt {attempt+1}/{max_retries+1}): {e}")
                time.sleep(1)

        if not success:
            append_error(error_path, postlink, last_error or "unknown error")
            print(f"[SKIP] bỏ qua bài: {postlink}")

    print("✅ DONE stream → xem file:", output_path)


if __name__ == "__main__":
    d = start_driver_with_proxy(PROXY_URL, headless=False)
    d.set_script_timeout(40)
    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception:
        pass

    # Nếu đang dùng profile thật (USER_DATA_DIR), có thể bỏ bootstrap_auth.
    bootstrap_auth(d)
    install_early_hook(d)

    crawl_from_excel_stream(
        INPUT_EXCEL,
        OUTPUT_EXCEL,
        ERROR_EXCEL,
        driver=d,
        max_retries=MAX_RETRIES,
    )

    # d.quit()  # nếu crawl xong muốn đóng luôn
