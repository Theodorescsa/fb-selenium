from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from pathlib import Path
from typing import List, Dict, Any, Optional
import json, time, os
ALLOWED_COOKIE_DOMAINS = {".facebook.com", "facebook.com", "m.facebook.com", "web.facebook.com"}
HERE = Path(__file__).resolve().parent

# Page/Group/Profile gốc bạn muốn crawl
GROUP_URL     = "https://www.facebook.com/thoibao.de"

# (Optional) Nếu muốn nạp login thủ công từ file, set path 2 hằng dưới; nếu không, để None:
COOKIES_PATH         = HERE / "authen" / "cookies.json"
LOCALSTORAGE_PATH    = HERE / "authen" / "localstorage.json"
SESSIONSTORAGE_PATH  = HERE / "authen" / "sessionstorage.json"
def _coerce_epoch(v):
    try:
        vv = float(v)
        if vv > 10_000_000_000:  # ms -> s
            vv = vv / 1000.0
        return int(vv)
    except Exception:
        return None
def _normalize_cookie(c: dict) -> Optional[dict]:
    if not isinstance(c, dict): 
        return None
    name  = c.get("name")
    value = c.get("value")
    if not name or value is None:
        return None

    domain = c.get("domain")
    host_only = c.get("hostOnly", False)
    if domain:
        domain = domain.strip()
        if host_only and domain.startswith("."):
            domain = domain.lstrip(".")
    if not domain:
        domain = "facebook.com"

    if not any(domain.endswith(d) or ("."+domain).endswith(d) for d in ALLOWED_COOKIE_DOMAINS):
        return None

    path = c.get("path") or "/"
    secure    = bool(c.get("secure", True))
    httpOnly  = bool(c.get("httpOnly", c.get("httponly", False)))

    expiry = c.get("expiry", None)
    if expiry is None:
        expiry = c.get("expirationDate", None)
    if expiry is None:
        expiry = c.get("expires", None)
    expiry = _coerce_epoch(expiry) if expiry is not None else None

    out = {
        "name": name,
        "value": value,
        "domain": domain,
        "path": path,
        "secure": secure,
        "httpOnly": httpOnly,
    }
    if expiry is not None:
        out["expiry"] = expiry
    return out

def _add_cookies_safely(driver, cookies_path: Path):
    with open(cookies_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if isinstance(raw, dict) and "cookies" in raw:
        raw = raw["cookies"]
    if not isinstance(raw, list):
        raise ValueError("File cookies không phải mảng JSON.")

    added = 0
    for c in raw:
        nc = _normalize_cookie(c)
        if not nc:
            continue
        try:
            driver.add_cookie(nc)
            added += 1
        except Exception:
            pass
    return added
def _set_kv_storage(driver, kv_path: Path, storage: str = "localStorage"):
    with open(kv_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        for k, v in data.items():
            driver.execute_script(f"{storage}.setItem(arguments[0], arguments[1]);", k, v)
def bootstrap_auth(d):
    d.get("https://www.facebook.com/")
    time.sleep(1.0)

    if COOKIES_PATH and os.path.exists(COOKIES_PATH):
        try:
            count = _add_cookies_safely(d, Path(COOKIES_PATH))
            d.get("https://www.facebook.com/")
            time.sleep(1.0)
            print(f"[AUTH] Added cookies: {count}")
        except Exception as e:
            print("[WARN] bootstrap cookies:", e)

    # if LOCALSTORAGE_PATH and os.path.exists(LOCALSTORAGE_PATH):
    #     try:
    #         d.get("https://www.facebook.com/")
    #         _set_kv_storage(d, Path(LOCALSTORAGE_PATH), "localStorage")
    #         d.get("https://www.facebook.com/")
    #         time.sleep(0.8)
    #     except Exception as e:
    #         print("[WARN] bootstrap localStorage:", e)

    # if SESSIONSTORAGE_PATH and os.path.exists(SESSIONSTORAGE_PATH):
    #     try:
    #         d.get("https://www.facebook.com/")
    #         _set_kv_storage(d, Path(SESSIONSTORAGE_PATH), "sessionStorage")
    #         d.get("https://www.facebook.com/")
    #         time.sleep(0.8)
    #     except Exception as e:
    #         print("[WARN] bootstrap sessionStorage:", e)

    try:
        all_cookies = {c["name"]: c.get("value") for c in d.get_cookies()}
        has_cuser = "c_user" in all_cookies
        has_xs    = "xs" in all_cookies
        print(f"[AUTH] c_user={has_cuser}, xs={has_xs}")
    except Exception:
        pass
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
                "http":  proxy_url,
                "https": proxy_url,
                "no_proxy": "localhost,127.0.0.1",
            },
            # "verify_ssl": False,
        }

    driver = webdriver.Chrome(options=chrome_opts, seleniumwire_options=sw_options)
    driver.scopes = [r".*"]
    return driver
