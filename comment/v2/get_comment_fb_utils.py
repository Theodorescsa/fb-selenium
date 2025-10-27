from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, StaleElementReferenceException
)
import time

# ====== Generic helpers ======
def _wait(d, timeout=10):
    return WebDriverWait(d, timeout)

def scroll_into_view(d, el, block="center"):
    d.execute_script(f"arguments[0].scrollIntoView({{block:'{block}'}});", el)

def safe_click(d, el, sleep_after=0.25):
    """Scroll → try click (handles intercept/stale)."""
    try:
        scroll_into_view(d, el, "center")
        time.sleep(0.15)
        el.click()
        time.sleep(sleep_after)
        return True
    except (ElementClickInterceptedException, StaleElementReferenceException):
        try:
            d.execute_script("arguments[0].click()", el)
            time.sleep(sleep_after)
            return True
        except Exception:
            return False

def find_first(container, xpaths):
    """Find the first displayed+enabled element matching any xpath (relative to container)."""
    for xp in xpaths:
        try:
            el = container.find_element(By.XPATH, xp)
            if el.is_displayed() and el.is_enabled():
                return el
        except Exception:
            pass
    return None

def wait_first(d, container, xpaths, timeout=8, poll=0.2):
    """Wait until any of xpaths appears (displayed+enabled)."""
    end = time.time() + timeout
    while time.time() < end:
        el = find_first(container, xpaths)
        if el:
            return el
        time.sleep(poll)
    raise TimeoutException("Element for any provided xpath not found/ready.")

# ====== Post dialog ======
def get_post_dialog(driver, timeout=10):
    """Lấy dialog mới nhất (post mở kiểu popup)."""
    return _wait(driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, "(//div[@role='dialog'])[last()]"))
    )

# ====== Sort menu (scoped to dialog) ======
# Text variants (add more locales if cần)
SORT_BUTTON_TEXTS = [
    "Phù hợp nhất",      # VI
    "Most relevant",     # EN
]
ALL_COMMENTS_TEXTS = [
    "Tất cả bình luận",  # VI
    "All comments",      # EN
]

def _button_with_span_text_xpath(texts):
    # .//div[@role='button'][.//span[normalize-space()='...']]
    parts = [f".//div[@role='button'][.//span[normalize-space()='{t}']]" for t in texts]
    # fallback contains (ít ưu tiên hơn)
    parts += [f".//div[@role='button'][contains(., '{t}')]" for t in texts]
    return parts

def _menuitem_with_span_text_xpath(texts):
    # Tìm trong popup menu: role=menu / menuitem hoặc button
    parts = [
        f"(//div[@role='menu'] | //div[@role='dialog'] | //div[@role='menuitem'] | //div[@role='listbox'])"
        f"//div[@role='menuitem' or @role='button'][.//span[normalize-space()='{t}']]"
        for t in texts
    ]
    # fallback: bất kỳ nút có span text đó
    parts += [f"(.//div[@role='button'] | .//div[@role='menuitem'])[.//span[normalize-space()='{t}']]" for t in texts]
    return parts

def open_sort_menu_scoped(driver, container, timeout=10):
    """
    Tìm & click nút sort ('Phù hợp nhất' / 'Most relevant').
    Không lệ thuộc class/aria-label dễ vỡ.
    """
    xpaths = _button_with_span_text_xpath(SORT_BUTTON_TEXTS)
    btn = wait_first(driver, container, xpaths, timeout=timeout)
    if not safe_click(driver, btn):
        raise ElementClickInterceptedException("Không click được nút Sort.")
    return True

def choose_all_comments_scoped(driver, container, timeout=10):
    """
    Trong menu vừa mở, chọn 'Tất cả bình luận' / 'All comments'.
    """
    # Scope tìm trong toàn trang vì menu thường mount ở root (không nằm trong dialog container)
    root = driver.find_element(By.XPATH, "/html/body")
    xpaths = _menuitem_with_span_text_xpath(ALL_COMMENTS_TEXTS)
    opt = wait_first(driver, root, xpaths, timeout=timeout)
    if not safe_click(driver, opt):
        raise ElementClickInterceptedException("Không click được option 'All comments'.")
    return True

def set_sort_to_all_comments(driver, max_retry=2):
    """
    Public API: đặt filter về 'Tất cả bình luận' theo cách ổn định.
    """
    last_err = None
    for _ in range(max_retry):
        try:
            dlg = get_post_dialog(driver, timeout=10)
            open_sort_menu_scoped(driver, dlg, timeout=10)
            choose_all_comments_scoped(driver, dlg, timeout=10)
            return True
        except Exception as e:
            last_err = e
            time.sleep(0.6)
    if last_err:
        raise last_err
    return False
