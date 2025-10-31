from configs import *
from get_comment_fb_automation import start_driver

CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"
PROFILE_NAME = "Profile 40"
if __name__ == "__main__":
   driver = start_driver(CHROME_PATH, USER_DATA_DIR, PROFILE_NAME, port=REMOTE_PORT, headless=False)