
"""
Hàm main của dự án
"""
# import os
# import sys
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.log_utils import logger
import os
#from src.scraper_manager_thread_with_db import ScarperManager
from src.scraper_manager_thead import ScarperManager
path_db = {
    "PATH_GROUP" : "db/Groups",
    "PATH_PAGE" : "db/Pages",
    "PATH_ERROR" : "db/Error",
    "PATH_LINK" : "db/Links",
    "PATH_SEARCH" : "db/Search",
    "PATH_SHARE_POST" : "db/Share_post",
    "PATH_UPDATE" : "db/Update"
}

for key, val in path_db.items():
    if not os.path.exists(val):
        os.makedirs(val)

def main():
    logger.warning(
        
    "\n                    _oo0oo_                      \n"
    "                   o8888888o                       \n"
    "                   88' . '88                       \n"
    "                   (| -_- |)                       \n"
    "                   0\  =  /0                       \n"
    "                 ___/`---'\___                     \n"
    "               .' \\|     |// '.                   \n"
    "              / \\|||  :  |||// \                  \n"
    "             / _||||| -:- |||||- \                 \n"
    "            |   | \\\  -  /// |   |                \n"
    "            | \_|  ''\---/''  |_/ |                \n"
    "             \ .-\__  '-' ___/-. /                 \n"
    "           ___'. .' /--.--\ `. .'___               \n"
    "        ."" '< `.___\_<|>_/___.' >' "".            \n"
    "      | | :  `- \`.;`\ _ /`;.`/ - ` : | |          \n"
    "      \  \ `_.   \_ __\ /__ _/   .-` /  /          \n"
    "  =====`-.____`.___ \_____/___.-`___.-'=====       \n"
    "                    `=---='                        \n"
    "            It works … on my machine               \n"
    "  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~      \n"
 
    )

    sc = ScarperManager()
    sc.run()
    


if __name__ == "__main__":
    main()

import threading
from utils.utils import get_local_device_config
from utils.log_utils import logger
import schedule
from datetime import datetime, timedelta
from datetime import time as time_dt
import time
from queue import Queue
from src.scraper_thread import ScraperThread
from src.get_link_es import ElasticFunc
from utils.utils import read_data_from_file
import copy
from configs import config as cfg
# from multiprocessing import Manager
import threading
import os
"""
Class đọc file config, create task chạy config
"""


class ScarperManager:
    process_scrapers = {}

    def __init__(self) -> None:
        self.config_device = None
    def run(self):
        self.config_device = get_local_device_config()
        num_config = len(self.config_device)
        logger.info(f"Số config có trong file là {num_config}")
        for config in self.config_device:
            if config['mode']['time_run_schedule'] == 0:
                current_time = datetime.now().time()
                target_time = time_dt(int(config['mode']['start_time_run'].split(":")[0]), int(config['mode']['start_time_run'].split(":")[1]), 0) 
                
                # if self.config_device[i]['mode']['is_login'] == 1:
                #     pass # trường hợp cần login tài khoản
                # else: #trường hợp k cần login
                #     pass
                
                if current_time > target_time:
                    check_time = True
                else:
                    check_time = False
                if int(config['mode']['time_expired']) > 1 or check_time:
                    if config['mode']['is_login'] == 1:
                        pass # trường hợp cần login tài khoản
                    else: #trường hợp k cần login
                        self.parser_config_and_create_process(config)
                schedule.every(config['mode']['time_expired']).days.at(config['mode']['start_time_run'], "Asia/Ho_Chi_Minh").do(lambda cfg=config: self.parser_config_and_create_process(cfg))
            else:
                # schedule.every(config['mode']['time_run_schedule']).hours.do(lambda cfg=config: self.parser_config_and_create_process(cfg))
                # self.parser_config_and_create_process(config)
                threading.Thread(target=self.wait_run_schedule, args=(config,)).start()
        #self.run_job()
        run_job_thread = threading.Thread(target=self.run_job)
        run_job_thread.start()
        run_job_thread.join()
    def wait_run_schedule(self, config):
        target_time = time_dt(int(config['mode']['start_time_run'].split(":")[0]), int(config['mode']['start_time_run'].split(":")[1]), 0) 
                
        while datetime.now().time() < target_time:
            print(f"Đang đợi đến {target_time} để chạy. Hiện tại {datetime.now().time()}")
            time.sleep(20)
            
        schedule.every(config['mode']['time_run_schedule']).hours.do(lambda cfg=config: self.parser_config_and_create_process(cfg))
        self.parser_config_and_create_process(config)
    def run_job(self):
        all_jobs = schedule.get_jobs()
        logger.debug(self.process_scrapers)
        for key, val in list(self.process_scrapers.items()):
            if not val.is_alive():
                del self.process_scrapers[key]
        logger.warning(f"Pendding job {all_jobs}.......")
        schedule.run_pending()
        threading.Timer(20, self.run_job).start()

    def parser_config_and_create_process(self, config):
        if config["mode"]["is_login"]:
            pass
        else:
            if config['mode']['id'] == 2:
                if config['mode']['is_type'] == 0:
                    logger.info(f"Số thread là {config['mode']['num_thread']}")
                    queue_ = Queue()
                    
                    #Đọc danh sách các group từ file
                    # for _ in config['mode']['group_id']:
                    #     queue_.put(_)
                    
                    groups = read_data_from_file(path_file=cfg.PATH_TO_GROUP_ID)
                    for _ in groups: #config['mode']['group_id']:
                        queue_.put(_)
                    logger.info(f"Tổng số group cần crawl là {queue_.qsize()}")
                        
                    ########
                    if int(config['mode']['num_thread']) > 1:
                        config_temp = copy.deepcopy(config)
                        for i in range(config['mode']['num_thread']):
                            config_temp['mode']['group_id'] = []
                            config_temp['id'] = str(config['id'])  + "_" + str(i)
                            self.start_scraper_not_login(config=config_temp, queue_=queue_)
                    else:
                        self.start_scraper_not_login(config=config, queue_=queue_)
                elif config['mode']['is_type'] == 1:
                    logger.info(f"Số thread là {config['mode']['num_thread']}")
                    queue_ = Queue()
                    pages = read_data_from_file(path_file=cfg.PATH_TO_PAGE_ID)
                    
                    for _ in pages: #config['mode']['page_id']:
                        queue_.put(_)
                        
                    logger.info(f"Tổng số page cần crawl là {queue_.qsize()}")
                    if int(config['mode']['num_thread']) > 1:
                        config_temp = copy.deepcopy(config)
                        for i in range(config['mode']['num_thread']):
                            config_temp['mode']['page_id'] = []
                            config_temp['id'] = str(config['id'])  + "_" + str(i)
                            self.start_scraper_not_login(config=config_temp, queue_=queue_)
                    else:
                        self.start_scraper_not_login(config=config, queue_=queue_)
            elif config['mode']['id'] == 3:
                #lấy thông tin các post
                
                current_time = datetime.now()
                time_check = current_time + timedelta(hours=config['mode']['time_expired'])#self.job_n_day[group_thread].next_run#current_time + timedelta(days=(config["mode"]["range_date"][-1]-config["mode"]["range_date"][0]))

                queue_ = Queue()
                files_page = [
                    f for f in os.listdir("db/Pages_1")
                    if os.path.isfile(os.path.join("db/Pages_1", f))
                ]
                files_group = [
                    f for f in os.listdir("db/Groups")
                    if os.path.isfile(os.path.join("db/Groups", f))
                ]
                files_page_done = [
                    f for f in os.listdir("db/Update")
                    if os.path.isfile(os.path.join("db/Update", f))
                ]
                list_posts_page = []
                list_posts_group = []
                list_posts_page_done = []
                for file_page in files_page:
                    data_p = read_data_from_file(path_file=f"db/Pages_1/{file_page}")
                    list_posts_page.extend(data_p)
                logger.info(f"Tổng số bài viết trong file page là {len(list_posts_page)}")
                for file_group in files_group:
                    data_g = read_data_from_file(path_file=f"db/Groups/{file_group}")
                    list_posts_group.extend(data_g)
                logger.info(f"Tổng số bài viết trong file group là {len(list_posts_group)}")
                for file_page_done in files_page_done:
                    try:
                        data_d = read_data_from_file(path_file=f"db/Update/{file_page_done}")
                        list_posts_page_done.extend(data_d)
                    except:
                        continue
                logger.info(f"Tổng số bài viết đã done trong file page là {len(list_posts_page_done)}")
                list_posts = list((set(list_posts_page + list_posts_group)) - set(list_posts_page_done))
                logger.info(f"Tổng số bài viết cần crawl là {len(list_posts)}")#lấy tất cả các bài viết trong page và group

                for data in list_posts:
                    if data.strip() == "":
                        continue
                    queue_.put(data.strip())
                logger.info(f"Tổng số bài viết cần crawl là {queue_.qsize()}")
                if queue_.qsize() == 0:
                    logger.warning("Không có bài viết nào cần được update tong khoảng thời gian trên")
                    return
                config_temp = copy.deepcopy(config)
              
                if int(config['mode']['num_thread']) > 1:
                    for i in range(int(config['mode']['num_thread'])):
                        #config_temp = config
                        id_new = str(config['id'])  + "_" + str(i)
                        config_temp['id'] = id_new
                        self.start_scraper_not_login(config_temp, queue_update=queue_, time_check=time_check)#, shared_dict_3=shared_dict_3, lock_3=lock_3)
                else:
                    self.start_scraper_not_login(config, queue_update=queue_, time_check=time_check)#, shared_dict_3=shared_dict_3, lock_3=lock_3)

                

    """
    Hàm để tạo thread chạy scraper facebook theo các mode
    """
    def start_scraper_not_login(self, config, **kwargs):
        sp = ScraperThread(config, **kwargs)
        sp.daemon = True
        sp.start()
        time.sleep(2)
        self.process_scrapers[str(config["id"])] = sp
        # if config["id"] not in self.process_scrapers:
        #     self.process_scrapers[str(config["id"])] = sp
        # else:
        #     logger.warning(f"Key {str(config['id'])} đã tồn tại")

"""
Lấy thông tin cơ bản của tất cả các post trong 1 page trong 1 khoảng thời gian nhất định hoặc số bài nhất định
"""

from utils.log_utils import logger
from fb_api_wrapper.libs.fb_core import FaceApiWrapper
from utils.common_utils import CommonUtils
import json
from configs.config import is_data_raw_post, SLEEP_RANDOM_GROUP
from utils.check_utils import check_time_line_expired
from utils.utils import read_data_from_file, write_data_to_file, write_data_raw_post, get_proxy_from_list, get_proxy, get_random_proxy
# from utils.api_utils import get_ids_post, insert_ids_post
from src.post import Post
from src.parse_json_post import ParserInfoPost
from typing import Optional, Union
import os
import requests
from configs.define_api import HEADERS
from queue import Queue
from datetime import datetime
from threading import Lock

class ScaperPostInforPage:

    """
    Khởi tạo đối tượng ScaperPostInforPage
    - datasets: list danh sách các page id cần lấy thông tin 
    - PROXY: danh sách các proxy, bắt buộc phải dùng proxy khi chạy
    - time_line: thời gian lấy bài viết
    - MAX_SIZE_POST: số bài viết tối đa
    - process_name: tên process
    """
    def __init__(
            self, 
            PROXY: Optional[dict] = None, 
            PROXYS: Optional[list] = None,
            queue_: Optional[Queue] = None,
            datasets: Optional[list] = None,
            MAX_SIZE_POST: Optional[int]=1000, 
            time_line: Optional[datetime]=None, 
            process_name: Optional[str]="", 
            lock: Optional[Lock] = None
        ) -> None:
        self.queue_ = queue_
        self.datasets = datasets
        self.PROXY = PROXY
        self.PROXYS = PROXYS
        self.MAX_SIZE_POST = MAX_SIZE_POST
        self.process_name = process_name
        self.time_line = time_line
        self.idem = 0
        self.bCheck = True
        self.iLinkDuplicate = 0
        self.check_time_expired = False
        self.lock = lock
        self.page_backup = self.__load_data_backup()
        self.SESSION = None
        self.proxy_use = None
        #self.tenancy_id = []
        self.__create_session()

    def __create_session(self):
        logger.info(f"Process {self.process_name} >> Create session")
        self.SESSION = requests.Session()
        if self.PROXYS:
            #self.proxy_use = get_proxy_from_list(self.PROXYS)
            self.proxy_use = get_random_proxy(self.PROXYS)
        else:
            #self.proxy_use = get_proxy(self.PROXY)
            self.proxy_use = self.PROXY
        self.SESSION.headers.update(HEADERS)
        if self.proxy_use:
            self.SESSION.proxies.update(self.proxy_use)
   
    def __reset_params(self):
        self.idem = 0
        self.bCheck = True
        self.iLinkDuplicate = 0
        self.check_time_expired = False
    def __processing_data(self, node_posts: Optional[list], page_id: Union[str, int], link_posts_all: Optional[list]):
        page_id = str(page_id)
        for _p in node_posts:
            try:
                post_id = _p['post_id']
                #lấy thông tin cơ bản của bài viết
                post = Post(id=post_id, parent_id=page_id, type="facebook page")
                info_post = ParserInfoPost(post=post, path_file=f"db/Pages/{page_id}/{page_id}_info.json", id_topic=0)
                info_post.extract(jsondata=_p)
                author_id = post.author_id
            
                self.idem += 1
                ## lấy thời gian bài post 
                create_time_post = post.created_time
                if is_data_raw_post:
                    write_data_raw_post(path_file=f"db/Pages/{page_id}/{page_id}_{author_id}_{post_id}.txt", data=json.dumps(_p))
                
                logger.info(f"Process {self.process_name} >> Time create post {post_id} {create_time_post}")
                if self.time_line is not None:
                    if check_time_line_expired(create_time_post, self.time_line):
                        logger.warning(f"Process {self.process_name} >> Thời gian bài đăng đã quá {self.time_line}")
                        self.check_time_expired =  True
                        break

                #data_link ="facebook page" + "|" + page_id + "|" + author_id + "|" + post_id
                data_link = f"facebook page|{page_id}|{post.id_encode}|{post_id}"
                logger.info(data_link)
                if data_link in link_posts_all:
                    logger.info(f"Process {self.process_name} >> id post đã có trong db {data_link}")
                    self.iLinkDuplicate += 1
                    continue 
                else:
                    self.iLinkDuplicate = 0
                    write_data_to_file(path_file=f"db/Pages/{page_id}.txt", message=str(data_link))
                    ## Comment lại để test
                    #insert_ids_post(ids=[str(data_link)], table_name="facebook_page", object_id=str(page_id))
                            
                del info_post
                del post
            except Exception as ex:
                logger.warning(f"Process {self.process_name} >> Error func __processing_data: {ex}")
    def __check_done(self):
        if self.iLinkDuplicate >= 3:
            logger.warning(f"Process {self.process_name} >> Tất cả bài viết trong page, page đã trùng")
            self.bCheck = False
        if self.check_time_expired:
            if self.time_line:
                logger.warning(f"Process {self.process_name} >> Tất cả bài viết trong page đã quá thời gian {self.time_line}")
                self.bCheck = False
        if self.idem > self.MAX_SIZE_POST and self.MAX_SIZE_POST != -1:
            logger.warning(f"Process {self.process_name} >> Đã lấy đủ số lượng bài ưu cầu")
            self.bCheck = False

    def __scaper_page(self, page_id: Union[str, int], link_posts_all: Optional[list]):
        try:
            self.__reset_params()
            
            posts_generator = FaceApiWrapper.get_node_posts_page(page_id=page_id, session=self.SESSION, post_limit=self.MAX_SIZE_POST)

            for node_posts in posts_generator:
                self.__processing_data(node_posts=node_posts, page_id=page_id, link_posts_all=link_posts_all)
                self.__check_done()
                if not self.bCheck:
                    break
            del posts_generator
            logger.info(f"Process {self.process_name} >> Scraper done group {page_id}, number post {self.idem}")
                  
            # if self.idem == 0:
            #     with open(f"db/Groups/{group_id}_noposst.txt", "w", encoding="utf-8") as fp:
            #         fp.write(group_id)
        except Exception as e:
            logger.error(f"Process {self.process_name} >> Error func scaper_group: {e}")

    def __load_data_backup(self):
        try:
            return read_data_from_file(path_file=f"db/Pages/backup_page_{self.process_name}.txt")
        except:
            return []
        
    def run_error_page(self):
        try:
            pages_id_error = list(set(read_data_from_file(path_file=f"db/Pages/page_error_{self.process_name}.txt")))
            logger.warning(f"Process {self.process_name} >> Số page lỗi cần chạy lại là {len(pages_id_error)}")
            for page_ in pages_id_error:
                # try:
                #     link_posts_all = list(set(read_data_from_file(path_file=f"db/Pages/{page_}.txt")))
                # except Exception as ex:
                #     logger.warning(ex)
                #     link_posts_all = []
                #link_posts_all = get_ids_post(table_name="facebook_page", object_id=str(page_))
                try:
                    link_posts_all = list(set(read_data_from_file(path_file=f"db/Pages/{page_}.txt")))
                except Exception as ex:
                    logger.warning(ex)
                    link_posts_all = []
                self.__scaper_page(page_, link_posts_all)
                CommonUtils.sleep_random_in_range(SLEEP_RANDOM_GROUP[0], SLEEP_RANDOM_GROUP[1])
            try:
                logger.info(f"Process {self.process_name} >> Remove err_file")
                os.remove(f"db/Pages/page_error_{self.process_name}.txt")
            except:
                pass  
        except Exception as ex:
            logger.warning(ex)

    def start_scraper_with_datasets(self):
        logger.info(f"Process {self.process_name} >> Số Page có trong dataset là: {len(self.datasets)}")

        for index, page_id in enumerate(self.datasets):
            page_id = str(page_id)

            if str(page_id) in self.page_backup:
                logger.warning(f"Process {self.process_name} >> Page {page_id} đã được crawl trong job hiện tại. Next page")
                continue
            self.page_backup.append(str(page_id))
            logger.info(f"Process {self.process_name} >> Scaper page {page_id}")
            #try:
                #link_posts_all = list(set(read_data_from_file(path_file=f"db/Pages/{page_id}.txt")))
                
            # except Exception as ex:
            #     logger.warning(ex)
            #     link_posts_all = []
            #link_posts_all = get_ids_post(table_name="facebook_page", object_id=str(page_id))
            try:
                link_posts_all = list(set(read_data_from_file(path_file=f"db/Pages/{page_id}.txt")))
            except Exception as ex:
                logger.warning(ex)
                link_posts_all = []
            path_data = f"db/Pages/{page_id}"
            if not os.path.exists(path_data):
                os.makedirs(path_data)
            self.__scaper_page(page_id, link_posts_all)
            write_data_to_file(path_file=f"db/Pages/backup_page_{self.process_name}.txt", message=str(page_id))
            CommonUtils.sleep_random_in_range(SLEEP_RANDOM_GROUP[0], SLEEP_RANDOM_GROUP[1])
        try:
            logger.info(f"Process {self.process_name} >> Remove backup_file")
            os.remove(f"db/Pages/backup_page_{self.process_name}.txt")
        except:
            pass  
        self.run_error_page()
    def start_scraper_with_queue(self):
        while not self.queue_.empty():
            try:
                logger.info(f"Process {self.process_name} >> Size of queue {self.queue_.qsize()}")

                page_id = self.queue_.get()
                # page_id = data['id']
                # self.tenancy_id = data['tenancy_id']
                if str(page_id) in self.page_backup:
                    logger.warning(f"Process {self.process_name} >> Page {page_id} đã được crawl trong job hiện tại. Next page")
                    continue
                self.page_backup.append(str(page_id))
                logger.info(f"Process {self.process_name} >> Scaper page {page_id}")
                link_posts_all = [] #get_ids_post(table_name="facebook_page", object_id=str(page_id))
                path_data = f"db/Pages/{page_id}"
                if not os.path.exists(path_data):
                    os.makedirs(path_data)
                self.__scaper_page(page_id, link_posts_all)
                write_data_to_file(path_file=f"db/Pages/backup_page_{self.process_name}.txt", message=str(page_id))
                CommonUtils.sleep_random_in_range(SLEEP_RANDOM_GROUP[0], SLEEP_RANDOM_GROUP[1])
                try:
                    logger.info(f"Process {self.process_name} >> Remove backup_file")
                    os.remove(f"db/Pages/backup_page_{self.process_name}.txt")
                except:
                    pass  
                self.run_error_page()

            except Exception as ex:
                logger.error(f"Process {self.process_name} >> Error func start_with_queue {ex}")
    def start_scraper(self):
        # if self.proxy_use is None:
        #     logger.critical(f"Process {self.process_name} >> Tool cần proxy để chạy...")
        #     return
        logger.info(f"Process {self.process_name} >> Start scraper with proxy {self.proxy_use}")
        if self.datasets:
            self.start_scraper_with_datasets()
        elif self.queue_:
            self.start_scraper_with_queue()
        else:
            logger.warning(f"Process {self.process_name} >> Không có data update")
# hiện tại code trên đang sử dụng thư viện fb_api_wrapper để crawler dữ liệu bài viết mà đang không có checkpoint bằng cursor, tôi muốn 