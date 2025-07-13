import os
import sys
import json
import zipfile
import shutil
import pickle
import re
import requests
import logging
import argparse
import signal
import http.client
import time
import threading
from http.cookiejar import MozillaCookieJar
from datetime import datetime
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
from colorlog import ColoredFormatter
from tqdm import tqdm
from requests.exceptions import ChunkedEncodingError, ReadTimeout, RequestException, HTTPError
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import concurrent.futures.thread as _thread
from concurrent.futures import _base
import weakref
from queue import Queue
from threading import Lock, RLock, Event
from functools import partial

SERVICE = "patreon"
USERS_POSTS_FILE = "users_posts.json"
COOKIE_FILE = "cookies.txt"
DOWNLOADED_LIST_FILE = "downloaded_files.pkl"
DOWNLOAD_DIR = "downloads"
REPOSITORY_DIR = "maps_repository"
LOG_DIR = "logs"
LOG_FILE = "czepeku-dl.log"

listener = None
_download_lock = Lock()
progress_lock = RLock()
shutdown_event = Event()
thread_local = threading.local()
position_queue = None

class DaemonThreadPoolExecutor(ThreadPoolExecutor):
    def _adjust_thread_count(self):
        if self._idle_semaphore.acquire(timeout=0):
            return

        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            thread_name = '%s_%d' % (self._thread_name_prefix or self,
                                     num_threads)
            t = threading.Thread(name=thread_name, target=_thread._worker,
                                 args=(weakref.ref(self, weakref_cb),
                                       self._work_queue,
                                       self._initializer,
                                       self._initargs))
            t.daemon = True
            t.start()
            self._threads.add(t)
            _thread._threads_queues[t] = self._work_queue

class TqdmHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)

def setup_logging(log_level, progress_only):
    global listener
    os.makedirs(LOG_DIR, exist_ok=True)
    log_queue = Queue(-1)
    
    console_formatter = ColoredFormatter(
        "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        }
    )
    console_handler = TqdmHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)

    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    log_path = os.path.join(LOG_DIR, LOG_FILE)
    file_handler = RotatingFileHandler(
        filename=log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(file_formatter)

    if progress_only:
        listener = QueueListener(log_queue, file_handler)
    else:
        listener = QueueListener(log_queue, console_handler, file_handler)
    listener.daemon = True
    listener.start()

    queue_handler = QueueHandler(log_queue)
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()
    root_logger.addHandler(queue_handler)

    if os.path.exists(log_path) and os.path.getsize(log_path) > 0:
        try:
            file_handler.doRollover()
        except Exception as e:
            root_logger.error(f"Failed to rollover log file: {e}")

def load_config_file(filename):
    logger = logging.getLogger(f"{__name__}.load_config_file")
    if not os.path.exists(filename):
        logger.critical(f"Config file {filename} not found")
        return None
    try:
        with open(filename, 'r') as f:
            content = f.read()
        content = re.sub(r'//.*|/\*[\s\S]*?\*/', '', content)
        return content
    except Exception as e:
        logger.critical(f"Failed to read config file {filename}: {e}")
        return None

def load_users_posts():
    logger = logging.getLogger(f"{__name__}.load_users_posts")
    logger.info("Loading users and posts configuration")
    content = load_config_file(USERS_POSTS_FILE)
    if content is None:
        sys.exit(1)
    try:
        data = json.loads(content)
        logger.debug(f"Loaded users_posts data: {json.dumps(data, indent=2)}")
        return data
    except json.JSONDecodeError as e:
        logger.critical(f"Invalid JSON in {USERS_POSTS_FILE}: {e}")
        sys.exit(1)

def load_cookies(cookie_file):
    logger = logging.getLogger(f"{__name__}.load_cookies")
    logger.info(f"Loading cookies from {cookie_file}")
    jar = None
    expired_count = 0
    
    if os.path.exists(cookie_file):
        try:
            jar = MozillaCookieJar(cookie_file)
            jar.load(ignore_discard=True, ignore_expires=True)
            
            current_time = time.time()
            for cookie in list(jar):
                if cookie.expires and cookie.expires < current_time:
                    expired_count += 1
                    logger.warning(f"Expired cookie: {cookie.name} (expired {datetime.fromtimestamp(cookie.expires).strftime('%Y-%m-%d')})")
            
            if expired_count > 0:
                logger.warning(f"Found {expired_count} expired cookies - you may need to update your cookie file")
            logger.debug(f"Loaded cookies: {[c.name for c in jar]}")
        except Exception as e:
            logger.error(f"Failed to load cookies: {e}")
            jar = None
    else:
        logger.warning(f"Cookie file {cookie_file} not found. Proceeding without cookies.")
    
    return jar

def load_downloaded_list():
    logger = logging.getLogger(f"{__name__}.load_downloaded_list")
    logger.info("Loading previously downloaded files list")
    if os.path.exists(DOWNLOADED_LIST_FILE):
        try:
            with open(DOWNLOADED_LIST_FILE, 'rb') as f:
                data = pickle.load(f)
            logger.debug(f"Loaded downloaded list: {list(data)[:10]}... (total {len(data)})")
            return data
        except (pickle.PickleError, EOFError, Exception) as e:
            logger.error(f"Failed to load downloaded list: {e}. Starting fresh.")
    logger.info("Starting with empty download list")
    return set()

def save_downloaded_list(downloaded_set):
    logger = logging.getLogger(f"{__name__}.save_downloaded_list")
    logger.info(f"Saving downloaded list with {len(downloaded_set)} entries")
    try:
        with open(DOWNLOADED_LIST_FILE + '.tmp', 'wb') as f:
            pickle.dump(downloaded_set, f)
        os.replace(DOWNLOADED_LIST_FILE + '.tmp', DOWNLOADED_LIST_FILE)
        logger.debug(f"Saved downloaded list successfully")
    except Exception as e:
        logger.error(f"Failed to save downloaded list: {e}")

def fetch_post_data(user_id, post_id, max_retries, backoff_factor, max_backoff):
    session = thread_local.session
    logger = logging.getLogger(f"{__name__}.fetch_post_data")
    logger.info(f"Fetching post {post_id} for user {user_id}")
    if shutdown_event.is_set():
        return None
        
    url = f"https://kemono.su/api/v1/{SERVICE}/user/{user_id}/post/{post_id}"
    
    attempt = 0
    
    while attempt < max_retries and not shutdown_event.is_set():
        attempt += 1
        try:
            response = session.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            logger.debug(f"Fetched post data for {post_id}: {json.dumps(data, indent=2)}")
            return data
        except RequestException as e:
            if shutdown_event.is_set():
                return None
                
            if attempt < max_retries:
                delay = min(backoff_factor * (2 ** (attempt - 1)), max_backoff)
                logger.debug(f"Retrying post {post_id} in {delay:.1f}s: {str(e)[:100]}")
                time.sleep(delay)
            else:
                logger.error(f"Failed to fetch post {post_id} after {max_retries} attempts: {str(e)[:100]}")
    return None

def download_file(session, url, path, max_retries, backoff_factor, max_backoff, position):
    logger = logging.getLogger(f"{__name__}.download_file")
    if shutdown_event.is_set():
        return False
        
    temp_path = path + '.part'
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to create directory for {path}: {e}")
        return False
    
    downloaded = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0
    
    headers = {'Range': f'bytes={downloaded}-'} if downloaded else {}
    total_size = None
    retry_count = 0
    initial_downloaded = downloaded
    filename = os.path.basename(path)

    try:
        progress_bar = tqdm(
            total=total_size,
            initial=downloaded,
            unit='B',
            unit_scale=True,
            desc=filename,
            leave=False,
            mininterval=0.5,
            position=position,
            dynamic_ncols=True
        )
    except Exception as e:
        logger.error(f"Failed to create progress bar for {filename}: {e}")
        return False

    while not shutdown_event.is_set() and retry_count < max_retries:
        logger.info(f"Starting download attempt {retry_count + 1}/{max_retries} for {filename}")
        logger.debug(f"Download headers: {headers}")
        try:
            with session.get(url, stream=True, headers=headers, timeout=(10, 5)) as r:
                r.raise_for_status()
                
                if total_size is None:
                    if 'content-range' in r.headers:
                        total_size = int(r.headers['content-range'].split('/')[-1])
                    else:
                        total_size = int(r.headers.get('content-length', 0)) + downloaded
                    with progress_lock:
                        logger.debug(f"Setting total_size to {total_size} for {filename}")
                        progress_bar.total = total_size
                        progress_bar.refresh()
                
                with open(temp_path, 'ab') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if shutdown_event.is_set():
                            break
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            with progress_lock:
                                progress_bar.update(len(chunk))
                                progress_bar.refresh()
                            initial_downloaded = max(initial_downloaded, downloaded)
                
                if shutdown_event.is_set():
                    with progress_lock:
                        progress_bar.close()
                    return False
                    
                if downloaded >= total_size:
                    with progress_lock:
                        progress_bar.close()
                    logger.info(f"Download completed for {filename}")
                    try:
                        os.rename(temp_path, path)
                    except OSError as e:
                        logger.error(f"Failed to rename temp file for {filename}: {e}")
                        return False
                    return True
                headers['Range'] = f'bytes={downloaded}-'

        except HTTPError as e:
            status_code = e.response.status_code if hasattr(e, 'response') else None
            if status_code == 416:
                if downloaded == 0:
                    with progress_lock:
                        progress_bar.close()
                    logger.error(f"File not available for {filename}: {str(e)[:100]}")
                    return False
                else:
                    with progress_lock:
                        progress_bar.close()
                    logger.info(f"Assuming download complete for {filename} due to 416 error with downloaded {downloaded} bytes")
                    try:
                        os.rename(temp_path, path)
                    except OSError as e:
                        logger.error(f"Failed to rename temp file for {filename}: {e}")
                        return False
                    return True
            retry_count += 1
            if retry_count < max_retries and not shutdown_event.is_set():
                delay = min(backoff_factor * (2 ** (retry_count - 1)), max_backoff)
                logger.debug(f"Download retry {retry_count}/{max_retries} for {filename}: {str(e)[:100]}, waiting {delay:.1f}s")
                time.sleep(delay)
                headers['Range'] = f'bytes={downloaded}-'
            else:
                with progress_lock:
                    progress_bar.close()
                logger.error(f"Download failed for {filename} after {max_retries} retries: {str(e)[:100]}")
                return False
        except (http.client.IncompleteRead, ChunkedEncodingError, ReadTimeout, RequestException) as e:
            retry_count += 1
            if retry_count < max_retries and not shutdown_event.is_set():
                delay = min(backoff_factor * (2 ** (retry_count - 1)), max_backoff)
                logger.debug(f"Download retry {retry_count}/{max_retries} for {filename}: {str(e)[:100]}, waiting {delay:.1f}s")
                time.sleep(delay)
                headers['Range'] = f'bytes={downloaded}-'
            else:
                with progress_lock:
                    progress_bar.close()
                logger.error(f"Download failed for {filename} after {max_retries} retries: {str(e)[:100]}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error during download of {filename}: {e}")
            retry_count += 1
            if retry_count < max_retries and not shutdown_event.is_set():
                delay = min(backoff_factor * (2 ** (retry_count - 1)), max_backoff)
                time.sleep(delay)
            else:
                with progress_lock:
                    progress_bar.close()
                return False
    
    with progress_lock:
        progress_bar.close()
    return False

def safe_remove(path):
    logger = logging.getLogger(f"{__name__}.safe_remove")
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            os.remove(path)
            logger.debug(f"Successfully removed {path} on attempt {attempt + 1}")
            return True
        except PermissionError as e:
            if attempt < max_attempts - 1:
                delay = 0.2 * (2 ** attempt)
                logger.debug(f"PermissionError removing {path}: {str(e)[:100]}. Retrying in {delay:.1f}s")
                time.sleep(delay)
            else:
                logger.error(f"Failed to remove {path} after {max_attempts} attempts: {str(e)[:100]}")
                return False
        except FileNotFoundError:
            logger.debug(f"File {path} not found for removal")
            return True
        except Exception as e:
            logger.error(f"Unexpected error removing {path}: {e}")
            return False
    return False

def extract_archive(file_path, extract_dir):
    logger = logging.getLogger(f"{__name__}.extract_archive")
    logger.info(f"Starting extraction of {os.path.basename(file_path)} to {extract_dir}")
    if shutdown_event.is_set():
        logger.debug("Shutdown event set, aborting extraction")
        return False
        
    if not zipfile.is_zipfile(file_path):
        logger.error(f"Invalid ZIP archive: {os.path.basename(file_path)}")
        return False

    try:
        filename = os.path.basename(file_path)
        with zipfile.ZipFile(file_path) as zf:
            logger.debug(f"Opened zip file {filename}")
            infolist = zf.infolist()
            logger.debug(f"Zip contents: {[m.filename for m in infolist]}")
            top_dirs = {m.filename.split('/')[0] for m in infolist if '/' in m.filename and not m.filename.startswith(('__MACOSX/', '.DS_Store'))}
            logger.debug(f"Top directories: {top_dirs}")
            
            if len(top_dirs) == 1 and list(top_dirs)[0]:
                target = extract_dir
            else:
                base_name = os.path.splitext(filename)[0]
                target = os.path.join(extract_dir, base_name)
            
            logger.debug(f"Determined target directory: {target}")
            try:
                os.makedirs(target, exist_ok=True)
            except OSError as e:
                logger.error(f"Failed to create target directory {target}: {e}")
                return False

            for member in infolist:
                if shutdown_event.is_set():
                    return False
                if member.filename.startswith(('__MACOSX/', '.DS_Store')) or member.filename.endswith('/.DS_Store'):
                    continue
                target_path = os.path.join(target, member.filename)
                target_dir = os.path.dirname(target_path)
                try:
                    os.makedirs(target_dir, exist_ok=True)
                except OSError as e:
                    logger.error(f"Failed to create directory {target_dir}: {e}")
                    continue
                if not member.is_dir():
                    if os.path.exists(target_path):
                        if os.path.isdir(target_path):
                            try:
                                shutil.rmtree(target_path, ignore_errors=True)
                            except Exception as e:
                                logger.error(f"Failed to remove directory {target_path}: {e}")
                                continue
                        else:
                            if not safe_remove(target_path):
                                continue
                    try:
                        with open(target_path, 'wb') as f:
                            f.write(zf.read(member.filename))
                        logger.debug(f"Extracted {member.filename} to {target_path}")
                    except Exception as e:
                        logger.error(f"Failed to extract {member.filename}: {e}")
                        continue
            logger.debug(f"Extracted all files to {target}")

        if not safe_remove(file_path):
            return False
        logger.debug(f"Removed original zip file {file_path}")
        
        if len(top_dirs) == 1 and list(top_dirs)[0]:
            extract_path = os.path.join(target, list(top_dirs)[0])
        else:
            extract_path = target

        for root, dirs, files in os.walk(extract_path, topdown=False):
            if shutdown_event.is_set():
                logger.debug("Shutdown event set during cleaning, aborting")
                return False
            logger.debug(f"Cleaning directory: {root}")
            if '__MACOSX' in dirs:
                path = os.path.join(root, '__MACOSX')
                try:
                    shutil.rmtree(path, ignore_errors=True)
                except Exception as e:
                    logger.error(f"Failed to remove __MACOSX directory at {path}: {e}")
                logger.debug(f"Removed __MACOSX directory at {path}")
            for file in files:
                if file == '.DS_Store':
                    path = os.path.join(root, file)
                    if not safe_remove(path):
                        continue
        
        for root, _, files in os.walk(extract_path):
            if shutdown_event.is_set():
                logger.debug("Shutdown event set during nested extraction, aborting")
                return False
            logger.debug(f"Checking for nested zips in {root}")
            for file in files:
                if file.lower().endswith('.zip'):
                    nested_path = os.path.join(root, file)
                    logger.debug(f"Found nested zip: {nested_path}")
                    if extract_archive(nested_path, root):
                        logger.debug(f"Successfully extracted nested zip {file}")
                    else:
                        logger.error(f"Failed to extract nested zip {file}")
                        return False
        logger.info(f"Finished extraction of {filename}")
        return True
        
    except zipfile.BadZipFile as e:
        logger.error(f"Bad ZIP archive: {filename} - {str(e)[:100]}")
        return False
        
    except Exception as e:
        logger.error(f"Extraction failed for {filename}: {str(e)[:100]}")
        return False

def process_attachment(attachment, downloaded_set, max_retries, backoff_factor, max_backoff):
    logger = logging.getLogger(f"{__name__}.process_attachment")
    logger.info(f"Starting process_attachment for {attachment.get('name')}")
    logger.debug(f"Attachment details: {json.dumps(attachment, indent=2)}")
    if shutdown_event.is_set():
        return False
        
    filename = attachment.get('name')
    attachment_path = attachment['path']
    
    with _download_lock:
        if attachment_path in downloaded_set:
            logger.info(f"Skipping already downloaded {filename}")
            return False
    
    session = thread_local.session
    
    server = attachment.get('server', 'https://kemono.su')
    file_url = f"{server}/data{attachment_path}"
    local_path = os.path.join(DOWNLOAD_DIR, filename)
    
    position = position_queue.get()
    
    if shutdown_event.is_set():
        position_queue.put(position)
        return False

    outer_retry = 0
    max_outer_retries = 3
    while outer_retry < max_outer_retries and not shutdown_event.is_set():
        success = download_file(session, file_url, local_path, max_retries, backoff_factor, max_backoff, position)
        
        if not success:
            outer_retry += 1
            if outer_retry < max_outer_retries:
                delay = min(backoff_factor * (2 ** (outer_retry - 1)), max_backoff)
                time.sleep(delay)
            continue
        
        if extract_archive(local_path, REPOSITORY_DIR):
            with _download_lock:
                downloaded_set.add(attachment_path)
                save_downloaded_list(downloaded_set)
            logger.info(f"Successfully processed {filename}")
            position_queue.put(position)
            return True
        else:
            logger.error(f"Extraction failed for {filename}, retrying download {outer_retry + 1}/{max_outer_retries}")
            safe_remove(local_path)
            outer_retry += 1
            if outer_retry < max_outer_retries:
                delay = min(backoff_factor * (2 ** (outer_retry - 1)), max_backoff)
                time.sleep(delay)

    position_queue.put(position)
    return False

def collect_attachments(executor, users_posts, max_retries, backoff_factor, max_backoff):
    logger = logging.getLogger(f"{__name__}.collect_attachments")
    logger.info("Starting to collect attachments")
    if shutdown_event.is_set():
        return []
        
    post_tasks = [(user_id, post_id) for user_id, post_ids in users_posts.items() for post_id in post_ids]
    post_count = len(post_tasks)
    
    attachments = []
    fetch_futures = []
    for user_id, post_id in post_tasks:
        try:
            future = executor.submit(fetch_post_data, user_id, post_id, max_retries, backoff_factor, max_backoff)
            fetch_futures.append(future)
        except Exception as e:
            logger.error(f"Failed to submit fetch task for post {post_id}: {e}")
    
    not_done = set(fetch_futures)
    
    try:
        with tqdm(total=post_count, desc="Fetching posts", leave=True, position=0) as post_bar:
            while not_done and not shutdown_event.is_set():
                done, not_done = wait(not_done, timeout=0.5, return_when=FIRST_COMPLETED)
                for future in done:
                    try:
                        post_data = future.result()
                        if post_data:
                            for attachment in post_data.get('attachments', []):
                                if attachment.get('name', '').lower().endswith('.zip'):
                                    logger.debug(f"Found ZIP attachment: {attachment.get('name')}, path: {attachment['path']}")
                                    attachments.append(attachment)
                    except Exception as e:
                        logger.error(f"Error fetching post: {str(e)[:100]}")
                    with progress_lock:
                        post_bar.update(1)
                        post_bar.refresh()
                        
                if shutdown_event.is_set():
                    logger.warning("Cancelling pending fetch tasks")
                    for future in not_done:
                        future.cancel()
    except Exception as e:
        logger.error(f"Error in attachment collection progress: {e}")
    
    logger.info(f"Collected {len(attachments)} attachments")
    logger.debug(f"Attachment list: {[att.get('name') for att in attachments]}")
    return attachments

def parse_arguments():
    logger = logging.getLogger(f"{__name__}.parse_arguments")
    parser = argparse.ArgumentParser(
        description="Download and extract Czepeku maps from Patreon via Kemono.su",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-l', '--log-level', default='INFO', 
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set logging verbosity level")
    parser.add_argument('-u', '--users-posts', default=USERS_POSTS_FILE,
                        help="Path to JSON file containing users and posts to download")
    parser.add_argument('-c', '--cookies', default=COOKIE_FILE,
                        help="Path to cookies.txt file for authentication")
    parser.add_argument('-d', '--download-dir', default=DOWNLOAD_DIR,
                        help="Directory where downloaded files will be stored")
    parser.add_argument('-r', '--repo-dir', default=REPOSITORY_DIR,
                        help="Directory where extracted maps will be stored")
    parser.add_argument('--workers', type=int, default=8,
                        help="Number of parallel worker threads for downloading")
    parser.add_argument('--max-retries', type=int, default=15,
                        help="Maximum number of retries for failed requests")
    parser.add_argument('--backoff-factor', type=float, default=0.5,
                        help="Exponential backoff factor between retries")
    parser.add_argument('--max-backoff', type=int, default=10,
                        help="Maximum backoff delay in seconds between retries")
    parser.add_argument('--progress-only', action='store_true',
                        help="Show logs on console in addition to file (default: False)")
    try:
        args = parser.parse_args()
        logger.debug(f"Parsed arguments: {vars(args)}")
        return args
    except SystemExit:
        logger.critical("Argument parsing failed")
        sys.exit(1)

def signal_handler(signum, frame):
    logger = logging.getLogger(f"{__name__}.signal_handler")
    logger.warning(f"Received signal {signum}, initiating shutdown...")
    shutdown_event.set()

def init_worker(jar):
    try:
        thread_local.session = requests.Session()
        if jar is not None:
            thread_local.session.cookies = jar
    except Exception as e:
        logging.getLogger(f"{__name__}.init_worker").error(f"Failed to initialize session: {e}")

def main():
    args = parse_arguments()
    setup_logging(getattr(logging, args.log_level), args.progress_only)
    
    logger = logging.getLogger(f"{__name__}.main")
    logger.info("Starting Czepeku downloader")
    
    global USERS_POSTS_FILE, COOKIE_FILE, DOWNLOAD_DIR, REPOSITORY_DIR, position_queue
    USERS_POSTS_FILE = args.users_posts
    COOKIE_FILE = args.cookies
    DOWNLOAD_DIR = args.download_dir
    REPOSITORY_DIR = args.repo_dir
    position_queue = Queue()
    for i in range(1, args.workers + 1):
        position_queue.put(i)
    
    logger.info(f"Download directory: {DOWNLOAD_DIR}")
    logger.info(f"Repository directory: {REPOSITORY_DIR}")
    logger.info(f"Retry settings: max_retries={args.max_retries}, backoff_factor={args.backoff_factor}, max_backoff={args.max_backoff}")
    logger.info(f"Worker threads: {args.workers}")
    
    try:
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        os.makedirs(REPOSITORY_DIR, exist_ok=True)
    except OSError as e:
        logger.critical(f"Failed to create directories: {e}")
        sys.exit(1)
    
    jar = load_cookies(COOKIE_FILE)
    
    users_posts = load_users_posts()
    user_count = len(users_posts)
    post_count = sum(len(posts) for posts in users_posts.values())
    logger.info(f"Loaded {user_count} users with {post_count} posts")
    
    downloaded_set = load_downloaded_list()
    logger.info(f"Loaded {len(downloaded_set)} previously downloaded files")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    init_func = partial(init_worker, jar)
    
    try:
        executor = DaemonThreadPoolExecutor(max_workers=args.workers, initializer=init_func)
    except Exception as e:
        logger.critical(f"Failed to create executor: {e}")
        sys.exit(1)
    
    attachments = collect_attachments(executor, users_posts, args.max_retries, args.backoff_factor, args.max_backoff)
    
    if shutdown_event.is_set():
        logger.warning("Shutdown during attachment collection")
        executor.shutdown(wait=False)
        if listener:
            listener.stop()
        sys.exit(0)
    
    to_process = [att for att in attachments if att['path'] not in downloaded_set]
    
    total_attachments = len(to_process)
    if total_attachments == 0:
        logger.info("No new attachments found")
        executor.shutdown(wait=False)
        if listener:
            listener.stop()
        sys.exit(0)
    
    logger.info(f"Processing {total_attachments} attachments")
    
    completed_count = 0
    failed_count = 0
    
    try:
        futures = []
        for attachment in to_process:
            try:
                future = executor.submit(
                    process_attachment,
                    attachment,
                    downloaded_set,
                    args.max_retries,
                    args.backoff_factor,
                    args.max_backoff
                )
                futures.append(future)
            except Exception as e:
                logger.error(f"Failed to submit process task for {attachment.get('name')}: {e}")
                failed_count += 1
        
        not_done = set(futures)
        try:
            with tqdm(total=total_attachments, desc="Overall progress", leave=True, position=0) as overall_progress:
                while not_done and not shutdown_event.is_set():
                    logger.debug(f"Waiting for {len(not_done)} remaining tasks")
                    done, not_done = wait(not_done, timeout=0.5, return_when=FIRST_COMPLETED)
                    logger.debug(f"Processed {len(done)} tasks this iteration, {len(not_done)} remaining")
                    for future in done:
                        try:
                            success = future.result()
                            if success:
                                completed_count += 1
                            else:
                                failed_count += 1
                        except Exception as e:
                            logger.error(f"Attachment processing failed: {str(e)[:100]}")
                            failed_count += 1
                        with progress_lock:
                            overall_progress.update(1)
                            overall_progress.refresh()
                            
                    if shutdown_event.is_set():
                        logger.warning("Cancelling pending tasks")
                        for future in not_done:
                            future.cancel()
        except Exception as e:
            logger.error(f"Error in overall progress: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
    finally:
        executor.shutdown(wait=False)
        logger.info(f"Processing complete: {completed_count} succeeded, {failed_count} failed")
        if listener:
            listener.stop()
        logger.info("Shutdown complete")

if __name__ == '__main__':
    main()