import os
import sys
import json
import zipfile
import shutil
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
import sqlite3
import subprocess
from typing import Any, Dict, List, Optional, Union, Tuple

SERVICE: str = "patreon"
USERS_POSTS_FILE: str = "users_posts.json"
COOKIE_FILE: str = "cookies.txt"
DOWNLOADED_LIST_FILE: str = "downloaded_files.db"
DOWNLOAD_DIR: str = "downloads"
REPOSITORY_DIR: str = "maps_repository"
LOG_DIR: str = "logs"
LOG_FILE: str = "czepeku-dl.log"

listener: Optional[QueueListener] = None
_download_lock: RLock = RLock()
shutdown_event: Event = Event()
thread_local: threading.local = threading.local()
position_queue: Optional[Queue] = None

class DownloadedDB:
    def __init__(self, db_file: str) -> None:
        self.db_file: str = db_file
        self.init_db()

    def init_db(self) -> None:
        conn: sqlite3.Connection = sqlite3.connect(self.db_file)
        c: sqlite3.Cursor = conn.cursor()
        c.execute('PRAGMA journal_mode=WAL')
        c.execute('''CREATE TABLE IF NOT EXISTS downloaded 
                     (attachment_path TEXT PRIMARY KEY, zip_name TEXT, extract_path TEXT, status TEXT)''')
        c.execute("PRAGMA table_info(downloaded)")
        columns = [row[1] for row in c.fetchall()]
        if 'status' not in columns:
            c.execute("ALTER TABLE downloaded ADD COLUMN status TEXT")
        conn.commit()
        conn.close()

    def load(self) -> Dict[str, Dict[str, str]]:
        def retry_op():
            conn: sqlite3.Connection = sqlite3.connect(self.db_file)
            c: sqlite3.Cursor = conn.cursor()
            c.execute('SELECT attachment_path, zip_name, extract_path, status FROM downloaded')
            data: Dict[str, Dict[str, str]] = {row[0]: {'zip_name': row[1], 'extract_path': row[2], 'status': row[3]} for row in c}
            conn.close()
            return data

        return self._with_retry(retry_op)

    def claim(self, attachment_path: str, zip_name: str) -> bool:
        def retry_op():
            conn: sqlite3.Connection = sqlite3.connect(self.db_file)
            c: sqlite3.Cursor = conn.cursor()
            c.execute('INSERT OR IGNORE INTO downloaded (attachment_path, zip_name, extract_path, status) VALUES (?, ?, ?, ?)',
                      (attachment_path, zip_name, None, 'processing'))
            conn.commit()
            inserted: bool = c.rowcount > 0
            conn.close()
            return inserted

        return self._with_retry(retry_op)

    def set_completed(self, attachment_path: str, zip_name: str, extract_path: str) -> None:
        def retry_op():
            conn: sqlite3.Connection = sqlite3.connect(self.db_file)
            c: sqlite3.Cursor = conn.cursor()
            c.execute('UPDATE downloaded SET zip_name = ?, extract_path = ?, status = ? WHERE attachment_path = ?',
                      (zip_name, extract_path, 'completed', attachment_path))
            conn.commit()
            conn.close()

        self._with_retry(retry_op)

    def set_failed(self, attachment_path: str) -> None:
        def retry_op():
            conn: sqlite3.Connection = sqlite3.connect(self.db_file)
            c: sqlite3.Cursor = conn.cursor()
            c.execute('UPDATE downloaded SET status = ? WHERE attachment_path = ?',
                      ('failed', attachment_path))
            conn.commit()
            conn.close()

        self._with_retry(retry_op)

    def _with_retry(self, func: callable, max_retries: int = 5, delay: float = 0.5) -> Any:
        for attempt in range(max_retries):
            try:
                return func()
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower():
                    if attempt < max_retries - 1:
                        time.sleep(delay * (2 ** attempt))
                    else:
                        raise
                else:
                    raise

class DaemonThreadPoolExecutor(ThreadPoolExecutor):
    def _adjust_thread_count(self) -> None:
        if self._idle_semaphore.acquire(timeout=0):
            return

        def weakref_cb(_, q: Queue = self._work_queue) -> None:
            q.put(None)

        num_threads: int = len(self._threads)
        if num_threads < self._max_workers:
            thread_name: str = '%s_%d' % (self._thread_name_prefix or self,
                                     num_threads)
            t: threading.Thread = threading.Thread(name=thread_name, target=_thread._worker,
                                 args=(weakref.ref(self, weakref_cb),
                                       self._work_queue,
                                       self._initializer,
                                       self._initargs))
            t.daemon = True
            t.start()
            self._threads.add(t)
            _thread._threads_queues[t] = self._work_queue

class TqdmHandler(logging.StreamHandler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg: str = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)

def setup_logging(log_level: str, progress_only: bool) -> None:
    global listener
    os.makedirs(LOG_DIR, exist_ok=True)
    log_queue: Queue = Queue(-1)
    
    console_formatter: ColoredFormatter = ColoredFormatter(
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
    console_handler: TqdmHandler = TqdmHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)

    file_formatter: logging.Formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    log_path: str = os.path.join(LOG_DIR, LOG_FILE)
    file_handler: RotatingFileHandler = RotatingFileHandler(
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

    queue_handler: QueueHandler = QueueHandler(log_queue)
    root_logger: logging.Logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()
    root_logger.addHandler(queue_handler)

    if os.path.exists(log_path) and os.path.getsize(log_path) > 0:
        try:
            file_handler.doRollover()
        except Exception as e:
            root_logger.error(f"Failed to rollover log file: {e}")

def load_config_file(filename: str) -> Optional[str]:
    logger: logging.Logger = logging.getLogger(f"{__name__}.load_config_file")
    if not os.path.exists(filename):
        logger.critical(f"Config file {filename} not found")
        return None
    try:
        with open(filename, 'r') as f:
            content: str = f.read()
        content = re.sub(r'//.*|/\*[\s\S]*?\*/', '', content)
        return content
    except Exception as e:
        logger.critical(f"Failed to read config file {filename}: {e}")
        return None

def load_users_posts() -> Dict[str, Any]:
    logger: logging.Logger = logging.getLogger(f"{__name__}.load_users_posts")
    logger.info("Loading users and posts configuration")
    content: Optional[str] = load_config_file(USERS_POSTS_FILE)
    if content is None:
        sys.exit(1)
    try:
        data: Dict[str, Any] = json.loads(content)
        logger.debug(f"Loaded users_posts data: {json.dumps(data, indent=2)}")
        return data
    except json.JSONDecodeError as e:
        logger.critical(f"Invalid JSON in {USERS_POSTS_FILE}: {e}")
        sys.exit(1)

def load_cookies(cookie_file: str) -> Optional[MozillaCookieJar]:
    logger: logging.Logger = logging.getLogger(f"{__name__}.load_cookies")
    logger.info(f"Loading cookies from {cookie_file}")
    jar: Optional[MozillaCookieJar] = None
    expired_count: int = 0
    
    if os.path.exists(cookie_file):
        try:
            jar = MozillaCookieJar(cookie_file)
            jar.load(ignore_discard=True, ignore_expires=True)
            
            current_time: float = time.time()
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

def fetch_post_data(user_id: str, post_id: str, max_retries: int, backoff_factor: float, max_backoff: int) -> Optional[Dict[str, Any]]:
    session: requests.Session = thread_local.session
    logger: logging.Logger = logging.getLogger(f"{__name__}.fetch_post_data")
    logger.info(f"Fetching post {post_id} for user {user_id}")
    if shutdown_event.is_set():
        return None
        
    url: str = f"https://kemono.su/api/v1/{SERVICE}/user/{user_id}/post/{post_id}"
    
    attempt: int = 0
    
    while attempt < max_retries and not shutdown_event.is_set():
        attempt += 1
        try:
            response: requests.Response = session.get(url, timeout=15)
            response.raise_for_status()
            data: Dict[str, Any] = response.json()
            logger.debug(f"Fetched post data for {post_id}: {json.dumps(data, indent=2)}")
            return data
        except RequestException as e:
            if shutdown_event.is_set():
                return None
                
            if attempt < max_retries:
                delay: float = min(backoff_factor * (2 ** (attempt - 1)), max_backoff)
                logger.debug(f"Retrying post {post_id} in {delay:.1f}s: {str(e)[:100]}")
                time.sleep(delay)
            else:
                logger.error(f"Failed to fetch post {post_id} after {max_retries} attempts: {str(e)[:100]}")
    return None

def download_file(session: requests.Session, url: str, path: str, max_retries: int, backoff_factor: float, max_backoff: int, pbar: tqdm) -> bool:
    logger: logging.Logger = logging.getLogger(f"{__name__}.download_file")
    if shutdown_event.is_set():
        return False
    
    total_size: Optional[int] = None
    retry_count: int = 0
    filename: str = os.path.basename(path)

    pbar.reset()
    pbar.set_description(f"Downloading {filename}")

    temp_path: str = path + '.part'
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to create directory for {path}: {e}")
        return False
    
    downloaded: int = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0
    headers: Dict[str, str] = {'Range': f'bytes={downloaded}-'} if downloaded else {}
    
    initial_downloaded: int = downloaded

    pbar.unit = 'B'
    pbar.unit_scale = True
    pbar.n = initial_downloaded
    pbar.refresh()

    while not shutdown_event.is_set() and retry_count < max_retries:
        logger.info(f"Starting download attempt {retry_count + 1}/{max_retries} for {filename}")
        logger.debug(f"Download headers: {headers}")
        try:
            with session.get(url, stream=True, headers=headers, timeout=(10, 5)) as r:
                r.raise_for_status()
                
                total_size_in_header: Optional[int] = None
                if 'content-range' in r.headers:
                    total_size_in_header = int(r.headers['content-range'].split('/')[-1])
                else:
                    total_size_in_header = int(r.headers.get('content-length', 0)) + downloaded

                if total_size_in_header is not None and total_size_in_header != total_size:
                    total_size = total_size_in_header
                    logger.debug(f"Setting total_size to {total_size} for {filename}")
                    pbar.total = total_size
                    pbar.refresh()
                
                with open(temp_path, 'ab') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if shutdown_event.is_set():
                            break
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            pbar.update(len(chunk))
                            pbar.refresh()
                            initial_downloaded = max(initial_downloaded, downloaded)
                
                if shutdown_event.is_set():
                    return False
                    
                if downloaded >= total_size:
                    logger.info(f"Download completed for {filename}")
                    try:
                        os.rename(temp_path, path)
                    except OSError as e:
                        logger.error(f"Failed to rename temp file for {filename}: {e}")
                        return False
                    return True
                headers['Range'] = f'bytes={downloaded}-'

        except HTTPError as e:
            status_code: Optional[int] = e.response.status_code if hasattr(e, 'response') else None
            if status_code == 416:
                if downloaded == 0:
                    logger.error(f"File not available for {filename}: {str(e)[:100]}")
                    return False
                else:
                    logger.info(f"Assuming download complete for {filename} due to 416 error with downloaded {downloaded} bytes")
                    try:
                        os.rename(temp_path, path)
                    except OSError as e:
                        logger.error(f"Failed to rename temp file for {filename}: {e}")
                        return False
                    return True
            retry_count += 1
            if retry_count < max_retries and not shutdown_event.is_set():
                delay: float = min(backoff_factor * (2 ** (retry_count - 1)), max_backoff)
                logger.debug(f"Download retry {retry_count}/{max_retries} for {filename}: {str(e)[:100]}, waiting {delay:.1f}s")
                time.sleep(delay)
                headers['Range'] = f'bytes={downloaded}-'
            else:
                logger.error(f"Download failed for {filename} after {max_retries} retries: {str(e)[:100]}")
                return False
        except (http.client.IncompleteRead, ChunkedEncodingError, ReadTimeout, RequestException) as e:
            retry_count += 1
            if retry_count < max_retries and not shutdown_event.is_set():
                delay: float = min(backoff_factor * (2 ** (retry_count - 1)), max_backoff)
                logger.debug(f"Download retry {retry_count}/{max_retries} for {filename}: {str(e)[:100]}, waiting {delay:.1f}s")
                time.sleep(delay)
                headers['Range'] = f'bytes={downloaded}-'
            else:
                logger.error(f"Download failed for {filename} after {max_retries} retries: {str(e)[:100]}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error during download of {filename}: {e}")
            retry_count += 1
            if retry_count < max_retries and not shutdown_event.is_set():
                delay: float = min(backoff_factor * (2 ** (retry_count - 1)), max_backoff)
                time.sleep(delay)
            else:
                return False
    
    return False

def safe_remove(path: str) -> bool:
    logger: logging.Logger = logging.getLogger(f"{__name__}.safe_remove")
    max_attempts: int = 5
    for attempt in range(max_attempts):
        try:
            os.remove(path)
            logger.debug(f"Successfully removed {path} on attempt {attempt + 1}")
            return True
        except PermissionError as e:
            if attempt < max_attempts - 1:
                delay: float = 0.2 * (2 ** attempt)
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

def extract_archive(file_path: str, extract_dir: str, pbar: tqdm) -> Optional[str]:
    logger: logging.Logger = logging.getLogger(f"{__name__}.extract_archive")
    logger.info(f"Starting extraction of {os.path.basename(file_path)} to {extract_dir}")
    if shutdown_event.is_set():
        logger.debug("Shutdown event set, aborting extraction")
        return None
        
    if not zipfile.is_zipfile(file_path):
        logger.error(f"Invalid ZIP archive: {os.path.basename(file_path)}")
        return None

    def sanitize_path(path: str) -> str:
        parts = path.lstrip('/').split('/')
        sanitized_parts = [p.strip() for p in parts if p.strip()]
        return os.path.join(*sanitized_parts)

    try:
        filename: str = os.path.basename(file_path)
        base_name: str = os.path.splitext(filename)[0].strip()
        with zipfile.ZipFile(file_path) as zf:
            logger.debug(f"Opened zip file {filename}")
            infolist: List[zipfile.ZipInfo] = zf.infolist()
            logger.debug(f"Zip contents: {[m.filename for m in infolist]}")
            logger.debug("Zip structure:")
            all_paths: List[str] = sorted([m.filename for m in infolist])
            for path in all_paths:
                parts: List[str] = path.split('/')
                indent: str = '  ' * (len(parts) - 1)
                name: str = parts[-1]
                if not name and len(parts) > 1:
                    indent = '  ' * (len(parts) - 2)
                    name = parts[-2] + '/'
                logger.debug(f"{indent}- {name}")
            junk_prefixes: Tuple[str, str] = ('__MACOSX', '.DS_Store')
            supplement_names: Tuple[str, str, str] = ('Gridded', 'Gridless', 'Supplement')
            top_dirs: set[str] = set()
            has_supplements: bool = False
            for m in infolist:
                if '/' in m.filename:
                    top: str = m.filename.split('/')[0].strip()
                    if any(top.startswith(j) for j in junk_prefixes):
                        continue
                    if any(top.startswith(s) for s in supplement_names):
                        has_supplements = True
                        continue
                    top_dirs.add(top)
            logger.debug(f"Top directories: {top_dirs}")
            logger.debug(f"Has supplements: {has_supplements}")
            
            if len(top_dirs) == 1 and not has_supplements and list(top_dirs)[0]:
                target: str = extract_dir
            else:
                target = os.path.join(extract_dir, base_name)
            
            logger.debug(f"Determined target directory: {target}")
            try:
                os.makedirs(target, exist_ok=True)
            except OSError as e:
                logger.error(f"Failed to create target directory {target}: {e}")
                return None

            extractable: List[zipfile.ZipInfo] = []
            for member in infolist:
                if member.filename.startswith(('__MACOSX/', '.DS_Store')) or member.filename.endswith('/.DS_Store'):
                    continue
                if not member.is_dir():
                    extractable.append(member)
            total_files: int = len(extractable)

            pbar.reset(total=total_files)
            pbar.unit = ' files'
            pbar.set_description(f"Extracting {filename}")

            for member in infolist:
                if shutdown_event.is_set():
                    return None
                if member.filename.startswith(('__MACOSX/', '.DS_Store')) or member.filename.endswith('/.DS_Store'):
                    continue
                sanitized_member_path = sanitize_path(member.filename)
                target_path: str = os.path.join(target, sanitized_member_path)
                target_dir: str = os.path.dirname(target_path)
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
                        pbar.update(1)
                        pbar.refresh()
                    except Exception as e:
                        logger.error(f"Failed to extract {member.filename}: {e}")
                        continue
            logger.debug(f"Extracted all files to {target}")

        if not safe_remove(file_path):
            return None
        logger.debug(f"Removed original zip file {file_path}")
        
        extract_path: str = target

        pbar.reset(total=None)
        pbar.unit = ''
        pbar.set_description(f"Cleaning {filename}")

        for root, dirs, files in os.walk(extract_path, topdown=False):
            if shutdown_event.is_set():
                logger.debug("Shutdown event set during cleaning, aborting")
                return None
            logger.debug(f"Cleaning directory: {root}")
            if '__MACOSX' in dirs:
                path: str = os.path.join(root, '__MACOSX')
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
                return None
            logger.debug(f"Checking for nested zips in {root}")
            for file in files:
                if file.lower().endswith('.zip'):
                    nested_path: str = os.path.join(root, file)
                    logger.debug(f"Found nested zip: {nested_path}")
                    nested_target: Optional[str] = extract_archive(nested_path, root, pbar)
                    if nested_target is None:
                        logger.error(f"Failed to extract nested zip {file}")
                        return None
                    else:
                        logger.debug(f"Successfully extracted nested zip {file} to {nested_target}")
        logger.info(f"Finished extraction of {filename}")
        return target
        
    except zipfile.BadZipFile as e:
        logger.error(f"Bad ZIP archive: {filename} - {str(e)[:100]}")
        return None
        
    except Exception as e:
        logger.error(f"Extraction failed for {filename}: {str(e)[:100]}")
        return None

def compress_folder_ntfs_lzx(folder_path: str, pbar: tqdm) -> bool:
    logger: logging.Logger = logging.getLogger(f"{__name__}.compress_folder_ntfs_lzx")
    logger.info(f"Applying NTFS LZX compression to {folder_path}")
    pbar.reset(total=None)
    pbar.unit = ''
    pbar.set_description(f"Compressing {os.path.basename(folder_path)}")
    try:
        cmd: List[str] = ['compact', '/c', '/s', '/a', '/i', '/exe:LZX', f'{folder_path}\\*']
        result: subprocess.CompletedProcess = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.debug(f"Compression command output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Compression failed: {e.stderr}")
        return False

def process_attachment(attachment: Dict[str, Any], downloaded_dict: Dict[str, Dict[str, str]], db: DownloadedDB, max_retries: int, backoff_factor: float, max_backoff: int, compress: bool) -> bool:
    logger: logging.Logger = logging.getLogger(f"{__name__}.process_attachment")
    logger.info(f"Starting process_attachment for {attachment.get('name')}")
    logger.debug(f"Attachment details: {json.dumps(attachment, indent=2)}")
    if shutdown_event.is_set():
        return False
        
    filename: Optional[str] = attachment.get('name').strip()
    attachment_path: str = attachment['path']
    
    with _download_lock:
        if attachment_path in downloaded_dict and downloaded_dict[attachment_path]['status'] == 'completed':
            logger.info(f"Skipping already downloaded {filename}")
            return False
    
    position: int = position_queue.get()
    
    if shutdown_event.is_set():
        position_queue.put(position)
        return False
    
    pbar: tqdm = tqdm(total=None, unit='', desc=f"Processing {filename}", position=position, leave=False, dynamic_ncols=True)
    pbar.refresh()
    
    if attachment_path not in downloaded_dict:
        pbar.set_description(f"Claiming {filename}")
        pbar.refresh()
        inserted: bool = db.claim(attachment_path, filename)
        
        if not inserted:
            logger.info(f"Skipping {filename} (already claimed by another thread)")
            pbar.close()
            position_queue.put(position)
            return False
    # Else, it's 'processing', proceed to resume
        
    session: requests.Session = thread_local.session
    
    server: str = attachment.get('server', 'https://kemono.su')
    file_url: str = f"{server}/data{attachment_path}"
    local_path: str = os.path.join(DOWNLOAD_DIR, filename)
    

    outer_retry: int = 0
    max_outer_retries: int = 3
    while outer_retry < max_outer_retries and not shutdown_event.is_set():
        success: bool = download_file(session, file_url, local_path, max_retries, backoff_factor, max_backoff, pbar)
        
        if not success:
            outer_retry += 1
            if outer_retry < max_outer_retries:
                delay: float = min(backoff_factor * (2 ** (outer_retry - 1)), max_backoff)
                time.sleep(delay)
            continue
        
        extract_path: Optional[str] = extract_archive(local_path, REPOSITORY_DIR, pbar)
        if extract_path:
            relative_extract_path: str = os.path.relpath(extract_path, REPOSITORY_DIR)
            if compress:
                compress_success: bool = compress_folder_ntfs_lzx(extract_path, pbar)
                if not compress_success:
                    logger.warning(f"Compression skipped for {extract_path} due to error")
            with _download_lock:
                downloaded_dict[attachment_path] = {'zip_name': filename, 'extract_path': relative_extract_path, 'status': 'completed'}
            pbar.set_description(f"Updating DB for {filename}")
            pbar.refresh()
            db.set_completed(attachment_path, filename, relative_extract_path)
            logger.info(f"Successfully processed {filename}")
            pbar.close()
            position_queue.put(position)
            return True
        else:
            logger.error(f"Extraction failed for {filename}, retrying download {outer_retry + 1}/{max_outer_retries}")
            safe_remove(local_path)
            outer_retry += 1
            if outer_retry < max_outer_retries:
                delay: float = min(backoff_factor * (2 ** (outer_retry - 1)), max_backoff)
                time.sleep(delay)

    if os.path.exists(local_path):
        safe_remove(local_path)
    pbar.set_description(f"Marking as failed {filename}")
    pbar.refresh()
    db.set_failed(attachment_path)
    pbar.close()
    position_queue.put(position)
    return False

def collect_attachments(executor: ThreadPoolExecutor, users_posts: Dict[str, List[str]], max_retries: int, backoff_factor: float, max_backoff: int) -> List[Dict[str, Any]]:
    logger: logging.Logger = logging.getLogger(f"{__name__}.collect_attachments")
    logger.info("Starting to collect attachments")
    if shutdown_event.is_set():
        return []
        
    post_tasks: List[Tuple[str, str]] = [(user_id, post_id) for user_id, post_ids in users_posts.items() for post_id in post_ids]
    post_count: int = len(post_tasks)
    
    attachments: List[Dict[str, Any]] = []
    seen_paths: set[str] = set()
    fetch_futures: List[_base.Future] = []
    for user_id, post_id in post_tasks:
        try:
            future: _base.Future = executor.submit(fetch_post_data, user_id, post_id, max_retries, backoff_factor, max_backoff)
            fetch_futures.append(future)
        except Exception as e:
            logger.error(f"Failed to submit fetch task for post {post_id}: {e}")
    
    not_done: set[_base.Future] = set(fetch_futures)
    
    try:
        with tqdm(total=post_count, desc="Fetching posts", leave=True, position=0) as post_bar:
            while not_done and not shutdown_event.is_set():
                done, not_done = wait(not_done, timeout=0.5, return_when=FIRST_COMPLETED)
                for future in done:
                    try:
                        post_data: Optional[Dict[str, Any]] = future.result()
                        if post_data:
                            for attachment in post_data.get('attachments', []):
                                if attachment.get('name', '').lower().endswith('.zip'):
                                    if attachment['path'] not in seen_paths:
                                        seen_paths.add(attachment['path'])
                                        attachments.append(attachment)
                                    else:
                                        logger.debug(f"Skipping duplicate attachment path: {attachment['path']}")
                    except Exception as e:
                        logger.error(f"Error fetching post: {str(e)[:100]}")
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

def parse_arguments() -> argparse.Namespace:
    logger: logging.Logger = logging.getLogger(f"{__name__}.parse_arguments")
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
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
    parser.add_argument('--creators', type=str, nargs='+',
                        help="Creators to process (e.g., czepeku czepekuscifi)")
    parser.add_argument('--sets', type=str, nargs='+',
                        help="Sets to process (main animated)")
    parser.add_argument('--compress', action='store_true',
                        help="Apply NTFS LZX compression to extracted folders (Windows only)")
    try:
        args: argparse.Namespace = parser.parse_args()
        logger.debug(f"Parsed arguments: {vars(args)}")
        return args
    except SystemExit:
        logger.critical("Argument parsing failed")
        sys.exit(1)

def signal_handler(signum: int, frame: Any) -> None:
    logger: logging.Logger = logging.getLogger(f"{__name__}.signal_handler")
    logger.warning(f"Received signal {signum}, initiating shutdown...")
    shutdown_event.set()

def init_worker(jar: Optional[MozillaCookieJar]) -> None:
    try:
        thread_local.session = requests.Session()
        if jar is not None:
            thread_local.session.cookies = jar
    except Exception as e:
        logging.getLogger(f"{__name__}.init_worker").error(f"Failed to initialize session: {e}")

def main() -> None:
    args: argparse.Namespace = parse_arguments()
    setup_logging(args.log_level, args.progress_only)
    
    logger: logging.Logger = logging.getLogger(f"{__name__}.main")
    logger.info("Starting Czepeku downloader")
    
    global USERS_POSTS_FILE, COOKIE_FILE, DOWNLOAD_DIR, REPOSITORY_DIR, position_queue, DOWNLOADED_LIST_FILE
    USERS_POSTS_FILE = args.users_posts
    COOKIE_FILE = args.cookies
    DOWNLOAD_DIR = args.download_dir
    REPOSITORY_DIR = args.repo_dir
    DOWNLOADED_LIST_FILE = "downloaded_files.db"
    position_queue = Queue()
    for i in range(1, args.workers + 1):
        position_queue.put(i)
    
    logger.info(f"Download directory: {DOWNLOAD_DIR}")
    logger.info(f"Repository directory: {REPOSITORY_DIR}")
    logger.info(f"Retry settings: max_retries={args.max_retries}, backoff_factor={args.backoff_factor}, max_backoff={args.max_backoff}")
    logger.info(f"Worker threads: {args.workers}")
    if args.compress:
        logger.info("NTFS LZX compression enabled for extracted folders")
    
    try:
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        os.makedirs(REPOSITORY_DIR, exist_ok=True)
    except OSError as e:
        logger.critical(f"Failed to create directories: {e}")
        sys.exit(1)
    
    jar: Optional[MozillaCookieJar] = load_cookies(COOKIE_FILE)
    
    users_posts_raw: Dict[str, Any] = load_users_posts()
    
    if args.creators:
        users_posts_raw = {k: v for k, v in users_posts_raw.items() if k in args.creators}
    
    users_posts: Dict[str, List[str]] = {}
    for creator, info in users_posts_raw.items():
        user_id: str = info['user_id']
        post_dict: Dict[str, str] = info['posts']
        selected_posts: List[str] = list(post_dict.values())
        if args.sets:
            selected_posts = [post_dict[s] for s in args.sets if s in post_dict]
        if selected_posts:
            users_posts[user_id] = selected_posts
    
    user_count: int = len(users_posts)
    post_count: int = sum(len(posts) for posts in users_posts.values())
    logger.info(f"Loaded {user_count} users with {post_count} posts")
    
    db: DownloadedDB = DownloadedDB(DOWNLOADED_LIST_FILE)
    downloaded_dict: Dict[str, Dict[str, str]] = db.load()
    logger.info(f"Loaded {len(downloaded_dict)} previously downloaded files")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    init_func = partial(init_worker, jar)
    
    try:
        executor: DaemonThreadPoolExecutor = DaemonThreadPoolExecutor(max_workers=args.workers, initializer=init_func)
    except Exception as e:
        logger.critical(f"Failed to create executor: {e}")
        sys.exit(1)
    
    attachments: List[Dict[str, Any]] = collect_attachments(executor, users_posts, args.max_retries, args.backoff_factor, args.max_backoff)
    
    if shutdown_event.is_set():
        logger.warning("Shutdown during attachment collection")
        executor.shutdown(wait=False)
        if listener:
            listener.stop()
        sys.exit(0)
    
    with _download_lock:
        to_process: List[Dict[str, Any]] = [att for att in attachments if att['path'] not in downloaded_dict or downloaded_dict[att['path']]['status'] != 'completed']
    
    total_attachments: int = len(to_process)
    if total_attachments == 0:
        logger.info("No new attachments found")
        executor.shutdown(wait=False)
        if listener:
            listener.stop()
        sys.exit(0)
    
    logger.info(f"Processing {total_attachments} attachments")
    
    completed_count: int = 0
    failed_count: int = 0
    
    try:
        futures: List[_base.Future] = []
        for attachment in to_process:
            try:
                future: _base.Future = executor.submit(
                    process_attachment,
                    attachment,
                    downloaded_dict,
                    db,
                    args.max_retries,
                    args.backoff_factor,
                    args.max_backoff,
                    args.compress
                )
                futures.append(future)
            except Exception as e:
                logger.error(f"Failed to submit process task for {attachment.get('name')}: {e}")
                failed_count += 1
        
        not_done: set[_base.Future] = set(futures)
        try:
            with tqdm(total=total_attachments, desc="Overall progress", leave=True, position=0) as overall_progress:
                while not_done and not shutdown_event.is_set():
                    logger.debug(f"Waiting for {len(not_done)} remaining tasks")
                    done, not_done = wait(not_done, timeout=0.5, return_when=FIRST_COMPLETED)
                    logger.debug(f"Processed {len(done)} tasks this iteration, {len(not_done)} remaining")
                    for future in done:
                        try:
                            success: bool = future.result()
                            if success:
                                completed_count += 1
                            else:
                                failed_count += 1
                        except Exception as e:
                            logger.error(f"Attachment processing failed: {str(e)[:100]}")
                            failed_count += 1
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