import requests
import os
import zipfile
import json
from http.cookiejar import MozillaCookieJar
import logging
import pickle
from tqdm import tqdm
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import http.client
from requests.exceptions import ChunkedEncodingError
import shutil
import re
from colorlog import ColoredFormatter

SERVICE = "patreon"
USERS_POSTS_FILE = "users_posts.json" 
COOKIE_FILE = "cookies.txt"
DOWNLOADED_LIST_FILE = "downloaded_files.pkl"
DOWNLOAD_DIR = "downloads"
REPOSITORY_DIR = "maps_repository"


def setup_logging():
    """
    Configures the root logger to output colored debug and higher level logs to both a file and the console.
    """
    console_formatter = ColoredFormatter(
        "%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        }
    )

    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(console_formatter)

    file_handler = logging.FileHandler("patreon_downloader.log", mode='a')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    for h in root.handlers[:]:
        root.removeHandler(h)
    root.addHandler(console_handler)
    root.addHandler(file_handler)


def load_users_posts():
    """
    Loads a JSONC (JSON with comments) file by stripping // and /* */ comments before parsing.
    """
    logger = logging.getLogger('load_users_posts')
    if not os.path.exists(USERS_POSTS_FILE):
        logger.error(f"Users-posts config {USERS_POSTS_FILE} not found")
        exit(1)
    try:
        with open(USERS_POSTS_FILE, 'r') as f:
            content = f.read()
        content = re.sub(r'//.*', '', content)
        content = re.sub(r'/\*[\s\S]*?\*/', '', content)
        data = json.loads(content)
        logger.info(f"Loaded users-posts mapping for {len(data)} users")
        return data
    except Exception as e:
        logger.critical(f"Failed to load {USERS_POSTS_FILE}: {e}")
        exit(1)


def load_cookies():
    """
    Loads cookies from COOKIE_FILE into a requests.Session with retry logic.

    Returns:
        requests.Session: A session object with loaded cookies and retry-enabled adapters.
    """
    logger = logging.getLogger('load_cookies')
    logger.debug(f"Looking for cookie file at {COOKIE_FILE}")
    if not os.path.exists(COOKIE_FILE):
        logger.error(f"Cookie file {COOKIE_FILE} not found")
        exit(1)
    cookie_jar = MozillaCookieJar(COOKIE_FILE)
    cookie_jar.load()
    session = requests.Session()
    retry = Retry(total=15, backoff_factor=1, backoff_jitter=1, backoff_max=10, status_forcelist=[500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.cookies = cookie_jar
    logger.info("Cookies loaded into session")
    return session


def load_downloaded_list():
    """
    Loads the set of already downloaded file identifiers from DOWNLOADED_LIST_FILE.

    Returns:
        set: A set of downloaded file paths or identifiers.
    """
    logger = logging.getLogger('load_downloaded_list')
    if os.path.exists(DOWNLOADED_LIST_FILE):
        try:
            with open(DOWNLOADED_LIST_FILE, 'rb') as f:
                dl = pickle.load(f)
            logger.info(f"Loaded downloaded list with {len(dl)} entries")
            return dl
        except Exception as e:
            logger.error(f"Error loading downloaded list: {e}")
    else:
        logger.debug("No existing downloaded list found, starting fresh")
    return set()


def fetch_post_data(session, user_id, post_id):
    """
    Fetches JSON data for a specific post from the kemono API.

    Args:
        session (requests.Session): The HTTP session with cookies and retries configured.
        post_id (str): The identifier of the post to fetch.

    Returns:
        dict or None: Parsed JSON response if successful, otherwise None.
    """
    logger = logging.getLogger('fetch_post_data')
    url = f"https://kemono.su/api/v1/{SERVICE}/user/{user_id}/post/{post_id}"
    logger.debug(f"Fetching post data from {url}")
    try:
        r = session.get(url)
        r.raise_for_status()
        logger.info(f"Successfully fetched data for user {user_id}, post {post_id}")
        return r.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch post {post_id} for user {user_id}: {e}")
        return None


def download_file(session, url, path):
    """
    Downloads a file from the given URL to the specified local path, supporting resume on failure.

    Args:
        session (requests.Session): The HTTP session for downloading.
        url (str): The remote file URL.
        path (str): The local file path to save the download to.

    Returns:
        bool: True if download completed successfully, False otherwise.
    """
    logger = logging.getLogger('download_file')
    logger.info(f"Starting download: {url}")
    temp_path = path + '.part'
    os.makedirs(os.path.dirname(path), exist_ok=True)
    downloaded = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0
    if downloaded:
        logger.debug(f"Resuming download for {path} at byte {downloaded}")
    headers = {'Range': f'bytes={downloaded}-'} if downloaded else {}
    total_size = None

    while True:
        try:
            r = session.get(url, stream=True, headers=headers, timeout=30)
            if total_size is None:
                if 'content-range' in r.headers:
                    total_size = int(r.headers['content-range'].split('/')[-1])
                else:
                    total_size = int(r.headers.get('content-length', 0))
                logger.debug(f"Total size for {url}: {total_size} bytes")

            if r.status_code not in (200, 206):
                logger.error(f"Unexpected status {r.status_code} for {url}")
                return False

            with open(temp_path, 'ab') as f, tqdm(total=total_size, initial=downloaded, unit='B', unit_scale=True, desc=os.path.basename(path)) as pbar:
                for chunk in r.iter_content(8192):
                    if not chunk:
                        continue
                    f.write(chunk)
                    downloaded += len(chunk)
                    pbar.update(len(chunk))

            if downloaded >= total_size:
                logger.info(f"Download completed for {path}")
                break
            headers['Range'] = f'bytes={downloaded}-'

        except (http.client.IncompleteRead, ChunkedEncodingError) as e:
            logger.warning(f"Incomplete read error: {e}, retrying from {downloaded}")
            partial = getattr(e, 'partial', None)
            if partial:
                with open(temp_path, 'ab') as f:
                    f.write(partial)
                    downloaded += len(partial)
            headers['Range'] = f'bytes={downloaded}-'
            continue
        except requests.RequestException as e:
            logger.error(f"Download error for {url}: {e}")
            return False

    os.rename(temp_path, path)
    return True


def extract_archive(file_path, extract_dir):
    """
    Extracts a ZIP archive to the given directory.
    If the archive contains exactly one top-level folder (excluding any '__MACOSX' entries
    and ignoring '.DS_Store' files), contents are extracted directly into extract_dir. Otherwise,
    a new folder named after the archive (without extension) is created under extract_dir and
    contents are extracted into it. After extraction, the original archive is removed, nested
    archives are recursively extracted, and any '__MACOSX' directories or '.DS_Store' files
    are cleaned up.

    Args:
        file_path (str): Path to the ZIP file to extract.
        extract_dir (str): Directory to extract contents into.

    Returns:
        bool: True on success, False on failure.
    """
    logger = logging.getLogger('extract_archive')
    try:
        if not zipfile.is_zipfile(file_path):
            logger.error(f"{file_path} is not a valid ZIP archive")
            return False

        with zipfile.ZipFile(file_path, 'r') as zf:
            names = zf.namelist()
            top_levels = set(
                entry.split('/')[0]
                for entry in names
                if entry
                and not entry.split('/')[0].startswith('__MACOSX')
                and entry.split('/')[0] != '.DS_Store'
            )

            if len(top_levels) == 1 and any(
                entry.endswith('/') and entry.split('/')[0] == next(iter(top_levels))
                for entry in names
            ):
                target = extract_dir
                logger.info(
                    f"Archive contains a single folder '{next(iter(top_levels))}/', extracting into {extract_dir}"
                )
            else:
                base_name = os.path.splitext(os.path.basename(file_path))[0]
                target = os.path.join(extract_dir, base_name)
                os.makedirs(target, exist_ok=True)
                logger.info(
                    f"No single top-level folder (excluding '__MACOSX' and '.DS_Store') found; extracting into new directory {target}"
                )

            zf.extractall(target)

        try:
            os.remove(file_path)
            logger.debug(f"Removed archive {file_path} after extraction")
        except OSError as e:
            logger.warning(f"Could not remove archive {file_path}: {e}")

        for root, dirs, files in os.walk(target):
            if '__MACOSX' in dirs:
                macosx_path = os.path.join(root, '__MACOSX')
                shutil.rmtree(macosx_path, ignore_errors=True)
                logger.debug(f"Removed '__MACOSX' directory {macosx_path}")
            for fname in list(files):
                if fname == '.DS_Store':
                    ds_path = os.path.join(root, fname)
                    try:
                        os.remove(ds_path)
                        logger.debug(f"Removed '.DS_Store' file {ds_path}")
                    except OSError as e:
                        logger.warning(f"Could not remove '.DS_Store' file {ds_path}: {e}")

        for root, _, files in os.walk(target):
            for fname in files:
                if fname.lower().endswith('.zip'):
                    nested = os.path.join(root, fname)
                    if nested != file_path:
                        extract_archive(nested, root)
        return True
    except Exception as e:
        logger.error(f"Extraction failed for {file_path}: {e}")
        return False



def save_downloaded_list(dl):
    """
    Persists the set of downloaded file identifiers to DOWNLOADED_LIST_FILE using pickle.

    Args:
        dl (set): The set of downloaded file paths or identifiers.
    """
    logger = logging.getLogger('save_downloaded_list')
    try:
        with open(DOWNLOADED_LIST_FILE, 'wb') as f:
            pickle.dump(dl, f)
        logger.debug(f"Saved downloaded list with {len(dl)} entries")
    except Exception as e:
        logger.error(f"Failed to save downloaded list: {e}")


def process_attachments(session, post_data, user_id, pid, dl):
    """
    Downloads and extracts new .zip attachments from post data, skipping already downloaded items.

    Args:
        session (requests.Session): The HTTP session for downloading.
        post_data (dict): JSON data for the post containing attachments.
        pid (str): Post identifier, used for logging.
        dl (set): Set of already downloaded file paths or identifiers.
    """
    logger = logging.getLogger('process_attachments')
    logger.debug(f"Processing attachments for user {user_id}, post {pid}")
    for att in post_data.get('attachments', []):
        ext = att.get('name_extension')
        path = att.get('path')
        name = att.get('name')
        logger.debug(f"Found attachment: name={name}, extension={ext}, path={path}")
        if ext != '.zip':
            logger.debug("Skipping non-archive attachment")
            continue
        if not path or not name or path in dl:
            logger.debug("Already downloaded or invalid attachment, skipping")
            continue
        url = f"{att.get('server')}/data{path}"
        local_path = os.path.join(DOWNLOAD_DIR, name)
        if download_file(session, url, local_path):
            if extract_archive(local_path, REPOSITORY_DIR):
                dl.add(path)
                save_downloaded_list(dl)
            else:
                logger.error(f"Extraction failed for {local_path}")
        else:
            logger.error(f"Download failed for {url}")


def process_existing_archives():
    """
    Scans and extracts any .zip files already present in the REPOSITORY_DIR, including nested archives.
    """
    logger = logging.getLogger('process_existing_archives')
    logger.info(f"Scanning {REPOSITORY_DIR} for existing archives")
    for root, _, files in os.walk(REPOSITORY_DIR):
        for fname in files:
            if fname.lower().endswith('.zip'):
                path = os.path.join(root, fname)
                extract_archive(path, root)


def main():
    """
    Main entry point for the Patreon downloader. Initializes logging, loads cookies and state,
    ensures directories exist, processes existing archives, and iterates through specified posts.
    """
    setup_logging()
    logger = logging.getLogger('main')
    logger.info("Starting Patreon downloader script")

    users_posts = load_users_posts()
    session = load_cookies()
    downloaded_list = load_downloaded_list()
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(REPOSITORY_DIR, exist_ok=True)

    process_existing_archives()

    for user_id, post_ids in users_posts.items():
        for pid in post_ids:
            logger.info(f"Handling user {user_id}, post ID: {pid}")
            data = fetch_post_data(session, user_id, pid)
            if data:
                process_attachments(session, data, user_id, pid, downloaded_list)
            else:
                logger.error(f"No data for user {user_id}, post {pid}, skipping")

    logger.info("All users and posts processed, exiting")

if __name__ == "__main__":
    main()
