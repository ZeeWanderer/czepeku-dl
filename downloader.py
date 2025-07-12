import requests
import os
import zipfile
from http.cookiejar import MozillaCookieJar
import logging
import pickle
from tqdm import tqdm
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import http.client
from requests.exceptions import ChunkedEncodingError

SERVICE = "patreon"
USER_ID = "16010661"
POST_IDS = ["27816327"]
COOKIE_FILE = "cookies.txt"
DOWNLOADED_LIST_FILE = "downloaded_files.pkl"
DOWNLOAD_DIR = "downloads"
REPOSITORY_DIR = "maps_repository"


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("patreon_downloader.log", mode='a'),
            logging.StreamHandler()
        ]
    )


def load_cookies():
    logger = logging.getLogger('load_cookies')
    logger.debug(f"Looking for cookie file at {COOKIE_FILE}")
    if not os.path.exists(COOKIE_FILE):
        logger.error(f"Cookie file {COOKIE_FILE} not found")
        exit(1)
    cookie_jar = MozillaCookieJar(COOKIE_FILE)
    cookie_jar.load()
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.cookies = cookie_jar
    logger.info("Cookies loaded into session")
    return session


def load_downloaded_list():
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


def fetch_post_data(session, post_id):
    logger = logging.getLogger('fetch_post_data')
    url = f"https://kemono.su/api/v1/{SERVICE}/user/{USER_ID}/post/{post_id}"
    logger.debug(f"Fetching post data from {url}")
    try:
        r = session.get(url)
        r.raise_for_status()
        logger.info(f"Successfully fetched data for post {post_id}")
        return r.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch post {post_id}: {e}")
        return None


def download_file(session, url, path):
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
    Extracts .zip archives. Recursively extracts nested archives and removes them.
    """
    logger = logging.getLogger('extract_archive')
    base, ext = os.path.splitext(file_path.lower())
    try:
        if ext == '.zip':
            logger.info(f"Extracting ZIP {file_path} to {extract_dir}")
            if not zipfile.is_zipfile(file_path):
                logger.error(f"{file_path} is not a valid ZIP")
                return False
            with zipfile.ZipFile(file_path, 'r') as z:
                z.extractall(extract_dir)
        else:
            logger.debug(f"Skipping non-archive {file_path}")
            return False
    except Exception as e:
        logger.error(f"Extraction failed for {file_path}: {e}")
        return False
    try:
        os.remove(file_path)
        logger.debug(f"Removed archive {file_path} after extraction")
    except OSError as e:
        logger.warning(f"Could not remove archive {file_path}: {e}")
    for root, _, files in os.walk(extract_dir):
        for fname in files:
            if fname.lower().endswith(('.zip')):
                nested = os.path.join(root, fname)
                if nested != file_path:
                    extract_archive(nested, root)
    return True


def save_downloaded_list(dl):
    logger = logging.getLogger('save_downloaded_list')
    try:
        with open(DOWNLOADED_LIST_FILE, 'wb') as f:
            pickle.dump(dl, f)
        logger.debug(f"Saved downloaded list with {len(dl)} entries")
    except Exception as e:
        logger.error(f"Failed to save downloaded list: {e}")


def process_attachments(session, post_data, pid, dl):
    logger = logging.getLogger('process_attachments')
    logger.debug(f"Processing attachments for post {pid}")
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
    logger = logging.getLogger('process_existing_archives')
    logger.info(f"Scanning {REPOSITORY_DIR} for existing archives")
    for root, _, files in os.walk(REPOSITORY_DIR):
        for fname in files:
            if fname.lower().endswith(('.zip')):
                path = os.path.join(root, fname)
                extract_archive(path, root)


def main():
    setup_logging()
    logger = logging.getLogger('main')
    logger.info("Starting Patreon downloader script")

    session = load_cookies()
    downloaded_list = load_downloaded_list()
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(REPOSITORY_DIR, exist_ok=True)

    process_existing_archives()

    for pid in POST_IDS:
        logger.info(f"Handling post ID: {pid}")
        data = fetch_post_data(session, pid)
        if data:
            process_attachments(session, data, pid, downloaded_list)
        else:
            logger.error(f"No data for post {pid}, skipping")

    logger.info("All posts processed, exiting")


if __name__ == "__main__":
    main()
