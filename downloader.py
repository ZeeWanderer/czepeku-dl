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

SERVICE = "patreon"
USER_ID = "16010661"
POST_IDS = ["27816327"]
COOKIE_FILE = "cookies.txt"
DOWNLOADED_LIST_FILE = "downloaded_files.pkl"
DOWNLOAD_DIR = "downloads"
REPOSITORY_DIR = "maps_repository"


def load_cookies():
    if not os.path.exists(COOKIE_FILE):
        logging.error(f"Cookie file {COOKIE_FILE} not found")
        exit(1)
    cookie_jar = MozillaCookieJar(COOKIE_FILE)
    cookie_jar.load()
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.cookies = cookie_jar
    logging.info("Cookies loaded")
    return session


def load_downloaded_list():
    try:
        if os.path.exists(DOWNLOADED_LIST_FILE):
            with open(DOWNLOADED_LIST_FILE, 'rb') as f:
                return pickle.load(f)
        return set()
    except pickle.PickleError:
        logging.error("Error loading downloaded list, starting fresh")
        return set()


def fetch_post_data(session, post_id):
    url = f"https://kemono.su/api/v1/{SERVICE}/user/{USER_ID}/post/{post_id}"
    try:
        response = session.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch post {post_id}: {e}")
        if 'response' in locals():
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Response content: {response.text}")
        return None


def download_file(session, url, path):
    temp_path = path + '.part'
    headers = {}
    if os.path.exists(temp_path):
        resume_byte_pos = os.path.getsize(temp_path)
        headers['Range'] = f'bytes={resume_byte_pos}-'
    try:
        response = session.get(url, stream=True, headers=headers)
        response.raise_for_status()
        mode = 'ab' if response.status_code == 206 else 'wb'
        total_size = int(response.headers.get('content-length', 0))
        with open(temp_path, mode) as f, tqdm(total=total_size, unit='B', unit_scale=True, desc=os.path.basename(path)) as pbar:
            for chunk in response.iter_content(8192):
                try:
                    f.write(chunk)
                except http.client.IncompleteRead as e:
                    f.write(e.partial)
                pbar.update(len(chunk))
        os.rename(temp_path, path)
        return True
    except requests.RequestException as e:
        logging.error(f"Download failed for {os.path.basename(path)}: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False


def extract_zip(file_path, extract_dir):
    if not zipfile.is_zipfile(file_path):
        logging.error(f"{file_path} is not a zip file")
        return False
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        return True
    except zipfile.BadZipFile:
        logging.error(f"{file_path} is a bad zip file")
        return False
    except Exception as e:
        logging.error(f"Extraction failed for {file_path}: {e}")
        return False


def save_downloaded_list(downloaded_list):
    with open(DOWNLOADED_LIST_FILE, 'wb') as f:
        pickle.dump(downloaded_list, f)


def process_attachments(session, post_data, post_id, downloaded_list):
    if 'attachments' not in post_data:
        logging.warning(f"No attachments found in post {post_id}")
        return
    for attachment in post_data['attachments']:
        if attachment.get('name_extension') != '.zip':
            logging.info(f"Skipping non-zip attachment: {attachment.get('name')}")
            continue
        file_path = attachment.get('path')
        file_name = attachment.get('name')
        if not file_path or not file_name:
            logging.warning(f"Invalid attachment in post {post_id}")
            continue
        if file_path in downloaded_list:
            logging.info(f"{file_name} already downloaded")
            continue
        download_url = f"{attachment.get('server')}/data{file_path}"
        local_path = os.path.join(DOWNLOAD_DIR, file_name)
        if download_file(session, download_url, local_path):
            if extract_zip(local_path, REPOSITORY_DIR):
                try:
                    os.remove(local_path)
                    logging.info(f"Deleted zip file {file_name} after extraction")
                except OSError as e:
                    logging.error(f"Failed to delete zip file {file_name}: {e}")
                downloaded_list.add(file_path)
                save_downloaded_list(downloaded_list)
                logging.info(f"Processed {file_name} from post {post_id}")
            else:
                logging.error(f"Failed to extract {file_name}")
        else:
            logging.error(f"Failed to download {file_name}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Script started")
    session = load_cookies()
    downloaded_list = load_downloaded_list()
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(REPOSITORY_DIR, exist_ok=True)
    for post_id in POST_IDS:
        post_data = fetch_post_data(session, post_id)
        if post_data:
            process_attachments(session, post_data, post_id, downloaded_list)
    logging.info("Script completed")
