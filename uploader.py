import os
import sys
import logging
from pathlib import Path

import requests
from decouple import config as env_config
from dotenv import load_dotenv

# Resolve configs/ folder relative to this script or exe
ORIGINAL_DIR = Path(os.getcwd())
if getattr(sys, 'frozen', False):
    SCRIPT_DIR = Path(sys.executable).parent
else:
    SCRIPT_DIR = Path(__file__).parent

CONFIGS_DIR = SCRIPT_DIR / 'configs'

load_dotenv(str(CONFIGS_DIR / '.env'))
load_dotenv(str(CONFIGS_DIR / f'{env_config("DEV_ENV", "staging")}.env'))

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('m3u_upload.log', encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {'.m3u', '.m3u8', '.txt'}

API_URL = os.getenv('API_URL', 'https://api-v1.rightshero.com/api/v1/links_management/live_streaming/m3u/files')
X_API_KEY = os.getenv('X_API_KEY', '')
LAST_UPDATED_BY_ID = int(os.getenv('LAST_UPDATED_BY_ID', '1'))


class M3UUploader:
    def __init__(self):
        self.api_url = API_URL
        self.headers = {'x-api-key': X_API_KEY}
        logger.info(f"Using API: {self.api_url}")

    def upload_file(self, local_path, last_updated_by_id=LAST_UPDATED_BY_ID):
        local_path = Path(local_path)
        if not local_path.exists() or not local_path.is_file():
            logger.warning(f"Skipping, not a file: {local_path}")
            return None

        file_type = local_path.suffix[1:]

        with open(local_path, 'rb') as f:
            files = {'file_path_0': (local_path.name, f, 'application/octet-stream')}
            data = {
                'file_type_0': file_type,
                'last_updated_by_id_0': str(last_updated_by_id),
            }
            try:
                response = requests.post(self.api_url, headers=self.headers, files=files, data=data, timeout=60)
                if response.status_code in (200, 201):
                    result = response.json()
                    logger.info(f"Uploaded: {local_path.name} -> {result}")
                    return result
                elif response.status_code == 409:
                    logger.info(f"[SKIP] Already exists: {local_path.name}")
                    return None
                else:
                    logger.error(f"Failed {local_path.name}: {response.status_code} {response.text}")
                    return None
            except Exception as e:
                logger.error(f"Error uploading {local_path.name}: {e}")
                return None

    def upload_folder(self, folder_path, last_updated_by_id=LAST_UPDATED_BY_ID):
        folder = Path(folder_path)
        if not folder.exists() or not folder.is_dir():
            logger.error(f"Folder does not exist: {folder}")
            return []

        files = [f for f in folder.rglob("*") if f.suffix.lower() in SUPPORTED_EXTENSIONS]
        logger.info(f"Found {len(files)} supported files in {folder}")

        results = []
        for file in files:
            res = self.upload_file(file, last_updated_by_id)
            if res:
                results.append(res)

        return results


def main():
    if len(sys.argv) < 2:
        target = SCRIPT_DIR / 'files'
    else:
        target = Path(os.path.join(ORIGINAL_DIR, sys.argv[1])).resolve()

    uploader = M3UUploader()

    if target.is_file():
        result = uploader.upload_file(target)
        print(result)
    elif target.is_dir():
        results = uploader.upload_folder(target)
        for r in results:
            print(r)
    else:
        print(f"Error: '{target}' is not a valid file or directory.")
        sys.exit(1)


if __name__ == "__main__":
    main()
