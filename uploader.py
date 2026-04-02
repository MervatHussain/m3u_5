import os
import sys
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import boto3
import psycopg2
from botocore.exceptions import ClientError
from decouple import config as env_config
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

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


class ConfigurationManager:
    def __init__(self):
        self.secret_dict_db = {}
        # Use DB env vars directly if available, otherwise fetch from Secrets Manager
        if os.getenv('DB_HOST'):
            self.secret_dict_db = {
                'dbname': os.getenv('DB_NAME'),
                'host': os.getenv('DB_HOST'),
                'port': os.getenv('DB_PORT', '5432'),
                'username': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD'),
            }
        else:
            profile = os.getenv('AWS_PROFILE')
            session = boto3.session.Session(profile_name=profile) if profile else boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=os.getenv('REGION_NAME', 'us-east-1'),
            )
            secret_db = client.get_secret_value(SecretId=os.getenv('DB_SECRET'))
            self.secret_dict_db = json.loads(secret_db['SecretString'])

    @property
    def master_db(self):
        return self.secret_dict_db['dbname']

    @property
    def database_host(self):
        return self.secret_dict_db['host']

    @property
    def database_port(self):
        return self.secret_dict_db['port']

    @property
    def database_username(self):
        return self.secret_dict_db['username']

    @property
    def database_password(self):
        return self.secret_dict_db['password']

    @property
    def aws_access_key_id(self):
        return os.getenv('AWS_ACCESS_KEY_ID')

    @property
    def aws_secret_access_key(self):
        return os.getenv('AWS_SECRET_ACCESS_KEY')

    @property
    def aws_session_token(self):
        return os.getenv('AWS_SESSION_TOKEN')

    @property
    def aws_region(self):
        return os.getenv('AWS_S3_REGION_NAME', 'us-east-1')

    @property
    def aws_bucket_name(self):
        return os.getenv('AWS_BUCKET_NAME', 'rh5-livestreaming-us-east-1')

    @property
    def aws_folder(self):
        return os.getenv('AWS_FOLDER', 'public/uploads/m3u/')


class M3UUploader:
    def __init__(self):
        self.config = ConfigurationManager()
        profile = os.getenv('AWS_PROFILE')
        if profile:
            session = boto3.Session(profile_name=profile, region_name=self.config.aws_region)
        elif self.config.aws_access_key_id:
            session = boto3.Session(
                aws_access_key_id=self.config.aws_access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key,
                aws_session_token=self.config.aws_session_token,
                region_name=self.config.aws_region,
            )
        else:
            session = boto3.Session(region_name=self.config.aws_region)
        self.s3_client = session.client('s3')
        self._validate_aws_connection()

    def _validate_aws_connection(self):
        try:
            self.s3_client.head_bucket(Bucket=self.config.aws_bucket_name)
            logger.info(f"Connected to S3 bucket: {self.config.aws_bucket_name}")
        except ClientError as e:
            logger.error(f"Failed AWS connection: {e}")
            raise

    def _get_db_connection(self):
        return psycopg2.connect(
            host=self.config.database_host,
            port=self.config.database_port,
            dbname=self.config.master_db,
            user=self.config.database_username,
            password=self.config.database_password,
            cursor_factory=RealDictCursor
        )

    def _file_exists_in_db(self, file_path):
        conn = self._get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SET search_path TO live_streaming_db;")
            cursor.execute("SELECT id FROM m3u_files WHERE file_path=%s LIMIT 1", (file_path,))
            return cursor.fetchone() is not None
        finally:
            cursor.close()
            conn.close()

    def _file_exists_in_s3(self, file_path):
        try:
            self.s3_client.head_object(Bucket=self.config.aws_bucket_name, Key=file_path)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"S3 check error: {e}")
                raise

    def _upload_to_s3(self, local_path, s3_path):
        self.s3_client.upload_file(local_path, self.config.aws_bucket_name, s3_path)
        logger.info(f"Uploaded {local_path} -> S3: {s3_path}")

    def _insert_to_db(self, s3_path, local_path, last_updated_by_id=1):
        conn = self._get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SET search_path TO live_streaming_db;")
            file_type = local_path.suffix[1:]
            cursor.execute(
                """
                INSERT INTO m3u_files (file_path, file_type, last_updated_by_id, created_at)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (s3_path, file_type, last_updated_by_id, datetime.now(timezone.utc))
            )
            conn.commit()
            file_id = cursor.fetchone()['id']
            logger.info(f"DB record created: {s3_path} (ID: {file_id})")
            return file_id
        finally:
            cursor.close()
            conn.close()

    def upload_file(self, local_path, last_updated_by_id=1):
        local_path = Path(local_path)
        if not local_path.exists() or not local_path.is_file():
            logger.warning(f"Skipping, not a file: {local_path}")
            return None

        today = datetime.now(timezone.utc)
        date_prefix = today.strftime("%Y/%m/%d")
        s3_path = f"{self.config.aws_folder}{date_prefix}/{local_path.name}"
        file_id = None

        if self._file_exists_in_db(s3_path):
            logger.info(f"[SKIP] Already in DB: {s3_path}")
            conn = self._get_db_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("SET search_path TO live_streaming_db;")
                cursor.execute("SELECT id FROM m3u_files WHERE file_path=%s LIMIT 1", (s3_path,))
                file_id = cursor.fetchone()['id']
            finally:
                cursor.close()
                conn.close()
                return {'id': file_id, 'url': f"https://{self.config.aws_bucket_name}.s3.{self.config.aws_region}.amazonaws.com/{s3_path}"}

        if self._file_exists_in_s3(s3_path):
            logger.info(f"[S3] File exists in S3 but not in DB, inserting: {s3_path}")
            file_id = self._insert_to_db(s3_path, local_path, last_updated_by_id)
            return {'id': file_id, 'url': f"https://{self.config.aws_bucket_name}.s3.{self.config.aws_region}.amazonaws.com/{s3_path}"}

        self._upload_to_s3(local_path, s3_path)
        file_id = self._insert_to_db(s3_path, local_path, last_updated_by_id)
        return {'id': file_id, 'url': f"https://{self.config.aws_bucket_name}.s3.{self.config.aws_region}.amazonaws.com/{s3_path}"}

    def upload_folder(self, folder_path, last_updated_by_id=1):
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
