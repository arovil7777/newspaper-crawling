import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
import glob
import zipfile

# env 파일 로드
load_dotenv()

# 로그 폴더 경로
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, f"app_{datetime.now().strftime('%Y%m%d')}.log")


def compress_old_logs(log_dir: str = log_dir, days: int = 3):
    # 지정한 일수 (days)보다 오래된 로그 파일 압축
    now = datetime.now()
    cutoff = now - timedelta(days=days)

    for log_file in glob.glob(os.path.join(log_dir, "*.log")):
        file_time = datetime.fromtimestamp(os.path.getctime(log_file))
        if file_time < cutoff:
            zip_file = log_file + ".zip"
            with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(log_file, os.path.basename(log_file))
            os.remove(log_file)
            logger.info(f"{log_file} 압축 완료")


def setup_logging(log_file: str):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    # WebDriver Manager 로깅 비활성화
    os.environ["WDM_LOG"] = "0"
    logger = logging.getLogger(__name__)
    return logger


class Config:
    INTERNAL_HDFS_URL = os.getenv("INTERNAL_HDFS_URL", "http://localhost:9870")
    EXTERNAL_HDFS_URL = os.getenv("EXTERNAL_HDFS_URL", "http://localhost:9870")
    HDFS_USER = os.getenv("HDFS_USER", "hadoop")
    HDFS_DIR = os.getenv("HDFS_DIR", "/data")
    INTERNAL_HBASE_HOST = os.getenv("INTERNAL_HBASE_HOST", "localhost")
    INTERNAL_HBASE_PORT = int(os.getenv("INTERNAL_HBASE_PORT", 9090))
    EXTERNAL_HBASE_HOST = os.getenv("EXTERNAL_HBASE_HOST", "localhost")
    EXTERNAL_HBASE_PORT = int(os.getenv("EXTERNAL_HBASE_PORT", 9090))
    TABLE_NAME = os.getenv("TABLE_NAME", "articles_table")
    STOP_WORD_PATH = os.path.join(os.getcwd(), "app/utils/StopWords.txt")

    # 현재 환경에 따라 선택 (기본값: 내부망)
    USE_EXTERNAL = True  # True로 설정하면 외부망 사용

    def get_hdfs_url(self):
        return self.EXTERNAL_HDFS_URL if self.USE_EXTERNAL else self.INTERNAL_HDFS_URL

    def get_hbase_url(self):
        if self.USE_EXTERNAL:
            return self.EXTERNAL_HBASE_HOST, self.EXTERNAL_HBASE_PORT
        else:
            return self.INTERNAL_HBASE_HOST, self.INTERNAL_HBASE_PORT


# 로깅 설정
logger = setup_logging(log_file)
