import os
from dotenv import load_dotenv
from datetime import datetime
import logging

# env 파일 로드
load_dotenv()

# 로그 폴더 경로
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, f"app_{datetime.now().strftime('%Y%m%d')}.log")


class Config:
    MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    DATABASE_NAME = os.getenv("DATABASE_NAME", "crawling_db")
    CRAWLING_COLLECTION = os.getenv("CRAWLING_COLLECTION", "crawling_contents")
    HDFS_URL = os.getenv("HDFS_URL", "http://localhost:9870")
    HDFS_USER = os.getenv("HDFS_USER", "hadoop")
    HDFS_DIR = os.getenv("HDFS_DIR", "/data")


# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
)

# WebDriver Manager 로깅 비활성화
os.environ["WDM_LOG"] = "0"
logger = logging.getLogger(__name__)
