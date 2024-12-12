import csv
from app.config import logger

def save_to_csv(data, file_path):
    # 데이터를 CSV 파일로 저장
    if not data:
        logger.warning("저장할 데이터가 존재하지 않습니다.")
        return
