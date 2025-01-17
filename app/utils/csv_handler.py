import csv
import os
from app.config import logger


def load_from_csv(file_path):
    # CSV 파일에서 데이터 로드
    try:
        with open(file_path, mode="r", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            data = [row for row in reader]

        logger.info(f"{file_path}에서 데이터가 성공적으로 로드되었습니다.")
        return data
    except Exception as e:
        logger.error(f"CSV 로드 중 에러 발생")
        return []


def append_to_csv(data, file_path):
    # 데이터를 CSV 파일에 추가 (중복 확인)
    try:
        existing_urls = set()
        if os.path.exists(file_path):
            with open(file_path, mode="r", encoding="utf-8-sig") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    existing_urls.add(row["url"])

        new_data = [item for item in data if item["url"] not in existing_urls]

        if not new_data:
            return

        with open(file_path, mode="a", encoding="utf-8-sig", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=new_data[0].keys())
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerows(new_data)

        logger.info(f"데이터가 성공적으로 {file_path}에 추가되었습니다.")
    except Exception as e:
        logger.error(f"CSV 추가 중 에러 발생: {e}")
