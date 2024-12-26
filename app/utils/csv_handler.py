import csv
from app.config import logger


def save_to_csv(data, file_path):
    # 데이터를 CSV 파일로 저장
    try:
        keys = data[0].keys()
        with open(file_path, mode="w", encoding="utf-8-sig", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)

        logger.info(f"데이터가 성공적으로 {file_path}에 저장되었습니다.")
    except Exception as e:
        logger.error(f"CSV 저장 중 에러 발생: {e}")


def load_from_csv(file_path):
    # CSV 파일에서 데이터 로드
    try:
        with open(file_path, mode="r", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            data = [row for row in reader]

        logger.info(f"{file_path}에서 데이터가 성공적으로 로드되었습니다.")
        return data
    except Exception as e:
        logger.error(f"CSV 로드 중 에러 발생: {e}")
        return []
