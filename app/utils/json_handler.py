import json
from app.config import logger
from datetime import datetime


def datetime_convert(o):
    # JSON 파일 저장 시 datetime 객체 변환
    if isinstance(o, datetime):
        return o.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"{type(o)} 형식은 직렬화할 수 없습니다.")


def save_to_json(data, file_path):
    # 데이터를 JSON 파일로 저장
    if not data:
        logger.warning("저장할 데이터가 존재하지 않습니다.")
        return

    try:
        with open(file_path, mode="w", encoding="utf-8") as jsonfile:
            json.dump(
                data, jsonfile, ensure_ascii=False, indent=4, default=datetime_convert
            )

        logger.info(f"데이터가 성공적으로 {file_path}에 저장되었습니다.")
    except Exception as e:
        logger.error(f"JSON 저장 중 오류 발생: {e}")


def load_from_json(file_path):
    # JSON 파일에서 데이터 로드
    try:
        with open(file_path, mode="r", encoding="utf-8") as jsonfile:
            data = json.load(jsonfile)

        logger.info(f"{file_path}에서 데이터가 성공적으로 로드되었습니다.")
        return data
    except Exception as e:
        logger.error(f"JSON 로드 중 오류 발생: {e}")
        return []
