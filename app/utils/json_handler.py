import json
import os
import traceback
from app.config import logger
from datetime import datetime


def datetime_convert(o):
    # JSON 파일 저장 시 datetime 객체 변환
    if isinstance(o, datetime):
        return o.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"{type(o)} 형식은 직렬화할 수 없습니다.")


def save_to_json(data, file_path):
    # 데이터를 JSON 파일로 저장
    try:
        # 디렉터리가 존재하지 않으면 생성
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, mode="w", encoding="utf-8") as jsonfile:
            json.dump(
                data, jsonfile, ensure_ascii=False, indent=4, default=datetime_convert
            )
    except Exception as e:
        logger.error(f"JSON 저장 중 에러 발생: {e}")
        logger.error(traceback.format_exc())


def load_from_json(file_path):
    # JSON 파일에서 데이터 로드
    try:
        with open(file_path, mode="r", encoding="utf-8") as jsonfile:
            data = json.load(jsonfile)
        return data
    except Exception as e:
        logger.error(f"JSON 로드 중 에러 발생: {e}")
        return []
