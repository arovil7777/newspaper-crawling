import os
from app.utils.db_handler import MongoDBConnector
from app.utils.csv_handler import save_to_csv, load_from_csv
from app.utils.json_handler import save_to_json, load_from_json
from app.utils.hdfs_handler import HDFSConnector
from app.utils.hbase_handler import HBaseConnector
from app.config import Config, logger
from datetime import datetime


data_dir = "data"
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

data_dir_with_date = data_dir + f"/{datetime.now().strftime('%Y%m%d')}"
if not os.path.exists(data_dir_with_date):
    os.makedirs(data_dir_with_date)


def save_articles_to_db(data, collection_name=Config.CRAWLING_COLLECTION):
    # 크롤링된 기사를 MongoDB에 저장
    db = MongoDBConnector()
    collection = db.get_collection(collection_name)

    try:
        for item in data:
            # 중복 기사 (기사 url)
            if not collection.find_one({"url": item["url"]}):
                collection.insert_one(item)

        logger.info(f"{len(data)}개의 데이터 저장 완료")
    except Exception as e:
        logger.error(f"MongoDB 저장 중 오류 발생: {e}")
    finally:
        db.close_connection()


def save_articles_to_csv(data):
    # 크롤링 기사를 CSV 파일로 저장
    file_path = os.path.join(
        data_dir_with_date, f"articles_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    )
    try:
        save_to_csv(data, file_path)
        logger.info(f"로컬에 CSV 파일 저장 완료: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"CSV 저장 중 오류 발생: {e}")
        return None


def load_articles_from_csv(file_path):
    # CSV 파일에서 기사 데이터 로드
    try:
        return load_from_csv(file_path)
    except Exception as e:
        logger.error(f"CSV 로드 중 오류 발생: {e}")
        return []


def save_articles_to_json(data):
    # 크롤링 기사를 JSON 파일로 저장
    file_path = os.path.join(
        data_dir_with_date, f"articles_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    )
    try:
        save_to_json(data, file_path)
        logger.info(f"로컬에 JSON 파일 저장 완료: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"JSON 저장 중 오류 발생: {e}")
        return None


def load_articles_from_json(file_path):
    # JSON 파일에서 기사 데이터 로드
    try:
        return load_from_json(file_path)
    except Exception as e:
        logger.error(f"JSON 로드 중 오류 발생: {e}")
        return []


def send_to_hdfs(local_file_path):
    # 크롤링 데이터를 HDFS로 전송
    if not local_file_path:
        logger.error("유효하지 않은 파일 경로입니다.")
        return None

    try:
        hdfs = HDFSConnector()

        hdfs_dir = Config.HDFS_DIR
        hdfs_file_path = os.path.join(hdfs_dir, os.path.basename(local_file_path))
        hdfs.upload_file(local_file_path, hdfs_file_path)
        logger.info(f"HDFS에 파일 업로드 완료: {hdfs_file_path}")

        # 업로드된 HDFS 경로 반환
        return hdfs_file_path
    except Exception as e:
        logger.error(f"HDFS 전송 중 오류 발생: {e}")
        return None


def send_to_hbase(hdfs_path, local_path):
    try:
        # HDFS에서 파일 다운로드
        hdfs_connector = HDFSConnector()
        hdfs_connector.client.download(hdfs_path, local_path, overwrite=True)
        logger.info(f"HDFS에서 파일 다운로드 완료: {local_path}")

        # HBase에 파일 삽입
        hbase_connector = HBaseConnector()
        if local_path.endswith(".csv"):
            hbase_connector.insert_csv_to_table(Config.TABLE_NAME, local_path)
        elif local_path.endswith(".json"):
            hbase_connector.insert_json_to_table(Config.TABLE_NAME, local_path)
        else:
            logger.error("지원하지 않는 파일 형식입니다. CSV 또는 JSON을 지원합니다.")
        hbase_connector.close_connection()
    except Exception as e:
        logger.error(f"HBase로 데이터 전송 중 오류 발생: {e}")


def hbase_data_to_hdfs(table_name, output_path):
    # HBase 데이터 샘플링 후 HDFS에 저장 (추후 구현 예정)
    logger.info(f"HBase 테이블 {table_name}에서 데이터 샘플링 및 HDFS 저장")
    # 샘플링 및 HDFS 저장 로직 구현 필요
    pass
