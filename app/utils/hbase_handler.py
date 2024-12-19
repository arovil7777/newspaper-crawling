import happybase
import csv
import json
from app.config import Config, logger


class HBaseConnector:
    def __init__(self, host=Config.HBASE_HOST, port=Config.HBASE_PORT):
        # HBase 연결 설정
        try:
            self.connection = happybase.Connection(host=host, port=port)
            self.connection.open()
            logger.info(f"HBase 연결 성공: {host}:{port}")
        except Exception as e:
            logger.critical(f"HBase 연결 실패: {e}")
            raise

    def get_table(self, table_name):
        # HBase 테이블 객체 반환
        return self.connection.table(table_name)

    def insert_csv_to_table(self, table_name, csv_path):
        # CSV 파일을 읽어 HBase 테이블에 삽입
        try:
            table = self.get_table(table_name)
            # 모든
            with open(csv_path, "r", encoding="utf-8-sig") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    row_key = row["id"]
                    data = {f"info:{k}": v for k, v in row.items()}
                    table.put(row_key, data)
            logger.info(
                f"CSV 데이터를 HBase 테이블 '{table_name}'에 성공적으로 삽입했습니다."
            )
        except Exception as e:
            logger.error(f"CSV 데이터를 HBase 테이블로 삽입 중 오류 발생: {e}")

    def insert_json_to_table(self, table_name, json_path):
        # JSON 파일을 읽어 HBase 테이블에 삽입
        try:
            table = self.get_table(table_name)
            with open(json_path, "r", encoding="utf-8-sig") as file:
                data = json.load(file)
                for item in data:
                    row_key = item["id"]
                    hbase_data = {f"info:{k}": str(v) for k, v in item.items()}
                    table.put(row_key, hbase_data)
            logger.info(
                f"JSON 데이터를 HBase 테이블 '{table_name}'에 성공적으로 삽입했습니다."
            )
        except Exception as e:
            logger.error(f"JSON 데이터를 HBase 테이블로 삽입 중 오류 발생: {e}")

    def close_connection(self):
        # HBase 연결 종료
        self.connection.close()
        logger.debug("HBase 연결 종료")
