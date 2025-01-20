import happybase
import pandas as pd
import numpy as np
import json
import traceback
from app.config import Config, logger
from tqdm import tqdm


class HBaseConnector:
    def __init__(self):
        # HBase 연결 설정
        config = Config()
        host, port = config.get_hbase_url()
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
            chunk_size = 1000  # 청크 크기 설정 (메모리 이슈)
            for chunk in pd.read_csv(
                csv_path, encoding="utf-8-sig", chunksize=chunk_size, dtype=str
            ):
                chunk = chunk.fillna("")  # 모든 NaN 값을 빈 문자열로 대체
                for col in chunk.columns:
                    chunk[col] = chunk[col].map(
                        lambda x: (
                            str(x)
                            if not isinstance(x, (list, dict, set, tuple, np.ndarray))
                            else json.dumps(x)
                        )
                    )  # 배열/리스트/딕셔너리 데이터를 문자열로 변환

                for _, row in tqdm(
                    chunk.iterrows(),
                    total=chunk.shape[0],
                    desc="HBase로 데이터 저장 중",
                ):
                    if pd.isna(row["article_id"]):
                        continue

                    row_key = str(row["article_id"])

                    # 중복 여부 확인
                    if table.row(row_key):
                        continue

                    hbase_data = {
                        (
                            f"article:{k}"
                            if k in ["site", "title", "url", "publisher"]
                            else f"article_content:{k}"
                        ): str(v)
                        for k, v in row.items()
                    }
                    table.put(row_key, hbase_data)
            logger.info(
                f"CSV 데이터를 HBase 테이블 '{table_name}'에 성공적으로 삽입했습니다."
            )
        except Exception as e:
            logger.error(f"CSV 데이터를 HBase 테이블로 삽입 중 에러 발생: {e}")
            logger.error(traceback.format_exc())

    def insert_json_to_table(self, table_name, json_path):
        # JSON 파일을 읽어 HBase 테이블에 삽입
        try:
            table = self.get_table(table_name)
            with open(json_path, "r", encoding="utf-8-sig") as file:
                data = json.load(file)
                for item in tqdm(data, desc="HBase로 데이터 저장 중"):
                    row_key = str(item["article_id"])

                    # 중복 여부 확인
                    if table.row(row_key):
                        continue

                    hbase_data = {
                        (
                            f"article:{k}"
                            if k in ["site", "title", "url", "publisher"]
                            else f"article_content:{k}"
                        ): str(v)
                        for k, v in item.items()
                    }
                    table.put(row_key, hbase_data)
            logger.info(
                f"JSON 데이터를 HBase 테이블 '{table_name}'에 성공적으로 삽입했습니다."
            )
        except Exception as e:
            logger.error(f"JSON 데이터를 HBase 테이블로 삽입 중 에러 발생: {e}")

    def insert_contents_to_table(self, table_name, contents, chunk_size=1000):
        # 크롤링한 데이터를 HBase 테이블에 삽입
        try:
            table = self.get_table(table_name)
            for i in tqdm(
                range(0, len(contents), chunk_size), desc="HBase로 데이터 저장 중"
            ):
                chunk = contents[i : i + chunk_size]
                for content in chunk:
                    row_key = str(content["article_id"])

                    # 중복 여부 확인
                    if table.row(row_key):
                        continue

                    hbase_data = {
                        (
                            f"article:{k}"
                            if k in ["site", "title", "url", "publisher"]
                            else f"article_content:{k}"
                        ): str(v)
                        for k, v in content.items()
                    }
                    table.put(row_key, hbase_data)
            logger.info(
                f"크롤링 데이터를 HBase 테이블 '{table_name}'에 성공적으로 삽입했습니다."
            )
        except Exception as e:
            logger.error(f"크롤링 데이터를 HBase 테이블로 삽입 중 에러 발생: {e}")

    def get_row(self, table_name, row_key):
        # HBase 테이블에서 주어진 row key로 데이터 조회
        try:
            table = self.get_table(table_name)
            row = table.row(row_key)
            if row:
                logger.info(f"Row key '{row_key}'로 조회한 데이터: {row}")
                return row
            else:
                logger.warning(f"Row key '{row_key}'로 조회한 데이터가 없습니다.")
                return None
        except Exception as e:
            logger.error(f"Row key '{row_key}'로 데이터 조회 중 에러 발생: {e}")
            return None

    def get_table_data(self, table_name):
        table = self.get_table(table_name)
        try:
            for key, row in table.scan():
                decoded_key = key.decode("utf-8")
                decoded_row = {
                    k.decode("utf-8"): v.decode("utf-8") for k, v in row.items()
                }
                logger.info(f"Row Key: {decoded_key}, Row Data: {decoded_row}")
                logger.info("HBase 테이블 선택 성공")
        except Exception as e:
            logger.error(f"테이블 스캔 중 에러 발생: {e}")

    def close_connection(self):
        # HBase 연결 종료
        self.connection.close()
        logger.debug("HBase 연결 종료")
