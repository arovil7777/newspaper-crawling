import os
from collections import defaultdict
from app.utils.db_handler import MongoDBConnector
from app.utils.csv_handler import load_from_csv, append_to_csv
from app.utils.json_handler import save_to_json, load_from_json
from app.utils.hdfs_handler import HDFSConnector
from app.utils.hbase_handler import HBaseConnector
from app.config import Config, logger
from datetime import datetime, timedelta
import re

# 주요 언론사 목록
main_publisher_list = [
    "경향신문",
    "국민일보",
    "동아일보",
    "문화일보",
    "서울신문",
    "세계일보",
    "조선일보",
    "중앙일보",
    "한겨레",
    "한국일보",
]


def create_data_dir_with_date(date_str: str) -> str:
    data_dir = "data"
    data_dir_with_date = os.path.join(data_dir, date_str)
    if not os.path.exists(data_dir_with_date):
        os.makedirs(data_dir_with_date)
    return data_dir_with_date


def group_articles_by_site_and_publisher(articles: list, date_str: str) -> dict:
    grouped_articles = defaultdict(lambda: defaultdict(list))
    for article in articles:
        publisher = article.get("publisher", "unknown")
        site = article.get("site", "unknown")
        # 주요 언론사는 별도로 저장
        if publisher in main_publisher_list:
            grouped_articles[site][publisher].append(article)

        # 주요 언론사를 포함한 모든 크롤링 데이터 저장
        grouped_articles[site][date_str].append(article)
    return grouped_articles


def extract_nouns_and_count(data: list) -> dict:
    nouns_dict = {}
    for article in data:
        nouns = article.get("nouns", [])
        for noun in nouns:
            if noun in nouns_dict:
                nouns_dict[noun] += 1
            else:
                nouns_dict[noun] = 1
    return nouns_dict


def save_articles_to_db(
    data: list, collection_name: str = Config.CRAWLING_COLLECTION
) -> None:
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
        logger.error(f"MongoDB 저장 중 에러 발생: {e}")
    finally:
        db.close_connection()


def save_articles_to_csv(data: list, date_str: str) -> list:
    # 크롤링 기사를 CSV 파일로 저장
    if not data:
        logger.error("저장할 데이터가 없습니다.")
        return None

    file_paths = []
    data_dir_with_date = create_data_dir_with_date(date_str)
    file_path = os.path.join(data_dir_with_date, f"articles_{date_str}.csv")
    try:
        append_to_csv(data, file_path)
        logger.info(f"로컬에 CSV 파일 저장 완료: {file_path}")
        file_paths.append(file_path)
    except Exception as e:
        logger.error(f"CSV 저장 중 에러 발생: {e}")

    return file_paths


def load_articles_from_csv(file_path: str) -> list:
    # CSV 파일에서 기사 데이터 로드
    try:
        return load_from_csv(file_path)
    except Exception as e:
        logger.error(f"CSV 로드 중 에러 발생: {e}")
        return []


def save_articles_to_json_by_site_and_publisher(data: list, date_str: str) -> list:
    # 크롤링 기사를 JSON 파일로 저장
    if not data:
        logger.error("저장할 데이터가 없습니다.")
        return None

    file_paths = []
    data_dir_with_date = create_data_dir_with_date(date_str)  # 일별 디렉터리 생성
    grouped_articles = group_articles_by_site_and_publisher(data, date_str)

    for site, publishers in grouped_articles.items():
        site_dir = os.path.join(data_dir_with_date, site)
        os.makedirs(site_dir, exist_ok=True)

        total_nouns_dict = defaultdict(int)

        for key, articles in publishers.items():
            nouns_dict = extract_nouns_and_count(articles)
            if key in main_publisher_list:
                file_name = f"{key}.json"
                publisher_file_path = os.path.join(site_dir, file_name)

                try:
                    if os.path.exists(publisher_file_path):
                        existing_data = load_from_json(publisher_file_path)
                        for noun, count in nouns_dict.items():
                            if noun in existing_data:
                                existing_data[noun] += count
                            else:
                                existing_data[noun] = count
                        save_to_json(existing_data, publisher_file_path)
                    else:
                        save_to_json(nouns_dict, publisher_file_path)
                    logger.info(f"로컬에 JSON 파일 저장 완료: {publisher_file_path}")
                    file_paths.append(publisher_file_path)
                except Exception as e:
                    logger.error(f"JSON 저장 중 에러 발생: {e}")

            # 종합 데이터 업데이트
            for noun, count in nouns_dict.items():
                total_nouns_dict[noun] += count

        # 종합 데이터 저장
        total_file_name = f"{date_str}.json"
        total_file_path = os.path.join(site_dir, total_file_name)
        try:
            if os.path.exists(total_file_path):
                existing_total_data = load_from_json(total_file_path)
                for noun, count in total_nouns_dict.items():
                    if noun in existing_total_data:
                        existing_total_data[noun] += count
                    else:
                        existing_total_data[noun] = count
                save_to_json(existing_total_data, total_file_path)
            else:
                save_to_json(total_nouns_dict, total_file_path)
            logger.info(f"로컬에 JSON 파일 저장 완료: {total_file_path}")
            file_paths.append(total_file_path)
        except Exception as e:
            logger.error(f"JSON 저장 중 에러 발생: {e}")

    return file_paths


def load_articles_from_json(file_path: str) -> list:
    # JSON 파일에서 기사 데이터 로드
    try:
        return load_from_json(file_path)
    except Exception as e:
        logger.error(f"JSON 로드 중 에러 발생: {e}")
        return []


def aggregate_nouns(data: dict) -> dict:
    aggregated_nouns = defaultdict(int)
    for noun, count in data.items():
        try:
            aggregated_nouns[noun] += int(count)
        except ValueError:
            logger.error(f"형식이 잘못된 데이터: {noun} = {count}")
    return aggregated_nouns


def calculate_date_ranges(start_date: str, end_date: str, interval: str) -> list:
    # 시작일과 종료일을 interval 단위로 분할
    date_ranges = []
    current_start_date = datetime.strptime(start_date, "%Y%m%d")
    final_end_date = datetime.strptime(end_date, "%Y%m%d")

    while current_start_date <= final_end_date:
        if interval == "daily":
            current_end_date = current_start_date
        elif interval == "weekly":
            current_end_date = current_start_date + timedelta(days=6)
        elif interval == "monthly":
            current_end_date = (
                current_start_date.replace(day=1) + timedelta(days=32)
            ).replace(day=1) - timedelta(days=1)
        elif interval == "yearly":
            current_end_date = current_start_date.replace(month=12, day=31)
        else:
            raise ValueError("Invalid interval")

        current_end_date = min(current_start_date, final_end_date)
        date_ranges.append(
            (current_start_date.strftime("%Y%m%d"), current_end_date.strftime("%Y%m%d"))
        )
        current_start_date = current_end_date + timedelta(days=1)

    return date_ranges


def process_and_save_aggregated_data_from_directories():
    base_dir = "data"  # 데이터가 저장된 기본 디렉터리

    # 디렉터리 명을 기준으로 날짜 범위 계산
    date_dirs = sorted(
        [
            d
            for d in os.listdir(base_dir)
            if os.path.isdir(os.path.join(base_dir, d)) and re.match(r"^\d{8}$", d)
        ]
    )
    if not date_dirs:
        logger.error("저장된 데이터가 없습니다.")
        return

    start_date = date_dirs[0]
    end_date = date_dirs[-1]

    intervals = ["weekly", "monthly", "yearly"]
    for interval in intervals:
        date_ranges = calculate_date_ranges(start_date, end_date, interval)
        aggregate_and_save_data(date_ranges, date_dirs, base_dir, interval)

    # # 주별 데이터 집계
    # date_ranges = calculate_date_ranges(start_date, end_date, "weekly")
    # for start, end in date_ranges:
    #     aggregated_nouns = defaultdict(list)
    #     for date_dir in date_dirs:
    #         if start <= date_dir <= end:
    #             daily_dir = os.path.join(base_dir, date_dir)
    #             for site in os.listdir(daily_dir):
    #                 site_dir = os.path.join(daily_dir, site)
    #                 if os.path.isdir(site_dir):
    #                     for publisher_file in os.listdir(site_dir):
    #                         file_path = os.path.join(site_dir, publisher_file)
    #                         # 날짜로 명명된 파일만 집계
    #                         if (
    #                             file_path.endswith(".json")
    #                             and date_dir in publisher_file
    #                         ):
    #                             daily_data = load_articles_from_json(file_path)
    #                             daily_nouns = extract_nouns_and_count(daily_data)
    #                             for noun, count in daily_nouns.items():
    #                                 aggregated_nouns[noun] += count

    #     week_number = datetime.strptime(start, "%Y%m%d").isocalendar()[1]
    #     site_dir = os.path.join(base_dir, f"{start[:4]}년 {week_number}주", site)
    #     os.makedirs(site_dir, exist_ok=True)
    #     file_name = f"articles_{start[:4]}년_{week_number}주.json"
    #     file_path = os.path.join(site_dir, file_name)
    #     try:
    #         save_to_json(aggregated_nouns, file_path)
    #         # logger.info(f"로컬에 JSON 파일 저장 완료: {file_path}")
    #     except Exception as e:
    #         logger.error(f"JSON 저장 중 에러 발생: {e}")

    # # 월별 데이터 집계
    # date_ranges = calculate_date_ranges(start_date, end_date, "monthly")
    # for start, end in date_ranges:
    #     aggregated_nouns = defaultdict(list)
    #     for date_dir in date_dirs:
    #         if start <= date_dir <= end:
    #             daily_dir = os.path.join(base_dir, date_dir)
    #             for site in os.listdir(daily_dir):
    #                 site_dir = os.path.join(daily_dir, site)
    #                 if os.path.isdir(site_dir):
    #                     for publisher_file in os.listdir(site_dir):
    #                         file_path = os.path.join(site_dir, publisher_file)
    #                         # 날짜로 명명된 파일만 집계
    #                         if (
    #                             file_path.endswith(".json")
    #                             and date_dir in publisher_file
    #                         ):
    #                             daily_data = load_articles_from_json(file_path)
    #                             daily_nouns = extract_nouns_and_count(daily_data)
    #                             for noun, count in daily_nouns.items():
    #                                 aggregated_nouns[noun] += count

    #     site_dir = os.path.join(base_dir, f"{start[:4]}년 {start[4:6]}월", site)
    #     os.makedirs(site_dir, exist_ok=True)
    #     file_name = f"articles_{start[:4]}년_{start[4:6]}월.json"
    #     file_path = os.path.join(site_dir, file_name)
    #     try:
    #         save_to_json(aggregated_nouns, file_path)
    #         logger.info(f"로컬에 JSON 파일 저장 완료: {file_path}")
    #     except Exception as e:
    #         logger.error(f"JSON 저장 중 에러 발생: {e}")

    # # 연별 데이터 집계
    # date_ranges = calculate_date_ranges(start_date, end_date, "yearly")
    # for start, end in date_ranges:
    #     aggregated_nouns = defaultdict(list)
    #     for date_dir in date_dirs:
    #         if start <= date_dir <= end:
    #             daily_dir = os.path.join(base_dir, date_dir)
    #             for site in os.listdir(daily_dir):
    #                 site_dir = os.path.join(daily_dir, site)
    #                 if os.path.isdir(site_dir):
    #                     for publisher_file in os.listdir(site_dir):
    #                         file_path = os.path.join(site_dir, publisher_file)
    #                         # 날짜로 명명된 파일만 집계
    #                         if (
    #                             file_path.endswith(".json")
    #                             and date_dir in publisher_file
    #                         ):
    #                             daily_data = load_articles_from_json(file_path)
    #                             daily_nouns = extract_nouns_and_count(daily_data)
    #                             for noun, count in daily_nouns.items():
    #                                 aggregated_nouns[noun] += count

    #     site_dir = os.path.join(base_dir, f"{start[:4]}년", site)
    #     os.makedirs(site_dir, exist_ok=True)
    #     file_name = f"articles_{start[:4]}년.json"
    #     file_path = os.path.join(site_dir, file_name)
    #     try:
    #         save_to_json(aggregated_nouns, file_path)
    #         logger.info(f"로컬에 JSON 파일 저장 완료: {file_path}")
    #     except Exception as e:
    #         logger.error(f"JSON 저장 중 에러 발생: {e}")


def aggregate_and_save_data(date_ranges, date_dirs, base_dir, interval):
    for start, end in date_ranges:
        aggregated_nouns = defaultdict(int)
        for date_dir in date_dirs:
            if start <= date_dir <= end:
                daily_dir = os.path.join(base_dir, date_dir)
                for site in os.listdir(daily_dir):
                    site_dir = os.path.join(daily_dir, site)
                    if os.path.isdir(site_dir):
                        for publisher_file in os.listdir(site_dir):
                            file_path = os.path.join(site_dir, publisher_file)
                            # JSON 파일 중 날짜로 명명된 파일만 집계 (언론사 명이 포함된 파일 제외)
                            if (
                                file_path.endswith(".json")
                                and date_dir in publisher_file
                            ):
                                try:
                                    daily_data = load_articles_from_json(file_path)
                                    if not isinstance(daily_data, dict):
                                        logger.error(
                                            f"유효하지 않은 데이터 형식: {file_path}"
                                        )
                                        continue

                                    daily_nouns = aggregate_nouns(daily_data)
                                    for noun, count in daily_nouns.items():
                                        aggregated_nouns[noun] += count
                                except Exception as e:
                                    logger.error(
                                        f"파일 처리 중 에러 발생: {file_path}, {e}"
                                    )

        try:
            if interval == "weekly":
                period = f"{start[:4]}년 {datetime.strptime(start, '%Y%m%d').isocalendar()[1]}주"
            elif interval == "monthly":
                period = f"{start[:4]}년 {start[4:6]}월"
            elif interval == "yearly":
                period = f"{start[:4]}년"
            else:
                raise ValueError(f"Interval 값이 유효하지 않습니다: {interval}")

            site_dir = os.path.join(base_dir, period)
            os.makedirs(site_dir, exist_ok=True)
            file_name = f"articles_{period}.json"
            file_path = os.path.join(site_dir, file_name)
            save_to_json(aggregated_nouns, file_path)
            logger.info(f"로컬에 JSON 파일 저장 완료: {file_path}")
        except Exception as e:
            logger.error(f"JSON 저장 중 에러 발생: {e}")


def send_to_hdfs(local_file_path: str) -> str:
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
        logger.error(f"HDFS 전송 중 에러 발생: {e}")
        return None


def send_to_hbase_with_local_file(hdfs_path: str, local_path: str) -> None:
    try:
        # # HDFS에서 파일 다운로드
        # hdfs_connector = HDFSConnector()
        # hdfs_connector.client.download(hdfs_path, local_path, overwrite=True)
        # logger.info(f"HDFS에서 파일 다운로드 완료: {local_path}")

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
        logger.error(f"HBase로 데이터 전송 중 에러 발생: {e}")


def send_to_hbase_with_contents(contents) -> None:
    try:
        # HBase에 크롤링 데이터 삽입
        hbase_connector = HBaseConnector()
        hbase_connector.insert_contents_to_table(Config.TABLE_NAME, contents)
        hbase_connector.close_connection()
    except Exception as e:
        logger.error(f"HBase로 데이터 전송 중 에러 발생: {e}")


def get_row_from_hbase(table_name: str) -> dict:  # , row_key):
    try:
        hbase_connector = HBaseConnector()
        # row = hbase_connector.get_row(table_name, row_key)
        row = hbase_connector.get_table_data(table_name)
        hbase_connector.close_connection()
        return row
    except Exception as e:
        logger.error(f"HBase에서 데이터 조회 중 에러 발생: {e}")
        return None


def hbase_data_to_hdfs(table_name: str, output_path: str) -> None:
    # HBase 데이터 샘플링 후 HDFS에 저장 (추후 구현 예정)
    logger.info(f"HBase 테이블 {table_name}에서 데이터 샘플링 및 HDFS 저장")
    # 샘플링 및 HDFS 저장 로직 구현 필요
    pass
