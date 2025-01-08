import os
from collections import defaultdict
from app.utils.csv_handler import append_to_csv
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

data_dir = "data"


def create_data_dir_with_date(site: str, date_str: str) -> str:
    data_dir_with_site_and_date = os.path.join(data_dir, site, date_str)
    if not os.path.exists(data_dir_with_site_and_date):
        os.makedirs(data_dir_with_site_and_date)
    return data_dir_with_site_and_date


def group_articles_by_site_and_publisher(articles: list, date_str: str) -> dict:
    grouped_articles = defaultdict(lambda: defaultdict(list))
    for article in articles:
        publisher = article.get("publisher", "unknown")
        site = article.get("site", "unknown")

        channel = None
        if site == "네이버 뉴스":
            channel = "news"
        else:
            channel = "unknown"

        # 주요 언론사는 별도로 저장
        if publisher in main_publisher_list:
            grouped_articles[channel][publisher].append(article)

        # 주요 언론사를 포함한 모든 크롤링 데이터 저장
        grouped_articles[channel][date_str].append(article)
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


def save_articles_to_csv(data: list, date_str: str) -> list:
    # 크롤링 기사를 CSV 파일로 저장
    if not data:
        logger.error("저장할 데이터가 없습니다.")
        return None

    file_paths = []
    grouped_articles = group_articles_by_site_and_publisher(data, date_str)

    for site, publishers in grouped_articles.items():
        data_dir_with_date = create_data_dir_with_date(site, date_str)
        for key, articles in publishers.items():
            file_path = os.path.join(data_dir_with_date, f"articles_{date_str}.csv")
            try:
                append_to_csv(data, file_path)
                logger.info(f"로컬에 CSV 파일 저장 완료: {file_path}")
                file_paths.append(file_path)
            except Exception as e:
                logger.error(f"CSV 저장 중 에러 발생: {e}")

    return file_paths


def save_articles_to_json_by_site_and_publisher(data: list, date_str: str) -> list:
    # 크롤링 기사를 JSON 파일로 저장
    if not data:
        logger.error("저장할 데이터가 없습니다.")
        return None

    file_paths = []
    date = ""
    if len(date_str) == 8 and date_str.isdigit():
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        date = date_obj.strftime("%Y-%m-%d")
    else:
        date = date_str

    grouped_articles = group_articles_by_site_and_publisher(data, date)

    for site, publishers in grouped_articles.items():
        site_dir = os.path.join(data_dir, site)
        os.makedirs(site_dir, exist_ok=True)

        total_nouns_dict = defaultdict(int)

        for key, articles in publishers.items():
            nouns_dict = extract_nouns_and_count(articles)
            if key in main_publisher_list:
                file_name = f"{key}.json"
                publisher_file_path = os.path.join(site_dir, date, file_name)

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
                    logger.error(f"'publisher_file_path' JSON 저장 중 에러 발생: {e}")

            # 종합 데이터 업데이트
            for noun, count in nouns_dict.items():
                total_nouns_dict[noun] += count

        # 종합 데이터 저장
        total_file_name = f"{date}.json"
        total_file_path = os.path.join(site_dir, date, total_file_name)
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
            logger.error(f"'total_file_path' JSON 저장 중 에러 발생: {e}")

    return file_paths


def load_articles_from_json(file_path: str) -> list:
    # JSON 파일에서 기사 데이터 로드
    try:
        return load_from_json(file_path)
    except Exception as e:
        logger.error(f"JSON 로드 중 에러 발생: {e}")
        return []


def calculate_date_ranges(start_date: str, end_date: str, interval: str) -> list:
    # 시작일과 종료일을 interval 단위로 분할
    date_ranges = []
    current_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    final_end_date = datetime.strptime(end_date, "%Y-%m-%d")

    while current_start_date <= final_end_date:
        if interval == "daily":
            current_end_date = current_start_date
        elif interval == "weekly":
            start_of_week = current_start_date - timedelta(
                days=current_start_date.weekday()
            )
            end_of_week = start_of_week + timedelta(days=6)
            current_end_date = min(end_of_week, final_end_date)
        elif interval == "monthly":
            current_end_date = (
                current_start_date.replace(day=1) + timedelta(days=32)
            ).replace(day=1) - timedelta(days=1)
        elif interval == "yearly":
            current_end_date = current_start_date.replace(month=12, day=31)
        else:
            raise ValueError("Invalid interval")

        current_end_date = min(current_end_date, final_end_date)
        date_ranges.append(
            (
                current_start_date.strftime("%Y-%m-%d"),
                current_end_date.strftime("%Y-%m-%d"),
            )
        )
        current_start_date = current_end_date + timedelta(days=1)

    return date_ranges


def process_and_save_aggregated_data_from_directories(site: str):
    base_dir = os.path.join(data_dir, site)  # 데이터가 저장된 기본 디렉터리

    # 디렉터리 명을 기준으로 날짜 범위 계산
    date_dirs = sorted(
        [
            d
            for d in os.listdir(base_dir)
            if os.path.isdir(os.path.join(base_dir, d))
            and re.match(r"^\d{4}-\d{2}-\d{2}$", d)
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

    # HDFS로 업로드
    upload_aggregated_files_to_hdfs(base_dir)


def aggregate_and_save_data(date_ranges, date_dirs, base_dir, interval):
    for start, end in date_ranges:
        aggregated_nouns = defaultdict(int)
        for date_dir in date_dirs:
            if start <= date_dir <= end:
                for content in os.listdir(base_dir):
                    content_dir = os.path.join(base_dir, content)
                    if os.path.isdir(content_dir):
                        for content_file in os.listdir(content_dir):
                            file_path = os.path.join(content_dir, content_file)
                            if file_path.endswith(".json") and date_dir in content_file:
                                try:
                                    daily_data = load_articles_from_json(file_path)
                                    if not isinstance(daily_data, dict):
                                        logger.error(
                                            f"유효하지 않은 데이터 형식: {file_path}"
                                        )
                                        continue

                                    for noun, count in daily_data.items():
                                        aggregated_nouns[noun] += count
                                except Exception as e:
                                    logger.error(
                                        f"파일 처리 중 에러 발생: {file_path}, {e}"
                                    )

        if interval == "weekly":
            period = f"{end[:4]}-W{datetime.strptime(end, '%Y-%m-%d').isocalendar()[1]}"
        elif interval == "monthly":
            period = f"{start[:4]}-{start[5:7]}"
        elif interval == "yearly":
            period = f"{start[:4]}"
        else:
            raise ValueError(f"Interval 값이 유효하지 않습니다: {interval}")

        period_dir = os.path.join(base_dir, period)
        os.makedirs(period_dir, exist_ok=True)

        file_name = f"{period}.json"
        file_path = os.path.join(period_dir, file_name)
        try:
            save_to_json(aggregated_nouns, file_path)
        except Exception as e:
            logger.error(f"'aggregate_and_save_data' JSON 저장 중 에러 발생: {e}")


def upload_aggregated_files_to_hdfs(base_dir: str):
    # 주어진 디렉터리에서 생성된 JSON 파일들을 HDFS로 업로드
    try:
        hdfs_connector = HDFSConnector()
        for root, dirs, _ in os.walk(base_dir):
            for dir_name in dirs:
                local_file_path = os.path.join(root, dir_name)
                hdfs_file_path = os.path.join(Config.HDFS_DIR, local_file_path)

                # HDFS 업로드
                hdfs_connector.upload_directory(local_file_path, hdfs_file_path)
        logger.info(f"HDFS로 모든 파일 업로드 완료: {base_dir}")
    except Exception as e:
        logger.error(f"HDFS 업로드 중 에러 발생: {e}")


def send_to_hbase_with_contents(contents) -> None:
    try:
        # HBase에 크롤링 데이터 삽입
        hbase_connector = HBaseConnector()
        hbase_connector.insert_contents_to_table(Config.TABLE_NAME, contents)
        hbase_connector.close_connection()
    except Exception as e:
        logger.error(f"HBase로 데이터 전송 중 에러 발생: {e}")
