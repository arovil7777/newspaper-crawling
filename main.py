import traceback
import sys
from datetime import datetime, timedelta
from app.crawling import ArticleCrawler
from app.config import logger
from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.processing import (
    save_articles_to_db,
    save_articles_to_csv,
    save_articles_to_json,
    send_to_hdfs,
    send_to_hbase,
)


def save_data_format(format, articles):
    if format == "CSV":
        # 로컬 CSV 파일에 크롤링 데이터 저장
        return save_articles_to_csv(articles)
    elif format == "JSON":
        # 로컬 JSON 파일에 크롤링 데이터 저장
        return save_articles_to_json(articles)
    else:
        return None


def calculate_date_ranges(start_date: str, end_date: str, interval: int = 7):
    # 시작일과 종료일을 interval일 단위로 분할 (기본 7일)
    date_ranges = []
    current_start_date = datetime.strptime(start_date, "%Y%m%d")
    final_end_date = datetime.strptime(end_date, "%Y%m%d")

    while current_start_date <= final_end_date:
        current_end_date = min(
            current_start_date + timedelta(days=interval - 1), final_end_date
        )
        date_ranges.append(
            (current_start_date.strftime("%Y%m%d"), current_end_date.strftime("%Y%m%d"))
        )
        current_start_date = current_end_date + timedelta(days=1)

    return date_ranges


def main():
    # 네이버 전체 언론사 URL
    all_publisher_url = (
        "https://news.naver.com/main/list.naver?mode=LPOD&mid=sec&oid=032"
    )

    # 기간 설정
    start_date = "20241201"  # 시작 날짜 (YYYYMMDD 형식)
    end_date = "20241207"  # 종료 날짜 (YYYYMMDD 형식)

    crawler = ArticleCrawler()
    try:
        logger.info("뉴스 링크 수집 중...")

        # 7일 단위로 날짜를 분할
        date_ranges = calculate_date_ranges(start_date, end_date, interval=7)
        logger.info(f"수집할 날짜 범위: {date_ranges}")

        # 뉴스 링크 수집
        all_article_links = []
        for start, end in date_ranges:
            article_links = crawler.fetch_article_links(all_publisher_url, start, end)
            all_article_links.extend(article_links)

        if not article_links:
            logger.warning("수집된 뉴스 링크가 없습니다.")
            return

        # 뉴스 본문 내용 추출
        articles = crawler.fetch_articles(article_links)
        logger.info(f"{len(articles)}개의 기사를 크롤링했습니다.")

        if articles:
            logger.info(f"크롤링 완료. 총 {len(articles)}개의 기사 수집")

            # 1. 로컬에 데이터 저장 (MongoDB 또는 CSV)
            local_file_path = save_data_format("CSV", articles)
            """
            # save_articles_to_db(articles) # MongoDB에 크롤링 데이터 저장

            # 2. CSV 데이터를 HBase로 저장
            # if local_file_path:
            #     send_to_hbase(local_file_path, Config.TABLE_NAME)
            # # HDFS로 전송
            # hdfs_file_path = send_to_hdfs(local_file_path)

            # # HDFS에서 HBase로 전송
            # if hdfs_file_path:
            #     send_to_hbase(hdfs_file_path, local_file_path)
            """

        else:
            logger.warning("크롤링된 기사가 없습니다.")
            return
        logger.info("작업이 완료되었습니다.")
    except Exception as e:
        logger.critical(f"예기치 못한 에러 발생: {e}")
        exc_type, exc_value, exc_tb = sys.exc_info()
        logger.critical(
            "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        )


if __name__ == "__main__":
    main()
