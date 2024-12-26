import traceback
import sys
from datetime import datetime, timedelta
from app.crawling import ArticleCrawler
from app.config import Config, logger
from app.processing import (
    save_articles_to_db,
    save_articles_to_csv,
    save_articles_to_json,
    send_to_hdfs,
    send_to_hbase,
    get_row_from_hbase,
)


def save_data_format(format, articles, date):
    if format == "CSV":
        # 로컬 CSV 파일에 크롤링 데이터 저장
        return save_articles_to_csv(articles, date)
    elif format == "JSON":
        # 로컬 JSON 파일에 크롤링 데이터 저장
        return save_articles_to_json(articles, date)
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
    all_articles = []  # 크롤링한 전체 기사 데이터
    try:
        logger.info("뉴스 링크 수집 중...")

        # 7일 단위로 날짜를 분할
        date_ranges = calculate_date_ranges(start_date, end_date, interval=1)
        logger.info(f"수집할 날짜 범위: {date_ranges}")

        for start, end in date_ranges:
            logger.info(f"{start}부터 {end}까지 뉴스 링크 수집 중...")

            # 뉴스 링크 수집
            article_links = crawler.fetch_article_links(all_publisher_url, start, end)

            if not article_links:
                logger.warning(f"{start}부터 {end}까지 수집된 뉴스 링크가 없습니다.")
                continue

            # 뉴스 본문 내용 추출
            articles = crawler.fetch_articles(article_links)
            logger.info(f"{len(articles)}개의 기사를 크롤링했습니다.")

            if articles:
                logger.info(f"크롤링 완료. 총 {len(articles)}개의 기사 수집")
                all_articles.extend(articles)
            else:
                logger.warning(f"{start}부터 {end}까지 크롤링된 기사가 없습니다.")
                continue

            if all_articles:
                # 1. 로컬에 데이터 저장 (MongoDB 또는 CSV)
                local_file_paths = save_data_format("CSV", articles, date=start)
                # # save_articles_to_db(articles) # MongoDB에 크롤링 데이터 저장

                # 2. CSV 데이터를 HBase로 저장
                if local_file_paths:
                    for local_file_path in local_file_paths:
                        send_to_hbase(None, local_file_path)
                # # HDFS로 전송
                # hdfs_file_path = send_to_hdfs(local_file_path)

                # # HDFS에서 HBase로 전송
                # if hdfs_file_path:
                #     send_to_hbase(hdfs_file_path, local_file_path)

                # # HBase에서 데이터 조회
                # if articles:
                #     get_row_from_hbase(Config.TABLE_NAME)
            else:
                logger.warning(f"{start}부터 {end}까지 크롤링된 기사가 없습니다.")
                continue
        logger.info("작업이 완료되었습니다.")
    except Exception as e:
        logger.critical(f"예기치 못한 에러 발생: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()
