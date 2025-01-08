import os
import traceback
from app.article_crawling import ArticleCrawler
from app.config import logger, compress_old_logs
from app.processing import (
    save_articles_to_csv,
    save_articles_to_json_by_site_and_publisher,
    send_to_hbase_with_contents,
    calculate_date_ranges,
    process_and_save_aggregated_data_from_directories,
)
from datetime import datetime


def save_data_format(format, articles, date):
    if format == "CSV":
        # 로컬 CSV 파일에 크롤링 데이터 저장
        return save_articles_to_csv(articles, date)
    elif format == "JSON":
        # 로컬 JSON 파일에 크롤링 데이터 저장
        return save_articles_to_json_by_site_and_publisher(articles, date)
    elif format == "HBASE":
        # HBase에 크롤링 데이터 저장
        send_to_hbase_with_contents(articles)
        return None
    else:
        return None


def main():
    # 네이버 전체 언론사 URL
    all_publisher_url = (
        "https://news.naver.com/main/list.naver?mode=LPOD&mid=sec&oid=032"
    )

    # 기간 설정
    start_date = "2024-09-01"  # 시작 날짜 (YYYY-MM-DD 형식)
    end_date = "2024-09-30"  # 종료 날짜 (YYYY-MM-DD 형식)
    interval = "daily"  # 수집 주기 (daily, weekly, monthly, yearly)

    article_crawler = ArticleCrawler()
    all_articles = []  # 크롤링한 전체 기사 데이터
    try:
        logger.info("뉴스 링크 수집 중...")

        # 7일 단위로 날짜를 분할
        date_ranges = calculate_date_ranges(start_date, end_date, interval)
        logger.info(f"수집할 날짜 범위: {date_ranges}")

        for start, end in date_ranges:
            logger.info(f"{start}부터 {end}까지 뉴스 링크 수집 중...")
            start = datetime.strptime(start, "%Y-%m-%d")
            start_str = datetime.strftime(start, "%Y%m%d")
            end = datetime.strptime(end, "%Y-%m-%d")
            end_str = datetime.strftime(end, "%Y%m%d")

            # 뉴스 링크 수집
            article_links = article_crawler.fetch_article_links(
                all_publisher_url, start_str, end_str
            )

            if not article_links:
                logger.warning(
                    f"{start_str}부터 {end_str}까지 수집된 뉴스 링크가 없습니다."
                )
                continue

            # 뉴스 본문 내용 추출
            articles = article_crawler.fetch_articles(article_links)
            logger.info(f"{len(articles)}개의 기사를 크롤링했습니다.")

            if articles:
                logger.info(f"크롤링 완료. 총 {len(articles)}개의 기사 수집")
                all_articles.extend(articles)
            else:
                logger.warning(
                    f"{start_str}부터 {end_str}까지 크롤링된 기사가 없습니다."
                )
                continue

            if all_articles:
                try:
                    # HBase에 크롤링 데이터 저장
                    save_data_format("HBASE", articles, date=start_str)

                    # JSON 파일로 언급량 데이터 저장
                    save_data_format("JSON", articles, date=start_str)
                except Exception as e:
                    logger.warning(f"데이터 저장 중 에러 발생: {e}")
                    continue
            else:
                logger.warning(
                    f"{start_str}부터 {end_str}까지 크롤링된 기사가 없습니다."
                )
                continue

        # 데이터 디렉터리 내의 목록 가져옴
        base_dir = "data"
        site_dirs = [
            d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))
        ]
        if not site_dirs:
            logger.warning("데이터 디렉터리가 비어있습니다.")
            return

        # 각 사이트별 일별 데이터를 기반으로 주/월/연 별 데이터 가공 및 저장
        for site in site_dirs:
            process_and_save_aggregated_data_from_directories(site)

        logger.info("작업이 완료되었습니다.")
    except Exception as e:
        logger.critical(f"예기치 못한 에러 발생: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    compress_old_logs()
    main()
