import traceback
import sys
from app.crawling import ArticleCrawler
from app.config import logger
from app.processing import (
    save_articles_to_db,
    save_articles_to_csv,
    save_articles_to_json,
    send_csv_to_hdfs,
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


def main():
    # 네이버 전체 언론사 URL
    all_publisher_url = (
        "https://news.naver.com/main/list.naver?mode=LPOD&mid=sec&oid=032"
    )

    crawler = ArticleCrawler()
    try:
        logger.info("뉴스 링크 수집 중...")

        # 뉴스 링크 수집
        article_links = crawler.fetch_article_links(all_publisher_url)
        if not article_links:
            logger.warning("수집된 뉴스 링크가 없습니다.")
            return

        # 뉴스 본문 내용 추출
        articles = crawler.fetch_articles(article_links)
        logger.info(f"{len(articles)}개의 기사를 크롤링했습니다.")

        if articles:
            logger.info(f"크롤링 완료. 총 {len(articles)}개의 기사 수집")

            # 데이터 저장 (MongoDB 또는 CSV)
            local_file_path = save_data_format("JSON", articles)
            # save_articles_to_db(articles) # MongoDB에 크롤링 데이터 저장

            # if local_file_path:
            #     # HDFS로 전송
            #     send_csv_to_hdfs(local_file_path)

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
