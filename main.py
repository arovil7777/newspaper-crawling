import traceback
from app.article_crawling import ArticleCrawler
from app.blog_crawling import BlogCrawler
from app.config import logger, compress_old_logs
from app.processing import (
    save_articles_to_db,
    save_articles_to_csv,
    save_articles_to_json_by_site_and_publisher,
    send_to_hdfs,
    send_to_hbase_with_local_file,
    send_to_hbase_with_contents,
    get_row_from_hbase,
    calculate_date_ranges,
    process_and_save_aggregated_data_from_directories,
)


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

    # 네이버 블로그 URL
    blog_category_url = [
        "https://section.blog.naver.com/ThemePost.naver?directoryNo=5&activeDirectorySeq=1",  # 문학/책
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=6&activeDirectorySeq=1",  # 영화
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=8&activeDirectorySeq=1",  # 미술/디자인
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=7&activeDirectorySeq=1",  # 공연/전시
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=11&activeDirectorySeq=1",  # 음악
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=9&activeDirectorySeq=1",  # 드라마
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=12&activeDirectorySeq=1",  # 스타/연예인
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=13&activeDirectorySeq=1",  # 만화/애니
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=10&activeDirectorySeq=1",  # 방송
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=14&activeDirectorySeq=2",  # 일상/생각
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=15&activeDirectorySeq=2",  # 육아/결혼
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=16&activeDirectorySeq=2",  # 반려동물
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=17&activeDirectorySeq=2",  # 좋은글/이미지
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=18&activeDirectorySeq=2",  # 패션/미용
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=19&activeDirectorySeq=2",  # 인테리어/DIY
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=20&activeDirectorySeq=2",  # 요리/레시피
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=21&activeDirectorySeq=2",  # 상품리뷰
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=36&activeDirectorySeq=2",  # 원예/재배
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=22&activeDirectorySeq=3",  # 게임
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=23&activeDirectorySeq=3",  # 스포츠
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=24&activeDirectorySeq=3",  # 사진
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=25&activeDirectorySeq=3",  # 자동차
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=26&activeDirectorySeq=3",  # 취미
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=27&activeDirectorySeq=3",  # 국내여행
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=28&activeDirectorySeq=3",  # 세계여행
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=29&activeDirectorySeq=3",  # 맛집
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=30&activeDirectorySeq=4",  # IT/컴퓨터
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=31&activeDirectorySeq=4",  # 사회/정치
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=32&activeDirectorySeq=4",  # 건강/의학
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=33&activeDirectorySeq=4",  # 비즈니스/경제
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=35&activeDirectorySeq=4",  # 어학/외국어
        # "https://section.blog.naver.com/ThemePost.naver?directoryNo=34&activeDirectorySeq=4",  # 교육/학문
    ]

    # 기간 설정
    start_date = "20241222"  # 시작 날짜 (YYYYMMDD 형식)
    end_date = "20241231"  # 종료 날짜 (YYYYMMDD 형식)
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

            # 뉴스 링크 수집
            article_links = article_crawler.fetch_article_links(
                all_publisher_url, start, end
            )

            if not article_links:
                logger.warning(f"{start}부터 {end}까지 수집된 뉴스 링크가 없습니다.")
                continue

            # 뉴스 본문 내용 추출
            articles = article_crawler.fetch_articles(article_links)
            logger.info(f"{len(articles)}개의 기사를 크롤링했습니다.")

            if articles:
                logger.info(f"크롤링 완료. 총 {len(articles)}개의 기사 수집")
                all_articles.extend(articles)
            else:
                logger.warning(f"{start}부터 {end}까지 크롤링된 기사가 없습니다.")
                continue

            if all_articles:
                try:
                    # HBase에 크롤링 데이터 저장
                    save_data_format("HBASE", articles, date=start)

                    # JSON 파일로 언급량 데이터 저장
                    save_data_format("JSON", articles, date=start)
                except Exception as e:
                    logger.warning(f"데이터 저장 중 에러 발생: {e}")
                    continue
            else:
                logger.warning(f"{start}부터 {end}까지 크롤링된 기사가 없습니다.")
                continue

        # 일별 데이터를 기반으로 주/월/연 별 데이터 가공 및 저장
        process_and_save_aggregated_data_from_directories()

        logger.info("작업이 완료되었습니다.")
    except Exception as e:
        logger.critical(f"예기치 못한 에러 발생: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    compress_old_logs()
    main()
