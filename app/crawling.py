import requests
from bs4 import BeautifulSoup
from newspaper import Article
import re
from app.config import logger
from datetime import datetime
from typing import List, Dict
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


class ArticleCrawler:
    BASE_URL = "https://news.naver.com"

    def fetch_html(self, url: str) -> BeautifulSoup:
        # URL에서 HTML을 가져와서 BeautifulSoup 객체로 반환
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as e:
            logger.error(f"URL에서 HTML 가져오기 중 에러 발생 {url}: {e}")
            return None

    def fetch_category_links(self, all_publisher_url: str) -> List[str]:
        # 전체 언론사 URL에서 카테고리 링크 추출
        soup = self.fetch_html(all_publisher_url)
        if not soup:
            return []

        return [self.BASE_URL + tag["href"] for tag in soup.select("ul.nav > li > a")]

    def fetch_publisher_links(self, category_url: str) -> List[str]:
        # 카테고리 URL에서 언론사 링크 추출
        soup = self.fetch_html(category_url)
        if not soup:
            return []

        return [
            self.BASE_URL + tag["href"] + f"&date={datetime.now().strftime('%Y%m%d')}"
            for tag in soup.select("ul.massmedia > li > a")
        ]

    def fetch_page_links(self, paginated_url: str) -> List[str]:
        soup = self.fetch_html(paginated_url)
        if not soup:
            return []

        news_body = soup.find("div", class_="list_body newsflash_body")
        if not news_body:
            return []

        return [
            li.find("dt").find("a")["href"]
            for ul in news_body.find_all("ul")
            for li in ul.find_all("li")
            if li.find("dt") and li.find("dt").find("a")
        ]

    def fetch_news_links_parallel(
        self, publisher_url: str, max_pages: int = 20
    ) -> List[str]:
        links = []
        max_workers = max(1, (cpu_count() * 2) + 1)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.fetch_page_links, f"{publisher_url}&page={page}")
                for page in range(1, max_pages + 1)
            ]

            for future in futures:
                try:
                    page_links = future.result()
                    links.extend(page_links)
                except Exception as e:
                    logger.error(f"페이지 크롤링 중 오류: {e}")

        return list(set(links))  # 중복 제거거

    def fetch_article_links(self, all_publisher_url: str) -> List[str]:
        # 전체 언론사 URL에서 모든 뉴스 기사 링크 추출
        category_links = self.fetch_category_links(all_publisher_url)
        all_links = []

        for category_url in tqdm(
            category_links, desc="카테고리 수집 중", unit="카테고리", colour="blue"
        ):
            publisher_links = self.fetch_publisher_links(category_url)
            for publisher_url in tqdm(
                publisher_links, desc="언론사 수집 중", unit="언론사", colour="blue"
            ):
                all_links.extend(self.fetch_news_links_parallel(publisher_url))

        return all_links

    def fetch_article(self, article_url: str) -> Dict[str, str]:
        # 기사 URL에서 본문 내용 추출
        try:
            # 기사 데이터 초기화
            data = {
                "site": "사이트명 확인할 수 없음",
                "article_id": "기사 ID 확인할 수 없음",
                "url": article_url,
                "summary": "요약 확인할 수 없음",
                "title": "제목 확인할 수 없음",
                "content": "본문 확인할 수 없음",
                "writer": "작성자 확인할 수 없음",
                "publisher": "언론사 확인할 수 없음",
                "category": "카테고리 확인할 수 없음",
                "published_at": "작성일 확인할 수 없음",
                "updated_at": "수정일 확인할 수 없음",
                "scraped_at": datetime.now(),
            }

            # 기사 ID 추출
            match = re.search(r"aid=(\d+)", article_url)
            if match:
                data["article_id"] = match.group(1)

            article = Article(article_url, language="ko")
            article.download()
            article.parse()
            article.nlp()

            data["summary"] = article.summary
            data["title"] = article.title
            data["content"] = article.text

            soup = self.fetch_html(article_url)
            if soup:
                # 언론사 수집
                publisher_tag = soup.find("meta", property="og:article:author")
                if publisher_tag:
                    full_publisher = publisher_tag.get("content")
                    if full_publisher:
                        # 언론사 명만 가져오기
                        data["publisher"] = full_publisher.split("|")[0].strip()

                # 카테고리 수집
                category_tag = soup.find("meta", property="og:article:section")
                if category_tag:
                    data["category"] = category_tag.get("content")

                # 기사 작성일 수집
                published_at_tag = soup.find("span", class_="t11")
                if published_at_tag:
                    data["published_at"] = published_at_tag.text

                # 기사 수정일 수집 (존재할 경우)
                updated_at_tag = soup.find("span", class_="t11_2")
                if updated_at_tag:
                    data["updated_at"] = updated_at_tag.text

                # 기사 작성자 수집
                writer_tag = soup.find("span", class_="journalist_name")
                if writer_tag:
                    data["writer"] = writer_tag.text.strip()

            return data
        except Exception as e:
            logger.error(f"기사 본문 처리 중 에러 발생 {article_url}: {e}")
            return None

    def fetch_articles(self, article_links: List[str]) -> List[Dict[str, str]]:
        # 멀티 프로세싱으로 기사 본문 내용 추출
        num_workers = min(len(article_links), cpu_count() * 2)
        with Pool(processes=num_workers) as pool:
            articles = list(
                tqdm(
                    pool.imap(self.fetch_article, article_links),
                    total=len(article_links),
                    desc="기사 본문 크롤링 진행 중",
                    colour="green",
                )
            )
        return [article for article in articles if article]
