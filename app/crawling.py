import requests
from bs4 import BeautifulSoup
from newspaper import Article, Config
from app.templates.template_select import get_content_template
from kiwipiepy import Kiwi
from typing import Tuple, List, Dict
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from app.config import logger
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import certifi
import re
import traceback
import sys

# from sklearn.feature_extraction.text import TfidfVectorizer


class ArticleCrawler:
    BASE_URL = "https://news.naver.com"
    template_cache = {}  # 템플릿 캐시를 위한 변수
    kiwi = Kiwi()  # Kiwi 형태소 분석기 초기화

    article_categories = {
        "정치": 100,
        "경제": 101,
        "사회": 102,
        "생활/문화": 103,
        "IT/과학": 105,
        "세계": 104,
        "오피니언": 110,
        "스포츠": 120,
        "엔터": 130,
    }

    # 기사 데이터 초기화
    data_template = {
        "site": "사이트명 확인할 수 없음",
        "article_id": "기사 ID 확인할 수 없음",
        "url": "기사 URL 확인할 수 없음",
        # "summary": "요약 확인할 수 없음",
        "title": "제목 확인할 수 없음",
        "content": "본문 확인할 수 없음",
        # "writer": "작성자 확인할 수 없음",
        "publisher": "언론사 확인할 수 없음",
        "category": "카테고리 확인할 수 없음",
        "nouns": "형태소 분석 확인할 수 없음",
        "published_at": "작성일 확인할 수 없음",
        # "updated_at": "수정일 확인할 수 없음",
        "scraped_at": datetime.now(),
    }

    def fetch_html(self, url: str) -> Tuple[BeautifulSoup, str]:
        # URL에서 HTML을 가져와서 BeautifulSoup 객체로 반환
        try:
            response = requests.get(url, verify=certifi.where(), timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser"), response.url
        except requests.RequestException as e:
            logger.error(f"URL에서 HTML 가져오기 중 에러 발생 {url}: {e}")

            # 예외 발생 시 원문 URL을 가져와서 다시 시도 (네이버 엔터, 스포츠 등에서 주로 발생)
            origin_url = self.get_origin_url(url)
            if origin_url:
                try:
                    response = requests.get(
                        origin_url, verify=certifi.where(), timeout=10
                    )
                    response.raise_for_status()
                    return BeautifulSoup(response.text, "html.parser"), response.url
                except requests.RequestException as e:
                    logger.error(
                        f"원문 URL에서 HTML 가져오기 중 에러 발생 {origin_url}: {e}"
                    )
                    return None, url
            else:
                return None, url

    def parse_category_links(self, soup: BeautifulSoup) -> List[str]:
        return [
            self.BASE_URL + tag["href"]
            for tag in soup.select("ul.nav > li > a")
            if "href" in tag.attrs
        ]

    def fetch_publisher_links(
        self, category_url: str, start_date: str, end_date: str
    ) -> List[Dict[str, str]]:
        # 카테고리 URL에서 언론사 링크 추출
        soup, _ = self.fetch_html(category_url)
        if not soup:
            return []

        try:
            # 날짜 범위 설정
            start = datetime.strptime(start_date, "%Y%m%d")
            end = datetime.strptime(end_date, "%Y%m%d")
        except ValueError as e:
            logger.error(f"잘못된 날짜 형식: {e}")
            return []

        date_range = [
            (start + timedelta(days=i)).strftime("%Y%m%d")
            for i in range((end - start).days + 1)
        ]

        # 언론사 링크 생성
        return [
            f"{self.BASE_URL}{tag['href']}&date={date}"
            for date in date_range
            for tag in soup.select("ul.massmedia > li > a")
            if "href" in tag.attrs
        ]

    def fetch_page_links(self, paginated_url: str) -> List[str]:
        soup, _ = self.fetch_html(paginated_url)
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
        self, publisher_url: str, date_str: str, max_pages: int = 10
    ) -> List[Dict[str, str]]:
        def process_page(page):
            return self.fetch_page_links(f"{publisher_url}&page={page}")

        max_workers = min(max(1, cpu_count() - 2), max_pages)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(process_page, page) for page in range(1, max_pages + 1)
            ]
            results = []
            for future in as_completed(futures):
                try:
                    page_links = future.result()
                    results.extend(
                        {
                            "url": link,
                            "published_at": date_str,
                        }
                        for link in page_links
                    )
                except Exception as e:
                    logger.error(f"페이지 크롤링 중 에러: {e}")

        return results

    def fetch_article_links(
        self, all_publisher_url: str, start_date: str, end_date: str
    ) -> List[Dict[str, str]]:
        # 사이트 추출
        soup, _ = self.fetch_html(all_publisher_url)
        if not soup:
            return []

        self.data_template["site"] = next(
            (tag.title.text.split(":")[1].strip() for tag in soup.select("head")),
            "사이트명 확인할 수 없음",
        )

        # 전체 언론사 URL에서 모든 뉴스 기사 링크 추출
        category_links = self.parse_category_links(soup)
        all_links = []
        for category_url in tqdm(
            category_links, desc="카테고리 수집 중", unit="카테고리"
        ):
            publisher_links = self.fetch_publisher_links(
                category_url, start_date, end_date
            )
            for publisher_url in tqdm(
                publisher_links, desc="언론사 수집 중", unit="언론사"
            ):
                all_links.extend(
                    self.fetch_news_links_parallel(
                        publisher_url,
                        start_date,
                    )
                )

        return all_links

    def fetch_article(self, article_info: Dict[str, str]) -> Dict[str, str]:
        # 기사 URL에서 본문 내용 추출
        try:
            url = article_info["url"]
            self.data_template.update(
                {
                    "url": url,
                    "published_at": article_info["published_at"],
                }
            )

            soup, final_url = self.fetch_html(url)
            if not soup:
                logger.error(f"HTML을 가져올 수 없습니다: {url}")
                return None

            article = Article(final_url, language="ko")
            article.download()
            article.parse()

            content = article.text or self.get_crawling_data(final_url)["content"]
            title = article.title
            if not title or title in [
                "뉴스 : 네이버스포츠",
                "뉴스 : 네이버 엔터",
            ]:
                title = self.get_crawling_data(final_url)["title"]

            if soup:
                # 기사 카테고리 수집
                category_tag = soup.find("li", class_="is_active")
                if category_tag:
                    self.data_template["category"] = self.article_categories[
                        category_tag.text.strip()
                    ]

                if "sports" in final_url:
                    self.data_template["category"] = self.article_categories["스포츠"]
                elif "entertain" in final_url:
                    self.data_template["category"] = self.article_categories["엔터"]

                # 기사 언론사 수집
                publisher_tag = soup.find("a", class_="media_end_head_top_logo")
                if publisher_tag:
                    for img in publisher_tag.find_all("img"):
                        self.data_template["publisher"] = img.attrs["title"]

            self.data_template.update(
                {
                    "title": title,
                    "content": content,
                    "published_at": article.publish_date
                    or self.get_crawling_data(final_url)["published_at"],
                    "nouns": self.extract_nouns(content),
                }
            )

            # 기사 ID 추출
            match = re.search(r"/article(?:/\d+)?/(\d+)", final_url)
            if match:
                self.data_template["article_id"] = match.group(1)
            return self.data_template.copy()
        except Exception as e:
            logger.error(f"기사 본문 처리 중 에러 발생 {article_info['url']}: {e}")
            return None

    def fetch_articles(self, article_links: List[str]) -> List[Dict[str, str]]:
        # 멀티 프로세싱으로 기사 본문 내용 추출
        num_workers = min(len(article_links), max(1, cpu_count() - 2))
        try:
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
        except BrokenPipeError as e:
            logger.error(f"BrokenPipeError 발생: {e}")
            return []
        except Exception as e:
            logger.error(f"예기치 않은 에러 발생: {e}")
            return []

    def get_crawling_data(self, url: str):
        crawling_data = {"title": "", "content": "", "published_at": ""}

        try:
            soup, final_url = self.fetch_html(url)
            if not soup:
                return crawling_data

            # 기사 본문 템플릿 호출
            template = self.get_template_with_cache(
                final_url, get_content_template, final_url
            )
            if not template:
                logger.error(f"템플릿을 찾을 수 없습니다: {final_url}")
                return crawling_data

            title_element = soup.select_one(f".{template['title_selector']}")
            if title_element:
                crawling_data["title"] = title_element.get_text()

            content_element = soup.select_one(f".{template['content_selector']}")
            if content_element:
                crawling_data["content"] = content_element.find("article").get_text(
                    strip=True
                )

            date_element = next(
                (
                    date_element
                    for date_element in soup.select(f".{template['date_selector']}")
                ),
                None,
            )
            if date_element:
                date = (
                    date_element.find("span")
                    if template["site"] == "n.news.naver.com"
                    else date_element.find("em")
                )
                date_text = (
                    date.get(template["date_attribute"], "")
                    .replace("오전", "AM")
                    .replace("오후", "PM")
                )
                if date_text:
                    crawling_data["published_at"] = date_text

            return crawling_data
        except Exception as e:
            logger.error(f"본문 크롤링 작업 중 에러 {url}: {str(e)}")
            return crawling_data

    def get_origin_url(self, url: str) -> str:
        try:
            response = requests.get(url, verify=certifi.where(), timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # 기사 원문 URL 추출
            origin_url_element = soup.select_one("a[class*='link_origin_article']")
            if origin_url_element:
                return origin_url_element.get("href", "")
            else:
                return ""
        except Exception as e:
            logger.error(f"기사 원문 URL 추출 중 에러: {e}")
            return ""

    def get_template_with_cache(self, cache_key, fetch_function, *args):
        # 캐시를 활용한 템플릿 조회
        if cache_key not in self.template_cache:
            self.template_cache[cache_key] = fetch_function(*args)
        return self.template_cache[cache_key]

    def extract_nouns(self, content: str) -> List[str]:
        if not content:
            return []
        tokens = self.kiwi.analyze(content, top_n=1)[0][0]
        return [token[0] for token in tokens if token[1].startswith("NN")]

    """
    def extract_keywords_using_tfidf(self, articles: List[Dict[str, str]]):
        # TF-IDF 기법을 사용하여 명사만으로 키워드 추출
        noun_contents = [
            " ".join(article.get("nouns", []))
            for article in articles
            if "nouns" in article
        ]  # 명사 리스트를 텍스트 형태로 변환

        # TF-IDF 벡터라이저 사용
        vectorizer = TfidfVectorizer(
            min_df=1,
            max_df=1.0,
            max_features=20,
            ngram_range=(1, 1),
            stop_words="english",
        )
        X = vectorizer.fit_transform(noun_contents)

        # 단어와 TF-IDF 점수를 추출
        feature_names = vectorizer.get_feature_names_out()
        scores = X.sum(axis=0).A1  # 각 단어의 TF-IDF 합계 계산

        # 키워드 추출
        return sorted(zip(feature_names, scores), key=lambda x: x[1], reverse=True)
    """
