from bs4 import BeautifulSoup
from newspaper import Article

# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from app.utils.driver_handler import DriverUtils
from app.templates.template_select import get_content_template
from kiwipiepy import Kiwi

# from sklearn.feature_extraction.text import TfidfVectorizer
from typing import List, Dict
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from app.config import logger
from datetime import datetime, timedelta
import requests
import certifi
import re


class ArticleCrawler:
    BASE_URL = "https://news.naver.com"
    template_cache = {}  # 템플릿 캐시를 위한 변수
    kiwi = Kiwi()  # Kiwi 형태소 분석기 초기화

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
        # "nouns": "명사 확인할 수 없음",
        "published_at": "작성일 확인할 수 없음",
        # "updated_at": "수정일 확인할 수 없음",
        "scraped_at": datetime.now(),
    }

    def fetch_html(self, url: str) -> BeautifulSoup:
        # URL에서 HTML을 가져와서 BeautifulSoup 객체로 반환
        try:
            response = requests.get(url, verify=certifi.where(), timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as e:
            logger.error(f"URL에서 HTML 가져오기 중 에러 발생 {url}: {e}")
            return None

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
        soup = self.fetch_html(category_url)
        if not soup:
            return []

        category = next(
            (tag.text.split(" ")[0] for tag in soup.select("ul.nav > li.on > a")),
            "카테고리 확인할 수 없음",
        )

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
            {
                "category_name": category,
                "publisher_url": f"{self.BASE_URL}{tag['href']}&date={date}",
            }
            for date in date_range
            for tag in soup.select("ul.massmedia > li > a")
            if "href" in tag.attrs
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
        self, category_name: str, publisher_url: str, date_str: str, max_pages: int = 1
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
                            "category": category_name,
                            "url": link,
                            "published_at": date_str,
                        }
                        for link in page_links
                    )
                except Exception as e:
                    logger.error(f"페이지 크롤링 중 오류: {e}")

        return results

    def fetch_article_links(
        self, all_publisher_url: str, start_date: str, end_date: str
    ) -> List[Dict[str, str]]:
        # 사이트 추출
        soup = self.fetch_html(all_publisher_url)
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
            for publisher in tqdm(
                publisher_links, desc="언론사 수집 중", unit="언론사"
            ):
                all_links.extend(
                    self.fetch_news_links_parallel(
                        publisher["category_name"],
                        publisher["publisher_url"],
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
                    "category": article_info["category"],
                    "published_at": article_info["published_at"],
                }
            )

            article = Article(url, language="ko")
            article.download()
            article.parse()

            content = article.text or self.get_crawling_data(url)["content"]
            # if not article.text:
            # content = self.get_crawling_data(url)["content"]
            # pass
            published_at = (
                article.publish_date or self.get_crawling_data(url)["published_at"]
            )
            # if not article.publish_date:
            # published_at = self.get_crawling_data(url)["published_at"]
            # pass
            #     article.nlp()
            #     self.data_template["summary"] = article.summary

            self.data_template.update(
                {
                    "title": article.title,
                    "content": content,
                    "published_at": published_at,
                    # "nouns": self.extract_nouns(content),
                }
            )

            # 기사 ID 추출
            match = re.search(r"/article(?:/\d+)?/(\d+)", url)
            if match:
                self.data_template["article_id"] = match.group(1)

            """
            soup = self.fetch_html(url)
            if soup:
                # 기사 작성자 수집
                writer_tag = soup.find("span", class_="byline_s")
                if writer_tag is None:
                    writer_tag = soup.find("span", class_="NewsEndMain_author__sl+2K")

                if writer_tag is not None:
                    self.data_template["writer"] = writer_tag.text.strip()
            #     # 언론사 수집
            #     publisher_tag = soup.find("meta", property="og:article:author")
            #     if publisher_tag:
            #         full_publisher = publisher_tag.get("content")
            #         if full_publisher:
            #             # 언론사 명만 가져오기
            #             self.data_template["publisher"] = full_publisher.split("|")[0].strip()

            #     # 기사 작성일 수집
            #     published_at_tag = soup.find("span", class_="t11")
            #     if published_at_tag:
            #         self.data_template["published_at"] = published_at_tag.text

            #     # 기사 수정일 수집 (존재할 경우)
            #     updated_at_tag = soup.find("span", class_="t11_2")
            #     if updated_at_tag:
            #         self.data_template["updated_at"] = updated_at_tag.text
            """

            return self.data_template.copy()
        except Exception as e:
            logger.error(f"기사 본문 처리 중 에러 발생 {article_info['url']}: {e}")
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

    """
    def get_crawling_data(self, url: str):
        driver = DriverUtils.get_driver()
        crawling_data = {"content": "", "published_at": ""}

        try:
            driver.get(url)
            content_url = driver.current_url

            # 기사 본문 템플릿 호출
            template = self.get_template_with_cache(
                content_url, get_content_template, content_url
            )
            if not template:
                logger.error(f"템플릿을 찾을 수 없습니다: {url}")
                return crawling_data

            content_element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, template["content_selector"])
                )
            )
            crawling_data["content"] = content_element.find_element(
                By.TAG_NAME, "article"
            ).text

            date_elements = driver.find_elements(
                By.CLASS_NAME, template["date_selector"]
            )
            for date in date_elements:
                date_element = (
                    date.find_elements(By.TAG_NAME, "span")
                    if template["site"] == "n.news.naver.com"
                    else date.find_elements(By.TAG_NAME, "em")
                )
                published_at = (
                    (date_element[0].get_attribute(template["date_attribute"]))
                    .replace("오전", "AM")
                    .replace("오후", "PM")
                )
            crawling_data["published_at"] = published_at
            return crawling_data
        except Exception as e:
            logger.error(f"본문 크롤링 작업 중 오류 (Selenium) {url}: {str(e)}")
            return crawling_data
    """

    def get_crawling_data(self, url: str):
        crawling_data = {"content": "", "published_at": ""}

        try:
            soup = self.fetch_html(url)
            if not soup:
                return crawling_data

            # 기사 본문 템플릿 호출
            template = self.get_template_with_cache(url, get_content_template, url)
            if not template:
                logger.error(f"템플릿을 찾을 수 없습니다: {url}")
                return crawling_data

            content_element = soup.select_one(f".{template['content_selector']}")
            if content_element:
                crawling_data["content"] = content_element.get_text(strip=True)

            date_elements = soup.select(f".{template['date_selector']}")
            for date_element in date_elements:
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
                    break

            return crawling_data
        except Exception as e:
            logger.error(f"본문 크롤링 작업 중 오류 {url}: {str(e)}")
            return crawling_data

    def get_template_with_cache(self, cache_key, fetch_function, *args):
        # 캐시를 활용한 템플릿 조회
        if cache_key in self.template_cache:
            return self.template_cache[cache_key]

        template = fetch_function(*args)
        if template:
            self.template_cache[cache_key] = template
        return template

    """
    def extract_nouns(self, content: str) -> List[str]:
        if not content:
            return []
        tokens = self.kiwi.analyze(content)[0][0]
        return [token[0] for token in tokens if token[1].startswith("NN")]
    """

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
