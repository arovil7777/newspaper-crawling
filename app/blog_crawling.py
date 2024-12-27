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
from fake_useragent import UserAgent
import re
import traceback
import sys

# from sklearn.feature_extraction.text import TfidfVectorizer


class BlogCrawler:
    BASE_URL = "https://blog.naver.com"
    template_cache = {}  # 템플릿 캐시를 위한 변수
    kiwi = Kiwi()  # Kiwi 형태소 분석기 초기화

    blog_categories = {
        "문학·책": 5,
        "영화": 6,
        "미술·디자인": 8,
        "공연·전시": 7,
        "음악": 11,
        "드라마": 9,
        "스타·연예인": 12,
        "만화·애니": 13,
        "방송": 10,
        "일상·생각": 14,
        "육아·결혼": 15,
        "반려동물": 16,
        "좋은글·이미지": 17,
        "패션·미용": 18,
        "인테리어·DIY": 19,
        "요리·레시피": 20,
        "상품리뷰": 21,
        "원예·재배": 36,
        "게임": 22,
        "스포츠": 23,
        "사진": 24,
        "자동차": 25,
        "취미": 26,
        "국내여행": 27,
        "세계여행": 28,
        "맛집": 29,
        "IT·컴퓨터": 30,
        "사회·정치": 31,
        "건강·의학": 32,
        "비즈니스·경제": 33,
        "어학·외국어": 35,
        "교육·학문": 34,
    }

    # 기사 데이터 초기화
    data_template = {
        "site": "사이트명 확인할 수 없음",
        "blog_id": "블로그 ID 확인할 수 없음",
        "url": "블로그 URL 확인할 수 없음",
        # "summary": "요약 확인할 수 없음",
        "title": "제목 확인할 수 없음",
        "content": "본문 확인할 수 없음",
        # "writer": "작성자 확인할 수 없음",
        "bloger": "블로그 작성자자 확인할 수 없음",
        "category": "카테고리 확인할 수 없음",
        "nouns": "형태소 분석 확인할 수 없음",
        "published_at": "작성일 확인할 수 없음",
        # "updated_at": "수정일 확인할 수 없음",
        "scraped_at": datetime.now(),
    }

    def fetch_html(self, url: str) -> Tuple[BeautifulSoup, str]:
        # URL에서 HTML을 가져와서 BeautifulSoup 객체로 반환
        ua = UserAgent()
        try:
            headers = {"User-Agent": ua.chrome}
            response = requests.get(
                url, verify=certifi.where(), timeout=10, headers=headers
            )
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as e:
            logger.error(f"URL에서 HTML 가져오기 중 에러 발생 {url}: {e}")
            return None

    def fetch_page_links(self, paginated_url: str) -> List[str]:
        soup = self.fetch_html(paginated_url)
        if not soup:
            return []

        # 블로그 링크 생성
        return [
            tag["href"]
            for tag in soup.select("div.desc > a.desc_inner")
            if "href" in tag.attrs
        ]

    def fetch_blog_links_parallel(
        self, category_url: str, max_pages: int = 10
    ) -> List[Dict[str, str]]:
        def process_page(page):
            return self.fetch_page_links(f"{category_url}&currentPage={page}")

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
                        }
                        for link in page_links
                    )
                except Exception as e:
                    logger.error(f"페이지 크롤링 중 에러: {e}")

        return results

    def fetch_blog_links(
        self,
        blog_category_url: List[str],
    ) -> List[Dict[str, str]]:
        # 카테고리 URL에서 블로그 링크 추출
        soup = self.fetch_html(blog_category_url[0])
        if not soup:
            return []

        self.data_template["site"] = next(
            (tag.title.text for tag in soup.select("head")),
            "사이트명 확인할 수 없음",
        )

        # 블로그 URL에서 모든 블로그 링크 추출
        all_links = []
        for category_url in tqdm(
            blog_category_url, desc="블로그 수집 중", unit="블로그"
        ):
            all_links.extend(self.fetch_blog_links_parallel(category_url))

        return all_links

    def fetch_blogs(self, blog_info: Dict[str, str]) -> Dict[str, str]:
        # 기사 URL에서 본문 내용 추출
        try:
            url = blog_info["url"]
            self.data_template.update(
                {
                    "url": url,
                    "published_at": blog_info["published_at"],
                }
            )

            # soup = self.fetch_html(url)
            # if not soup:
            #     logger.error(f"HTML을 가져올 수 없습니다: {url}")
            #     return None

            # article = Article(final_url, language="ko")
            # article.download()
            # article.parse()

            # content = article.text or self.get_crawling_data(final_url)["content"]
            # title = article.title
            # if not title or title in [
            #     "뉴스 : 네이버스포츠",
            #     "뉴스 : 네이버 엔터",
            # ]:
            #     title = self.get_crawling_data(final_url)["title"]

            # if soup:
            #     # 기사 카테고리 수집
            #     category_tag = soup.find("li", class_="is_active")
            #     if category_tag:
            #         self.data_template["category"] = self.article_categories[
            #             category_tag.text.strip()
            #         ]

            #     if "sports" in final_url:
            #         self.data_template["category"] = self.article_categories["스포츠"]
            #     elif "entertain" in final_url:
            #         self.data_template["category"] = self.article_categories["엔터"]

            #     # 기사 언론사 수집
            #     publisher_tag = soup.find("a", class_="media_end_head_top_logo")
            #     if publisher_tag:
            #         for img in publisher_tag.find_all("img"):
            #             self.data_template["publisher"] = img.attrs["title"]

            # self.data_template.update(
            #     {
            #         "title": title,
            #         "content": content,
            #         "published_at": article.publish_date
            #         or self.get_crawling_data(final_url)["published_at"],
            #         "nouns": self.extract_nouns(content),
            #     }
            # )

            # # 기사 ID 추출
            # match = re.search(r"/article(?:/\d+)?/(\d+)", final_url)
            # if match:
            #     self.data_template["article_id"] = match.group(1)
            return self.data_template.copy()
        except Exception as e:
            logger.error(f"블로그 본문 처리 중 에러 발생 {blog_info['url']}: {e}")
            return None

    def fetch_blogs(self, blog_links: List[str]) -> List[Dict[str, str]]:
        # 멀티 프로세싱으로 블로그 본문 내용 추출
        num_workers = min(len(blog_links), cpu_count() * 2)
        try:
            with Pool(processes=num_workers) as pool:
                articles = list(
                    tqdm(
                        pool.imap(self.fetch_article, blog_links),
                        total=len(blog_links),
                        desc="블로그 본문 크롤링 진행 중",
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

    # def get_crawling_data(self, url: str):
    #     crawling_data = {"title": "", "content": "", "published_at": ""}

    #     try:
    #         soup = self.fetch_html(url)
    #         if not soup:
    #             return crawling_data

    #         # 기사 본문 템플릿 호출
    #         template = self.get_template_with_cache(
    #             final_url, get_content_template, final_url
    #         )
    #         if not template:
    #             logger.error(f"템플릿을 찾을 수 없습니다: {final_url}")
    #             return crawling_data

    #         title_element = soup.select_one(f".{template['title_selector']}")
    #         if title_element:
    #             crawling_data["title"] = title_element.get_text()

    #         content_element = soup.select_one(f".{template['content_selector']}")
    #         if content_element:
    #             crawling_data["content"] = content_element.find("article").get_text(
    #                 strip=True
    #             )

    #         date_element = next(
    #             (
    #                 date_element
    #                 for date_element in soup.select(f".{template['date_selector']}")
    #             ),
    #             None,
    #         )
    #         if date_element:
    #             date = (
    #                 date_element.find("span")
    #                 if template["site"] == "n.news.naver.com"
    #                 else date_element.find("em")
    #             )
    #             date_text = (
    #                 date.get(template["date_attribute"], "")
    #                 .replace("오전", "AM")
    #                 .replace("오후", "PM")
    #             )
    #             if date_text:
    #                 crawling_data["published_at"] = date_text

    #         return crawling_data
    #     except Exception as e:
    #         logger.error(f"본문 크롤링 작업 중 에러 {url}: {str(e)}")
    #         return crawling_data

    # def get_origin_url(self, url: str) -> str:
    #     try:
    #         response = requests.get(url, verify=certifi.where(), timeout=10)
    #         response.raise_for_status()
    #         soup = BeautifulSoup(response.text, "html.parser")

    #         # 기사 원문 URL 추출
    #         origin_url_element = soup.select_one("a[class*='link_origin_article']")
    #         if origin_url_element:
    #             return origin_url_element.get("href", "")
    #         else:
    #             return ""
    #     except Exception as e:
    #         logger.error(f"기사 원문 URL 추출 중 에러: {e}")
    #         return ""

    # def get_template_with_cache(self, cache_key, fetch_function, *args):
    #     # 캐시를 활용한 템플릿 조회
    #     if cache_key not in self.template_cache:
    #         self.template_cache[cache_key] = fetch_function(*args)
    #     return self.template_cache[cache_key]

    # def extract_nouns(self, content: str) -> List[str]:
    #     if not content:
    #         return []
    #     tokens = self.kiwi.analyze(content, top_n=1)[0][0]
    #     return [token[0] for token in tokens if token[1].startswith("NN")]

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
