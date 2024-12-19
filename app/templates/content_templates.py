# 기사 본문 페이지 템플릿 정의
CONTENT_TEMPLATES = [
    {
        "site": "n.news.naver.com",
        "article_id_selector": "return article.articleId",
        "content_selector": "newsct_wrapper",
        "writer_selector": ["media_and_head_jounalist_name", "byline_p"],
        "date_selector": "media_end_head_info_datestamp",
        "date_attribute": "data-date-time",
        "updated_attribute": "data-modify-date-time",
    },
    {
        "site": "m.entertain.naver.com",
        "article_id_selector": "",
        "content_selector": "NewsEnd_news_end__AzcQj",
        "writer_selector": ["NewsEndMain_article_journalist_info__Cdr3D"],
        "date_selector": "article_head_info",
        "date_attribute": "textContent",
        "updated_attribute": "textContent",
    },
    {
        "site": "m.sports.naver.com",
        "article_id_selector": "",
        "content_selector": "NewsEnd_news_end__AzcQj",
        "writer_selector": ["NewsEndMain_article_journalist_info__Cdr3D"],
        "date_selector": "article_head_info",
        "date_attribute": "textContent",
        "updated_attribute": "textContent",
    },
]
