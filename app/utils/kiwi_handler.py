from app.config import logger, Config
from typing import List
from kiwipiepy import Kiwi
from itertools import chain


class ContentAnalyzer:
    # 불용어를 메모리에 저장하고, 텍스트를 분석해 주요 단어 추출
    stop_words = None
    kiwi = Kiwi()

    @classmethod
    def load_stop_words(cls, stop_words_path: str):
        # 불용어 파일을 읽어 메모리에 저장
        if cls.stop_words is None:
            try:
                with open(stop_words_path, "r", encoding="utf-8") as file:
                    cls.stop_words = [line.strip() for line in file if line.strip()]
            except FileNotFoundError:
                logger.error(f"불용어 파일을 찾을 수 없습니다: {stop_words_path}")
                cls.stop_words = []
            except Exception as e:
                logger.error(f"불용어 파일 로드 중 에러 발생: {e}")
                cls.stop_words = []

    @classmethod
    def extract_nouns(cls, content: str):
        if not content:
            return []
        try:
            tokens = cls.kiwi.analyze(content, top_n=1)[0][0]
            unique_nouns = set(
                token[0] for token in tokens if token[1].startswith("NN")
            )
            return sorted(unique_nouns)
        except Exception as e:
            logger.error(f"명사 추출 중 에러 발생: {e}")
            return []

    @classmethod
    def extract_morphemes(
        cls, contents: List[str], stop_word_path: str = Config.STOP_WORD_PATH
    ) -> List[List[str]]:
        # 불용어 로드
        cls.load_stop_words(stop_word_path)
        result = set()

        # 명사 추출
        nouns = cls.extract_nouns(contents)
        result.update(nouns)

        for content in contents:
            words = []
            if not isinstance(content, str) or not content.strip():
                result.update(words)
                continue

            analyzed = cls.kiwi.analyze(content)
            sentence_tokens = []
            for sentence in analyzed:
                for token, pos, _, _ in sentence[0]:
                    if (
                        pos in ["NNG", "NNP"]
                        and len(token) > 1
                        and token not in cls.stop_words
                    ):
                        sentence_tokens.append(token)
                    elif ("VV" in pos or "VA" in pos) and (
                        token + "다"
                    ) not in cls.stop_words:
                        sentence_tokens.append(token + "다")
            if sentence_tokens:
                result.update(sentence_tokens)

        # 중복 제거 및 정렬
        return result # sorted(set(chain.from_iterable([tokens for tokens in result if tokens])))
