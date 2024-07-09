from typing import List, Literal

import pandas as pd
import spacy
from fastlangid.langid import LID  # type: ignore

# Max length in bytes of the text that can be passed into the parser
_MAX_CHUNK_LEN = 3 * int((2**16 - 1) / 4)

_punct = spacy.lang.punctuation.PUNCT.replace("\\", "").split("|")


def _split(texts: List[str]) -> List[str]:
    out_texts: list[str] = []

    for text in texts:
        while len(text.encode("utf-8")) > _MAX_CHUNK_LEN:
            # Divide by 4 as one utf-8 character can be up to 4 bytes long.
            cursor = _MAX_CHUNK_LEN // 4
            while text[cursor] not in _punct:
                cursor -= 1

            out_texts.append(text[0:cursor])
            text = text[cursor:]
        out_texts.append(text)

    return out_texts


def classify(text: str) -> str:
    classifier = LID()
    return classifier.predict(text)


def process(texts: List[str], lang: Literal["cn", "ja", "ko"]) -> List[pd.DataFrame]:
    texts = _split(texts)

    match lang:
        case "cn":
            nlp = spacy.load("zh_core_web_lg")
        case "ja":
            nlp = spacy.load("ja_core_news_lg")
        case "ko":
            nlp = spacy.load("ko_core_news_lg")

    return [
        pd.DataFrame(
            [
                (
                    t.text,
                    t.head,
                    t.ent_type_,
                    t.ent_iob_,
                    t.pos_,
                    t.is_digit,
                    t.is_punct,
                    t.is_sent_start,
                    t.is_sent_end,
                    t.is_bracket,
                    t.is_quote,
                    t.like_url,
                    t.like_num,
                    t.like_email,
                    t.is_stop,
                )
                for t in doc
            ],
            columns=[
                "token",
                "head",
                "ent_type",
                "ent_status",
                "pos",
                "is_digit",
                "is_punct",
                "is_sent_start",
                "is_sent_end",
                "is_bracket",
                "is_quote",
                "like_url",
                "like_num",
                "like_email",
                "is_stop",
            ],
        )
        for doc in nlp.pipe(texts, n_process=-1)
    ]
