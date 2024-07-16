from typing import List, Literal

import pandas as pd
import spacy
from fastlangid.langid import LID  # type: ignore

# Max length in bytes of the text that can be passed into the japanese parser
# From https://github.com/WorksApplications/sudachi.rs/blob/develop/sudachi/src/input_text/buffer/mod.rs#L32
MAX_JA_CHUNK_LEN = 3 * ((2**16 - 1) // 4)

_punct = spacy.lang.punctuation.PUNCT.replace("\\", "").replace(" ", "").split("|")


def split(text: str, max_len: int) -> List[str]:
    out_texts: list[str] = []

    while len(text.encode("utf-8")) > max_len:
        # Divide by 4 as one utf-8 character can be up to 4 bytes long.
        cursor = max_len // 4
        while text[cursor] not in _punct:
            cursor -= 1

        out_texts.append(text[:cursor])
        text = text[cursor:]
    out_texts.append(text)

    return out_texts


def classify(text: str) -> str:
    classifier = LID()
    return classifier.predict(text)


def process(
    texts: List[str], lang: Literal["cn", "en", "ja", "ko"]
) -> List[pd.DataFrame]:
    """Processes a list of texts into a list of `DataFrame`s. Note that the length of the returned\
 list may be greater than the length of the the input text.

    Args:
        texts (List[str]): The list of texts to process.
        lang (Literal["cn", "en", "ja", "ko"]): The language the text is in.

    Returns:
        List[pd.DataFrame]: The processed texts.
    """
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
        for doc in process_spacy(texts, lang)
    ]


def process_spacy(
    texts: List[str], lang: Literal["cn", "en", "ja", "ko"]
) -> List[spacy.tokens.Doc]:
    """Processes a list of texts into a list of `Doc`s. Note that the length of the returned\
 list may be greater than the length of the the input text.

    Args:
        texts (List[str]): The list of texts to process.
        lang (Literal["cn", "en", "ja", "ko"]): The language the text is in.

    Returns:
        List[spacy.tokens.Doc]: The processed texts.
    """
    all_texts = []
    for raw_text in texts:
        for split_text in split(raw_text, MAX_JA_CHUNK_LEN):
            for text in split_text:
                all_texts.append(text)

    match lang:
        case "cn":
            nlp = spacy.load("zh_core_web_lg")
        case "en":
            nlp = spacy.load("en_core_web_lg")
        case "ja":
            nlp = spacy.load("ja_core_news_lg")
        case "ko":
            nlp = spacy.load("ko_core_news_lg")

    return list(nlp.pipe(texts, n_process=-1))
