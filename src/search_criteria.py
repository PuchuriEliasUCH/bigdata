"""Single definition of the AI topic shared by the OpenAlex and Scopus ingesters.

Both sources must describe the same population, so their search expressions are
derived from one term list. Document-type and language filtering stays in Silver.
"""

from __future__ import annotations

import re
import unicodedata

# A trailing "*" is a Scopus wildcard; OpenAlex stems search terms, so it is dropped there.
AI_TERMS: tuple[str, ...] = (
    "artificial intelligence",
    "inteligencia artificial",
    "AI",
    "IA",
    "machine learning",
    "aprendizaje automático",
    "deep learning",
    "aprendizaje profundo",
    "neural network*",
    "red neuronal*",
    "natural language processing",
    "NLP",
    "procesamiento del lenguaje natural",
    "computer vision",
    "visión artificial",
    "reinforcement learning",
    "aprendizaje por refuerzo",
    "generative AI",
    "inteligencia artificial generativa",
    "large language model*",
    "LLM",
    "foundation model*",
    "AI agent*",
    "expert system*",
    "robotics",
    "AI trend*",
    "emerging artificial intelligence",
)

# The definition both sources share in Silver. The bare acronyms are dropped: they are
# too ambiguous to recheck locally ("Iowa (IA)", "Ai Weiwei").
COMMON_TERMS: tuple[str, ...] = tuple(term for term in AI_TERMS if term not in {"AI", "IA"})


def _term_pattern(term: str) -> str:
    words = unicodedata.normalize("NFC", term.lower()).split()
    prefix = words[-1].endswith("*")
    words[-1] = words[-1].rstrip("*")
    body = r"[\s\-]+".join(re.escape(word) for word in words)
    # OpenAlex stems search terms, so a plain term also accepts a simple plural.
    return body + (r"\w*" if prefix else "(?:s|es)?")


def common_query_pattern(terms: tuple[str, ...] = COMMON_TERMS) -> str:
    """One regex matching any term as whole words in lowercased, NFC-normalized text.

    Valid for both Python's re and Java (Spark rlike, with the (?U) flag so word
    boundaries understand accented letters).
    """
    return r"\b(?:" + "|".join(_term_pattern(term) for term in terms) + r")\b"


def scopus_query(terms: tuple[str, ...] = AI_TERMS) -> str:
    quoted = (f'"{term}"' if " " in term else term for term in terms)
    return f"TITLE-ABS-KEY({' OR '.join(quoted)})"


def openalex_filter(terms: tuple[str, ...] = AI_TERMS) -> str:
    quoted = (f'"{term.rstrip("*")}"' for term in terms)
    return f"title_and_abstract.search:({' OR '.join(quoted)})"


if __name__ == "__main__":
    print(f"SCOPUS_QUERY={scopus_query()}")
    print(f"OPENALEX_FILTER={openalex_filter()}")
