"""Single definition of the AI topic shared by the OpenAlex and Scopus ingesters.

Both sources must describe the same population, so their search expressions are
derived from one term list. Document-type and language filtering stays in Silver.
"""

from __future__ import annotations

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


def scopus_query(terms: tuple[str, ...] = AI_TERMS) -> str:
    quoted = (f'"{term}"' if " " in term else term for term in terms)
    return f"TITLE-ABS-KEY({' OR '.join(quoted)})"


def openalex_filter(terms: tuple[str, ...] = AI_TERMS) -> str:
    quoted = (f'"{term.rstrip("*")}"' for term in terms)
    return f"title_and_abstract.search:({' OR '.join(quoted)})"


if __name__ == "__main__":
    print(f"SCOPUS_QUERY={scopus_query()}")
    print(f"OPENALEX_FILTER={openalex_filter()}")
