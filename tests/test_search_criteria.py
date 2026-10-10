from src.search_criteria import AI_TERMS, openalex_filter, scopus_query

STORED_SCOPUS_QUERY = (
    'TITLE-ABS-KEY("artificial intelligence" OR "inteligencia artificial" OR AI OR IA OR "machine learning" '
    'OR "aprendizaje automático" OR "deep learning" OR "aprendizaje profundo" OR "neural network*" '
    'OR "red neuronal*" OR "natural language processing" OR NLP OR "procesamiento del lenguaje natural" '
    'OR "computer vision" OR "visión artificial" OR "reinforcement learning" OR "aprendizaje por refuerzo" '
    'OR "generative AI" OR "inteligencia artificial generativa" OR "large language model*" OR LLM '
    'OR "foundation model*" OR "AI agent*" OR "expert system*" OR robotics OR "AI trend*" '
    'OR "emerging artificial intelligence")'
)


def test_scopus_query_matches_the_query_already_used_for_ingestion():
    assert scopus_query() == STORED_SCOPUS_QUERY


def test_openalex_filter_uses_the_same_terms_without_scopus_wildcards():
    expression = openalex_filter()
    assert expression.startswith("title_and_abstract.search:(")
    assert "*" not in expression
    for term in AI_TERMS:
        assert f'"{term.rstrip("*")}"' in expression


def test_terms_are_defined_once_for_both_sources():
    assert len(AI_TERMS) == len(set(AI_TERMS))


def test_common_terms_drop_only_the_bare_acronyms():
    from src.search_criteria import COMMON_TERMS

    assert "AI" not in COMMON_TERMS and "IA" not in COMMON_TERMS
    assert set(COMMON_TERMS) == set(AI_TERMS) - {"AI", "IA"}
