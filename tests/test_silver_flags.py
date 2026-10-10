import re

import pytest

from src.search_criteria import common_query_pattern
from tests.test_silver_columns import OPENALEX, SCOPUS, _run


def _matches(text):
    return re.search(common_query_pattern(), text.lower()) is not None


@pytest.mark.parametrize("text", [
    "Neural networks for crop yield prediction",
    "Evaluating Large Language Models in medicine",
    "Un enfoque de aprendizaje automático para el agro",
    "Deep-learning methods for imaging",
    "A survey of expert systems",
    "Robotics in surgery",
])
def test_common_query_pattern_matches_shared_terms(text):
    assert _matches(text)


@pytest.mark.parametrize("text", [
    "Iowa (IA) farm data",
    "Ai Weiwei art",
    "How AI changes the classroom",
    "Machinery learning curves",
    "Roboticsization",
])
def test_common_query_pattern_ignores_bare_acronyms_and_partial_words(text):
    assert not _matches(text)


def _openalex(**overrides):
    return {**OPENALEX, **overrides}


def test_openalex_flags_for_an_eligible_ai_article(spark, tmp_path):
    (row,), _ = _run(spark, tmp_path, "openalex", [OPENALEX])
    assert (row["work_type_norm"], row["is_eligible"], row["matches_common_query"]) == ("article", True, True)


def test_openalex_flags_mark_off_topic_paratext_and_other_types(spark, tmp_path):
    records = [
        _openalex(id="W2", title="Ai Weiwei and the art market", abstract_inverted_index=None, keywords=[]),
        _openalex(id="W3", title="Front matter", type="paratext", is_paratext=True),
        _openalex(id="W4", title="Neural networks", type="book-chapter"),
        _openalex(id="W5", title="Retracted study", is_retracted=True),
        _openalex(id="W6", title="Aprendizaje automático", type=None, abstract_inverted_index=None, keywords=[]),
    ]
    works, _ = _run(spark, tmp_path, "openalex", records)
    flags = {r["source_id"]: (r["work_type_norm"], r["is_eligible"], r["matches_common_query"]) for r in works}
    assert flags == {
        "W2": ("article", True, False),
        "W3": ("other", False, True),
        "W4": ("book_chapter", False, True),
        "W5": ("article", False, True),
        "W6": ("other", False, True),
    }


def test_scopus_flags_trust_the_query_and_treat_null_retraction_as_false(spark, tmp_path):
    records = [
        SCOPUS,
        {**SCOPUS, "eid": "2-s2.0-2", "dc:title": "Unrelated title", "subtypeDescription": "Conference Paper"},
        {**SCOPUS, "eid": "2-s2.0-3", "subtypeDescription": "Short Survey"},
    ]
    works, _ = _run(spark, tmp_path, "scopus", records)
    flags = {r["source_id"]: (r["work_type_norm"], r["is_eligible"], r["matches_common_query"]) for r in works}
    assert flags == {
        "2-s2.0-105011832020": ("article", True, True),
        "2-s2.0-2": ("conference_paper", True, True),
        "2-s2.0-3": ("other", False, True),
    }
