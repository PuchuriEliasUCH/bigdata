import json
from pathlib import Path

from src.openalex_ingest import BatchStateStore
from src.spark_pipeline import SILVER_SCHEMA_VERSION, process_pending_batches

INGESTED_AT = "2026-10-02T10:00:00+00:00"


def _topic(identifier, name, score):
    return {"id": identifier, "display_name": name, "score": score,
            "subfield": {"id": "s1", "display_name": "Artificial Intelligence"},
            "field": {"id": "f17", "display_name": "Computer Science"},
            "domain": {"id": "d3", "display_name": "Physical Sciences"}}


OPENALEX = {
    "id": "https://openalex.org/W1", "title": "A <i>Survey</i>   on Evaluation", "type": "article", "language": "en",
    "publication_year": 2024, "publication_date": "2024-01-23", "doi": "https://doi.org/10.1145/3641289",
    "cited_by_count": 2827, "is_retracted": False, "is_paratext": False,
    "abstract_inverted_index": {"Large": [0], "language": [1], "models": [2], "are": [3, 5], "evaluated": [4]},
    "authorships": [
        {"author_position": "first", "is_corresponding": False,
         "author": {"id": "https://openalex.org/A1", "display_name": "Yupeng Chang", "orcid": "https://orcid.org/0000-0001-7178-6088"},
         "countries": ["CN"],
         "institutions": [{"id": "https://openalex.org/I1", "display_name": "Jilin University", "ror": "https://ror.org/00js3aw79",
                           "country_code": "CN", "type": "education"}]},
        {"author_position": "last", "is_corresponding": True,
         "author": {"id": "https://openalex.org/A2", "display_name": "Qiang Yang", "orcid": None},
         "countries": ["CN", "HK"],
         "institutions": [{"id": "https://openalex.org/I1", "display_name": "Jilin University", "ror": "https://ror.org/00js3aw79",
                           "country_code": "CN", "type": "education"},
                          {"id": "https://openalex.org/I2", "display_name": "HKUST", "ror": None, "country_code": "HK", "type": "education"}]},
    ],
    "topics": [_topic("T1", "Topic Modeling", 0.9989), _topic("T2", "Neural Networks", 0.8)],
    "primary_topic": _topic("T1", "Topic Modeling", 0.9989),
    "keywords": [{"id": "k1", "display_name": "large language models", "score": 0.99}, {"id": "k2", "display_name": "LLM evaluation", "score": 0.98}],
    "counts_by_year": [{"year": 2026, "cited_by_count": 802}, {"year": 2025, "cited_by_count": 1434}, {"year": 2024, "cited_by_count": 570}],
    "primary_location": {"source": {"display_name": "ACM Transactions on Intelligent Systems"}},
    "open_access": {"is_oa": False},
}

SCOPUS = {
    "eid": "2-s2.0-105011832020", "dc:title": "THE TRANSFORMATIVE IMPACT OF LARGE LANGUAGE MODELS", "dc:creator": "Shalko M.",
    "prism:publicationName": "Technology Audit and Production Reserves", "prism:coverDate": "2024-12-31",
    "prism:doi": "10.15587/2706-5448.2024.319006", "citedby-count": "7",
    "affiliation": [{"@_fa": "true", "affilname": "Classic Private University", "affiliation-city": "Zaporizhzhia", "affiliation-country": "Ukraine"}],
    "prism:aggregationType": "Journal", "subtype": "ar", "subtypeDescription": "Article",
    "openaccess": "1", "openaccessFlag": True,
}


def _run(spark, tmp_path, source, records):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    store = BatchStateStore(bronze, source)
    payload = {"results": records} if source == "openalex" else {"search-results": {"entry": records}}
    page = store.write_raw_page("b1", 1, payload)
    (bronze / "batch_metadata").mkdir(parents=True, exist_ok=True)
    (bronze / "batch_metadata" / "b1.json").write_text(json.dumps({
        "batch_id": "b1", "source": source, "status": "completed_target", "page_count": 1, "record_count": len(records),
        "raw_page_dir": f"{source}/b1", "raw_page_files": [str(page.relative_to(bronze))], "created_at": INGESTED_AT}))
    process_pending_batches(spark, bronze, silver)
    works = spark.read.parquet(str(silver / "works" / "b1")).collect()
    rejected = spark.read.parquet(str(silver / "rejected" / "b1")).collect() if (silver / "rejected" / "b1").exists() else []
    return works, rejected


def test_openalex_record_fills_every_silver_column(spark, tmp_path):
    (row,), rejected = _run(spark, tmp_path, "openalex", [OPENALEX])
    assert rejected == []
    assert (row["source"], row["source_id"], row["schema_version"], row["ingested_at"]) == ("openalex", "https://openalex.org/W1", SILVER_SCHEMA_VERSION, INGESTED_AT)
    assert row["title"] == "A Survey on Evaluation"
    assert row["abstract"] == "Large language models are evaluated are" and row["has_abstract"] is True
    assert (row["language"], row["work_type"], row["venue"]) == ("en", "article", "ACM Transactions on Intelligent Systems")
    assert row["normalized_doi"] == "10.1145/3641289"
    assert (row["is_oa"], row["is_retracted"], row["is_paratext"]) == (False, False, False)
    assert row["cited_by_count"] == 2827
    assert [(c["year"], c["cited_by_count"]) for c in row["counts_by_year"]] == [(2026, 802), (2025, 1434), (2024, 570)]
    assert row["keywords"] == ["large language models", "LLM evaluation"]


def test_openalex_authorship_institutions_and_countries(spark, tmp_path):
    (row,), _ = _run(spark, tmp_path, "openalex", [OPENALEX])
    assert row["author_count"] == 2
    first, last = row["authors"]
    assert (first["name"], first["position"], first["is_corresponding"], first["orcid"]) == ("Yupeng Chang", "first", False, "https://orcid.org/0000-0001-7178-6088")
    assert (last["name"], last["is_corresponding"], last["orcid"]) == ("Qiang Yang", True, None)
    assert sorted(i["name"] for i in row["institutions"]) == ["HKUST", "Jilin University"]
    assert sorted(row["country_codes"]) == ["CN", "HK"] and row["country_names"] == []


def test_openalex_topics_keep_the_hierarchy_and_a_primary_topic(spark, tmp_path):
    (row,), _ = _run(spark, tmp_path, "openalex", [OPENALEX])
    first = row["topics"][0]
    assert (first["id"], first["name"], first["subfield"], first["field"], first["domain"]) == ("T1", "Topic Modeling", "Artificial Intelligence", "Computer Science", "Physical Sciences")
    assert abs(first["score"] - 0.9989) < 1e-6
    assert row["topic_count"] == 2 and row["has_topics"] is True and row["primary_topic"]["name"] == "Topic Modeling"


def test_scopus_entry_keeps_what_the_standard_view_provides_and_marks_the_rest_unknown(spark, tmp_path):
    (row,), rejected = _run(spark, tmp_path, "scopus", [SCOPUS])
    assert rejected == []
    assert (row["source"], row["source_id"], row["work_type"], row["venue"]) == ("scopus", "2-s2.0-105011832020", "Article", "Technology Audit and Production Reserves")
    assert (row["cited_by_count"], row["is_oa"], row["publication_year"]) == (7, True, 2024)
    assert [a["name"] for a in row["authors"]] == ["Shalko M."] and row["author_count"] is None
    (institution,) = row["institutions"]
    assert (institution["name"], institution["city"], institution["country_name"], institution["country_code"]) == ("Classic Private University", "Zaporizhzhia", "Ukraine", None)
    assert row["country_names"] == ["Ukraine"] and row["country_codes"] == []
    assert row["abstract"] is None and row["has_abstract"] is False and row["keywords"] == [] and row["language"] is None
    assert row["topics"] == [] and row["has_topics"] is False and row["is_retracted"] is None


def test_a_sparse_record_is_kept_with_flags_instead_of_being_rejected(spark, tmp_path):
    sparse = {"id": "https://openalex.org/W9", "title": "Only a title", "publication_year": 2020}
    (row,), rejected = _run(spark, tmp_path, "openalex", [sparse])
    assert rejected == []
    assert (row["has_abstract"], row["has_topics"], row["abstract"], row["author_count"]) == (False, False, None, None)
    assert row["authors"] == [] and row["institutions"] == [] and row["topics"] == [] and row["keywords"] == []


def test_records_without_identifier_title_or_date_are_still_rejected(spark, tmp_path):
    bad = [{"title": "no id", "publication_year": 2020}, {"id": "W1", "title": "", "publication_year": 2020},
           {"id": "W2", "title": "no date"}]
    works, rejected = _run(spark, tmp_path, "openalex", bad)
    assert works == []
    reasons = sorted(r["rejection_reason"] for r in rejected)
    assert reasons == ["missing_publication_year_or_date", "missing_source_id", "missing_title"]


def test_title_and_abstract_are_unicode_normalized(spark, tmp_path):
    decomposed = "Caf" + "e" + chr(0x301)
    record = {"id": "W5", "title": decomposed, "publication_year": 2021,
              "abstract_inverted_index": {decomposed: [0]}}
    (row,), _ = _run(spark, tmp_path, "openalex", [record])
    assert row["title"] == "Caf" + chr(0xE9) and len(row["title"]) == 4
    assert row["abstract"] == "Caf" + chr(0xE9)
