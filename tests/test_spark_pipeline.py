import json
import os
from pathlib import Path

import pytest

import src.spark_pipeline as spark_pipeline
from src.spark_pipeline import process_pending_batches


def write_completed_batch(bronze_root: Path, batch_id: str, records):
    raw_dir = bronze_root / "openalex" / batch_id
    metadata_dir = bronze_root / "batch_metadata"
    raw_dir.mkdir(parents=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    (raw_dir / "page-000001.json").write_text(
        json.dumps({"meta": {"next_cursor": None}, "results": records}),
        encoding="utf-8",
    )
    (metadata_dir / f"{batch_id}.json").write_text(
        json.dumps(
            {
                "batch_id": batch_id,
                "mode": "initial",
                "status": "completed",
                "raw_page_dir": str(Path("openalex") / batch_id),
                "record_count": len(records),
                "page_count": 1,
            }
        ),
        encoding="utf-8",
    )


def test_processing_validates_normalizes_deduplicates_and_rejects(spark, tmp_path):
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    valid_record = {
        "id": "https://openalex.org/W1",
        "title": "A valid work",
        "publication_year": 2024,
        "publication_date": "2024-05-10",
        "topics": [
            {
                "id": "https://openalex.org/T1",
                "display_name": "Data Engineering",
            }
        ],
        "type": "article",
        "cited_by_count": 7,
        "open_access": {"is_oa": True},
    }
    duplicate_record = {**valid_record, "title": "Duplicate work"}
    missing_title = {
        "id": "https://openalex.org/W2",
        "publication_year": 2023,
        "topics": [{"display_name": "Databases"}],
    }
    missing_topics = {
        "id": "https://openalex.org/W3",
        "title": "No topic work",
        "publication_date": "2022-01-01",
        "topics": [],
    }
    write_completed_batch(
        bronze_root,
        "initial-test",
        [valid_record, duplicate_record, missing_title, missing_topics],
    )

    summary = process_pending_batches(spark, bronze_root, silver_root)

    assert summary.processed_batches == ("initial-test",)
    assert summary.valid_rows == 1
    assert summary.rejected_rows == 3
    assert len(list((silver_root / "works" / "initial-test").glob("part-*.parquet"))) == 1

    valid_rows = spark.read.parquet(str(silver_root / "works" / "initial-test")).collect()
    assert len(valid_rows) == 1
    assert valid_rows[0]["source_id"] == "https://openalex.org/W1"
    assert valid_rows[0]["publication_year"] == 2024
    assert valid_rows[0]["topic_names"] == ["Data Engineering"]

    rejected_rows = spark.read.parquet(str(silver_root / "rejected" / "initial-test")).collect()
    reasons = {row["rejection_reason"] for row in rejected_rows}
    assert len(rejected_rows) == 3
    assert "duplicate_source_id" in reasons
    assert any("missing_title" in reason for reason in reasons)
    assert any("missing_topics" in reason for reason in reasons)


def test_processing_reads_raw_pages_without_driver_row_list(spark, tmp_path, monkeypatch):
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    write_completed_batch(
        bronze_root,
        "spark-native",
        [
            {
                "id": "https://openalex.org/W11",
                "title": "Spark-native work",
                "publication_year": 2024,
                "topics": [{"display_name": "Distributed Systems"}],
            }
        ],
    )

    def fail_if_python_rows_are_loaded(*args, **kwargs):
        raise AssertionError("Bronze records must not be loaded into a Python list")

    # This guard fails against the old driver-side loader while the raw page
    # remains the only input used by the processing path.
    monkeypatch.setattr(
        spark_pipeline,
        "_load_batch_rows",
        fail_if_python_rows_are_loaded,
        raising=False,
    )

    summary = process_pending_batches(spark, bronze_root, silver_root)

    assert summary.processed_batches == ("spark-native",)
    assert summary.valid_rows == 1
    assert summary.rejected_rows == 0
    assert spark.read.parquet(str(silver_root / "works" / "spark-native")).count() == 1


def test_completed_batch_is_not_reprocessed(spark, tmp_path):
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    write_completed_batch(
        bronze_root,
        "already-processed",
        [
            {
                "id": "https://openalex.org/W10",
                "title": "Already processed",
                "publication_year": 2020,
                "topics": [{"display_name": "Testing"}],
            }
        ],
    )

    first_summary = process_pending_batches(spark, bronze_root, silver_root)
    second_summary = process_pending_batches(spark, bronze_root, silver_root)

    assert first_summary.processed_batches == ("already-processed",)
    assert second_summary.processed_batches == ()
    assert second_summary.skipped_batches == ("already-processed",)
    state = json.loads(
        (silver_root / "_processing" / "already-processed.json").read_text(
            encoding="utf-8"
        )
    )
    assert state["status"] == "completed"


def test_previous_batch_duplicate_is_rejected_without_using_invalid_ids(spark, tmp_path):
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    valid_record = {
        "id": "https://openalex.org/W20",
        "title": "A valid work",
        "publication_year": 2024,
        "publication_date": "2024-05-10",
        "topics": [{"display_name": "Data Engineering"}],
        "type": "article",
    }
    invalid_record = {**valid_record, "id": "https://openalex.org/W21"}
    invalid_record.pop("title")
    write_completed_batch(bronze_root, "batch-001", [valid_record, invalid_record])

    duplicate_record = {**valid_record, "title": "Repeated work"}
    newly_valid_record = {
        **valid_record,
        "id": "https://openalex.org/W21",
        "title": "Previously invalid work",
    }
    write_completed_batch(
        bronze_root,
        "batch-002",
        [duplicate_record, newly_valid_record],
    )

    summary = process_pending_batches(spark, bronze_root, silver_root)

    assert summary.processed_batches == ("batch-001", "batch-002")
    assert summary.valid_rows == 2
    assert summary.rejected_rows == 2

    first_batch_rows = spark.read.parquet(str(silver_root / "works" / "batch-001")).collect()
    assert [row["source_id"] for row in first_batch_rows] == [
        "https://openalex.org/W20"
    ]
    second_batch_rows = spark.read.parquet(str(silver_root / "works" / "batch-002")).collect()
    assert [row["source_id"] for row in second_batch_rows] == [
        "https://openalex.org/W21"
    ]

    first_rejected = spark.read.parquet(str(silver_root / "rejected" / "batch-001")).collect()
    assert len(first_rejected) == 1
    assert "missing_title" in first_rejected[0]["rejection_reason"]
    second_rejected = spark.read.parquet(str(silver_root / "rejected" / "batch-002")).collect()
    assert len(second_rejected) == 1
    assert second_rejected[0]["source_id"] == "https://openalex.org/W20"
    assert second_rejected[0]["rejection_reason"] == "duplicate_source_id_previous_batch"


def test_scopus_and_openalex_keep_provenance_and_share_normalized_doi(spark, tmp_path):
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    write_completed_batch(bronze, "batch-001", [{
        "id": "https://openalex.org/W1", "title": "Same article", "publication_year": 2024,
        "topics": [{"display_name": "Data"}], "doi": "https://doi.org/10.1234/ABC",
    }])
    batch = "batch-002"
    raw = bronze / "scopus" / batch
    raw.mkdir(parents=True)
    (raw / "page-000001.json").write_text(json.dumps({"search-results": {"entry": [{
        "eid": "2-s2.0-123", "dc:title": "Same article", "prism:coverDate": "2024-01-01",
        "prism:doi": "10.1234/abc", "subtypeDescription": "Article",
    }]}}))
    (bronze / "batch_metadata" / f"{batch}.json").write_text(json.dumps({
        "batch_id": batch, "source": "scopus", "status": "completed_target", "page_count": 1,
        "raw_page_dir": f"scopus/{batch}", "created_at": "z",
    }))
    summary = process_pending_batches(spark, bronze, silver)
    assert summary.valid_rows == 2
    openalex = spark.read.parquet(str(silver / "works" / "batch-001")).first()
    scopus = spark.read.parquet(str(silver / "works" / batch)).first()
    assert (openalex["source"], openalex["source_id"]) == ("openalex", "https://openalex.org/W1")
    assert (scopus["source"], scopus["source_id"]) == ("scopus", "2-s2.0-123")
    assert openalex["normalized_doi"] == scopus["normalized_doi"] == "10.1234/abc"


def test_large_batch_uses_more_than_one_output_partition():
    assert spark_pipeline.output_partitions(100_001, rows_per_file=100_000) == 2
    assert spark_pipeline.output_partitions(1000, rows_per_file=100_000) == 1


def test_unique_publications_preserves_source_rows_and_missing_doi(spark):
    from src.spark_pipeline import unique_publications
    rows = [
        ("openalex", "W1", "10.1/a"),
        ("scopus", "S1", "10.1/a"),
        ("scopus", "S2", None),
        ("openalex", "W2", None),
    ]
    frame = spark.createDataFrame(rows, ["source", "source_id", "normalized_doi"])
    unique = unique_publications(frame).collect()
    assert len(unique) == 3
    assert {row.source_id for row in unique if row.normalized_doi == "10.1/a"} == {"W1"}


def test_scopus_duplicate_across_pages_is_rejected(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    batch = "scopus-batch"
    raw = bronze / "scopus" / batch
    raw.mkdir(parents=True)
    metadata = bronze / "batch_metadata"
    metadata.mkdir()
    for page_number in (1, 2):
        (raw / f"page-{page_number:06d}.json").write_text(json.dumps({"search-results": {"entry": [{"eid": "duplicate", "dc:title": "Title", "prism:coverDate": "2024-01-01"}]}}))
    (metadata / f"{batch}.json").write_text(json.dumps({"batch_id": batch, "source": "scopus", "status": "completed_target", "page_count": 2, "raw_page_dir": f"scopus/{batch}"}))
    summary = process_pending_batches(spark, bronze, silver)
    assert (summary.valid_rows, summary.rejected_rows) == (1, 1)
