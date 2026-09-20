import json
import os
from pathlib import Path

import pytest

from src.spark_pipeline import process_pending_batches


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("openalex-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


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

    valid_rows = spark.read.parquet(str(silver_root / "works" / "initial-test")).collect()
    assert len(valid_rows) == 1
    assert valid_rows[0]["openalex_id"] == "https://openalex.org/W1"
    assert valid_rows[0]["publication_year"] == 2024
    assert valid_rows[0]["topic_names"] == ["Data Engineering"]

    rejected_rows = spark.read.json(str(bronze_root / "rejected" / "initial-test")).collect()
    reasons = {row["rejection_reason"] for row in rejected_rows}
    assert len(rejected_rows) == 3
    assert "duplicate_openalex_id" in reasons
    assert any("missing_title" in reason for reason in reasons)
    assert any("missing_topics" in reason for reason in reasons)


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
    assert [row["openalex_id"] for row in first_batch_rows] == [
        "https://openalex.org/W20"
    ]
    second_batch_rows = spark.read.parquet(str(silver_root / "works" / "batch-002")).collect()
    assert [row["openalex_id"] for row in second_batch_rows] == [
        "https://openalex.org/W21"
    ]

    first_rejected = spark.read.json(str(bronze_root / "rejected" / "batch-001")).collect()
    assert len(first_rejected) == 1
    assert "missing_title" in first_rejected[0]["rejection_reason"]
    second_rejected = spark.read.json(str(bronze_root / "rejected" / "batch-002")).collect()
    assert len(second_rejected) == 1
    assert second_rejected[0]["openalex_id"] == "https://openalex.org/W20"
    assert second_rejected[0]["rejection_reason"] == "duplicate_openalex_id_previous_batch"
