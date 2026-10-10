import json
from pathlib import Path

import pytest

from src.spark_pipeline import _terminal_metadata, process_pending_batches


def write_metadata(bronze_root: Path, batch_id: str, status: str, *, source: str | None = None, page_count: int = 1):
    metadata_dir = bronze_root / "batch_metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    record = {"batch_id": batch_id, "status": status, "page_count": page_count, "created_at": batch_id}
    if source:
        record["source"] = source
    (metadata_dir / f"{batch_id}.json").write_text(json.dumps(record), encoding="utf-8")


@pytest.mark.parametrize(
    "source, status",
    [
        (None, "completed"),
        ("openalex", "completed_target"),
        ("openalex", "quota_exceeded"),
        ("openalex", "failed"),
        ("scopus", "completed_exhausted"),
        ("scopus", "result_window_reached"),
        ("scopus", "failed"),
    ],
)
def test_terminal_batches_with_pages_reach_silver(tmp_path, source, status):
    write_metadata(tmp_path, "b1", status, source=source)
    assert [record["batch_id"] for record in _terminal_metadata(tmp_path)] == ["b1"]


@pytest.mark.parametrize(
    "source, status",
    [
        ("openalex", "running"),
        ("openalex", "rate_limited"),
        ("openalex", "transient_error"),
        ("openalex", "time_budget_reached"),
        # Scopus resumes a quota_exceeded batch, so it may still grow.
        ("scopus", "quota_exceeded"),
        ("scopus", "rate_limited"),
        ("scopus", "time_budget_reached"),
    ],
)
def test_resumable_batches_wait_for_silver(tmp_path, source, status):
    write_metadata(tmp_path, "b1", status, source=source)
    assert _terminal_metadata(tmp_path) == []


def test_terminal_batch_without_pages_is_skipped(tmp_path):
    write_metadata(tmp_path, "b1", "quota_exceeded", source="openalex", page_count=0)
    assert _terminal_metadata(tmp_path) == []


def test_quota_exceeded_openalex_batch_is_processed(spark, tmp_path):
    bronze_root = tmp_path / "bronze"
    raw_dir = bronze_root / "openalex" / "stranded"
    raw_dir.mkdir(parents=True)
    record = {"id": "https://openalex.org/W1", "title": "Stranded work", "publication_year": 2024,
              "topics": [{"display_name": "Recovery"}]}
    (raw_dir / "page-000001.json").write_text(json.dumps({"meta": {"next_cursor": "c"}, "results": [record]}), encoding="utf-8")
    write_metadata(bronze_root, "stranded", "quota_exceeded", source="openalex")
    metadata_path = bronze_root / "batch_metadata" / "stranded.json"
    metadata = json.loads(metadata_path.read_text())
    metadata.update(mode="initial", raw_page_dir="openalex/stranded", record_count=1)
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    summary = process_pending_batches(spark, bronze_root, tmp_path / "silver")

    assert summary.processed_batches == ("stranded",)
    assert summary.valid_rows == 1
