import json
import time
from pathlib import Path

import pytest

from src.spark_pipeline import SILVER_SCHEMA_VERSION, ProcessingError, process_pending_batches


def _bronze_batch(bronze: Path, batch: str, ids):
    raw = bronze / "openalex" / batch
    raw.mkdir(parents=True)
    records = [{"id": identifier, "title": f"Title {identifier}", "publication_year": 2024,
                "publication_date": "2024-01-01", "topics": [{"id": "T", "display_name": "x"}]} for identifier in ids]
    (raw / "page-000001.json").write_text(json.dumps({"results": records}))
    (bronze / "batch_metadata").mkdir(parents=True, exist_ok=True)
    (bronze / "batch_metadata" / f"{batch}.json").write_text(json.dumps({
        "batch_id": batch, "source": "openalex", "status": "completed_target", "page_count": 1,
        "record_count": len(records), "raw_page_dir": f"openalex/{batch}", "created_at": batch}))


def _marker(silver: Path):
    return json.loads((silver / "_schema_version.json").read_text())["version"]


def test_a_fresh_silver_is_stamped_with_the_current_schema_version(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _bronze_batch(bronze, "b1", ["W1"])
    summary = process_pending_batches(spark, bronze, silver)
    assert _marker(silver) == SILVER_SCHEMA_VERSION == summary.schema_version
    assert summary.rebuilt is False and summary.processed_batches == ("b1",)


def test_same_version_does_not_rebuild(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _bronze_batch(bronze, "b1", ["W1"])
    process_pending_batches(spark, bronze, silver)
    again = process_pending_batches(spark, bronze, silver)
    assert again.rebuilt is False and again.skipped_batches == ("b1",) and again.processed_batches == ()


def test_an_older_schema_is_rebuilt_from_bronze_without_manual_steps(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _bronze_batch(bronze, "b1", ["W1", "W2"])
    process_pending_batches(spark, bronze, silver)
    (silver / "_schema_version.json").write_text(json.dumps({"version": SILVER_SCHEMA_VERSION - 1}))
    (silver / "works" / "b1" / "stale-output.txt").write_text("written by the old schema")
    rebuilt = process_pending_batches(spark, bronze, silver)
    assert rebuilt.rebuilt is True and rebuilt.processed_batches == ("b1",)
    assert not (silver / "works" / "b1" / "stale-output.txt").exists()
    assert spark.read.parquet(str(silver / "works" / "b1")).count() == 2
    assert _marker(silver) == SILVER_SCHEMA_VERSION


def test_silver_without_a_marker_is_treated_as_an_older_schema(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _bronze_batch(bronze, "b1", ["W1"])
    process_pending_batches(spark, bronze, silver)
    (silver / "_schema_version.json").unlink()
    assert process_pending_batches(spark, bronze, silver).rebuilt is True


def test_rebuild_refuses_to_delete_silver_batches_whose_bronze_is_gone(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _bronze_batch(bronze, "b1", ["W1"])
    process_pending_batches(spark, bronze, silver)
    (silver / "works" / "ghost").mkdir()
    (silver / "_schema_version.json").write_text(json.dumps({"version": SILVER_SCHEMA_VERSION - 1}))
    with pytest.raises(ProcessingError, match="Bronze"):
        process_pending_batches(spark, bronze, silver)
    assert (silver / "works" / "ghost").is_dir() and (silver / "works" / "b1").is_dir()


def test_an_expired_deadline_still_makes_progress_and_reports_what_is_pending(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    for name in ("b1", "b2", "b3"):
        _bronze_batch(bronze, name, [f"W-{name}"])
    first = process_pending_batches(spark, bronze, silver, deadline=time.monotonic() - 1)
    assert len(first.processed_batches) == 1 and first.pending_batches == 2
    second = process_pending_batches(spark, bronze, silver)
    assert len(second.processed_batches) == 2 and second.pending_batches == 0


def test_a_batch_processed_after_its_neighbours_were_compacted_still_dedups_against_them(spark, tmp_path):
    from src.compaction import compact_batches, prune_compacted_originals

    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _bronze_batch(bronze, "b1", ["W1"])
    _bronze_batch(bronze, "b3", ["W3"])
    process_pending_batches(spark, bronze, silver)
    # The group holds b1 and b3, so b2 (created in between) is processed after its originals are pruned.
    compact_batches(spark, silver, ["b1", "b3"], group="openalex-2024-0001")
    prune_compacted_originals(spark, silver, "openalex-2024-0001")
    _bronze_batch(bronze, "b2", ["W2", "W1"])
    summary = process_pending_batches(spark, bronze, silver)
    assert summary.processed_batches == ("b2",)
    kept = [row["source_id"] for row in spark.read.parquet(str(silver / "works" / "b2")).collect()]
    rejected = spark.read.parquet(str(silver / "rejected" / "b2")).collect()
    assert kept == ["W2"]
    assert [row["rejection_reason"] for row in rejected] == ["duplicate_source_id_previous_batch"]
