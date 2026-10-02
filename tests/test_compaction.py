import json

import pytest

from src.compaction import active_silver_paths, compact_batches, resolve_completed_paths, prune_compacted_originals


def test_compaction_is_idempotent_and_readers_see_one_version(spark, tmp_path):
    root = tmp_path / "silver"
    for batch, id_value in (("one", "A"), ("two", "B")):
        spark.createDataFrame([(id_value, "openalex", 2024)], ["source_id", "source", "publication_year"]).write.parquet(str(root / "works" / batch))
    first = compact_batches(spark, root, ["one", "two"], group="openalex-2024")
    second = compact_batches(spark, root, ["one", "two"], group="openalex-2024")
    assert first == second
    active = active_silver_paths(root)
    assert active == [first]
    assert resolve_completed_paths(root, [root / "works" / "one", root / "works" / "two"]) == [first]
    assert spark.read.parquet(*(str(path) for path in active)).count() == 2


def test_failed_compaction_keeps_previous_publication(spark, tmp_path, monkeypatch):
    root = tmp_path / "silver"
    for batch in ("one", "two"):
        spark.createDataFrame([(batch, "openalex", 2024)], ["source_id", "source", "publication_year"]).write.parquet(str(root / "works" / batch))
    published = compact_batches(spark, root, ["one", "two"], group="openalex-2024")
    def fail(*args, **kwargs):
        raise OSError("publish failed")
    monkeypatch.setattr("src.compaction.os.replace", fail)
    with pytest.raises(OSError):
        compact_batches(spark, root, ["one", "two"], group="openalex-2024", force=True)
    assert active_silver_paths(root) == [published]


def test_prune_published_originals_reduces_retained_data_files(spark, tmp_path):
    root = tmp_path / "silver"
    for batch in ("one", "two"):
        spark.createDataFrame([(batch, "openalex")], ["source_id", "source"]).write.parquet(str(root / "works" / batch))
    published = compact_batches(spark, root, ["one", "two"], group="openalex-2024")
    assert prune_compacted_originals(spark, root, "openalex-2024") == 2
    assert not (root / "works" / "one").exists()
    assert active_silver_paths(root) == [published]
    assert spark.read.parquet(str(published)).count() == 2
    assert prune_compacted_originals(spark, root, "openalex-2024") == 0


# --- automatic, threshold-based compaction -------------------------------------------------

import re as _re

from src.compaction import active_rejected_paths, compact_pending

_DDL = "source string, source_id string, publication_year int"


def _silver_batch(spark, bronze, silver, batch, source, year, rows=2, rejected=0, state="completed"):
    base = f"title_and_abstract.search:(ai),publication_year:{year}" if source == "openalex" else f"(Q) AND PUBYEAR = {year}"
    key = "filter" if source == "openalex" else "base_query"
    (bronze / "batch_metadata").mkdir(parents=True, exist_ok=True)
    (bronze / "batch_metadata" / f"{batch}.json").write_text(json.dumps(
        {"batch_id": batch, "source": source, key: base, "status": "completed_target", "page_count": 1, "created_at": batch}))
    spark.createDataFrame([(source, f"{batch}-{i}", year) for i in range(rows)], _DDL).write.parquet(str(silver / "works" / batch))
    if rejected:
        spark.createDataFrame([(batch, f"r{i}", "missing_title") for i in range(rejected)], "batch_id string, source_id string, rejection_reason string").write.parquet(str(silver / "rejected" / batch))
    (silver / "_processing").mkdir(parents=True, exist_ok=True)
    (silver / "_processing" / f"{batch}.json").write_text(json.dumps({"batch_id": batch, "status": state}))


def _batches(spark, bronze, silver, source, year, count, **kw):
    names = [f"{source}-{year}-b{i:02d}" for i in range(count)]
    for name in names:
        _silver_batch(spark, bronze, silver, name, source, year, **kw)
    return names


def test_compaction_waits_for_the_batch_threshold(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _batches(spark, bronze, silver, "openalex", 2024, 3)
    summary = compact_pending(spark, bronze, silver, min_batches=4)
    assert summary.groups == () and len(active_silver_paths(silver)) == 3


def test_threshold_compacts_one_source_year_group_into_one_file_and_prunes_originals(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _batches(spark, bronze, silver, "openalex", 2024, 4)
    summary = compact_pending(spark, bronze, silver, min_batches=4)
    assert summary.groups == ("openalex-2024-0001",)
    (path,) = active_silver_paths(silver)
    assert len(list(path.glob("part-*.parquet"))) == 1
    assert spark.read.parquet(str(path)).count() == 8
    assert not list((silver / "works").glob("*"))


def test_groups_are_separated_by_source_and_year(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _batches(spark, bronze, silver, "openalex", 2024, 2)
    _batches(spark, bronze, silver, "openalex", 2025, 2)
    _batches(spark, bronze, silver, "scopus", 2024, 2)
    summary = compact_pending(spark, bronze, silver, min_batches=2)
    assert sorted(summary.groups) == ["openalex-2024-0001", "openalex-2025-0001", "scopus-2024-0001"]


def test_a_later_round_creates_a_new_group_and_leaves_the_first_untouched(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _batches(spark, bronze, silver, "openalex", 2024, 2)
    compact_pending(spark, bronze, silver, min_batches=2)
    (first,) = active_silver_paths(silver)
    for i in range(2):
        _silver_batch(spark, bronze, silver, f"openalex-2024-late{i}", "openalex", 2024)
    summary = compact_pending(spark, bronze, silver, min_batches=2)
    assert summary.groups == ("openalex-2024-0002",)
    assert first in active_silver_paths(silver) and len(active_silver_paths(silver)) == 2
    assert compact_pending(spark, bronze, silver, min_batches=2).groups == ()


def test_rejected_rows_are_compacted_with_their_group(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _batches(spark, bronze, silver, "openalex", 2024, 2, rejected=1)
    compact_pending(spark, bronze, silver, min_batches=2)
    (rejected,) = active_rejected_paths(silver)
    assert spark.read.parquet(str(rejected)).count() == 2
    assert not list((silver / "rejected").glob("*"))


def test_batches_that_did_not_finish_processing_are_not_compacted(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _batches(spark, bronze, silver, "openalex", 2024, 2)
    _silver_batch(spark, bronze, silver, "openalex-2024-broken", "openalex", 2024, state="running")
    compact_pending(spark, bronze, silver, min_batches=2)
    assert (silver / "works" / "openalex-2024-broken").is_dir()


def test_a_group_with_incompatible_schemas_is_skipped_without_stopping_the_run(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _batches(spark, bronze, silver, "openalex", 2024, 1)
    _silver_batch(spark, bronze, silver, "openalex-2024-odd", "openalex", 2024)
    spark.createDataFrame([("x", 1)], "id string, n int").write.mode("overwrite").parquet(str(silver / "works" / "openalex-2024-odd"))
    _batches(spark, bronze, silver, "scopus", 2024, 2)
    summary = compact_pending(spark, bronze, silver, min_batches=2)
    assert summary.groups == ("scopus-2024-0001",)
    assert summary.skipped == ("openalex-2024",)
