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
