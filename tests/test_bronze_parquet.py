import json
from pathlib import Path

import pyarrow.parquet as pq

from src.openalex_ingest import BatchStateStore
from src.spark_pipeline import process_pending_batches


def _work(identifier, title="A title"):
    return {"id": identifier, "title": title, "publication_year": 2024, "publication_date": "2024-01-01",
            "topics": [{"id": "T1", "display_name": "Testing"}]}


def _metadata(bronze, batch, source, pages, files):
    (bronze / "batch_metadata").mkdir(parents=True, exist_ok=True)
    (bronze / "batch_metadata" / f"{batch}.json").write_text(json.dumps({
        "batch_id": batch, "source": source, "status": "completed_target", "page_count": pages,
        "raw_page_dir": f"{source}/{batch}", "raw_page_files": files, "created_at": "a"}))


def _codecs(path):
    files = [path] if path.is_file() else list(path.glob("*.parquet"))
    metas = [pq.ParquetFile(file).metadata for file in files]
    return {m.row_group(g).column(c).compression for m in metas for g in range(m.num_row_groups) for c in range(m.num_columns)}


def test_raw_pages_are_compressed_with_zstd(tmp_path):
    store = BatchStateStore(tmp_path, "openalex")
    path = store.write_raw_page("b1", 1, {"results": [_work("W1")]})
    assert _codecs(path) == {"ZSTD"}


def test_raw_pages_are_stored_as_parquet_with_one_row_per_record(tmp_path):
    store = BatchStateStore(tmp_path, "openalex")
    path = store.write_raw_page("b1", 2, {"meta": {"next_cursor": "c"}, "results": [_work("W1"), _work("W2")]})
    assert path.name == "page-000002.parquet"
    rows = pq.read_table(path).to_pylist()
    assert [(r["batch_id"], r["page_number"], r["record_index"]) for r in rows] == [("b1", 2, 0), ("b1", 2, 1)]
    assert json.loads(rows[1]["record_json"])["id"] == "W2"
    assert not list(path.parent.glob(".*.tmp"))


def test_scopus_and_empty_pages_use_the_same_parquet_schema(tmp_path):
    store = BatchStateStore(tmp_path, "scopus")
    scopus = store.write_raw_page("s1", 1, {"search-results": {"entry": [{"eid": "2-s2.0-1"}]}})
    empty = store.write_raw_page("s1", 2, {"meta": {}, "results": []})
    assert json.loads(pq.read_table(scopus).to_pylist()[0]["record_json"])["eid"] == "2-s2.0-1"
    assert pq.read_table(empty).num_rows == 0
    assert pq.read_table(empty).schema == pq.read_table(scopus).schema


def test_spark_reads_parquet_pages_and_writes_rejected_as_parquet(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    store = BatchStateStore(bronze, "openalex")
    page = store.write_raw_page("p1", 1, {"results": [_work("W1"), {"id": "W2", "title": ""}]})
    _metadata(bronze, "p1", "openalex", 1, [str(page.relative_to(bronze))])
    summary = process_pending_batches(spark, bronze, silver)
    assert summary.valid_rows == 1 and summary.rejected_rows == 1
    rejected = spark.read.parquet(str(silver / "rejected" / "p1")).collect()
    assert [row["source_id"] for row in rejected] == ["W2"]
    assert not list((silver / "rejected" / "p1").glob("part-*.json"))
    assert _codecs(silver / "works" / "p1") == {"ZSTD"}
    assert _codecs(silver / "rejected" / "p1") == {"ZSTD"}


def test_batch_with_legacy_json_page_and_new_parquet_page_keeps_record_order(spark, tmp_path):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    store = BatchStateStore(bronze, "openalex")
    raw = bronze / "openalex" / "m1"
    raw.mkdir(parents=True)
    (raw / "page-000001.json").write_text(json.dumps({"results": [_work("W1", "first")]}))
    page = store.write_raw_page("m1", 2, {"results": [_work("W1", "later duplicate"), _work("W3")]})
    _metadata(bronze, "m1", "openalex", 2, [str((raw / "page-000001.json").relative_to(bronze)), str(page.relative_to(bronze))])
    summary = process_pending_batches(spark, bronze, silver)
    assert summary.valid_rows == 2 and summary.rejected_rows == 1
    titles = {row["source_id"]: row["title"] for row in spark.read.parquet(str(silver / "works" / "m1")).collect()}
    assert titles == {"W1": "first", "W3": "A title"}
