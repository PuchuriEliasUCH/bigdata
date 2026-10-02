from pathlib import Path

from src.gold import build_gold

SILVER_DDL = (
    "batch_id string, openalex_id string, source string, source_id string, title string, "
    "publication_year int, publication_date string, topic_ids array<string>, topic_names array<string>, "
    "topic_count int, work_type string, doi string, normalized_doi string, cited_by_count long, "
    "is_oa boolean, source_name string, processed_at string"
)


def row(source, source_id, doi=None, year=2024, cites=1, oa=None, batch="b"):
    normalized = doi.lower() if doi else None
    return (batch, source_id, source, source_id, f"Title {source_id}", year, f"{year}-01-01", [], [], 0,
            "article", doi, normalized, cites, oa, "Venue", "2026-01-01T00:00:00+00:00")


def write_silver(spark, silver_root: Path, batch: str, rows):
    spark.createDataFrame(rows, SILVER_DDL).write.mode("overwrite").parquet(str(silver_root / "works" / batch))


def test_gold_prefers_openalex_for_shared_doi_and_keeps_doi_less_rows(spark, tmp_path):
    silver, gold = tmp_path / "silver", tmp_path / "gold"
    write_silver(spark, silver, "oa", [row("openalex", "W1", "10.1/x", cites=5, oa=True), row("openalex", "W2")])
    write_silver(spark, silver, "sc", [row("scopus", "S1", "10.1/x", cites=9), row("scopus", "S2"), row("scopus", "S3", year=2023)])
    summary = build_gold(spark, silver, gold)
    works = {r["source_id"]: r for r in spark.read.parquet(str(gold / "works")).collect()}
    assert set(works) == {"W1", "W2", "S2", "S3"}
    assert works["W1"]["source"] == "openalex"
    assert summary.work_rows == 4


def test_gold_aggregates_by_year_and_source(spark, tmp_path):
    silver, gold = tmp_path / "silver", tmp_path / "gold"
    write_silver(spark, silver, "oa", [row("openalex", "W1", cites=5, oa=True), row("openalex", "W2", cites=3, oa=False)])
    write_silver(spark, silver, "sc", [row("scopus", "S1", year=2023, cites=2)])
    build_gold(spark, silver, gold)
    aggregate = {(r["publication_year"], r["source"]): r for r in spark.read.parquet(str(gold / "works_by_year_source")).collect()}
    assert aggregate[(2024, "openalex")]["publications"] == 2
    assert aggregate[(2024, "openalex")]["total_citations"] == 8
    assert aggregate[(2024, "openalex")]["open_access_publications"] == 1
    assert aggregate[(2023, "scopus")]["publications"] == 1


def test_gold_reads_legacy_silver_without_source_columns(spark, tmp_path):
    silver, gold = tmp_path / "silver", tmp_path / "gold"
    legacy = spark.createDataFrame([("old", "W9", "Old", 2020, "2020-01-01", [], [], 0, "article", "10.9/z", 1, None)],
        "batch_id string, openalex_id string, title string, publication_year int, publication_date string, "
        "topic_ids array<string>, topic_names array<string>, topic_count int, work_type string, doi string, "
        "cited_by_count long, is_oa boolean")
    legacy.write.parquet(str(silver / "works" / "old"))
    build_gold(spark, silver, gold)
    only = spark.read.parquet(str(gold / "works")).collect()[0]
    assert (only["source"], only["source_id"], only["normalized_doi"]) == ("openalex", "W9", "10.9/z")


def test_gold_rebuild_replaces_previous_output_without_leftovers(spark, tmp_path):
    silver, gold = tmp_path / "silver", tmp_path / "gold"
    write_silver(spark, silver, "oa", [row("openalex", "W1")])
    build_gold(spark, silver, gold)
    write_silver(spark, silver, "oa2", [row("openalex", "W2")])
    summary = build_gold(spark, silver, gold)
    assert summary.work_rows == 2
    assert sorted(p.name for p in gold.iterdir() if not p.name.startswith(".processing")) == ["works", "works_by_year_source"]
    assert not list(gold.rglob("*.json"))


def test_gold_without_silver_data_writes_nothing(spark, tmp_path):
    summary = build_gold(spark, tmp_path / "silver", tmp_path / "gold")
    assert summary.work_rows == 0
    assert not (tmp_path / "gold" / "works").exists()
