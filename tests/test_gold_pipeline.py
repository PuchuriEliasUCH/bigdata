import json

import pytest
from pyspark.sql import functions as F

import src.gold_pipeline as gold_pipeline
from src.gold_features import FEATURE_COLUMNS
from src.gold_pipeline import GoldError, build_gold, gold_universe, topic_year_grid, validate_grid, year_totals
from src.gold_rules import emergence_label

_SCHEMA = (
    "source string, source_id string, ingested_at string, publication_year int, is_eligible boolean, "
    "matches_common_query boolean, work_type_norm string, is_oa boolean, "
    "primary_topic struct<id:string,name:string,score:double,subfield:string,field:string,domain:string>, "
    "authors array<struct<id:string,name:string,orcid:string,position:string,is_corresponding:boolean>>, "
    "institutions array<struct<id:string,name:string,ror:string,country_code:string,country_name:string,city:string,type:string>>, "
    "country_codes array<string>"
)
YEARS = list(range(2011, 2026))


def _topic(topic_id):
    return (topic_id, f"Topic {topic_id}", 0.9, f"Sub {topic_id}", "Field", "Domain")


def _row(index, year, topic_id, source="openalex", eligible=True, common=True, topic=None):
    topic = topic or (_topic(topic_id) if topic_id else None)
    return (source, f"W{index}", f"2026-10-0{1 + index % 3}T00:00:00Z", year, eligible, common, "article", True,
            topic, [(f"A{index}", "n", None, "first", True)], [], ["US"])


def _counts():
    """Topic T1 doubles its share every year; T2 is constant; T3 appears only in 2013."""
    counts = {}
    for year in YEARS:
        counts[(year, "T1")] = 10 * 2 ** min(year - 2011, 3)
        counts[(year, "T2")] = 40
    counts[(2013, "T3")] = 5
    return counts


def _write_silver(spark, root):
    rows, index = [], 0
    for (year, topic), n in _counts().items():
        for _ in range(n):
            rows.append(_row(index, year, topic))
            index += 1
    noise = [
        _row(10**6, 2015, "T9", source="scopus"),
        _row(10**6 + 1, 2015, "T9", eligible=False),
        _row(10**6 + 2, 2015, "T9", common=False),
        _row(10**6 + 3, 2015, None),
        _row(10**6 + 4, 2010, "T9"),
        _row(10**6 + 5, 2026, "T9"),
    ]
    half = len(rows) // 2
    spark.createDataFrame(rows[:half] + noise, _SCHEMA).write.parquet(str(root / "works" / "one"))
    spark.createDataFrame(rows[half:], _SCHEMA).write.parquet(str(root / "works" / "two"))
    (root / "_schema_version.json").write_text(json.dumps({"version": 4}))


def _write_plan(bronze):
    (bronze / "plans").mkdir(parents=True)
    plan = {"key": {}, "plan": {"totals": {str(year): 1000 for year in YEARS + [2026]}}}
    (bronze / "plans" / "openalex.json").write_text(json.dumps(plan))


@pytest.fixture
def layout(spark, tmp_path):
    silver, bronze = tmp_path / "silver", tmp_path / "bronze"
    _write_silver(spark, silver)
    _write_plan(bronze)
    return silver, bronze, tmp_path / "gold"


def test_universe_keeps_only_comparable_openalex_rows(spark, layout):
    universe = gold_universe(spark, layout[0])
    assert universe.count() == sum(_counts().values())
    assert universe.filter(F.col("topic_id") == "T9").count() == 0
    assert {"topic_id", "topic_name", "subfield", "field", "domain", "publication_year", "authors",
            "institutions", "country_codes", "is_oa", "work_type_norm", "source_id", "ingested_at"} <= set(universe.columns)


def test_year_totals_use_universe_size_and_plan(spark, layout):
    totals = {row["year"]: row for row in year_totals(gold_universe(spark, layout[0]), {"2011": 100}).collect()}
    assert sorted(totals) == YEARS
    assert totals[2011]["n_total"] == 50
    assert totals[2011]["sampling_fraction"] == pytest.approx(0.5)
    assert totals[2012]["sampling_fraction"] is None


def test_grid_is_dense_with_zero_fill_and_label_rules(spark, layout):
    universe = gold_universe(spark, layout[0])
    grid = topic_year_grid(universe, year_totals(universe, {}))
    rows = {(row["topic_id"], row["year"]): row for row in grid.collect()}
    assert len(rows) == grid.count() == 3 * len(YEARS)
    t3 = rows[("T3", 2012)]
    assert t3["n_t"] == 0 and t3["prevalence_t"] == 0.0 and t3["subfield"] == "Sub T3" and t3["domain"] == "Domain"
    # T3 has fewer than 10 publications: never labeled.
    assert rows[("T3", 2013)]["target_emerging"] is None and rows[("T3", 2013)]["eligible_for_label"] is False
    # T1 grows 2x per year until 2014, then levels off.
    assert rows[("T1", 2011)]["target_emerging"] == 1
    assert rows[("T1", 2011)]["label_end_year"] == 2013
    assert rows[("T1", 2015)]["target_emerging"] == 0
    assert rows[("T2", 2023)]["target_emerging"] == 0 and rows[("T2", 2023)]["label_end_year"] == 2025
    for year in (2024, 2025):
        assert rows[("T2", year)]["target_emerging"] is None and rows[("T2", year)]["label_end_year"] is None
    sums = grid.groupBy("year").agg(F.sum("prevalence_t").alias("total")).collect()
    assert all(abs(row["total"] - 1) < 1e-9 for row in sums)


def test_spark_label_matches_pure_rule(spark, layout):
    universe = gold_universe(spark, layout[0])
    params = {"growth_threshold": 0.3, "min_support": 5}
    for row in topic_year_grid(universe, year_totals(universe, {}), **params).collect():
        path = [row["prevalence_t"], row["label_prevalence_t1"], row["label_prevalence_t2"]]
        assert row["target_emerging"] == emergence_label(row["n_t"], path, row["year"], **params)


def test_validation_rejects_duplicates_and_bad_prevalence(spark, layout):
    universe = gold_universe(spark, layout[0])
    totals = year_totals(universe, {})
    grid = topic_year_grid(universe, totals)
    with pytest.raises(GoldError, match="duplicate"):
        validate_grid(grid.unionByName(grid.limit(1)), totals)
    with pytest.raises(GoldError, match="prevalence"):
        validate_grid(grid.withColumn("prevalence_t", F.col("prevalence_t") * 2), totals)


def test_build_gold_writes_tables_and_manifest(spark, layout):
    silver, bronze, gold = layout
    summary = build_gold(spark, silver, bronze, gold)
    assert (summary.topic_count, summary.rows) == (3, 3 * len(YEARS))
    topic_year = spark.read.parquet(str(gold / "topic_year"))
    assert topic_year.count() == summary.rows
    assert spark.read.parquet(str(gold / "year_totals")).count() == len(YEARS)
    manifest = json.loads((gold / "_manifest.json").read_text())
    assert manifest["silver_schema_version"] == 4 and manifest["gold_version"] == 2
    assert manifest["feature_columns"] == list(FEATURE_COLUMNS)
    assert (manifest["first_year"], manifest["last_year"]) == (2011, 2025)
    assert manifest["rule"] == {"horizon_years": 2, "growth_threshold": 0.5, "min_support": 10, "last_complete_year": 2025}
    assert manifest["topic_assignment"] == "primary_topic" and manifest["source"] == "openalex"
    assert manifest["topic_count"] == 3 and manifest["rows"] == summary.rows
    assert manifest["labeled_rows"] == topic_year.filter(F.col("target_emerging").isNotNull()).count() == summary.labeled_rows
    assert manifest["positive_rows"] == topic_year.filter(F.col("target_emerging") == 1).count() == summary.positive_rows
    assert manifest["taxonomy_snapshot"] == {"ingested_at_min": "2026-10-01T00:00:00Z", "ingested_at_max": "2026-10-03T00:00:00Z"}
    assert manifest["created_at"]
    # Overwrite is idempotent.
    assert build_gold(spark, silver, bronze, gold) == summary


def test_topic_attributes_take_majority_then_value_order(spark, tmp_path):
    rows = [
        # T1: subfield "Sub X" 3 times vs "Sub Y" once -> majority.
        *(_row(i, 2011 + i % 2, "T1", topic=("T1", "One", 0.9, "Sub X", "F", "D")) for i in range(3)),
        _row(3, 2012, "T1", topic=("T1", "One", 0.9, "Sub Y", "F", "D")),
        # T2: a 2-2 tie between "Sub B" and "Sub A" -> value order picks "Sub A".
        *(_row(10 + i, 2011, "T2", topic=("T2", "Two", 0.9, "Sub B", "F", "D")) for i in range(2)),
        *(_row(20 + i, 2013, "T2", topic=("T2", "Two", 0.9, "Sub A", "F", "D")) for i in range(2)),
    ]
    spark.createDataFrame(rows, _SCHEMA).write.parquet(str(tmp_path / "silver" / "works" / "one"))
    universe = gold_universe(spark, tmp_path / "silver")
    grid = topic_year_grid(universe, year_totals(universe, {}))
    assert grid.count() == 2 * len(YEARS)
    assert grid.groupBy("topic_id", "year").count().filter("count > 1").count() == 0
    subfields = {row["topic_id"]: row["subfield"] for row in grid.select("topic_id", "subfield").distinct().collect()}
    assert subfields == {"T1": "Sub X", "T2": "Sub A"}


def test_missing_or_malformed_plan_raises_gold_error(spark, layout):
    silver, bronze, gold = layout
    plan = bronze / "plans" / "openalex.json"
    plan.write_text(json.dumps({"key": {}, "plan": {}}))
    with pytest.raises(GoldError, match="totals"):
        build_gold(spark, silver, bronze, gold)
    plan.write_text("{not json")
    with pytest.raises(GoldError):
        build_gold(spark, silver, bronze, gold)
    plan.unlink()
    with pytest.raises(GoldError, match="plan"):
        build_gold(spark, silver, bronze, gold)
    assert not (gold / "_manifest.json").exists()


def test_empty_silver_raises_gold_error(spark, tmp_path):
    bronze = tmp_path / "bronze"
    _write_plan(bronze)
    with pytest.raises(GoldError):
        build_gold(spark, tmp_path / "missing", bronze, tmp_path / "gold")
    only_noise = [_row(1, 2015, "T9", source="scopus"), _row(2, 2010, "T9")]
    spark.createDataFrame(only_noise, _SCHEMA).write.parquet(str(tmp_path / "silver" / "works" / "one"))
    with pytest.raises(GoldError, match="empty"):
        build_gold(spark, tmp_path / "silver", bronze, tmp_path / "gold")


def test_failed_write_keeps_previous_gold_consistent(spark, layout, monkeypatch):
    silver, bronze, gold = layout
    previous = build_gold(spark, silver, bronze, gold)
    manifest = (gold / "_manifest.json").read_text()
    original = gold_pipeline._write_parquet

    def failing(frame, path):
        if path.name == "topic_year":
            raise RuntimeError("disk full")
        original(frame, path)

    monkeypatch.setattr(gold_pipeline, "_write_parquet", failing)
    with pytest.raises(RuntimeError, match="disk full"):
        build_gold(spark, silver, bronze, gold, min_support=5)
    assert (gold / "_manifest.json").read_text() == manifest
    topic_year = spark.read.parquet(str(gold / "topic_year"))
    assert topic_year.count() == previous.rows
    assert topic_year.filter(F.col("target_emerging").isNotNull()).count() == previous.labeled_rows
    assert spark.read.parquet(str(gold / "year_totals")).count() == len(YEARS)
    assert sorted(path.name for path in gold.iterdir()) == ["_manifest.json", "topic_year", "year_totals"]
    monkeypatch.undo()
    assert build_gold(spark, silver, bronze, gold, min_support=5).labeled_rows > previous.labeled_rows
    assert sorted(path.name for path in gold.iterdir()) == ["_manifest.json", "topic_year", "year_totals"]
