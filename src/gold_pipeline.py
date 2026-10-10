"""Gold layer: dense topic x year table with the emergence label (`python -m src.gold_pipeline`)."""

from __future__ import annotations

import json
import os
import shutil
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.compaction import active_silver_paths
from src.gold_features import FEATURE_COLUMNS, add_features
from src.gold_rules import (
    FIRST_YEAR,
    GROWTH_THRESHOLD,
    HORIZON_YEARS,
    LAST_COMPLETE_YEAR,
    MIN_SUPPORT,
    eligible_column,
    emergence_label_column,
    growth_column,
)
from src.spark_pipeline import PARQUET_WRITE_OPTIONS, _write_json_atomic

GOLD_VERSION = 2
SOURCE = "openalex"
TOPIC_ASSIGNMENT = "primary_topic"
PREVALENCE_TOLERANCE = 1e-9
_TOPIC_ATTRIBUTES = ("topic_name", "subfield", "field", "domain")
_TABLES = ("year_totals", "topic_year")
_MANIFEST = "_manifest.json"


class GoldError(RuntimeError):
    """Raised when Gold inputs or outputs fail validation."""


@dataclass(frozen=True)
class GoldSummary:
    """Counts safe to print and compare across runs."""

    universe_rows: int
    topic_count: int
    rows: int
    labeled_rows: int
    positive_rows: int
    first_year: int
    last_year: int


def gold_universe(
    spark: SparkSession, silver_root: Path, *, first_year: int = FIRST_YEAR, last_year: int = LAST_COMPLETE_YEAR
) -> DataFrame:
    """Comparable OpenAlex publications, one row per publication with its primary topic."""
    paths = active_silver_paths(Path(silver_root))
    if not paths:
        raise GoldError("No published Silver data")
    silver = spark.read.parquet(*(str(path) for path in paths))
    topic = F.col("primary_topic")
    return silver.filter(
        (F.col("source") == SOURCE)
        & F.coalesce(F.col("is_eligible"), F.lit(False))
        & F.coalesce(F.col("matches_common_query"), F.lit(False))
        & F.col("publication_year").between(first_year, last_year)
        & topic["id"].isNotNull()
    ).select(
        topic["id"].alias("topic_id"),
        topic["name"].alias("topic_name"),
        topic["subfield"].alias("subfield"),
        topic["field"].alias("field"),
        topic["domain"].alias("domain"),
        "publication_year",
        "authors",
        "institutions",
        "country_codes",
        "is_oa",
        "work_type_norm",
        "source_id",
        "ingested_at",
    )


def _years(spark: SparkSession, first_year: int, last_year: int) -> DataFrame:
    return spark.range(first_year, last_year + 1).select(F.col("id").cast("int").alias("year"))


def year_totals(
    universe: DataFrame, plan_totals: dict[str, int], *, first_year: int = FIRST_YEAR, last_year: int = LAST_COMPLETE_YEAR
) -> DataFrame:
    """Universe size per year (the prevalence denominator) and its share of the plan's totals."""
    spark = universe.sparkSession
    counts = universe.groupBy(F.col("publication_year").alias("year")).agg(F.count(F.lit(1)).alias("n_total"))
    plan = spark.createDataFrame(
        [(int(year), int(total)) for year, total in plan_totals.items()], "year int, plan_total long"
    )
    return (
        _years(spark, first_year, last_year)
        .join(counts, "year", "left")
        .join(plan, "year", "left")
        .select(
            "year",
            F.coalesce("n_total", F.lit(0)).cast("long").alias("n_total"),
            "plan_total",
            F.when(F.col("plan_total") > 0, F.coalesce("n_total", F.lit(0)) / F.col("plan_total")).alias("sampling_fraction"),
        )
    )


def _topic_attributes(universe: DataFrame) -> DataFrame:
    """Most frequent name/subfield/field/domain per topic; ties resolved by value order."""
    counted = universe.groupBy("topic_id", *_TOPIC_ATTRIBUTES).agg(F.count(F.lit(1)).alias("_n"))
    rank = Window.partitionBy("topic_id").orderBy(F.desc("_n"), *(F.asc_nulls_last(name) for name in _TOPIC_ATTRIBUTES))
    return counted.withColumn("_rank", F.row_number().over(rank)).filter("_rank = 1").drop("_n", "_rank")


def topic_year_grid(
    universe: DataFrame,
    totals: DataFrame,
    *,
    first_year: int = FIRST_YEAR,
    last_year: int = LAST_COMPLETE_YEAR,
    horizon: int = HORIZON_YEARS,
    growth_threshold: float = GROWTH_THRESHOLD,
    min_support: int = MIN_SUPPORT,
) -> DataFrame:
    """Every topic seen x every year, with n_t (0 when absent), prevalence, FEATURE_COLUMNS and the label.

    `label_*` columns hold future values for audit only and must never be used as features.
    """
    counts = universe.groupBy("topic_id", F.col("publication_year").alias("year")).agg(F.count(F.lit(1)).alias("n_t"))
    grid = (
        _topic_attributes(universe)
        .crossJoin(_years(universe.sparkSession, first_year, last_year))
        .join(counts, ["topic_id", "year"], "left")
        .join(totals.select("year", "n_total"), "year", "left")
        .withColumn("n_t", F.coalesce("n_t", F.lit(0)).cast("long"))
        .withColumn("prevalence_t", F.when(F.col("n_total") > 0, F.col("n_t") / F.col("n_total")))
    )
    grid = add_features(grid, universe, first_year=first_year)
    # The grid is dense, so the k-th following row is exactly year t + k.
    ahead = Window.partitionBy("topic_id").orderBy("year")
    future = [F.lead("prevalence_t", step).over(ahead) for step in range(1, horizon + 1)]
    for step, column in enumerate(future, start=1):
        grid = grid.withColumn(f"label_prevalence_t{step}", column)
    path = [F.col("prevalence_t"), *(F.col(f"label_prevalence_t{step}") for step in range(1, horizon + 1))]
    rule = {"horizon": horizon, "min_support": min_support, "last_complete_year": last_year}
    eligible = eligible_column(F.col("n_t"), F.col("year"), **rule)
    return (
        grid.withColumn(f"label_growth_{horizon}y", growth_column(path[0], path[-1]))
        .withColumn("eligible_for_label", eligible)
        .withColumn("target_emerging", emergence_label_column(F.col("n_t"), path, F.col("year"), growth_threshold=growth_threshold, **rule))
        .withColumn("label_end_year", F.when(eligible, F.col("year") + horizon).cast("int"))
        .select("topic_id", "year", "topic_name", "n_total", *FEATURE_COLUMNS,
                *(name for name in grid.columns if name.startswith("label_")), f"label_growth_{horizon}y",
                "eligible_for_label", "target_emerging", "label_end_year")
    )


def validate_grid(grid: DataFrame, totals: DataFrame) -> None:
    """Raise unless (topic_id, year) is unique and prevalence sums to 1 in every non-empty year."""
    if grid.groupBy("topic_id", "year").count().filter("count > 1").limit(1).count():
        raise GoldError("topic_year has duplicate (topic_id, year) rows")
    sums = grid.groupBy("year").agg(F.sum("prevalence_t").alias("_sum"))
    bad = totals.filter("n_total > 0").join(sums, "year", "left").filter(
        F.col("_sum").isNull() | (F.abs(F.col("_sum") - 1) >= PREVALENCE_TOLERANCE)
    )
    if bad.limit(1).count():
        years = sorted(row["year"] for row in bad.select("year").collect())
        raise GoldError(f"prevalence does not sum to 1 in years {years}")


def _silver_schema_version(silver_root: Path) -> int | None:
    marker = Path(silver_root) / "_schema_version.json"
    return json.loads(marker.read_text(encoding="utf-8")).get("version") if marker.exists() else None


def _plan_totals(bronze_root: Path) -> dict[str, int]:
    plan = Path(bronze_root) / "plans" / f"{SOURCE}.json"
    if not plan.exists():
        raise GoldError(f"Missing ingestion plan {plan.name}")
    try:
        totals = json.loads(plan.read_text(encoding="utf-8"))["plan"]["totals"]
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise GoldError(f"Ingestion plan {plan.name} has no readable plan.totals") from error
    if not isinstance(totals, dict):
        raise GoldError(f"Ingestion plan {plan.name} has no readable plan.totals")
    return totals


def _write_parquet(frame: DataFrame, path: Path) -> None:
    frame.coalesce(1).write.mode("overwrite").options(**PARQUET_WRITE_OPTIONS).parquet(str(path))


def _publish(gold: Path, staging: Path, manifest: dict) -> None:
    """Swap the staged tables into place, then write the manifest last.

    The old manifest is moved away first, so a crash mid-swap leaves no manifest
    describing tables it does not match.
    """
    trash = gold / f"_trash-{uuid.uuid4().hex}"
    trash.mkdir()
    try:
        if (gold / _MANIFEST).exists():
            os.replace(gold / _MANIFEST, trash / _MANIFEST)
        for name in _TABLES:
            if (gold / name).exists():
                os.replace(gold / name, trash / name)
            os.replace(staging / name, gold / name)
        _write_json_atomic(gold / _MANIFEST, manifest)
    finally:
        shutil.rmtree(trash, ignore_errors=True)


def build_gold(
    spark: SparkSession,
    silver_root: Path,
    bronze_root: Path,
    gold_root: Path,
    *,
    first_year: int = FIRST_YEAR,
    last_year: int = LAST_COMPLETE_YEAR,
    horizon: int = HORIZON_YEARS,
    growth_threshold: float = GROWTH_THRESHOLD,
    min_support: int = MIN_SUPPORT,
) -> GoldSummary:
    """Write year_totals/, topic_year/ and _manifest.json (last) under gold_root.

    Tables are written to a staging directory and validated before they replace the
    previous Gold, so a failure before the swap leaves the previous Gold untouched.
    """
    gold = Path(gold_root)
    years = {"first_year": first_year, "last_year": last_year}
    plan_totals = _plan_totals(bronze_root)
    cached: list[DataFrame] = []
    staging = gold / f"_staging-{uuid.uuid4().hex}"
    try:
        universe = gold_universe(spark, silver_root, **years).cache()
        cached.append(universe)
        if universe.limit(1).count() == 0:
            raise GoldError("Gold universe is empty: no comparable OpenAlex publications in Silver")
        totals = year_totals(universe, plan_totals, **years).cache()
        cached.append(totals)
        grid = topic_year_grid(
            universe, totals, **years, horizon=horizon, growth_threshold=growth_threshold, min_support=min_support
        ).cache()
        cached.append(grid)
        validate_grid(grid, totals)
        stats = grid.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("topic_id").alias("topics"),
            F.count("target_emerging").alias("labeled"),
            F.coalesce(F.sum("target_emerging"), F.lit(0)).alias("positive"),
        ).first()
        snapshot = universe.agg(F.min("ingested_at").alias("min"), F.max("ingested_at").alias("max")).first()
        summary = GoldSummary(
            universe_rows=universe.count(), topic_count=stats["topics"], rows=stats["rows"],
            labeled_rows=stats["labeled"], positive_rows=int(stats["positive"]), **years,
        )
        if summary.rows != summary.topic_count * (last_year - first_year + 1):
            raise GoldError("topic_year is not a dense topic x year grid")
        staging.mkdir(parents=True)
        _write_parquet(totals, staging / "year_totals")
        _write_parquet(grid, staging / "topic_year")
        staged_rows = spark.read.parquet(str(staging / "topic_year")).count()
        staged_years = spark.read.parquet(str(staging / "year_totals")).count()
        if (staged_rows, staged_years) != (summary.rows, last_year - first_year + 1):
            raise GoldError("Staged Gold tables do not match the computed summary")
        _publish(gold, staging, {
            "gold_version": GOLD_VERSION,
            "silver_schema_version": _silver_schema_version(silver_root),
            **years,
            "rule": {"horizon_years": horizon, "growth_threshold": growth_threshold,
                     "min_support": min_support, "last_complete_year": last_year},
            "topic_assignment": TOPIC_ASSIGNMENT,
            "source": SOURCE,
            "universe_rows": summary.universe_rows,
            "topic_count": summary.topic_count,
            "rows": summary.rows,
            "labeled_rows": summary.labeled_rows,
            "positive_rows": summary.positive_rows,
            "taxonomy_snapshot": {"ingested_at_min": snapshot["min"], "ingested_at_max": snapshot["max"]},
            "feature_columns": list(FEATURE_COLUMNS),
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
        return summary
    finally:
        shutil.rmtree(staging, ignore_errors=True)
        for frame in cached:
            frame.unpersist()


def main() -> None:
    spark = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName("silver-gold")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        summary = build_gold(
            spark,
            Path(os.environ.get("SILVER_PATH", "/data/silver")),
            Path(os.environ.get("BRONZE_PATH", "/data/bronze")),
            Path(os.environ.get("GOLD_PATH", "/data/gold")),
        )
    finally:
        spark.stop()
    print(json.dumps({"event": "gold_complete", **asdict(summary)}, sort_keys=True))


if __name__ == "__main__":
    main()
