"""Build the Gold layer: one deduplicated works table and a per-year summary, all Parquet."""

from __future__ import annotations

import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.compaction import active_silver_paths
from src.spark_pipeline import _processing_lock, unique_publications

WORK_COLUMNS = (
    ("batch_id", "string"),
    ("openalex_id", "string"),
    ("source", "string"),
    ("source_id", "string"),
    ("title", "string"),
    ("publication_year", "int"),
    ("publication_date", "string"),
    ("topic_ids", "array<string>"),
    ("topic_names", "array<string>"),
    ("topic_count", "int"),
    ("work_type", "string"),
    ("doi", "string"),
    ("normalized_doi", "string"),
    ("cited_by_count", "long"),
    ("is_oa", "boolean"),
    ("source_name", "string"),
    ("processed_at", "string"),
)


@dataclass(frozen=True)
class GoldSummary:
    """Counts safe to print in scheduler logs."""

    work_rows: int
    summary_rows: int


def _conform(frame: DataFrame) -> DataFrame:
    """Give historic Silver batches the current columns so every batch can be unioned."""
    columns = set(frame.columns)
    expressions = {
        "source": F.lit("openalex"),
        "source_id": F.col("openalex_id"),
        "normalized_doi": F.regexp_replace(
            F.lower(F.trim(F.col("doi"))), r"^(https?://(dx\.)?doi\.org/|doi:)", ""
        ),
    }
    selected = []
    for name, data_type in WORK_COLUMNS:
        if name in columns:
            selected.append(F.col(name).cast(data_type).alias(name))
        else:
            fallback = expressions.get(name, F.lit(None))
            selected.append(fallback.cast(data_type).alias(name))
    return frame.select(*selected)


def _publish(frame: DataFrame, root: Path, name: str) -> None:
    """Write to a temporary directory, then swap it in so readers never see a partial table."""
    staging = root / f".staging-{name}-{uuid.uuid4().hex}"
    frame.write.mode("overwrite").option("compression", "snappy").parquet(str(staging))
    target = root / name
    retired = root / f".retired-{name}-{uuid.uuid4().hex}"
    if target.exists():
        target.rename(retired)
    staging.rename(target)
    shutil.rmtree(retired, ignore_errors=True)


def build_gold(spark: SparkSession, silver_root: str | Path, gold_root: str | Path) -> GoldSummary:
    silver_paths = [path for path in active_silver_paths(Path(silver_root)) if path.exists()]
    if not silver_paths:
        return GoldSummary(0, 0)
    gold_path = Path(gold_root)
    with _processing_lock(gold_path):
        frames = [_conform(spark.read.parquet(str(path))) for path in silver_paths]
        combined = frames[0]
        for other in frames[1:]:
            combined = combined.unionByName(other)
        works = unique_publications(combined).cache()
        try:
            summary = works.groupBy("publication_year", "source").agg(
                F.count(F.lit(1)).alias("publications"),
                F.coalesce(F.sum("cited_by_count"), F.lit(0)).alias("total_citations"),
                F.count(F.when(F.col("is_oa"), 1)).alias("open_access_publications"),
            )
            _publish(works, gold_path, "works")
            _publish(summary, gold_path, "works_by_year_source")
            work_rows = spark.read.parquet(str(gold_path / "works")).count()
            summary_rows = spark.read.parquet(str(gold_path / "works_by_year_source")).count()
        finally:
            works.unpersist()
    return GoldSummary(work_rows, summary_rows)
