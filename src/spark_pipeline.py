"""Spark validation, rejection, deduplication, and Silver Parquet output."""

from __future__ import annotations

import json
import math
import time
import os
import shutil
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# Bump when Silver's columns or rules change: the next run rebuilds Silver from Bronze.
SILVER_SCHEMA_VERSION = 2


class ProcessingError(RuntimeError):
    """Raised when a Bronze batch cannot be processed safely."""


@dataclass(frozen=True)
class ProcessingSummary:
    """Counts and batch identifiers safe to print in scheduler logs."""

    processed_batches: tuple[str, ...]
    skipped_batches: tuple[str, ...]
    valid_rows: int
    rejected_rows: int
    rebuilt: bool = False
    pending_batches: int = 0
    schema_version: int = SILVER_SCHEMA_VERSION


def output_partitions(rows: int, rows_per_file: int = 100_000) -> int:
    """Size output by measured batch volume, keeping small increments in one file."""
    if rows_per_file < 1:
        raise ValueError("rows_per_file must be positive")
    return max(1, math.ceil(rows / rows_per_file))


def unique_publications(records: DataFrame) -> DataFrame:
    """Analysis view: prefer OpenAlex for shared DOI; preserve every DOI-less source ID."""
    key = F.when(
        F.col("normalized_doi").isNotNull() & (F.trim("normalized_doi") != ""),
        F.concat(F.lit("doi:"), F.col("normalized_doi")),
    ).otherwise(F.concat_ws(":", F.lit("source"), F.col("source"), F.col("source_id")))
    window = Window.partitionBy(key).orderBy(F.col("source").asc(), F.col("source_id").asc())
    return records.withColumn("_rank", F.row_number().over(window)).where(F.col("_rank") == 1).drop("_rank")


RAW_SCHEMA = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("record_order", IntegerType(), False),
        StructField("record_json", StringType(), False),
        StructField("source_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("publication_year", IntegerType(), True),
        StructField("publication_date", StringType(), True),
        StructField("topic_ids", ArrayType(StringType(), containsNull=False), False),
        StructField("topic_names", ArrayType(StringType(), containsNull=False), False),
        StructField("work_type", StringType(), True),
        StructField("doi", StringType(), True),
        StructField("cited_by_count", LongType(), True),
        StructField("is_oa", BooleanType(), True),
        StructField("source_name", StringType(), True),
    ]
)


RAW_PAGE_SCHEMA = StructType(
    [
        # Parsing each result as JSON text avoids inferring the highly variable
        # OpenAlex record schema, including nested keys that differ by case.
        StructField("results", ArrayType(StringType(), containsNull=True), True),
    ]
)

SCOPUS_PAGE_SCHEMA = StructType([
    StructField("search-results", StructType([
        StructField("entry", ArrayType(StringType()), True),
    ]), True),
])

TOPIC_SCHEMA = ArrayType(
    StructType(
        [
            StructField("id", StringType(), True),
            StructField("display_name", StringType(), True),
            StructField("name", StringType(), True),
        ]
    ),
    containsNull=True,
)


def _raw_page_paths(bronze_root: Path, metadata: dict[str, Any]) -> list[Path]:
    raw_page_dir = Path(str(metadata.get("raw_page_dir", "")))
    if not raw_page_dir.is_absolute():
        raw_page_dir = bronze_root / raw_page_dir
    configured_files = metadata.get("raw_page_files")
    if isinstance(configured_files, list) and configured_files:
        paths = []
        for value in configured_files:
            path = Path(str(value))
            paths.append(path if path.is_absolute() else bronze_root / path)
        return sorted(paths)
    return sorted([*raw_page_dir.glob("page-*.json"), *raw_page_dir.glob("page-*.parquet")])


def _page_number_column() -> F.Column:
    return F.regexp_extract(F.col("_page_path"), r"page-(\d+)\.json$", 1).cast("int")


def _load_records(
    spark: SparkSession, batch_id: str, page_paths: list[Path], source: str
) -> DataFrame:
    """Return one row per record with its raw JSON text and a deterministic order.

    Parquet pages are the current Bronze format; JSON pages written before that
    stay readable, and a batch resumed across the change may contain both.
    """
    frames: list[DataFrame] = []
    parquet_paths = [path for path in page_paths if path.suffix == ".parquet"]
    json_paths = [path for path in page_paths if path.suffix != ".parquet"]
    if parquet_paths:
        frames.append(
            spark.read.parquet(*[str(path) for path in parquet_paths]).select(
                F.col("record_json").alias("_record_json"),
                ((F.col("page_number") - F.lit(1)) * F.lit(1000) + F.col("record_index"))
                .cast("int")
                .alias("record_order"),
            )
        )
    if json_paths:
        page_schema = SCOPUS_PAGE_SCHEMA if source == "scopus" else RAW_PAGE_SCHEMA
        pages = (
            spark.read.option("wholetext", "true")
            .text([str(path) for path in json_paths])
            .withColumn("_page_path", F.input_file_name())
            .withColumn("_page", F.from_json(F.col("value"), page_schema))
        )
        results = F.col("_page.`search-results`.entry") if source == "scopus" else F.col("_page.results")
        invalid_page = pages.filter(
            F.col("_page").isNull() | results.isNull()
        ).select("_page_path").first()
        if invalid_page is not None:
            page_name = str(invalid_page["_page_path"]).rsplit("/", 1)[-1]
            raise ProcessingError(f"Bronze page {page_name} has no results list")
        # Bronze caps each page below 1000 records, so page and index give a stable
        # order without globally sorting rows that still carry the raw JSON.
        frames.append(
            pages.select(
                "_page_path", F.posexplode(results).alias("_record_index", "_record_json")
            ).select(
                F.coalesce(F.col("_record_json"), F.lit("null")).alias("_record_json"),
                F.when(
                    _page_number_column() > 0,
                    (_page_number_column() - F.lit(1)) * F.lit(1000) + F.col("_record_index"),
                )
                .otherwise(F.col("_record_index"))
                .cast("int")
                .alias("record_order"),
            )
        )
    combined = frames[0]
    for other in frames[1:]:
        combined = combined.unionByName(other)
    return combined


def _load_batch_dataframe(
    spark: SparkSession, bronze_root: Path, metadata: dict[str, Any]
) -> DataFrame:
    batch_id = str(metadata["batch_id"])
    page_paths = _raw_page_paths(bronze_root, metadata)
    if not page_paths:
        raise ProcessingError(f"Bronze batch {batch_id} has no raw page files")
    for page_path in page_paths:
        if not page_path.exists():
            raise ProcessingError(f"Unable to read Bronze page {page_path.name}")

    source = metadata.get("source", "openalex")
    records = _load_records(spark, batch_id, page_paths, source)
    if source == "scopus":
        def field(name: str) -> F.Column:
            return F.trim(F.get_json_object(F.col("_record_json"), f"$.{name}"))

        date = field("prism:coverDate")
        return records.select(
            F.lit(batch_id).alias("batch_id"),
            "record_order",
            F.col("_record_json").alias("record_json"),
            field("eid").alias("source_id"),
            field("dc:title").alias("title"),
            F.regexp_extract(date, r"^(\d{4})", 1).cast("int").alias("publication_year"),
            date.alias("publication_date"),
            F.array().cast("array<string>").alias("topic_ids"),
            F.array().cast("array<string>").alias("topic_names"),
            field("subtypeDescription").alias("work_type"),
            field("prism:doi").alias("doi"),
            field("citedby-count").cast("long").alias("cited_by_count"),
            F.lit(None).cast("boolean").alias("is_oa"),
            field("prism:publicationName").alias("source_name"),
        )
    records = records.withColumn(
        "_topics",
        F.coalesce(
            F.from_json(F.get_json_object(F.col("_record_json"), "$.topics"), TOPIC_SCHEMA),
            F.array().cast(TOPIC_SCHEMA),
        ),
    )
    topic_ids = F.expr(
        "filter(transform(_topics, topic -> trim(topic.id)), "
        "topic -> topic is not null and topic <> '')"
    )
    topic_names = F.expr(
        "filter(transform(_topics, topic -> "
        "case when topic.display_name is not null and trim(topic.display_name) <> '' "
        "then trim(topic.display_name) else trim(topic.name) end), "
        "topic -> topic is not null and topic <> '')"
    )

    def json_value(path: str) -> F.Column:
        return F.get_json_object(F.col("_record_json"), path)

    publication_year_text = F.trim(json_value("$.publication_year"))
    cited_by_count_text = F.trim(json_value("$.cited_by_count"))
    is_oa_text = json_value("$.open_access.is_oa")
    return records.select(
        F.lit(batch_id).alias("batch_id"),
        "record_order",
        F.col("_record_json").alias("record_json"),
        F.trim(json_value("$.id")).alias("source_id"),
        F.trim(json_value("$.title")).alias("title"),
        F.when(
            publication_year_text.rlike(r"^-?\d+$"),
            publication_year_text.cast(IntegerType()),
        ).alias("publication_year"),
        F.trim(json_value("$.publication_date")).alias("publication_date"),
        topic_ids.alias("topic_ids"),
        topic_names.alias("topic_names"),
        F.trim(json_value("$.type")).alias("work_type"),
        F.trim(json_value("$.doi")).alias("doi"),
        F.when(
            cited_by_count_text.rlike(r"^-?\d+$"),
            cited_by_count_text.cast(LongType()),
        ).alias("cited_by_count"),
        F.when(is_oa_text.isin("true", "false"), is_oa_text.cast(BooleanType())).alias(
            "is_oa"
        ),
        F.trim(json_value("$.primary_location.source.display_name")).alias("source_name"),
    )


def _validation_reason(dataframe: DataFrame, source: str = "openalex") -> DataFrame:
    """Create auditable, composable reasons instead of dropping bad records."""

    reasons = F.array(
        F.when(
            F.col("source_id").isNull()
            | (F.length(F.trim(F.col("source_id"))) == 0),
            F.lit("missing_source_id"),
        ),
        F.when(
            F.col("title").isNull() | (F.length(F.trim(F.col("title"))) == 0),
            F.lit("missing_title"),
        ),
        F.when(
            F.col("publication_year").isNull()
            & (
                F.col("publication_date").isNull()
                | (F.length(F.trim(F.col("publication_date"))) == 0)
            ),
            F.lit("missing_publication_year_or_date"),
        ),
        F.when(
            (F.size(F.col("topic_ids")) + F.size(F.col("topic_names"))) == 0,
            F.lit("missing_topics"),
        ) if source == "openalex" else F.lit(None),
    )
    return dataframe.withColumn("rejection_reason", F.concat_ws("; ", reasons))


def _rejected_columns(dataframe: DataFrame) -> DataFrame:
    return dataframe.select(
        "batch_id",
        "record_order",
        "source_id",
        "record_json",
        "rejection_reason",
    )


def _silver_columns(dataframe: DataFrame, processed_at: str, source: str = "openalex") -> DataFrame:
    publication_year = F.when(
        F.col("publication_year").isNotNull(), F.col("publication_year")
    ).otherwise(F.regexp_extract(F.col("publication_date"), r"^(\d{4})", 1).cast("int"))
    return dataframe.select(
        "batch_id",
        F.lit(source).alias("source"),
        "source_id",
        "title",
        publication_year.alias("publication_year"),
        "publication_date",
        "topic_ids",
        "topic_names",
        (F.size(F.col("topic_ids")) + F.size(F.col("topic_names"))).alias("topic_count"),
        "work_type",
        "doi",
        F.regexp_replace(F.lower(F.trim(F.col("doi"))), r"^(https?://(dx\.)?doi\.org/|doi:)", "").alias("normalized_doi"),
        "cited_by_count",
        "is_oa",
        "source_name",
        F.lit(processed_at).alias("processed_at"),
    )


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def _processing_state_path(silver_root: Path, batch_id: str) -> Path:
    return silver_root / "_processing" / f"{batch_id}.json"


@contextmanager
def _processing_lock(silver_root: Path) -> Iterator[None]:
    import fcntl

    silver_root.mkdir(parents=True, exist_ok=True)
    lock_path = silver_root / ".processing.lock"
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _completed_metadata(bronze_root: Path) -> list[dict[str, Any]]:
    metadata_dir = bronze_root / "batch_metadata"
    if not metadata_dir.exists():
        return []
    records = []
    for path in sorted(metadata_dir.glob("*.json")):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ProcessingError(f"Unable to read batch metadata {path.name}") from exc
        if not isinstance(record, dict):
            raise ProcessingError(f"Batch metadata {path.name} is not an object")
        if record.get("status") in {"completed", "completed_target", "completed_exhausted"} and record.get("page_count", 0) > 0:
            records.append(record)
    return sorted(records, key=lambda record: (record.get("created_at", ""), record["batch_id"]))


def _process_one_batch(
    spark: SparkSession,
    bronze_root: Path,
    silver_root: Path,
    metadata: dict[str, Any],
    previous_silver_paths: tuple[Path, ...] = (),
) -> tuple[int, int]:
    batch_id = str(metadata["batch_id"])
    source = metadata.get("source", "openalex")
    base = _load_batch_dataframe(spark, bronze_root, metadata)
    validated = _validation_reason(base, source)
    invalid = validated.filter(F.col("rejection_reason") != "")
    valid = validated.filter(F.col("rejection_reason") == "")

    # Keep the first occurrence deterministic and make later duplicates visible
    # in Silver rejected rather than silently losing audit evidence.
    first_record_orders = valid.groupBy("source_id").agg(
        F.min("record_order").alias("_first_record_order")
    )
    duplicates = (
        valid.join(first_record_orders, on="source_id")
        .filter(F.col("record_order") != F.col("_first_record_order"))
        .drop("_first_record_order")
        .withColumn("rejection_reason", F.lit("duplicate_source_id"))
    )
    accepted = (
        valid.join(first_record_orders, on="source_id")
        .filter(F.col("record_order") == F.col("_first_record_order"))
        .drop("_first_record_order")
    )
    cross_batch_duplicates: DataFrame | None = None
    if previous_silver_paths:
        # Silver contains only validated rows, and completed paths exclude the current batch.
        same_source_ids = []
        for path in previous_silver_paths:
            previous = spark.read.parquet(str(path))
            if "source" in previous.columns:
                previous = previous.where(F.col("source") == source)
            elif source != "openalex":
                continue
            same_source_ids.append(previous.select("source_id"))
        if same_source_ids:
            previous_ids = same_source_ids[0]
            for other in same_source_ids[1:]:
                previous_ids = previous_ids.unionByName(other)
            previous_ids = previous_ids.where(F.col("source_id").isNotNull()).distinct()
            cross_batch_duplicates = accepted.join(
                previous_ids, on="source_id", how="left_semi"
            ).withColumn(
                "rejection_reason", F.lit("duplicate_source_id_previous_batch")
            )
            accepted = accepted.join(previous_ids, on="source_id", how="left_anti")

    rejected = _rejected_columns(invalid).unionByName(_rejected_columns(duplicates))
    if cross_batch_duplicates is not None:
        rejected = rejected.unionByName(_rejected_columns(cross_batch_duplicates))

    processed_at = datetime.now(timezone.utc).isoformat()
    silver_path = silver_root / "works" / batch_id
    rejected_path = silver_root / "rejected" / batch_id
    (
        _silver_columns(accepted, processed_at, source).repartition(output_partitions(int(metadata.get("record_count", 0))))
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(silver_path))
    )
    rejected.write.mode("overwrite").option("compression", "snappy").parquet(str(rejected_path))
    return accepted.count(), rejected.count()


def _ensure_schema_version(bronze_path: Path, silver_path: Path) -> bool:
    """Rebuild derived Silver when it was written with another schema version.

    Silver is derived from Bronze, so an older schema is discarded and reprocessed
    without manual steps. Nothing is deleted unless every Silver batch still has its
    Bronze metadata, so a missing source can never turn a rebuild into data loss.
    """
    marker = silver_path / "_schema_version.json"
    try:
        current = json.loads(marker.read_text(encoding="utf-8")).get("version") if marker.exists() else None
    except (OSError, ValueError):
        current = None
    if current == SILVER_SCHEMA_VERSION:
        return False
    derived = [silver_path / name for name in ("works", "rejected", "_processing", "_compact")]
    has_data = any(path.exists() for path in derived)
    if has_data:
        batches: set[str] = set()
        for folder in ("works", "rejected"):
            batches.update(path.name for path in (silver_path / folder).glob("*") if path.is_dir())
        batches.update(path.stem for path in (silver_path / "_processing").glob("*.json"))
        manifest = silver_path / "_compact" / "manifest.json"
        if manifest.exists():
            for entry in json.loads(manifest.read_text(encoding="utf-8")).values():
                batches.update(entry["batches"])
        missing = sorted(batch for batch in batches if not (bronze_path / "batch_metadata" / f"{batch}.json").exists())
        if missing:
            raise ProcessingError(f"Cannot rebuild Silver: Bronze is missing for {len(missing)} batches")
        for path in derived:
            shutil.rmtree(path, ignore_errors=True)
    _write_json_atomic(marker, {"version": SILVER_SCHEMA_VERSION})
    return has_data


def process_pending_batches(
    spark: SparkSession | None,
    bronze_root: str | Path,
    silver_root: str | Path,
    deadline: float | None = None,
) -> ProcessingSummary:
    """Process completed Bronze batches once and write Silver/Rejected zones.

    After the deadline no new batch is started, but at least one batch is always
    processed so a rebuild keeps progressing and finishes across several runs.
    """

    bronze_path = Path(bronze_root)
    silver_path = Path(silver_root)
    active_spark = spark or SparkSession.getActiveSession()
    if active_spark is None:
        active_spark = (
            SparkSession.builder.master("local[*]")
            .appName("openalex-bronze-silver")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    processed: list[str] = []
    skipped: list[str] = []
    valid_rows = 0
    rejected_rows = 0
    completed_silver_paths: list[Path] = []
    pending = 0
    with _processing_lock(silver_path):
        rebuilt = _ensure_schema_version(bronze_path, silver_path)
        for metadata in _completed_metadata(bronze_path):
            batch_id = str(metadata["batch_id"])
            state_path = _processing_state_path(silver_path, batch_id)
            if state_path.exists():
                try:
                    state = json.loads(state_path.read_text(encoding="utf-8"))
                except (OSError, ValueError) as exc:
                    raise ProcessingError(f"Unable to read processing state for {batch_id}") from exc
                if state.get("status") == "completed":
                    skipped.append(batch_id)
                    completed_silver_paths.append(silver_path / "works" / batch_id)
                    continue

            if deadline is not None and processed and time.monotonic() >= deadline:
                pending += 1
                continue
            _write_json_atomic(
                state_path,
                {
                    "batch_id": batch_id,
                    "status": "running",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            try:
                started = time.monotonic()
                from src.compaction import resolve_completed_paths

                batch_valid, batch_rejected = _process_one_batch(
                    active_spark,
                    bronze_path,
                    silver_path,
                    metadata,
                    tuple(resolve_completed_paths(silver_path, completed_silver_paths)),
                )
            except Exception as exc:
                _write_json_atomic(
                    state_path,
                    {
                        "batch_id": batch_id,
                        "status": "failed",
                        "error_type": type(exc).__name__,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                raise
            _write_json_atomic(
                state_path,
                {
                    "batch_id": batch_id,
                    "status": "completed",
                    "valid_rows": batch_valid,
                    "rejected_rows": batch_rejected,
                    "write_seconds": round(time.monotonic() - started, 3),
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            completed_silver_paths.append(silver_path / "works" / batch_id)
            processed.append(batch_id)
            valid_rows += batch_valid
            rejected_rows += batch_rejected

    return ProcessingSummary(
        processed_batches=tuple(processed),
        skipped_batches=tuple(skipped),
        valid_rows=valid_rows,
        rejected_rows=rejected_rows,
        rebuilt=rebuilt,
        pending_batches=pending,
    )
