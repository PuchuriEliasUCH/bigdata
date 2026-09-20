"""Spark validation, rejection, deduplication, and Silver Parquet output."""

from __future__ import annotations

import json
import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


class ProcessingError(RuntimeError):
    """Raised when a Bronze batch cannot be processed safely."""


@dataclass(frozen=True)
class ProcessingSummary:
    """Counts and batch identifiers safe to print in scheduler logs."""

    processed_batches: tuple[str, ...]
    skipped_batches: tuple[str, ...]
    valid_rows: int
    rejected_rows: int


RAW_SCHEMA = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("record_order", IntegerType(), False),
        StructField("record_json", StringType(), False),
        StructField("openalex_id", StringType(), True),
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


def _text(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _topic_values(value: Any) -> tuple[list[str], list[str]]:
    topic_ids: list[str] = []
    topic_names: list[str] = []
    if not isinstance(value, list):
        return topic_ids, topic_names
    for topic in value:
        if isinstance(topic, dict):
            topic_id = _text(topic.get("id"))
            topic_name = _text(topic.get("display_name")) or _text(topic.get("name"))
            if topic_id:
                topic_ids.append(topic_id)
            if topic_name:
                topic_names.append(topic_name)
        elif isinstance(topic, str) and topic.strip():
            topic_names.append(topic.strip())
    return topic_ids, topic_names


def _source_name(record: dict[str, Any]) -> str | None:
    primary_location = record.get("primary_location")
    if not isinstance(primary_location, dict):
        return None
    source = primary_location.get("source")
    if not isinstance(source, dict):
        return None
    return _text(source.get("display_name"))


def _normalize_record(batch_id: str, record_order: int, record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        record = {"_invalid_record": record}
    topic_ids, topic_names = _topic_values(record.get("topics"))
    open_access = record.get("open_access")
    is_oa = open_access.get("is_oa") if isinstance(open_access, dict) else None
    if not isinstance(is_oa, bool):
        is_oa = None
    return {
        "batch_id": batch_id,
        "record_order": record_order,
        "record_json": json.dumps(record, ensure_ascii=False, sort_keys=True),
        "openalex_id": _text(record.get("id")),
        "title": _text(record.get("title")),
        "publication_year": _integer(record.get("publication_year")),
        "publication_date": _text(record.get("publication_date")),
        "topic_ids": topic_ids,
        "topic_names": topic_names,
        "work_type": _text(record.get("type")),
        "doi": _text(record.get("doi")),
        "cited_by_count": _integer(record.get("cited_by_count")),
        "is_oa": is_oa,
        "source_name": _source_name(record),
    }


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
    return sorted(raw_page_dir.glob("page-*.json"))


def _load_batch_rows(bronze_root: Path, metadata: dict[str, Any]) -> list[dict[str, Any]]:
    batch_id = str(metadata["batch_id"])
    rows: list[dict[str, Any]] = []
    record_order = 0
    for page_path in _raw_page_paths(bronze_root, metadata):
        try:
            payload = json.loads(page_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ProcessingError(f"Unable to read Bronze page {page_path.name}") from exc
        results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(results, list):
            raise ProcessingError(f"Bronze page {page_path.name} has no results list")
        for record in results:
            rows.append(_normalize_record(batch_id, record_order, record))
            record_order += 1
    return rows


def _validation_reason(dataframe: DataFrame) -> DataFrame:
    """Create auditable, composable reasons instead of dropping bad records."""

    reasons = F.array(
        F.when(
            F.col("openalex_id").isNull()
            | (F.length(F.trim(F.col("openalex_id"))) == 0),
            F.lit("missing_openalex_id"),
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
        ),
    )
    return dataframe.withColumn("rejection_reason", F.concat_ws("; ", reasons))


def _rejected_columns(dataframe: DataFrame) -> DataFrame:
    return dataframe.select(
        "batch_id",
        "record_order",
        "openalex_id",
        "record_json",
        "rejection_reason",
    )


def _silver_columns(dataframe: DataFrame, processed_at: str) -> DataFrame:
    publication_year = F.when(
        F.col("publication_year").isNotNull(), F.col("publication_year")
    ).otherwise(F.regexp_extract(F.col("publication_date"), r"^(\d{4})", 1).cast("int"))
    return dataframe.select(
        "batch_id",
        "openalex_id",
        "title",
        publication_year.alias("publication_year"),
        "publication_date",
        "topic_ids",
        "topic_names",
        (F.size(F.col("topic_ids")) + F.size(F.col("topic_names"))).alias("topic_count"),
        "work_type",
        "doi",
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
        if record.get("status") == "completed":
            records.append(record)
    return sorted(records, key=lambda record: (record.get("created_at", ""), record["batch_id"]))


def _process_one_batch(
    spark: SparkSession,
    bronze_root: Path,
    silver_root: Path,
    metadata: dict[str, Any],
) -> tuple[int, int]:
    batch_id = str(metadata["batch_id"])
    rows = _load_batch_rows(bronze_root, metadata)
    base = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    validated = _validation_reason(base)
    invalid = validated.filter(F.col("rejection_reason") != "")
    valid = validated.filter(F.col("rejection_reason") == "")

    # Keep the first occurrence deterministic and make later duplicates visible
    # in Bronze rejected rather than silently losing audit evidence.
    window = Window.partitionBy("openalex_id").orderBy(F.col("record_order").asc())
    ranked = valid.withColumn("_duplicate_rank", F.row_number().over(window))
    duplicates = ranked.filter(F.col("_duplicate_rank") > 1).withColumn(
        "rejection_reason", F.lit("duplicate_openalex_id")
    )
    accepted = ranked.filter(F.col("_duplicate_rank") == 1).drop("_duplicate_rank")
    rejected = _rejected_columns(invalid).unionByName(_rejected_columns(duplicates))

    processed_at = datetime.now(timezone.utc).isoformat()
    silver_path = silver_root / "works" / batch_id
    rejected_path = bronze_root / "rejected" / batch_id
    _silver_columns(accepted, processed_at).write.mode("overwrite").parquet(str(silver_path))
    rejected.write.mode("overwrite").json(str(rejected_path))
    return accepted.count(), rejected.count()


def process_pending_batches(
    spark: SparkSession | None,
    bronze_root: str | Path,
    silver_root: str | Path,
) -> ProcessingSummary:
    """Process completed Bronze batches once and write Silver/Rejected zones."""

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
    with _processing_lock(silver_path):
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
                batch_valid, batch_rejected = _process_one_batch(
                    active_spark,
                    bronze_path,
                    silver_path,
                    metadata,
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
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            processed.append(batch_id)
            valid_rows += batch_valid
            rejected_rows += batch_rejected

    return ProcessingSummary(
        processed_batches=tuple(processed),
        skipped_batches=tuple(skipped),
        valid_rows=valid_rows,
        rejected_rows=rejected_rows,
    )
