"""Run one ingestion batch followed by pending Spark processing."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from src.compaction import compact_pending
from src.ingestion import ingest_openalex, ingest_scopus
from src.openalex_ingest import IngestionError, OpenAlexRequestError
from src.spark_pipeline import process_pending_batches


COMPLETE = {"completed", "completed_target", "completed_exhausted"}
SCOPUS_SETTLED = {"completed_target", "completed_exhausted", "result_window_reached"}
DEFAULT_FIRST_YEAR = 2011


def _overall(statuses: list[str], settled: set[str]) -> str | None:
    """First status that still needs attention, otherwise the last one."""
    return next((status for status in statuses if status not in settled), statuses[-1] if statuses else None)


def main() -> None:
    bronze_root = Path(os.environ.get("BRONZE_PATH", "/data/bronze"))
    silver_root = Path(os.environ.get("SILVER_PATH", "/data/silver"))
    # One year range and one budget for both sources keeps their samples comparable.
    shared = {
        "first_year": int(os.environ.get("INGEST_FIRST_YEAR") or DEFAULT_FIRST_YEAR),
        "last_year": int(os.environ.get("INGEST_LAST_YEAR") or datetime.now(timezone.utc).year),
        "initial_target": int(os.environ.get("INGEST_INITIAL_TARGET") or 100_000),
        "incremental_limit": int(os.environ.get("INCREMENTAL_LIMIT") or 1000),
    }
    try:
        openalex = ingest_openalex(bronze_root, **shared)
        openalex_status = _overall([summary.status for summary in openalex], COMPLETE) or "completed"
    except OpenAlexRequestError as exc:
        openalex, openalex_status = [], exc.status
    scopus_query = os.environ.get("SCOPUS_QUERY", "").strip()
    scopus: list = []
    scopus_status = None
    if scopus_query:
        try:
            scopus = ingest_scopus(bronze_root, query=scopus_query, **shared)
            scopus_status = _overall([summary.status for summary in scopus], SCOPUS_SETTLED)
        except IngestionError:
            # The message is already sanitized, and Silver must still run.
            scopus_status = "failed"

    # A local master keeps the single-container exercise self-contained. The
    # same processing function can receive a cluster SparkSession in deployment.
    spark = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName("openalex-bronze-silver")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        processing = process_pending_batches(spark, bronze_root, silver_root)
        # Small batches pile up (one per Scopus window, one per OpenAlex increment);
        # compaction only acts once a source and year have enough of them.
        compaction = compact_pending(
            spark, bronze_root, silver_root,
            min_batches=int(os.environ.get("COMPACT_MIN_BATCHES") or 10),
        )
    finally:
        spark.stop()

    # Only non-sensitive counts and identifiers are emitted. The API client
    # deliberately sanitizes transport errors before they reach this process.
    complete = openalex_status in COMPLETE and (scopus_status is None or scopus_status in {"completed_target", "completed_exhausted"})
    print(
        json.dumps(
            {
                "event": "pipeline_complete" if complete else "pipeline_partial",
                "batch_id": openalex[-1].batch_id if openalex else None,
                "batch_mode": openalex[-1].mode if openalex else None,
                "ingested_records": sum(summary.records for summary in openalex),
                "ingested_pages": sum(summary.pages for summary in openalex),
                "openalex_status": openalex_status,
                "scopus_batch_id": scopus[-1].batch_id if scopus else None,
                "scopus_status": scopus_status,
                "processed_batches": processing.processed_batches,
                "skipped_batches": processing.skipped_batches,
                "compacted_groups": list(compaction.groups),
                "compaction_skipped": list(compaction.skipped),
                "valid_rows": processing.valid_rows,
                "rejected_rows": processing.rejected_rows,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
