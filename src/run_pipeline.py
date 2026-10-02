"""Run one ingestion batch followed by pending Spark processing."""

from __future__ import annotations

import json
import os
from pathlib import Path

from pyspark.sql import SparkSession

from src.gold import build_gold
from src.openalex_ingest import OpenAlexRequestError, run_ingestion
from src.scopus_ingest import run_scopus_years
from src.spark_pipeline import process_pending_batches


def main() -> None:
    bronze_root = Path(os.environ.get("BRONZE_PATH", "/data/bronze"))
    silver_root = Path(os.environ.get("SILVER_PATH", "/data/silver"))
    gold_root = Path(os.environ.get("GOLD_PATH", "/data/gold"))
    try:
        ingestion = run_ingestion(bronze_root)
        openalex_status = ingestion.status
    except OpenAlexRequestError as exc:
        ingestion = None
        openalex_status = exc.status
    scopus_query = os.environ.get("SCOPUS_QUERY", "").strip()
    first_year = os.environ.get("SCOPUS_FIRST_YEAR")
    last_year = os.environ.get("SCOPUS_LAST_YEAR")
    scopus = None
    if scopus_query:
        # Always partition: a single query stops at the 5,000-result window.
        partitions = run_scopus_years(
            bronze_root,
            query=scopus_query,
            first_year=int(first_year) if first_year and last_year else None,
            last_year=int(last_year) if first_year and last_year else None,
        )
        scopus = partitions[-1] if partitions else None

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
        # Gold only changes when Silver does, so skip the rebuild on idle minutes.
        gold = None
        if processing.processed_batches or not (gold_root / "works").exists():
            gold = build_gold(spark, silver_root, gold_root)
    finally:
        spark.stop()

    # Only non-sensitive counts and identifiers are emitted. The API client
    # deliberately sanitizes transport errors before they reach this process.
    print(
        json.dumps(
            {
                "event": "pipeline_complete" if openalex_status in {"completed", "completed_target", "completed_exhausted"} and (scopus is None or scopus.status in {"completed_target", "completed_exhausted"}) else "pipeline_partial",
                "batch_id": ingestion.batch_id if ingestion else None,
                "batch_mode": ingestion.mode if ingestion else None,
                "ingested_records": ingestion.records if ingestion else 0,
                "ingested_pages": ingestion.pages if ingestion else 0,
                "openalex_status": openalex_status,
                "scopus_batch_id": scopus.batch_id if scopus else None,
                "scopus_status": scopus.status if scopus else None,
                "processed_batches": processing.processed_batches,
                "skipped_batches": processing.skipped_batches,
                "gold_work_rows": gold.work_rows if gold else None,
                "valid_rows": processing.valid_rows,
                "rejected_rows": processing.rejected_rows,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
