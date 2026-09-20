"""Run one ingestion batch followed by pending Spark processing."""

from __future__ import annotations

import json
import os
from pathlib import Path

from pyspark.sql import SparkSession

from src.openalex_ingest import run_ingestion
from src.spark_pipeline import process_pending_batches


def main() -> None:
    bronze_root = Path(os.environ.get("BRONZE_PATH", "/data/bronze"))
    silver_root = Path(os.environ.get("SILVER_PATH", "/data/silver"))
    ingestion = run_ingestion(bronze_root)

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
    finally:
        spark.stop()

    # Only non-sensitive counts and identifiers are emitted. The API client
    # deliberately sanitizes transport errors before they reach this process.
    print(
        json.dumps(
            {
                "event": "pipeline_complete",
                "batch_id": ingestion.batch_id,
                "batch_mode": ingestion.mode,
                "ingested_records": ingestion.records,
                "ingested_pages": ingestion.pages,
                "processed_batches": processing.processed_batches,
                "skipped_batches": processing.skipped_batches,
                "valid_rows": processing.valid_rows,
                "rejected_rows": processing.rejected_rows,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
