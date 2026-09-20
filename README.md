# OpenAlex Bronze-to-Silver pipeline

This repository contains one Dockerized Spark batch service. It ingests OpenAlex works into an auditable Bronze zone, validates and deduplicates them with PySpark, and writes useful Silver Parquet rows.

## Quick path

1. Keep the existing `.env` file in the repository root with `OPENALEX_TOKEN`; it is ignored and is never committed.
2. Start the single service:

   ```bash
   docker compose -f compose.yaml up --build
   ```

3. Open `http://localhost:8888/lab/tree/notebooks/openalex_pipeline_evidence.ipynb` after the first batch has completed.

The schedule is deliberately every two minutes for the base exercise only. Do not use the verification commands below to trigger the live 200,000-record initial ingest.

## Commands

| Purpose | Command |
| --- | --- |
| Validate Compose without printing its resolved configuration | `docker compose -f compose.yaml config --quiet` |
| Run the test suite in the image | `docker compose -f compose.yaml run --rm spark-pipeline pytest -q` |
| Follow scheduler and pipeline logs | `docker compose -f compose.yaml logs -f spark-pipeline` |
| Open a shell in the runtime | `docker compose -f compose.yaml exec spark-pipeline sh` |

The notebook is served by the same `spark-pipeline` service and uses the mounted `/data/bronze` and `/data/silver` paths. It is read-only and never calls the API.

## Architecture and data zones

```text
OpenAlex cursor API
        |
        v
Bronze: data/bronze/openalex/<batch-id>/page-*.json
        |  data/bronze/batch_metadata/<batch-id>.json
        |  data/bronze/rejected/<batch-id>/part-*.json
        v
Silver: data/silver/works/<batch-id>/part-*.parquet
        |
        v
Jupyter evidence notebook
```

- **Initial mode** follows cursors until at least 200,000 records are persisted. Its running metadata is resumed after a scheduler restart, so it cannot accidentally become an incremental batch.
- **Incremental mode** starts from the last completed cursor and requests at most 1,000 records per scheduled run.
- **Bronze** stores raw response pages, per-batch state, and rejected records with semicolon-separated reasons.
- **Silver** contains normalized fields such as OpenAlex ID, title, publication year/date, topics, type, DOI, citation count, OA flag, source, and processing time.
- Completed processing state lives under `data/silver/_processing/`; completed batches are skipped on later runs.

The API token is supplied only through the existing environment or `.env` injection. The application does not print it, persist it in batch metadata, or access it from the notebook.
