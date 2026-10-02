# Measure ingestion output

Run the per-batch report against the same Bronze and Silver roots before and after a change:

```sh
docker compose run --rm --no-deps -v "$PWD:/app" --entrypoint python spark-pipeline -m src.metrics --bronze /data/bronze --silver /data/silver
```

Each JSON line includes source, initial/incremental mode, valid and rejected rows, `part-*.parquet` count and bytes, and processing duration. Auxiliary Spark files are excluded. Older processing states did not record duration and report `null`; new states include `write_seconds` (processing time including validation, write, and counts). Compare batches with similar row counts and modes rather than summing an initial load and increments.

Baseline measured on September 26, 2026, using existing local data: the 200,000-record OpenAlex initial batch produced 199,984 valid rows, 16 rejected rows, two Parquet files (19,118,253 bytes). Most 1,000-record incremental batches produced five files; a few produced one, two, or four. Previous write duration is unavailable because it was not captured in their processing state. The new threshold is 100,000 input rows per output partition, so a 1,000-row increment targets one file; compare fresh runs using the command above before adjusting that threshold.
