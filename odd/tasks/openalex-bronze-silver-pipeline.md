# Automated OpenAlex Bronze-to-Silver Pipeline

## Objective

Build a Docker-based, scheduled data pipeline that retrieves an initial 200,000 OpenAlex works into Bronze and processes them with Spark into cleaned Silver datasets. Every later scheduled run retrieves up to 1,000 new works.

## Problem and scope

The team needs a reproducible Spark environment with no local Spark installation. This first exercise covers automated OpenAlex ingestion and Bronze-to-Silver cleansing only. Gold analytics, trend prediction, and additional sources are explicitly out of scope.

## Constraints

- Use `compose.yaml`, not `docker-compose.yml`.
- Read the API token from the existing `OPENALEX_TOKEN` environment variable.
- Run ingestion every two minutes for the exercise.
- Add explanatory English comments to configuration, Python code, tests, and the evidence notebook.
- Follow TDD: capture a failing test before implementing each behavior, then make it pass.
- Keep the initial dataset at least 200,000 records; incremental runs request at most 1,000 records.

## Delivery strategy

- Route: delegated direct.
- Trigger evidence: the implementation spans Docker configuration, scheduling, Python ingestion, PySpark processing, tests, and a notebook.
- TDD mode: enabled by explicit user instruction; test runner to be established during implementation.
- Work-unit commit: required after verification. Git is available; commit only the implementation files created for this feature.

## Tasks

- [x] OAX-001 Create the Docker runtime, dependency manifest, schedule, and safe environment configuration.
  - Acceptance: `docker compose config` validates and the scheduled command is visible in the container configuration.
  - Checks: Compose validation; configuration tests where applicable.
- [x] OAX-002 Implement test-driven OpenAlex ingestion into batch-partitioned Bronze storage.
  - Acceptance: first-run mode continues cursor pagination until at least 200,000 records are persisted; incremental mode caps the request at 1,000 records and preserves raw payloads plus batch metadata.
  - Checks: unit tests with mocked OpenAlex responses, including pagination and token validation.
- [x] OAX-003 Implement test-driven Spark validation, deduplication, rejection, and Parquet Silver output.
  - Acceptance: valid records with required fields become Silver rows; invalid or duplicate records are auditable in rejected output; processed batches are not repeated.
  - Checks: PySpark tests using temporary local storage.
- [x] OAX-004 Create a Jupyter notebook that evidences Bronze and Silver data, quality outcomes, and batch metadata without exposing the token.
  - Acceptance: notebook runs in the same Docker image and displays counts, schemas, samples, and rejected-record reasons.
  - Checks: notebook syntax/execution check where Docker is available.
- [x] OAX-005 Reject OpenAlex work IDs already accepted in a previous completed Silver batch.
  - Reason reopened: the initial implementation deduplicates only inside the current Bronze batch, so incremental runs can repeat a work already present in Silver.
  - Acceptance: duplicate work IDs across batches become auditable rejected rows with a distinct reason and are not written again to Silver.
  - Checks: a failing cross-batch PySpark test before implementation, then the full container test suite.
- [x] OAX-006 Process large Bronze batches through Spark-native JSON reads instead of a driver-side Python row list.
  - Reason reopened: the first live 200,000-record batch failed with `java.lang.OutOfMemoryError: Java heap space` while `spark.createDataFrame(rows, ...)` serialized the full Python list into the JVM.
  - Acceptance: Spark reads raw Bronze pages directly, applies the same validation and duplicate rules, and processes the existing initial batch without driver-side materialization.
  - Checks: a failing regression test that forbids the Python-list loading path, full container suite, and a manual Silver run over the preserved Bronze batch.

## Progress and evidence

- Status: complete.
- Initial repository state: Git repository with untracked `.env` and `.gitpeek`; no source files yet.
- TDD evidence: tests were authored before implementation. The host lacked pytest; Docker verification passed with `9 passed in 13.45s`.
- OAX-005 TDD evidence: the focused two-batch PySpark regression test failed before implementation with `1 failed, 2 deselected in 11.99s` because the duplicate was accepted; after implementation it passed with `1 passed, 2 deselected in 14.39s`.
- Verification: `docker compose -f compose.yaml config` completed successfully without printing environment values; notebook static syntax/token check passed; writer suite `docker compose -f compose.yaml run --build --rm --no-deps spark-pipeline pytest -q` passed with `10 passed in 16.62s`; parent spot check of that same command passed with `10 passed in 17.76s`. No live 200,000-record ingestion was run.
- Implementation commits: `9270797 feat(openalex): add scheduled bronze silver pipeline`; `c488599 fix(openalex): reject IDs across Silver batches` (OAX-005).
- RDD: disabled globally; no native review was started.
- OAX-005 implementation: previous completed Silver batch IDs are anti-joined from current accepted rows; cross-batch duplicates are written to the current Bronze rejected output as `duplicate_openalex_id_previous_batch`, while invalid prior records do not reserve IDs.
- OAX-006 diagnostic: the scheduled container was stopped after the initial live Silver attempt failed with `Py4JJavaError` caused by Java heap exhaustion. The Bronze batch remains preserved for post-fix processing.
- OAX-006 implementation: Spark reads each Bronze `page-*.json` file with `wholetext`, expands `results` with `posexplode`, and performs normalization and validation with DataFrame expressions. Silver Parquet writes explicitly select Snappy compression; this is not treated as an OOM fix.
- OAX-006 TDD evidence: the focused regression test failed against the old loader with `1 failed, 3 deselected in 2.95s` and passed after the change with `1 passed, 3 deselected in 9.23s`. The required Docker suite passed with `11 passed in 16.88s`; parent spot check passed with `11 passed in 16.24s`.
- OAX-006 runtime proof: `docker compose ps` showed no running scheduler. The manual Silver-only `docker compose -f compose.yaml run --build --rm --no-deps spark-pipeline python -c ...` invocation did not ingest or call OpenAlex; both preserved batches were already completed, so it returned `processed_batches=[]`, `skipped_batches=[initial-20260920T135400858916Z-e25ea9d5, incremental-20260920T145400655181Z-92b80215]`, `valid_rows=0`, and `rejected_rows=0`. The states remained completed with initial `199984/16` valid/rejected and incremental `1000/0`; 4 Silver Parquet files existed.
- OAX-006 implementation commit: `c616461 fix(openalex): process Bronze pages with Spark`.
