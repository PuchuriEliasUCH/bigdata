# Feature: scopus-window-bisection

## Objective
Complete massive Scopus ingestion (100,000 initial records, then minute-by-minute incrementals until quota or results end) without stopping at the 5,000-result window; then Parquet end to end and aligned search criteria.

## Problem (evidence)
- `data/bronze/batch_metadata/scopus-20260926T224109592589Z-d59d4865.json`: `status=result_window_reached`, `record_count=5000`, `total_results=4422695`, quota fine.
- `src/scopus_ingest.py:103-105` stops at `offset >= result_window`; `result_window_reached` is not terminal (`:76`), so every cron run re-picks it; `run_scopus_years` returns early (`:227`).
- Probe (read-only): `cursor=*` -> 403 ENTITLEMENTS_ERROR (restricted). `ORIG-LOAD-DATE AFT/BEF` works; `PUBYEAR=2020` for "machine learning" = 66,472 results, so year-only partitioning is insufficient. Windows must cover all history, not only the year (2020 papers loaded before/after 2020 exist).

## Constraints
- Ingestion is type-agnostic: no DOCTYPE/SUBJAREA partitioning; type filtering belongs to Silver.
- Partitions must be disjoint and exhaustive (union == original query).
- Never print or store `SCOPUS_API`.
- Control/state files stay JSON; data layers become Parquet (Task B).

## Tasks
- [x] SWB-0 Baseline: 57 tests passed; commits 1a6fb3d, dcf1f1c.
- [x] SWB-1 Scopus bisection (64 tests passed; route: inline, no delegation because the subagent writer hung): yearly query, bisect by `ORIG-LOAD-DATE` windows until each < 5,000; capped partition does not block others; cron re-run safe. (Task A)
- [x] SWB-2 Search criteria (70 tests passed): shared `src/search_criteria.py`, OpenAlex filter scoped state. Scopus terms kept unchanged (noisy AI/IA/robotics left for user decision); OpenAlex `title_and_abstract.search` syntax not yet verified against the real API.
- [x] SWB-3 Parquet end to end (Task B). Done: Bronze pages + rejected as Parquet, legacy JSON still readable (74 tests passed). Gold layer was built (66c2710) and then removed on request: the project is in the Bronze and Silver phase and Gold is not designed yet.

- [x] SWB-4 (101 tests passed) Year-stratified ingestion 2011..current for both sources: proportional initial quota per year (100k), fill-ratio ordered incremental toward >=1M records, per-year OpenAlex lineage, INCREMENTAL_LIMIT env. Silver filtering is out of scope (it will change).
  - [x] 4a quotas + Scopus per-year quotas
  - [x] 4b OpenAlex per-year
  - [x] 4c run_pipeline wiring + docs

## Acceptance
- A run over a year with >5,000 hits ingests every partition (mocked client) and records per-partition `total_results`/`status`.
- Re-running does not loop on a capped batch.
- `pytest` green.

## Progress / evidence
Route: inline for every task (the subagent writer hung twice with no changes). Tests run with `docker compose -f compose.yaml build spark-pipeline` BEFORE `run --rm spark-pipeline pytest -q`, because `src/` and `tests/` are copied into the image. State as of 2026-10-02: 94 tests pass.

Commits on `feat/global-bronze-cap`: 1a6fb3d, dcf1f1c (baseline) - 31afa9b (Scopus bisection) - 2656a36 (shared AI terms, OpenAlex filter) - 1cf064c (Bronze and rejected as Parquet) - 66c2710 (Gold, later removed) - a2b361c, 26614bf, 754c272 (year-stratified ingestion) - 0b0d7be (Gold removed) - 7720ccf (rejected moved to silver/rejected).

Context (context.md): predict emerging AI research topics (supervised, topic-per-year unit, temporal split, CRISP-DM). The course needs at least 1M records. Current phase is Bronze and Silver only; Gold and the evidence notebook are postponed (the notebook must be redone at the end and is stale).

Layout: `bronze/{openalex,scopus}/<batch>/page-N.parquet` (one row per record: batch_id, page_number, record_index, record_json raw text), `bronze/batch_metadata/*.json`, `bronze/plans/<source>.json`; `silver/works/<batch>` (valid rows, no reason field), `silver/rejected/<batch>` (record_json + rejection_reason), `silver/_processing/`. Legacy JSON pages stay readable.

Ingestion: INGEST_FIRST_YEAR (2011), INGEST_LAST_YEAR, INGEST_INITIAL_TARGET (100k per source, split by year in proportion to volume), INCREMENTAL_LIMIT (1000, least filled year first), OPENALEX_FILTER (`none` disables), SCOPUS_QUERY (never invented). Scopus cursor is restricted for this key (403 ENTITLEMENTS_ERROR). OpenAlex filter verified (HTTP 200, 7.1M results; Scopus about 4.4M for the same terms).

Known gaps: Silver drops abstract, authors, institutions, countries, keywords, language and counts_by_year (present in OpenAlex Bronze); Scopus standard view has no abstract (COMPLETE view not verified); cited_by_count is a snapshot, so it leaks future citations for features as of year t; cron overlap during the long initial phase is unverified; data/bronze and data/silver were deleted (root-owned files, removed through the container, not sudo).

Engram: `mem_save` and `mem_session_start` failed at 2026-10-02 with "Claude host session registration could not be confirmed"; this section is the fallback record until Engram is reconnected.

Real bounded read test (2024-2025, target 200, temp dir inside the container): pipeline_complete, 398 valid + 2 rejected Silver rows, quotas respected. It exposed a defect: empty Scopus windows were stored as one phantom record (record_count 1) and consumed quota; fixed in the commit after 24c4f04 (97 tests pass). Open item: Silver creates one folder per Scopus window (6 for 200 records), so compaction or batch merging is needed before the 100k load.

Compaction (068063e): compact_pending publishes immutable groups per source and year by threshold (COMPACT_MIN_BATCHES=10), compacts rejected rows, prunes originals after validation. Time budget: INGEST_TIME_BUDGET_SECONDS=30 shared by active sources, batches pause as time_budget_reached and resume next run (no Silver fragmentation); INCREMENTAL_LIMIT=3000 is only a cap. 111 tests pass.

Real bounded read test with the 30 s budget (3 cycles, 2024-2025, temp dir): each cycle 44.5-46.8 s total (ingestion ~29-38 s, Spark + compaction ~9-15 s), so it fits the one-minute scheduler. Measured pace: OpenAlex ~800 records per run (~3 s per page), Scopus ~180 per run. Spark time grew 8.9 -> 15.3 s over 3 runs (cross-batch dedup reads all previous Silver): main scalability risk toward 1M. Not verified in that test: Silver row counts and whether compaction fires (inspection script failed). OpenAlex now checks credits once per batch instead of per page; documentacion/operacion.md describes a one-off continuous initial load. Scopus page size stays 25 by decision (a larger page size was discussed and NOT applied).

Scopus speed-up (decided): limits verified against dev.elsevier.com (20,000 requests per week, 9 per second, STANDARD max 200 but this key rejects 200 even with view=STANDARD, 5,000 depth without cursor). Pacing counted from request start, SCOPUS_REQUESTS_PER_SECOND=5, 2-minute cron (*/2), INGEST_TIME_BUDGET_SECONDS=80 with INGEST_SCOPUS_SHARE=0.6, INCREMENTAL_LIMIT=6000, report has ingestion_seconds and silver_seconds. Quota reset seen at 2026-10-03 11:37 UTC. Multiple Scopus keys were discussed and dropped by the user.

## Next step
1. Real bounded read test (2024-2025, INGEST_INITIAL_TARGET=200, temporary folder), authorized by the user.
2. Design the Silver schema (enrich OpenAlex fields) and decide the role of Scopus.
