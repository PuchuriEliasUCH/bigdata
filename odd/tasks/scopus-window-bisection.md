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
- [ ] SWB-3 Parquet end to end: Bronze pages, rejected, Gold layer. (Task B)

## Acceptance
- A run over a year with >5,000 hits ingests every partition (mocked client) and records per-partition `total_results`/`status`.
- Re-running does not loop on a capped batch.
- `pytest` green.

## Progress / evidence
(Route per task and commit ids are recorded here.)

## Next step
SWB-0 then SWB-1.
