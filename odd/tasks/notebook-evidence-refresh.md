# Feature: notebook-evidence-refresh

## Objective
Bring `notebooks/openalex_pipeline_evidence.ipynb` in line with the current pipeline (Parquet Bronze pages, Silver schema v3, Scopus window bisection, compaction) so it gives accurate, readable evidence after each cycle.

## Problem (found while exploring, 2026-10-03)
- Cell 4 counts raw pages with `page-*.json`; Bronze pages are Parquet now, so it always reports 0.
- `display(spark_df)` is not a table in plain Jupyter (no pandas in the image): it prints the DataFrame repr. Use `.show`.
- Silver cell shows only 7 columns and ignores schema v3 (topics, authors, geography, citations, quality flags).
- Scopus bisection (oversized / exhausted / time_budget_reached windows) is invisible; a run with 0 Scopus records looks like a failure.
- Legacy-compat code for pre-`source_id` Silver is dead since Silver rebuilds automatically on a schema change.
- Cell 7 reads rejected rows only from `part-*.parquet` per batch and ignores compacted rejected groups.

## Scope
Only `notebooks/openalex_pipeline_evidence.ipynb`, its test in `tests/test_project_artifacts.py`, and the README line describing it. Notebook stays read-only: no API calls, no credentials.

## Tasks
- [x] N1 RED test: notebook counts Parquet pages, avoids `display(`, covers v3 columns and Scopus window status. RED observed (page-*.parquet assertion failed), GREEN after N2.
- [x] N2 Rewrite the notebook cells (pages, processing, Silver v3 profile, Scopus windows, rejected incl. compacted). Generated from a scratch script; outputs left empty.
- [x] N3 Executed with `jupyter nbconvert --execute` in the running container on live data: all 10 cells ok (35 metadata rows, 10,679 Silver rows, 17 rejected). README paragraph updated.

## Acceptance criteria
- Notebook executes top to bottom with `nbconvert --execute` with no errors on current data and on empty zones.
- `pytest` passes (131 + new assertions).

## Route
Inline: one non-trivial file (notebook) plus a small test; no mapping trigger fired.

## Test-first
Runner: pytest in the container (`tests/test_project_artifacts.py`). RED is observable for N1.

## Progress / evidence
Created 2026-10-03.

Observed: Scopus rows show avg_authors NULL, has_abstract 0 and has_topics 0 (standard view limits, known). 3 artifact tests pass.
Next step: none; delivery (push/PR) is the user's call.
