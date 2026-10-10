# Silver consolidation before Gold

## Objective
Make Bronze/Silver a solid base for the Gold layer (topic × year dataset to train a neural network that
predicts emerging AI topics), working only with the data already downloaded.

## Problem and rationale
A read-only audit (2026-10-09) found the code sound but the dataset not ready:
- OpenAlex batches that turn `quota_exceeded` while holding pages are never resumed, and Silver only
  reads `completed*` batches (`src/spark_pipeline.py:269`, `src/openalex_ingest.py:334`) → ~44k records
  stranded. Scopus `result_window_reached` (3,403) and `failed` (850) batches are excluded too.
- The sources search different populations (Scopus query lacks bare AI/IA).
- Within-year sample order is biased (OpenAlex has no `sort`; Scopus walks earliest load-date windows first).
- 17% of OpenAlex rows are rejected in Silver, cause unknown.

## Scope and constraints
- Delivery deadline: no re-sampling and no new downloads. Unfixable biases are measured and documented.
- Silver keeps rows and exposes quality as flags (existing design in `src/silver_rules.py`).
- Ingestion is frozen: no container is running since the audit (dataset cut 2026-10-09).
- Gold and the neural network are the next milestone, out of scope here.

## Tasks
- [x] SC-1 Freeze the dataset (scheduler stopped; no running containers on 2026-10-09).
- [x] SC-2 Recover stranded rows: Silver accepts terminal batches that hold pages; OpenAlex paused batches
  with pages stay resumable when credits run low and honour `next_attempt_at`. Tests for both paths.
  Evidence: commit ce9ea16; `pytest -q` in container → 152 passed. Per-source `TERMINAL_STATUSES` in
  `src/spark_pipeline.py` (Scopus `quota_exceeded` stays excluded because Scopus resumes it). RDD assess:
  medium, `under_budget` (pending in slice).
- [x] SC-3 Explain the 17% OpenAlex rejection rate (breakdown by `rejection_reason`, source, year).
  Result (Spark over `silver/rejected` + `_compact/*-rejected-*`, 104,424 rows): every OpenAlex rejection
  is a duplicate — 89,263 `duplicate_source_id_previous_batch` (pages re-downloaded after batches restart
  from the last completed cursor; fixed by SC-2) and 14,111 `duplicate_source_id` within a batch. Scopus:
  1,048 duplicates, 2 `missing_title`. No valid record is lost; the cost was wasted API quota. Documented,
  no code fix beyond SC-2.
- [x] SC-4 Silver v4 flags: `matches_common_query`, `is_eligible`, `work_type_norm`. Tests.
  Evidence: 167 passed (writer, full suite); parent spot check flags + criteria tests 18 passed.
- [x] SC-5 Measure and document sampling bias and limitations in `documentacion/`.
  Evidence: `documentacion/5.limitaciones-muestreo.md`; OpenAlex order verified on Bronze pages (relevance and
  citations fall from first to last page, 2019-2024). Index links fixed after the renumbering.
- [x] SC-2b Freeze paused batches with pages (`time_budget_reached`: 39,200 OpenAlex + 900 Scopus records)
  as terminal `frozen` via `python -m src.freeze_batches`; a metadata record without `source` accepts only
  completed statuses (review WARNING R3-source-default-openalex, latent: every record on disk has `source`).
  Evidence: writer full suite 192 passed; parent spot check freeze + recovery tests passed.
- [x] SC-7 Rebuild Silver v4 from Bronze without ingestion (scratch script calling `process_pending_batches`
  and `compact_pending`); compare row counts with the baseline.
  Evidence (2026-10-10): 303/303 batches completed, 14 compacted groups, 2,489 s for the final run. Silver v4:
  1,007,148 rows, no duplicate (source, source_id). OpenAlex 557,846 (+57k vs baseline), Scopus 449,302 (+4.3k).
  42,310 DOIs appear in both sources. matches_common_query: OpenAlex 98.9%, Scopus 100%. is_eligible: OpenAlex
  74.1% (preprints 61,623, book chapters 33,386, other 38,407), Scopus 94.2%.
- [x] SC-6 Quality report in `notebooks/openalex_pipeline_evidence.ipynb` (include count of batches that hold
  pages but are not terminal, per review suggestion).
  Evidence: executed with nbconvert (data mounted read-only, run as root), 29 cells, 0 errors. Values match
  SC-7; non-terminal batches with pages = 0; rejected rows after rebuild 131,287 (supersedes SC-3 count).

## Review log
- Slice 89a6457..75e6bcd (SC-2, SC-4, SC-5): medium risk, `slice_budget_reached`, consent granted; lineage
  `review-3c8cac8fc6047bf9` approved and acknowledged (authority burned). Advisory findings: missing-source
  default (fixed in SC-2b), untested `_attempt_deferred` branches (follow-up), paused batches never terminal
  after the freeze (SC-2b).
- Slice 75e6bcd..99f1c1b (SC-2b, SC-6, SC-7 docs): medium, `slice_budget_reached`, consent granted; lineage
  `review-bf2118330a900082` approved and acknowledged. Advisory follow-ups (non-blocking, all latent while
  ingestion is frozen): freeze skips records without `source` (none on disk); `run_scopus_years` treats a
  frozen window as finished (contradicts the operations doc if ingestion resumes); null `page_count` aborts
  the freeze loop; broad `pytest.raises(Exception)` in the frozen-resume test.

## Acceptance criteria
- Silver includes every Bronze batch that holds pages; no batch counted twice with `_compact/`.
- `_schema_version.json` = 4 with the three flags populated for both sources.
- Rejection breakdown known and either fixed or documented.
- Limitations documented; quality report reproducible from the notebook.

## Checks
- `docker compose -f compose.yaml run --rm --build spark-pipeline pytest -q`
- One pipeline run after SC-2/SC-4; compare Silver row counts before/after.

## Progress and verification
- Branch `feat/silver-consolidation` created from `feat/global-bronze-cap` (89a6457).
- Baseline Silver: OpenAlex ~500,626 valid rows, Scopus ~445,032 (manifest + `_processing`).

## Route log
- SC-1 inline (state check only).
- SC-2 delegated writer (3+ non-trivial files); parent renamed the compaction import inline.
- SC-3 inline (one Spark query in the existing image, read-only data mount).
- SC-4 delegated writer (rules, columns, schema bump, tests).

## Next step
Consolidation complete. Next milestone: define the emergence label (horizon, threshold) and design Gold.
