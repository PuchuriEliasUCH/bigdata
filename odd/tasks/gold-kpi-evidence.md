# Feature: gold-kpi-evidence

## Objective
Make `notebooks/gold_evidence.ipynb` usable as evidence of results: every cell states what it does, what
it reads and how to read its output, and a new section reports measurable KPIs computed from Gold.

## Problem (2026-10-10)
- The team is asked for results, and Gold has no KPIs named as such: only features, the label and year totals.
- The notebook cells are terse: a one-line comment per cell, with no statement of input, output or reading.
- Commit 7760575 broke `tests/test_project_artifacts.py::test_notebook_uses_mounted_zones_without_token_access`:
  the OpenAlex notebook no longer contains the literal `/data/bronze` and `/data/silver` paths.

## Scope
`notebooks/gold_evidence.ipynb`, `documentacion/4.gold.md` (KPI section), the README line describing the
notebook, and the artifact test for the OpenAlex notebook. Sections 1–7 keep their numbers because
`documentacion/4.gold.md` cites them. No change to `src/` or to Gold data.

## KPI definitions (section 8)
Volumes are sample counts (about 6.4% of the planned universe per year); rates, shares and growth are the reportable values.
- 8.1 Growth: sample publications per year, year-over-year change, compound annual growth 2011–2025, active topics (`n_t > 0`).
- 8.2 Concentration: share of the 10 largest topics and Herfindahl index (sum of squared prevalences) per year.
- 8.3 Production profile, weighted by `n_t`: open access, conference and review ratios, authors per publication, new-author ratio.
- 8.4 Emergence: positive rate overall and per base year, emergent topics of the last labelled base year (2023).
- 8.5 Subfields: share of the year's publications in 2023 and 2025, change in points and relative change.
- 8.6 Current signals (2025, unlabelled): topics with `n_t >= MIN_SUPPORT` ranked by past two-year prevalence growth.
- 8.7 KPI summary table.

## Tasks
- [x] K1 Fix the OpenAlex artifact test regression. RED observed (1 failed, 2 passed), GREEN 3 passed. Commit 3a2e8f3.
- [x] K2 Rewrite the Gold notebook descriptions: purpose, input, output and reading for every code cell.
- [x] K3 Add section 8 (KPIs) with invariant checks (sum of `n_t` equals `n_total`; prevalences sum to 1).
- [x] K4 Execute the notebook with the documented container command and copy the executed notebook back.
- [x] K5 Document the KPIs in `documentacion/4.gold.md` and the README line.

## Acceptance criteria
- The notebook executes top to bottom with no errors in the container (`documentacion/5.operacion.md`).
- Sections 1–7 keep their results (52,380 rows, 3,308 labelable, 289 positives).
- `pytest tests/test_project_artifacts.py` passes.

## Route
Inline: the KPI queries were already computed and checked with DuckDB in this session; one notebook plus
small doc and test edits, no mapping needed. Delegation was not used.

## Test-first
K1: pytest on `tests/test_project_artifacts.py` (RED expected on the current commit). K2–K3: no meaningful
unit RED for notebook prose; the check is a full container execution plus the invariant asserts.

## Progress / evidence
Created 2026-10-10. Delivery strategy: commits on `main` (user preference, 2026-10-10).

- K2–K4: notebook rebuilt from a scratch script (43 cells, 16 code cells), executed with the documented
  container command capped at 4 CPUs and 6 GB: no errors, sections 1–7 asserts pass (52,380 rows, 3,308
  labelable, 289 positives), section 8 invariants pass (sum of `n_t` = `n_total`, prevalences sum to 1).
- K5: KPI table added to `documentacion/4.gold.md` (section "Indicadores (sección 8)") and the README line.
- Full suite: 222 passed with `PYSPARK_PYTHON` set to the `.venv` interpreter. Without it, 56 Spark tests
  fail locally with PYTHON_VERSION_MISMATCH (worker 3.14, driver 3.11): environment, not code.
- Correction recorded: subfield growth is reported as share of the year's publications, not counts; with
  counts every subfield grew because the sample doubled between 2023 and 2025.

Next step: review of the work-unit commit; push is the user's call.
