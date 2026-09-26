# ODD task: Document the OpenAlex and Scopus Bronze-to-Silver pipeline

## Objective

Create a Spanish documentation set under `documentacion/` that explains the implemented end-to-end pipeline for both selected sources, including ingestion, filtering, Silver normalization, operations, and known limits.

## Problem and rationale

The README predates the current Scopus support and contains some operational statements that differ from the scheduler configuration. A focused documentation set should describe current code behavior and distinguish it from older or conflicting descriptions.

## Scope and constraints

- Authorized scope: new Markdown documentation under `documentacion/` and this ODD task file.
- Do not modify existing README, application code, tests, runtime configuration, or unrelated working-tree changes.
- Technical artifacts are in English by default, but the user explicitly requested Spanish documentation; use neutral professional Spanish.
- Sources of truth: `src/run_pipeline.py`, `src/openalex_ingest.py`, `src/scopus_ingest.py`, `src/spark_pipeline.py`, `src/compaction.py`, `src/metrics.py`, `compose.yaml`, and `scheduler/pipeline.cron`.
- Route: delegated direct documentation writer; mapping/preparation spans more than three implementation/configuration files and the output comprises multiple non-trivial documents.
- TDD: not applicable to documentation-only work; no test-first claims.
- Forecast: approximately 350 authored lines across a small documentation set (heuristic only).

## Acceptance criteria

- [x] A landing page links the documentation set and describes when both sources are actually ingested.
- [x] Separate documents explain source selection and ingestion, shared Bronze layout/checkpoints, Spark validation/rejections, and the Silver schema.
- [x] Operational guidance documents environment variables, scheduling, storage paths, and execution/log inspection based on current configuration.
- [x] Any discrepancy with the existing README or notable limits is called out without silently changing the README.
- [x] Documents are read back and checked for accurate internal links and claims.

## Checks

- Structural readback of each new Markdown document.
- Verify relative links resolve within the documentation set.
- No application test suite is needed for documentation-only changes.

## Progress

- Route evidence: source/config mapping requires more than three files; one documentation writer is appropriate.
- [x] Documentation authored and reviewed; one incorrect `docker-compose.yml` reference in the index was corrected to `compose.yaml` after parent spot-check.
- [x] Verification evidence: independent reviewer found no material issues; parent read all four documents; local-link validation passed; `git diff --check` passed.
- [ ] Work-unit commit identity recorded.

## Next step

Create a work-unit commit containing only the task file and four new documentation files; preserve all unrelated worktree changes.
