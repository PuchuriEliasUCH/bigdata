# Documentation flow refresh

## Objective and scope
Refresh the four existing Spanish Markdown files in `documentacion/` against the current implementation. Prioritize this task; do not change source code, runtime configuration, notebooks, or unrelated files.

## Plan
- [x] D1: Map current ingestion, validation, Silver and compaction behavior.
- [x] D2: Update the four documents with accurate defaults, selection, checkpoints, recovery and outputs.
- [x] D3: Read back documents, check links and formatting, and record verification evidence.

## Routing and constraints
Delegated direct: multi-file documentation and source preparation require one bounded writer. Preserve the historical completed documentation plan. Documentation is professional Spanish. Test-first is not applicable to passive Markdown; validate structurally without running ingestion or APIs. Forecast: under 400 authored changed lines; ask-on-risk delivery strategy. No push or PR authorized.

## Acceptance and checks
Distinguish effective orchestrator defaults from module defaults; explain AI selection, proportional yearly plans, Parquet Bronze, Scopus bisection, Silver v3, automatic compaction/pruning, soft deadlines and connectivity limitations. Verify relative links and `git diff --check`. Do not inspect credentials or live data.

## Evidence and next step
Mapping completed using CodeGraph and bounded fallback reads. Parent spot-check confirmed automatic compaction and pruning in `src/compaction.py:160–192`. Writer read back all four documents and validated 35 local links. Parent reviewed all four diffs, corrected the operational year-range checklist and reran `git diff --check`. No runtime tests or ingestion performed: passive documentation only. RDD is enabled, but trivial passive documentation is exempt from native review. Next: record documentation work-unit commit.
