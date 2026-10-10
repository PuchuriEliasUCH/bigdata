# Detailed pipeline guides (Bronze, Silver, Gold)

## Objective
Rewrite `documentacion/` into one coherent, detailed guide set: the end-to-end process, each layer
(what it does, how data moves to the next phase, what results each layer can already produce), the
ingestion process in depth, and the filtering process. Verified against the code, in Spanish.

## Scope
- Documentation only (passive). No code or data changes.
- Restructure: overview + ingestion + Bronze + Silver (merge the two current Silver guides) + Gold +
  operations + limitations; fix every link (README.md, notebooks, docs).

## Tasks
- [x] PG-1 Rewrite and restructure `documentacion/` and update links.
  Evidence: 7 guides (0.README, 1.ingesta, 2.bronze, 3.silver, 4.gold, 5.operacion, 6.limitaciones); README
  rewritten; link check 148 links + 23 anchors, 0 problems; notebooks changed only in markdown cells.
  Code/doc discrepancies found: `scopus_query()` default includes AI/IA while the used query (plan key) does not,
  and its test name says otherwise; notebook `pct_authors` uses author_count (null for Scopus);
  docs/ (English, stale) left untouched.

## Acceptance criteria
- Every factual claim traceable to code (`path:line` or function names) or to measured results.
- Each layer guide has: purpose, inputs, process, outputs, guarantees, "how it moves to the next phase",
  "results you can obtain here" with current figures.
- No broken relative links.

## Route log
- PG-1 delegated writer (7+ documents, needs code reading).

## Next step
Done. Open: fold or update the English docs/ folder.
