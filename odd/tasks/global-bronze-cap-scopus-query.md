# Global Bronze seed cap and Scopus AI query

## Objective
Seed a combined 200,000-record Bronze corpus with an initial allocation of 100,000 raw API records per source (OpenAlex and Scopus), and configure a broad Scopus query for AI research. Bronze remains the source of truth; the accepted initial cap counts fetched raw records, including duplicate records across sources. Later incremental batches retain their existing per-run limits and are outside this initial-seed cap.

## Problem and rationale
The initial OpenAlex target is currently 200,000 by itself, while Scopus runs independently. This can produce more than 200,000 initial Bronze records and can starve the second source. Scopus currently has no configured search query. A reproducible broad query is needed at the Bronze collection stage; Silver filtering remains disposable and can evolve later.

## Scope
- Allocate up to 100,000 initial Bronze records to each source, with a combined hard limit of 200,000.
- Count provider records written to Bronze, including cross-source DOI duplicates; do not count deduplicated Silver rows toward the Bronze cap.
- Preserve each source's confirmed cursor/offset on pause and resume. A 429 pause for one source must not prevent the other source from using its own allocation.
- Configure a high-recall Scopus TITLE-ABS-KEY query with AI/ML and major subfield terms in the existing `.env`, preserving `SCOPUS_API` without reading or printing its value.
- Do not alter API credentials, perform live API calls, delete or rewrite data, or change the later incremental target policy.

## Constraints and decisions
- User-approved initial allocation: OpenAlex 100,000; Scopus 100,000. If one source exhausts its results before its allocation, the combined seed may be below 200,000; allocations are not transferred.
- TDD: strict red/green/refactor, as explicitly requested earlier by the user. Test runner: `docker compose run --rm --no-deps -v "$PWD:/app" --entrypoint python spark-pipeline -m pytest -q`.
- Route: delegated direct implementation. Evidence: the global cap crosses source clients, orchestration and checkpoint semantics, and requires changes/tests across multiple non-trivial files.
- Estimated authored change: approximately 300 lines, tests included; ordinary repository delivery policy applies.

## Tasks
- [x] GBC-1 Add tests proving the initial OpenAlex default is 100,000 and Scopus initial target is separately persisted; incremental default and combined allocation are documented.
- [x] GBC-2 Implement persisted per-source initial allocations and enforce each limit before fetching a page; keep checkpoints resumable after 429/restart. OpenAlex and Scopus initial defaults are 100,000; yearly Scopus partitions account for all persisted `mode == initial` records and stop when the shared allocation is consumed. Existing in-progress initial batches resume their stored targets/cursors and their checkpointed records count toward the allocation.
- [ ] GBC-3 Configure the broad Scopus AI/ML query without altering `SCOPUS_API`; test query loading and safe source configuration behavior. (Blocked: workspace permission policy denied access to `.env`; query not configured.)
- [x] GBC-4 Add integration tests for independent source pauses, completed/empty sources, total counts and repeated scheduler calls; run the full test suite and record results. Focused tests and full suite pass as recorded below.

## Acceptance criteria
- Initial Bronze API records across OpenAlex and Scopus never exceed 200,000; neither source exceeds its approved 100,000 allocation.
- Duplicate cross-source records count toward Bronze allocation; Silver cleanup does not affect the cap.
- A paused source resumes from its stored cursor/offset and the other source can continue within its own allocation.
- The Scopus query covers broad AI/ML research terms without restricting retrieval to only papers that describe something as an “emerging trend.”
- Subsequent incremental targets are unchanged.

## Progress and verification
- Branch: `feat/global-bronze-cap` (created from `main` before implementation).
- Task tracking route: delegated; trigger evidence recorded above.
- Engram mirror: pending; Engram could not confirm session registration. Local task document is authoritative until the mirror can be retried.
- Applicable check: full Docker Compose pytest command above.
- Strict TDD: focused tests were added and run before source edits (RED: 4 failed, 34 passed); after implementation the focused OpenAlex/Scopus/pipeline tests passed (GREEN: 41 passed).
- Strict TDD correction for shared Scopus year-partition allocation: added `test_year_partitions_share_one_initial_allocation` first and ran it RED (1 failed due exhaustion of mocked response sequence while later partition/batch fetched); after implementing persisted-metadata accounting, related yearly tests passed.
- Focused command: `docker compose run --rm --no-deps -v "$PWD:/app" --entrypoint python spark-pipeline -m pytest -q tests/test_scopus_ingest.py tests/test_openalex_ingest.py tests/test_run_pipeline.py` — 42 passed.
- Full suite command: `docker compose run --rm --no-deps -v "$PWD:/app" --entrypoint python spark-pipeline -m pytest -q` — 57 passed. This run supersedes the earlier failing run; scheduler.cron was not changed by this task.
- `.env` query/configuration and safe variable-name/count verification are pending. `SCOPUS_API` has not been read, printed, or modified.
- Runtime/API checks: not authorized or applicable for implementation tests; do not call external APIs.
- Rollback boundary: the new global initial allocation/configuration and Scopus query changes in this feature; restore prior per-source initial target and remove the query setting without reverting unrelated existing work.

## Next step
Complete GBC-3 by adding the approved broad search string to `.env` once the workspace permits updating that file, without reading or changing `SCOPUS_API`; then re-run the environment-safe configuration check. The initial-cap implementation and tests are complete.
