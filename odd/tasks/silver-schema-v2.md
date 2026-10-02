# Feature: silver-schema-v2

## Objective
Give Silver the columns the study needs (context.md: topic-per-year prediction with NLP, bibliometric and temporal features) and make Silver fully automatic: derived only from Bronze, rebuilt without manual steps when its schema changes, bounded by time so it never overruns the two-minute cycle.

## Approved schema (user approved on 2026-10-02)
- Identity and trace: source, source_id, batch_id, doi, normalized_doi, ingested_at, schema_version.
- Bibliographic: title, publication_year, publication_date, work_type, language, venue.
- NLP text: abstract (rebuilt from OpenAlex abstract_inverted_index), keywords. Scopus standard view has neither.
- Topics: topics (id, name, subfield, field, domain), primary_topic (OpenAlex only).
- Authorship and geography: author_count, authors, institutions, countries (OpenAlex full, Scopus partial: first author and affiliation).
- Citations without temporal leakage: cited_by_count (snapshot), counts_by_year (OpenAlex only).
- Quality flags that never reject: has_abstract, has_topics, is_oa, is_retracted, is_paratext.
- Text cleaning of title and abstract (whitespace, HTML tags, Unicode normalization).

## Constraints
- Bronze keeps the original record (record_json), so Silver can always be rebuilt.
- The Silver filtering algorithm will change later: flags instead of hard rejections, and validation rules in one module.
- Never print credentials. Real API calls need explicit user authorization.

## Tasks
- [x] S1 source_id is the only identifier; reasons missing_source_id / duplicate_source_id(_previous_batch) (commit 920d20f).
- [x] S2 SILVER_SCHEMA_VERSION + automatic rebuild from Bronze, guarded so nothing is deleted if Bronze is missing (123 tests pass).
- [x] S3 Spark time budget (SILVER_TIME_BUDGET_SECONDS=25), at least one batch per run, pending count in the report.
- [ ] S4 Validation rules in a single module (src/silver_rules.py).
- [ ] S5 New columns above. Blocked on confirming the real structure of OpenAlex authorships, institutions, topics, keywords and Scopus affiliation with one read-only sample record per API (needs user authorization). Bump SILVER_SCHEMA_VERSION to 3 when done.

## Progress / evidence
Route: inline. Engram unavailable (session registration error), so this file is the record.

## Next step
Get authorization for one-record samples from OpenAlex and Scopus, then implement S5 with tests built from the real structures.
