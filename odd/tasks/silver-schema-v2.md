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
- [x] S4 Validation rules in a single module (src/silver_rules.py); missing topics is now a flag, not a rejection.
- [x] S5 New columns above, schema version 3 (src/silver_columns.py; 130 tests pass). Unicode NFC runs as a Python UDF only on the final rows because Spark cannot evaluate it inside the dedup joins and this image's PyArrow normalizer does not work. Was blocked on confirming the real structure of OpenAlex authorships, institutions, topics, keywords and Scopus affiliation with one read-only sample record per API (needs user authorization). Bump SILVER_SCHEMA_VERSION to 3 when done.

## Progress / evidence
Route: inline. Engram unavailable (session registration error), so this file is the record.

## Real structures confirmed (one read-only record per API, 2026-10-02)
OpenAlex work: abstract_inverted_index is a map word -> positions (e.g. "language": [1, 118]); authorships[] has author_position, is_corresponding, author{id, display_name, orcid}, countries[] (ISO codes), institutions[]{id, display_name, ror, country_code, type}; topics[] and primary_topic have id, display_name, score, subfield{id, display_name}, field{...}, domain{...}; keywords[]{id, display_name, score}; counts_by_year[]{year, cited_by_count} (includes the current year); plus language, type, is_retracted, is_paratext, referenced_works_count, primary_location.source.display_name, open_access.is_oa.
Scopus entry (STANDARD view): dc:creator is only the first author ("Shalko M."); affiliation[]{affilname, affiliation-city, affiliation-country} with the country as a NAME (e.g. "Ukraine"), not an ISO code; citedby-count is a string; openaccessFlag is a boolean (so Scopus does have is_oa, contrary to the old docs); subtypeDescription ("Article"), prism:aggregationType, source-id, prism:publicationName, coverDate. No abstract, keywords, language or topics.
Consequence: country is kept honestly per source (country_codes from OpenAlex, country_names from Scopus) instead of inventing a name-to-ISO mapping; reconciling them is a later step.
Decision: missing topics no longer rejects an OpenAlex record; it becomes the has_topics flag, as approved (flags never reject).

Real bounded run of Silver v3 (3 cycles, 2024-2025, target 3000, compaction threshold 2): all 31 expected columns present, schema_version 3 on every row. Fill rates, OpenAlex (1,203 rows): abstract 85%, language 100%, venue 89%, authors 99%, institutions 97%, country_codes 96%, topics 100%, keywords 100%, counts_by_year 100%. Scopus (1,302 rows): abstract 0%, authors 100% (first author only), institutions 99%, country_names 99%, is_oa filled. Timings: cycle 99.8 s and 116.9 s; the second spent 85.7 s ingesting and 30.2 s on Spark plus compaction (Spark budget is 25 s but a batch already started cannot be cut), leaving about 3 s of margin in the two-minute cycle.
Bug found by the real run and fixed (regression test added, 131 tests pass): cross-batch dedup listed previous Silver paths only for batches created before the current one, so a compacted group containing a later batch left the pruned originals in the list and Spark raised PATH_NOT_FOUND on every following run. Dedup now compares against every completed Silver batch regardless of creation order.
Observations for later: Scopus work_type includes Retracted and Erratum records (type filtering belongs to the Silver filtering step) while is_retracted stays null for Scopus; it could be derived from the subtype.

## Next step
Validate the new Silver with a real bounded run (needs user authorization), then start the real ingestion.
