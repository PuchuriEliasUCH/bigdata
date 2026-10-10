# Gold layer: topic × year training dataset

## Objective
Turn Silver v4 publications into a dense topic × year table with features computed only from data up
to the base year t and the binary target `target_emerging`, ready for the neural network and simple
baselines. Gold is model-independent: no scaling, encoding or imputation.

## Rationale
The emergence label was defined with the team on 2026-10-10 (Engram decision "Emergence label defined").
Measured on Silver v4: 3,492 topics, 3,308 labelable topic-years, 289 positives (8.7%) with the default
parameters; the positive rate is stable (8.3–8.7%) across min supports 1/5/10/20.

## Specification

### Universe
Silver v4 via `active_silver_paths`; `source = 'openalex'`, `is_eligible`, `matches_common_query`,
`publication_year` 2011–2025, `primary_topic` not null. Each publication contributes to exactly one topic
(its primary topic). The prevalence denominator `N_t` is the size of this same universe per year.

### Label (`src/gold_rules.py`, single source of constants)
- `HORIZON_YEARS = 2`, `GROWTH_THRESHOLD = 0.50`, `MIN_SUPPORT = 10`, `FIRST_YEAR = 2011`,
  `LAST_COMPLETE_YEAR = 2025`.
- `prevalence_t = n_t / N_t`; `growth_2y = (prevalence_{t+2} - prevalence_t) / prevalence_t`.
- `eligible_for_label = n_t >= MIN_SUPPORT and t + HORIZON_YEARS <= LAST_COMPLETE_YEAR`.
- `target_emerging = 1` if eligible and `growth_2y >= GROWTH_THRESHOLD` and
  `prevalence_{t+1} > prevalence_t` and `prevalence_{t+2} > prevalence_{t+1}`; `0` if eligible otherwise;
  `NULL` if not eligible.
- `label_end_year = t + HORIZON_YEARS` (null when not eligible).
- Future columns (`prevalence_t1`, `prevalence_t2`, `growth_2y_future`) may be stored for audit but are
  prefixed `label_` and documented as never-features.

### Features (only information from years ≤ t)
- Volume: `n_t`, `prevalence_t`, `prevalence_percentile_t` (percent rank of prevalence among topics in t).
- Dynamics: `prevalence_t_minus_1`, `prevalence_t_minus_2`, `growth_1y`, `growth_2y_past`,
  `acceleration` (growth_1y_t − growth_1y_{t−1}), `consecutive_growth_years`.
- Lags before 2011 are NULL, never 0; ratios with a zero or NULL denominator are NULL.
- Age: `years_since_first_observed` (t − first year with n > 0, NULL before first observation),
  `age_left_censored` (topic observed in 2011).
- Community: `unique_authors_t`, `unique_institutions_t`, `unique_countries_t`, `authors_per_pub_t`,
  `new_author_ratio_t` = authors of k in t not seen in k during 2011..t−1 ÷ unique authors of k in t.
- Context: `subfield`, `field`, `domain`, `subfield_prevalence_t`, `subfield_growth_1y` (ends at t).
- Composition: `oa_ratio_t`, `conference_ratio_t`, `review_ratio_t`.
- Excluded on purpose: `cited_by_count` and any value from t+1 or later.

### Outputs (`data/gold/`)
- `year_totals/`: `year`, `n_total`, sampling fraction vs `data/bronze/plans/openalex.json` totals.
- `topic_year/`: dense grid 3,492 × 15 = 52,380 rows; unique (`topic_id`, `year`); years without
  publications have `n_t = 0`.
- `_manifest.json`: silver schema version, gold version, start/end year, horizon, threshold, min support,
  topic assignment `primary_topic`, source `openalex`, topic count, labeled rows, positive rows,
  OpenAlex download window (min/max `ingested_at`) as the taxonomy snapshot, created_at.

### Temporal split (documented here, applied at training time)
A training row is valid for an evaluation window starting at year E when `label_end_year <= E`
(the label is known at the end of year E, when the prediction is made).
- Validation: base years 2019–2020, trained on 2011–2017.
- Final test: base years 2022–2023, retrained on 2011–2020.

## Tasks
- [x] GD-1 `src/gold_rules.py` (constants + pure label function), `src/gold_pipeline.py` (universe,
  year totals, dense grid, label, manifest), CLI `python -m src.gold_pipeline`. Tests on a small synthetic
  Silver.
  Evidence: 208 passed (writer full suite). Real data: universe 300,171 publications (no duplicate source_id),
  3,492 topics, 52,380 rows, 3,308 labelable, 289 positives — matches the independent calculation.
- [x] GD-2 Features with tests proving no feature reads years after t; NULL lags before 2011.
  Evidence: 219 passed (writer); parent spot check of gold tests passed. Real data unchanged (52,380 / 3,308 / 289).
  Deviation: `prevalence_percentile_t` ranks only topics observed by t (dense-grid rank leaked; caught by the
  leakage test). Review follow-ups from GD-1 applied: staged atomic publish, local unpersist, GoldError paths.
  Signal check (labelable rows, mean by class): clearest gaps in growth_2y_past 0.40 vs 0.55,
  consecutive_growth_years 1.00 vs 1.26, subfield_growth_1y 0.03 vs 0.10; volume features nearly identical.
- [ ] GD-3 Run in the container, `documentacion/6.gold.md`, notebook section: class distribution,
  sensitivity grid (growth 0.30/0.50/0.70 × support 5/10/20), sample emergent topics for manual review.

## Acceptance criteria
- `topic_year` has 52,380 rows and no duplicate (`topic_id`, `year`).
- `target_emerging` is NULL when `n_t < 10` or `year > 2023`; `label_end_year = year + 2` when not NULL.
- Defaults reproduce 3,308 labelable rows and 289 positives; otherwise do not move to modelling.
- Every feature of row t uses only information ≤ t; lags before 2011 are NULL.
- Each publication contributes to exactly one topic; `abs(sum(prevalence_t) − 1) < 1e-9` per year.

## Checks
- `docker compose -f compose.yaml run --rm --build --no-deps spark-pipeline pytest -q`
- Gold run in a container with `data/` writable (root-owned): `docker run --rm -v "$PWD/data:/data" ...
  python -m src.gold_pipeline`.

## Progress
- `main` fast-forwarded to 64759a1 (silver consolidation + global-bronze-cap). Branch `feat/gold-topic-year`.
- Pending from global-bronze-cap: GBC-3 (Scopus query in `.env`), configuration only, irrelevant while
  ingestion is frozen.

## Route log
- GD-1 delegated writer (2 new modules + tests).

## Next step
GD-1.
