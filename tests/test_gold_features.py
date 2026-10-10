import pytest

from src.gold_features import FEATURE_COLUMNS
from src.gold_pipeline import topic_year_grid, year_totals
from src.gold_rules import growth

_UNIVERSE = (
    "topic_id string, topic_name string, subfield string, field string, domain string, publication_year int, "
    "authors array<struct<id:string>>, institutions array<struct<id:string>>, country_codes array<string>, "
    "is_oa boolean, work_type_norm string, source_id string, ingested_at string"
)
YEARS = list(range(2011, 2026))
LATER_YEAR = 2018


def _pub(topic, year, authors=(), institutions=(), countries=(), oa=False, kind="article", subfield=None):
    return (topic, f"Topic {topic}", subfield or f"Sub {topic}", "Field", "Domain", year,
            [(author,) for author in authors], [(institution,) for institution in institutions], list(countries),
            oa, kind, f"W-{topic}-{year}-{','.join(str(a) for a in authors)}", "2026-10-01T00:00:00Z")


def _base_pubs():
    """K is observed in 2011 with a hand-checked community; L appears in 2013 in K's subfield; M fills every year."""
    pubs = [
        _pub("K", 2011, ["a", "b"], ["I1"], ["US"], oa=True),
        _pub("K", 2012, ["b", "c"], ["I1", "I2"], ["US", "FR"], kind="conference_paper"),
        _pub("K", 2012, ["c", "d", None], ["I2", None], ["FR"], oa=True, kind="review"),
        _pub("K", 2013, ["a"]),
        *(_pub("K", 2015, [f"e{i}"]) for i in range(3)),
    ]
    for year, n in ((2013, 1), (2014, 2), (2015, 3), (2016, 4), (2017, 2)):
        pubs += [_pub("L", year, [f"l{year}-{i}"], subfield="Sub K") for i in range(n)]
    for year in YEARS:
        pubs += [_pub("M", year, [f"m{year}-{i}", "m-core"], [f"IM{i}"], ["DE"], oa=i % 2 == 0) for i in range(4)]
    return pubs


def _grid(spark, pubs):
    universe = spark.createDataFrame(pubs, _UNIVERSE)
    return topic_year_grid(universe, year_totals(universe, {}))


def _rows(grid):
    return {(row["topic_id"], row["year"]): row for row in grid.collect()}


@pytest.fixture
def rows(spark):
    return _rows(_grid(spark, _base_pubs()))


def test_feature_columns_exist_and_exclude_labels(spark, rows):
    columns = set(next(iter(rows.values())).asDict())
    assert set(FEATURE_COLUMNS) <= columns
    assert len(set(FEATURE_COLUMNS)) == len(FEATURE_COLUMNS)
    forbidden = [name for name in FEATURE_COLUMNS if name.startswith("label_") or name in
                 {"target_emerging", "eligible_for_label", "label_end_year", "cited_by_count"}]
    assert forbidden == []


def test_lags_are_null_before_first_year_and_real_zero_inside(rows):
    assert rows[("K", 2011)]["prevalence_t_minus_1"] is None and rows[("K", 2011)]["prevalence_t_minus_2"] is None
    assert rows[("K", 2012)]["prevalence_t_minus_1"] == rows[("K", 2011)]["prevalence_t"]
    assert rows[("K", 2012)]["prevalence_t_minus_2"] is None
    # L was not published in 2011-2012: inside the window a missing year is a real 0.
    assert rows[("L", 2013)]["prevalence_t_minus_1"] == 0.0 and rows[("L", 2013)]["prevalence_t_minus_2"] == 0.0
    assert rows[("L", 2013)]["growth_1y"] is None and rows[("L", 2013)]["growth_2y_past"] is None
    assert rows[("K", 2011)]["consecutive_growth_years"] is None and rows[("K", 2011)]["growth_1y"] is None


def test_dynamics_match_the_prevalence_series(rows):
    for topic in ("K", "L", "M"):
        series = [rows[(topic, year)] for year in YEARS]
        for index, row in enumerate(series):
            p = row["prevalence_t"]
            p1 = series[index - 1]["prevalence_t"] if index >= 1 else None
            p2 = series[index - 2]["prevalence_t"] if index >= 2 else None
            assert row["growth_1y"] == pytest.approx(growth(p1, p)) if growth(p1, p) is not None else row["growth_1y"] is None
            expected_2y = growth(p2, p)
            assert row["growth_2y_past"] == pytest.approx(expected_2y) if expected_2y is not None else row["growth_2y_past"] is None
            previous_growth = series[index - 1]["growth_1y"] if index >= 1 else None
            if row["growth_1y"] is None or previous_growth is None:
                assert row["acceleration"] is None
            else:
                assert row["acceleration"] == pytest.approx(row["growth_1y"] - previous_growth)
            if index == 0:
                assert row["consecutive_growth_years"] is None
            else:
                streak = 0
                while index - streak >= 1 and series[index - streak]["prevalence_t"] > series[index - streak - 1]["prevalence_t"]:
                    streak += 1
                assert row["consecutive_growth_years"] == streak


def test_community_and_composition_on_hand_computed_example(rows):
    k2011, k2012, k2013, k2014 = (rows[("K", year)] for year in (2011, 2012, 2013, 2014))
    assert (k2011["unique_authors_t"], k2011["new_author_ratio_t"], k2011["authors_per_pub_t"]) == (2, 1.0, 2.0)
    assert (k2011["unique_institutions_t"], k2011["unique_countries_t"], k2011["oa_ratio_t"]) == (1, 1, 1.0)
    # 2012: authors {b, c, d} (null id ignored), b already seen in 2011 -> 2 new of 3.
    assert k2012["unique_authors_t"] == 3 and k2012["new_author_ratio_t"] == pytest.approx(2 / 3)
    assert k2012["authors_per_pub_t"] == pytest.approx(2.5)
    assert (k2012["unique_institutions_t"], k2012["unique_countries_t"]) == (2, 2)
    assert (k2012["oa_ratio_t"], k2012["conference_ratio_t"], k2012["review_ratio_t"]) == (0.5, 0.5, 0.5)
    # 2013: only a, seen in 2011.
    assert k2013["unique_authors_t"] == 1 and k2013["new_author_ratio_t"] == 0.0
    # 2014: no publications -> counts are 0 and ratios are NULL.
    assert (k2014["n_t"], k2014["unique_authors_t"], k2014["unique_institutions_t"], k2014["unique_countries_t"]) == (0, 0, 0, 0)
    for name in ("new_author_ratio_t", "authors_per_pub_t", "oa_ratio_t", "conference_ratio_t", "review_ratio_t"):
        assert k2014[name] is None


def test_age_and_left_censoring(rows):
    assert rows[("L", 2012)]["years_since_first_observed"] is None
    assert rows[("L", 2013)]["years_since_first_observed"] == 0
    assert rows[("L", 2016)]["years_since_first_observed"] == 3
    assert rows[("K", 2014)]["years_since_first_observed"] == 3
    assert all(rows[("L", year)]["age_left_censored"] is False for year in YEARS)
    assert all(rows[("K", year)]["age_left_censored"] is True for year in YEARS)


def test_subfield_context_and_percentile(rows):
    k2013, l2013 = rows[("K", 2013)], rows[("L", 2013)]
    assert k2013["subfield_prevalence_t"] == pytest.approx(2 / 6) == l2013["subfield_prevalence_t"]
    k2012 = rows[("K", 2012)]
    assert k2012["subfield_prevalence_t"] == pytest.approx(2 / 6)
    assert k2013["subfield_growth_1y"] == pytest.approx(0.0)
    assert rows[("K", 2011)]["subfield_growth_1y"] is None
    assert all(0.0 <= row["prevalence_percentile_t"] <= 1.0 for row in rows.values())
    assert rows[("M", 2011)]["prevalence_percentile_t"] == 1.0


def test_no_feature_reads_years_after_t(spark, rows):
    later = [
        _pub("K", LATER_YEAR, ["a", "z"], ["I9"], ["JP"], oa=True),
        *(_pub("M", LATER_YEAR, [f"x{i}"]) for i in range(5)),
        # A topic first seen in the later year, sharing K's subfield.
        *(_pub("Q", LATER_YEAR, [f"q{i}"], subfield="Sub K") for i in range(7)),
    ]
    extended = _rows(_grid(spark, _base_pubs() + later))
    assert extended[("Q", LATER_YEAR)]["n_t"] == 7
    changed = [
        (key, name)
        for key, row in rows.items() if key[1] < LATER_YEAR
        for name in FEATURE_COLUMNS if extended[key][name] != row[name]
    ]
    assert changed == []
    # The later year itself does see the new publications.
    assert extended[("K", LATER_YEAR)]["n_t"] == rows[("K", LATER_YEAR)]["n_t"] + 1
