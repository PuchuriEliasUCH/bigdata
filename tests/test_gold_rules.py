import pytest

from src.gold_rules import (
    FIRST_YEAR,
    GROWTH_THRESHOLD,
    HORIZON_YEARS,
    LAST_COMPLETE_YEAR,
    MIN_SUPPORT,
    emergence_label,
    growth,
)


def test_constants_match_the_agreed_rule():
    assert (HORIZON_YEARS, GROWTH_THRESHOLD, MIN_SUPPORT, FIRST_YEAR, LAST_COMPLETE_YEAR) == (2, 0.50, 10, 2011, 2025)


def test_steady_growth_above_threshold_is_emerging():
    assert emergence_label(20, [0.010, 0.013, 0.020], 2015) == 1


def test_growth_below_threshold_is_not_emerging():
    assert emergence_label(20, [0.010, 0.011, 0.012], 2015) == 0


def test_non_monotonic_peak_is_not_emerging():
    # Enough growth at t+2, but it fell between t+1 and t+2.
    assert emergence_label(20, [0.010, 0.030, 0.020], 2015) == 0


def test_growth_exactly_at_threshold_is_emerging():
    # Binary-exact values: (0.375 - 0.25) / 0.25 == 0.5.
    assert emergence_label(20, [0.25, 0.3125, 0.375], 2015) == 1


def test_low_support_is_not_labeled():
    assert emergence_label(MIN_SUPPORT - 1, [0.010, 0.013, 0.020], 2015) is None
    assert emergence_label(MIN_SUPPORT, [0.010, 0.013, 0.020], 2015) == 1


def test_years_without_a_complete_horizon_are_not_labeled():
    assert emergence_label(20, [0.010, 0.013, 0.020], 2023) == 1
    assert emergence_label(20, [0.010, 0.013, 0.020], 2024) is None


def test_parameters_are_overridable():
    assert emergence_label(5, [0.010, 0.013, 0.020], 2015, min_support=5) == 1
    assert emergence_label(20, [0.010, 0.013, 0.020], 2015, growth_threshold=1.5) == 0


def test_horizon_length_must_match_the_path():
    with pytest.raises(ValueError):
        emergence_label(20, [0.010, 0.013], 2015)


def test_growth_guards_zero_and_missing_base():
    assert growth(0.0, 0.5) is None
    assert growth(None, 0.5) is None
    assert growth(0.2, None) is None
    assert growth(0.2, 0.3) == pytest.approx(0.5)
