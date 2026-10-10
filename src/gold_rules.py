"""Emergence label for the Gold topic x year table: single source of its constants.

A topic k in base year t is emerging when its prevalence (share of the year's universe)
rises strictly every year up to t + HORIZON_YEARS and grows by at least GROWTH_THRESHOLD
over that horizon. Rows without MIN_SUPPORT publications in t, or whose horizon passes
LAST_COMPLETE_YEAR, are not labeled (NULL, never 0).
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column
from pyspark.sql import functions as F

HORIZON_YEARS = 2
GROWTH_THRESHOLD = 0.50
MIN_SUPPORT = 10
FIRST_YEAR = 2011
LAST_COMPLETE_YEAR = 2025


def growth(base: float | None, end: float | None) -> float | None:
    """Relative change from base to end; None when base is missing or zero."""
    if base is None or end is None or base <= 0:
        return None
    return (end - base) / base


def is_eligible(
    n_t: int, year: int, *, horizon: int = HORIZON_YEARS, min_support: int = MIN_SUPPORT,
    last_complete_year: int = LAST_COMPLETE_YEAR,
) -> bool:
    """Enough support in t and a horizon that ends inside complete years."""
    return n_t >= min_support and year + horizon <= last_complete_year


def emergence_label(
    n_t: int,
    prevalences: Sequence[float | None],
    year: int,
    *,
    horizon: int = HORIZON_YEARS,
    growth_threshold: float = GROWTH_THRESHOLD,
    min_support: int = MIN_SUPPORT,
    last_complete_year: int = LAST_COMPLETE_YEAR,
) -> int | None:
    """1/0 emergence label for one topic-year; `prevalences` holds t, t+1, ..., t+horizon."""
    if len(prevalences) != horizon + 1:
        raise ValueError("prevalences must cover t through t + horizon")
    if not is_eligible(n_t, year, horizon=horizon, min_support=min_support, last_complete_year=last_complete_year):
        return None
    change = growth(prevalences[0], prevalences[-1])
    rising = all(
        before is not None and after is not None and after > before
        for before, after in zip(prevalences, prevalences[1:])
    )
    return int(change is not None and change >= growth_threshold and rising)


def growth_column(base: Column, end: Column) -> Column:
    """Spark twin of `growth`: NULL when base is NULL or not positive."""
    return F.when(base > 0, (end - base) / base)


def eligible_column(
    n_t: Column, year: Column, *, horizon: int = HORIZON_YEARS, min_support: int = MIN_SUPPORT,
    last_complete_year: int = LAST_COMPLETE_YEAR,
) -> Column:
    """Spark twin of `is_eligible`."""
    return (n_t >= min_support) & (year + horizon <= last_complete_year)


def emergence_label_column(
    n_t: Column,
    prevalences: Sequence[Column],
    year: Column,
    *,
    horizon: int = HORIZON_YEARS,
    growth_threshold: float = GROWTH_THRESHOLD,
    min_support: int = MIN_SUPPORT,
    last_complete_year: int = LAST_COMPLETE_YEAR,
) -> Column:
    """Spark twin of `emergence_label`; a NULL comparison counts as not emerging."""
    if len(prevalences) != horizon + 1:
        raise ValueError("prevalences must cover t through t + horizon")
    eligible = eligible_column(n_t, year, horizon=horizon, min_support=min_support, last_complete_year=last_complete_year)
    condition = growth_column(prevalences[0], prevalences[-1]) >= growth_threshold
    for before, after in zip(prevalences, prevalences[1:]):
        condition = condition & (after > before)
    emerging = F.when(F.coalesce(condition, F.lit(False)), 1).otherwise(0)
    return F.when(eligible, emerging).cast("int")
