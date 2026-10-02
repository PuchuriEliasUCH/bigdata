"""Hard rejection rules for Silver, kept in one place because the filtering will change.

Only records that cannot be identified, titled or dated are rejected. Everything else
(missing topics, abstract, authors) is kept and exposed as a quality flag column, so the
final filter can be decided downstream without reprocessing Bronze.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _blank(column: str):
    return F.col(column).isNull() | (F.length(F.trim(F.col(column))) == 0)


REJECTION_RULES = (
    ("missing_source_id", lambda: _blank("source_id")),
    ("missing_title", lambda: _blank("title")),
    (
        "missing_publication_year_or_date",
        lambda: F.col("publication_year").isNull() & _blank("publication_date"),
    ),
)


def with_rejection_reasons(dataframe: DataFrame) -> DataFrame:
    """Add rejection_reason: every failed rule joined by '; ', empty when the record is valid."""
    reasons = F.array(*[F.when(condition(), F.lit(reason)) for reason, condition in REJECTION_RULES])
    return dataframe.withColumn("rejection_reason", F.concat_ws("; ", reasons))
