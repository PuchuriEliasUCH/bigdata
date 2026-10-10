"""Hard rejection rules for Silver, kept in one place because the filtering will change.

Only records that cannot be identified, titled or dated are rejected. Everything else
(missing topics, abstract, authors) is kept and exposed as a quality flag column, so the
final filter can be decided downstream without reprocessing Bronze.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.search_criteria import common_query_pattern


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


# Source labels, lowercased with "-"/"_" read as spaces, mapped to one shared vocabulary.
WORK_TYPES = {
    "article": "article",
    "review": "review",
    "conference paper": "conference_paper",
    "proceedings article": "conference_paper",
    "book chapter": "book_chapter",
    "book": "book",
    "preprint": "preprint",
    "editorial": "editorial",
    "letter": "letter",
    "erratum": "erratum",
}
ELIGIBLE_WORK_TYPES = ("article", "review", "conference_paper")


def _work_type_norm():
    key = F.regexp_replace(F.lower(F.trim(F.col("work_type"))), r"[\s_\-]+", " ")
    mapping = F.create_map(*[F.lit(value) for pair in WORK_TYPES.items() for value in pair])
    return F.coalesce(mapping[key], F.lit("other"))


def _not_true(column: str):
    return ~F.coalesce(F.col(column), F.lit(False))


def with_quality_flags(dataframe: DataFrame) -> DataFrame:
    """Add work_type_norm, is_eligible and matches_common_query; rows are never dropped.

    Run it on NFC-normalized text. Scopus rows always match: the API already applied the
    same query to title, abstract and keywords, which the STANDARD view does not return.
    """
    text = F.lower(F.concat_ws(" ", F.col("title"), F.col("abstract"), F.col("keywords")))
    matches = F.when(F.col("source") == "scopus", F.lit(True)).otherwise(text.rlike("(?U)" + common_query_pattern()))
    work_type = _work_type_norm()
    eligible = work_type.isin(*ELIGIBLE_WORK_TYPES) & _not_true("is_retracted") & _not_true("is_paratext")
    return (
        dataframe.withColumn("work_type_norm", work_type)
        .withColumn("is_eligible", eligible)
        .withColumn("matches_common_query", matches)
    )
