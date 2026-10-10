"""Features of the Gold topic x year grid: every value of row (k, t) reads only years <= t.

Gold stays model-independent: no scaling, encoding or imputation, and `cited_by_count` is
never read. FEATURE_COLUMNS is the single list of model inputs; `label_*` audit columns,
`eligible_for_label` and `target_emerging` are never features.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.gold_rules import FIRST_YEAR, growth_column

FEATURE_COLUMNS = (
    # Volume
    "n_t",
    "prevalence_t",
    "prevalence_percentile_t",
    # Dynamics (lags before FIRST_YEAR are NULL, never 0)
    "prevalence_t_minus_1",
    "prevalence_t_minus_2",
    "growth_1y",
    "growth_2y_past",
    "acceleration",
    "consecutive_growth_years",
    # Age
    "years_since_first_observed",
    "age_left_censored",
    # Community
    "unique_authors_t",
    "unique_institutions_t",
    "unique_countries_t",
    "authors_per_pub_t",
    "new_author_ratio_t",
    # Context
    "subfield",
    "field",
    "domain",
    "subfield_prevalence_t",
    "subfield_growth_1y",
    # Composition
    "oa_ratio_t",
    "conference_ratio_t",
    "review_ratio_t",
)


def _ratio(numerator: Column, denominator: Column) -> Column:
    return F.when(denominator > 0, numerator / denominator)


def _share(condition: Column) -> Column:
    return F.avg(F.when(condition, 1.0).otherwise(0.0))


def _distinct_count(publications: DataFrame, values: Column, name: str) -> DataFrame:
    exploded = publications.select("topic_id", "year", F.explode(values).alias("_value"))
    return exploded.filter(F.col("_value").isNotNull()).groupBy("topic_id", "year").agg(
        F.countDistinct("_value").alias(name)
    )


def _community(universe: DataFrame) -> DataFrame:
    """Per (topic, year) with publications: community size, newcomers and composition."""
    publications = universe.select(
        "topic_id", F.col("publication_year").alias("year"), "authors", "institutions", "country_codes",
        "is_oa", "work_type_norm",
    )
    composition = publications.groupBy("topic_id", "year").agg(
        F.avg(F.when(F.col("authors").isNotNull(), F.size("authors"))).alias("authors_per_pub_t"),
        _share(F.coalesce(F.col("is_oa"), F.lit(False))).alias("oa_ratio_t"),
        _share(F.col("work_type_norm") == "conference_paper").alias("conference_ratio_t"),
        _share(F.col("work_type_norm") == "review").alias("review_ratio_t"),
    )
    # The first year an author appears in a topic is <= t whenever the author appears in t,
    # so later years never change whether an author of year t is new.
    authors = (
        publications.select("topic_id", "year", F.explode("authors.id").alias("author_id"))
        .filter(F.col("author_id").isNotNull())
        .distinct()
        .withColumn("_first_year", F.min("year").over(Window.partitionBy("topic_id", "author_id")))
        .groupBy("topic_id", "year")
        .agg(
            F.count(F.lit(1)).alias("unique_authors_t"),
            F.sum(F.when(F.col("_first_year") == F.col("year"), 1).otherwise(0)).alias("_new_authors"),
        )
    )
    return (
        composition.join(authors, ["topic_id", "year"], "left")
        .join(_distinct_count(publications, F.col("institutions.id"), "unique_institutions_t"), ["topic_id", "year"], "left")
        .join(_distinct_count(publications, F.col("country_codes"), "unique_countries_t"), ["topic_id", "year"], "left")
    )


def add_features(grid: DataFrame, universe: DataFrame, *, first_year: int = FIRST_YEAR) -> DataFrame:
    """Add FEATURE_COLUMNS to a dense grid holding topic_id, year, subfield, n_t, n_total and prevalence_t.

    The grid must hold every year from first_year, so the k-th preceding row of a topic is year t - k.
    """
    by_topic = Window.partitionBy("topic_id").orderBy("year")
    history = by_topic.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    by_subfield_year = Window.partitionBy("subfield", "year")
    previous_subfield_year = Window.partitionBy("subfield").orderBy("year").rangeBetween(-1, -1)
    prevalence, year = F.col("prevalence_t"), F.col("year")
    rising = F.coalesce(prevalence > F.col("prevalence_t_minus_1"), F.lit(False))
    observed = F.col("years_since_first_observed").isNotNull()

    community = _community(universe)
    featured = (
        grid.join(community, ["topic_id", "year"], "left")
        .withColumn("prevalence_t_minus_1", F.lag("prevalence_t", 1).over(by_topic))
        .withColumn("prevalence_t_minus_2", F.lag("prevalence_t", 2).over(by_topic))
        .withColumn("growth_1y", growth_column(F.col("prevalence_t_minus_1"), prevalence))
        .withColumn("growth_2y_past", growth_column(F.col("prevalence_t_minus_2"), prevalence))
        .withColumn("acceleration", F.col("growth_1y") - F.lag("growth_1y", 1).over(by_topic))
        # Last year (<= t) without growth; first_year always counts as one since it has no lag.
        .withColumn("_last_flat_year", F.max(F.when(~rising, year)).over(history))
        .withColumn("consecutive_growth_years", F.when(year > first_year, year - F.col("_last_flat_year")).cast("int"))
        .withColumn("years_since_first_observed", (year - F.min(F.when(F.col("n_t") > 0, year)).over(history)).cast("int"))
        .withColumn(
            "age_left_censored",
            F.coalesce(F.max(F.when((year == first_year) & (F.col("n_t") > 0), 1)).over(history) == 1, F.lit(False)),
        )
        # Rank only topics already observed by t: topics first seen later must not shift earlier ranks.
        .withColumn("_observed", observed)
        .withColumn(
            "prevalence_percentile_t",
            F.when(observed, F.percent_rank().over(Window.partitionBy("year", "_observed").orderBy(prevalence)))
            .otherwise(F.lit(0.0)),
        )
        .withColumn("unique_authors_t", F.coalesce("unique_authors_t", F.lit(0)).cast("long"))
        .withColumn("unique_institutions_t", F.coalesce("unique_institutions_t", F.lit(0)).cast("long"))
        .withColumn("unique_countries_t", F.coalesce("unique_countries_t", F.lit(0)).cast("long"))
        .withColumn("new_author_ratio_t", _ratio(F.col("_new_authors"), F.col("unique_authors_t")))
        .withColumn("subfield_prevalence_t", _ratio(F.sum("n_t").over(by_subfield_year), F.col("n_total")))
        .withColumn(
            "subfield_growth_1y",
            growth_column(F.max("subfield_prevalence_t").over(previous_subfield_year), F.col("subfield_prevalence_t")),
        )
    )
    return featured.drop("_new_authors", "_last_flat_year", "_observed")
