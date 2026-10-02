"""Silver column extraction: one function per source, same output columns and types."""

from __future__ import annotations

import unicodedata

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

AUTHOR_TYPE = "array<struct<id:string,name:string,orcid:string,position:string,is_corresponding:boolean>>"
INSTITUTION_TYPE = (
    "array<struct<id:string,name:string,ror:string,country_code:string,country_name:string,city:string,type:string>>"
)
TOPIC_TYPE = "array<struct<id:string,name:string,score:double,subfield:string,field:string,domain:string>>"
COUNT_TYPE = "array<struct<year:int,cited_by_count:long>>"

def clean_text(column: Column) -> Column:
    """Strip markup and collapse whitespace; blank text becomes null."""
    squeezed = F.trim(F.regexp_replace(F.regexp_replace(column, r"<[^>]+>", " "), r"\s+", " "))
    return F.when(squeezed == "", None).otherwise(squeezed)


_NFC = F.udf(lambda value: unicodedata.normalize("NFC", value) if value is not None else None, StringType())


def normalize_unicode(dataframe: DataFrame, columns: tuple[str, ...] = ("title", "abstract")) -> DataFrame:
    """NFC-normalize text columns.

    Apply it only to the final Silver rows, right before writing: a Python function
    cannot be evaluated inside the plans used by the deduplication joins, and PyArrow's
    own normalizer is not functional in this image.
    """
    for name in columns:
        dataframe = dataframe.withColumn(name, _NFC(F.col(name)))
    return dataframe


def _empty(ddl: str) -> Column:
    return F.array().cast(ddl)


def _bool(text: Column) -> Column:
    return F.when(text.isin("true", "false"), text.cast("boolean"))


def _int(text: Column, pattern: str = r"^-?\d+$") -> Column:
    return F.when(text.rlike(pattern), text.cast("long"))


_OA_TOPIC = (
    "array<struct<id:string,display_name:string,name:string,score:double,"
    "subfield:struct<id:string,display_name:string>,field:struct<id:string,display_name:string>,"
    "domain:struct<id:string,display_name:string>>>"
)
_OA_AUTHORSHIPS = (
    "array<struct<author_position:string,is_corresponding:boolean,"
    "author:struct<id:string,display_name:string,orcid:string>,countries:array<string>,"
    "institutions:array<struct<id:string,display_name:string,ror:string,country_code:string,type:string>>>>"
)
_OA_INSTITUTIONS = "array<struct<id:string,display_name:string,ror:string,country_code:string,type:string>>"


def _topic_struct(topic: Column) -> Column:
    return F.struct(
        F.trim(topic["id"]).alias("id"),
        F.coalesce(F.trim(topic["display_name"]), F.trim(topic["name"])).alias("name"),
        topic["score"].alias("score"),
        topic["subfield"]["display_name"].alias("subfield"),
        topic["field"]["display_name"].alias("field"),
        topic["domain"]["display_name"].alias("domain"),
    )


def _abstract(index: Column) -> Column:
    """Rebuild the abstract from OpenAlex's inverted index (word -> positions)."""
    pairs = F.flatten(
        F.transform(
            F.map_entries(index),
            lambda entry: F.transform(entry["value"], lambda pos: F.struct(pos.alias("pos"), entry["key"].alias("word"))),
        )
    )
    text = F.array_join(F.transform(F.array_sort(pairs), lambda pair: pair["word"]), " ")
    return clean_text(F.when(index.isNotNull() & (F.size(index) > 0), text))


def openalex_columns(records: DataFrame) -> DataFrame:
    """Extract Silver columns from OpenAlex works (`_record_json` holds the original record)."""

    def path(expression: str) -> Column:
        return F.get_json_object(F.col("_record_json"), expression)

    authorships = F.from_json(path("$.authorships"), _OA_AUTHORSHIPS)
    topics = F.from_json(path("$.topics"), _OA_TOPIC)
    primary = F.from_json(path("$.primary_topic"), _OA_TOPIC.replace("array<", "", 1)[:-1])
    keywords = F.from_json(path("$.keywords"), "array<struct<display_name:string>>")
    author_structs = F.transform(
        authorships,
        lambda a: F.struct(
            a["author"]["id"].alias("id"), a["author"]["display_name"].alias("name"), a["author"]["orcid"].alias("orcid"),
            a["author_position"].alias("position"), a["is_corresponding"].alias("is_corresponding"),
        ),
    )
    institution_structs = F.flatten(
        F.transform(
            authorships,
            lambda a: F.transform(
                F.coalesce(a["institutions"], _empty(_OA_INSTITUTIONS)),
                lambda i: F.struct(
                    i["id"].alias("id"), i["display_name"].alias("name"), i["ror"].alias("ror"),
                    i["country_code"].alias("country_code"), F.lit(None).cast("string").alias("country_name"),
                    F.lit(None).cast("string").alias("city"), i["type"].alias("type"),
                ),
            ),
        )
    )
    codes = F.flatten(F.transform(authorships, lambda a: F.coalesce(a["countries"], _empty("array<string>"))))
    return records.select(
        "record_order",
        F.col("_record_json").alias("record_json"),
        F.trim(path("$.id")).alias("source_id"),
        clean_text(path("$.title")).alias("title"),
        _int(F.trim(path("$.publication_year"))).cast("int").alias("publication_year"),
        F.trim(path("$.publication_date")).alias("publication_date"),
        F.trim(path("$.type")).alias("work_type"),
        F.trim(path("$.doi")).alias("doi"),
        F.trim(path("$.language")).alias("language"),
        F.trim(path("$.primary_location.source.display_name")).alias("venue"),
        _abstract(F.from_json(path("$.abstract_inverted_index"), "map<string,array<int>>")).alias("abstract"),
        F.coalesce(
            F.filter(F.transform(keywords, lambda k: F.trim(k["display_name"])), lambda x: x.isNotNull() & (x != "")),
            _empty("array<string>"),
        ).alias("keywords"),
        F.coalesce(F.transform(topics, _topic_struct).cast(TOPIC_TYPE), _empty(TOPIC_TYPE)).alias("topics"),
        F.when(primary.isNotNull(), _topic_struct(primary)).cast(
            "struct<id:string,name:string,score:double,subfield:string,field:string,domain:string>"
        ).alias("primary_topic"),
        F.when(authorships.isNotNull(), F.size(authorships)).cast("int").alias("author_count"),
        F.coalesce(author_structs.cast(AUTHOR_TYPE), _empty(AUTHOR_TYPE)).alias("authors"),
        F.coalesce(F.array_distinct(institution_structs).cast(INSTITUTION_TYPE), _empty(INSTITUTION_TYPE)).alias("institutions"),
        F.coalesce(F.array_sort(F.array_distinct(F.filter(codes, lambda c: c.isNotNull()))), _empty("array<string>")).alias("country_codes"),
        _empty("array<string>").alias("country_names"),
        _int(F.trim(path("$.cited_by_count"))).alias("cited_by_count"),
        F.coalesce(F.from_json(path("$.counts_by_year"), COUNT_TYPE), _empty(COUNT_TYPE)).alias("counts_by_year"),
        _bool(path("$.open_access.is_oa")).alias("is_oa"),
        _bool(path("$.is_retracted")).alias("is_retracted"),
        _bool(path("$.is_paratext")).alias("is_paratext"),
    )


_SCOPUS_AFFILIATION = ArrayType(
    StructType(
        [
            StructField("affilname", StringType()),
            StructField("affiliation-city", StringType()),
            StructField("affiliation-country", StringType()),
        ]
    )
)


def scopus_columns(records: DataFrame) -> DataFrame:
    """Extract Silver columns from Scopus STANDARD view entries.

    That view has no abstract, keywords, language or topics, so those stay empty or null
    instead of being invented; the country is a name, not an ISO code.
    """

    def field(name: str) -> Column:
        return F.trim(F.get_json_object(F.col("_record_json"), f"$.{name}"))

    raw_affiliation = F.get_json_object(F.col("_record_json"), "$.affiliation")
    # A single affiliation may arrive as an object instead of a one-element array.
    affiliation = F.from_json(
        F.when(raw_affiliation.startswith("{"), F.concat(F.lit("["), raw_affiliation, F.lit("]")))
        .otherwise(raw_affiliation),
        _SCOPUS_AFFILIATION,
    )
    creator = clean_text(field("dc:creator"))
    date = field("prism:coverDate")
    institution_structs = F.transform(
        affiliation,
        lambda a: F.struct(
            F.lit(None).cast("string").alias("id"), a["affilname"].alias("name"), F.lit(None).cast("string").alias("ror"),
            F.lit(None).cast("string").alias("country_code"), a["affiliation-country"].alias("country_name"),
            a["affiliation-city"].alias("city"), F.lit(None).cast("string").alias("type"),
        ),
    )
    country_names = F.filter(F.transform(affiliation, lambda a: F.trim(a["affiliation-country"])), lambda x: x.isNotNull() & (x != ""))
    return records.select(
        "record_order",
        F.col("_record_json").alias("record_json"),
        field("eid").alias("source_id"),
        clean_text(field("dc:title")).alias("title"),
        F.regexp_extract(date, r"^(\d{4})", 1).cast("int").alias("publication_year"),
        date.alias("publication_date"),
        field("subtypeDescription").alias("work_type"),
        field("prism:doi").alias("doi"),
        F.lit(None).cast("string").alias("language"),
        field("prism:publicationName").alias("venue"),
        F.lit(None).cast("string").alias("abstract"),
        _empty("array<string>").alias("keywords"),
        _empty(TOPIC_TYPE).alias("topics"),
        F.lit(None).cast("struct<id:string,name:string,score:double,subfield:string,field:string,domain:string>").alias("primary_topic"),
        F.lit(None).cast("int").alias("author_count"),
        F.when(
            creator.isNotNull(),
            F.array(F.struct(
                F.lit(None).cast("string").alias("id"), creator.alias("name"), F.lit(None).cast("string").alias("orcid"),
                F.lit("first").alias("position"), F.lit(None).cast("boolean").alias("is_corresponding"),
            )),
        ).otherwise(_empty(AUTHOR_TYPE)).cast(AUTHOR_TYPE).alias("authors"),
        F.coalesce(F.array_distinct(institution_structs).cast(INSTITUTION_TYPE), _empty(INSTITUTION_TYPE)).alias("institutions"),
        _empty("array<string>").alias("country_codes"),
        F.coalesce(F.array_sort(F.array_distinct(country_names)), _empty("array<string>")).alias("country_names"),
        _int(field("citedby-count")).alias("cited_by_count"),
        _empty(COUNT_TYPE).alias("counts_by_year"),
        _bool(F.get_json_object(F.col("_record_json"), "$.openaccessFlag")).alias("is_oa"),
        F.lit(None).cast("boolean").alias("is_retracted"),
        F.lit(None).cast("boolean").alias("is_paratext"),
    )
