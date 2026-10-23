"""Cleaning and typing rules shared by the batch and streaming pipelines.

Every function here is a pure DataFrame -> DataFrame (or Column -> Column)
transformation built only from Spark SQL functions (no Python UDFs). That keeps
the rules fast, testable and usable unchanged on a streaming DataFrame.

Cleaning rules (documented in the README):

* Whitespace is trimmed and categorical text is upper-cased.
* Missing-value tokens (``""``, ``ND``, ``NULL``, ``NO DEFINIDO``,
  ``SIN ESPECIFICAR``) become ``null``. Rows are **not** dropped because of them;
  analyses report them as "NOT REPORTED".
* ``SI``/``NO`` flags become booleans (``null`` when unknown).
* ``1900-01-01`` is a placeholder for "no date" and becomes ``null``.
* ``beneficiary_count`` uses ``.`` as thousands separator (``"1.414"`` = 1414).
* Ethnicity labels with spelling variants are unified.
* Department/municipality codes keep their leading zeros (they are identifiers,
  not numbers).
* A record is **valid** when it has a department and a positive beneficiary
  count. Invalid records are separated, counted and reported, never silently lost.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from mfa_analytics.schema import CURATED_SCHEMA, SOURCE_TO_CANONICAL

MISSING_TOKENS: tuple[str, ...] = ("", "ND", "NULL", "NO DEFINIDO", "SIN ESPECIFICAR")
PLACEHOLDER_YEAR = 1900
NOT_REPORTED = "NOT REPORTED"

# Spelling variants observed in the source -> unified label.
ETHNICITY_ALIASES: dict[str, str] = {
    "AFROCOLOMBIANO": "AFROCOLOMBIANO - NEGRO",
    "ROM": "ROM - GITANO",
    "ROM O GITANO": "ROM - GITANO",
}

# A positive integer, optionally written with "." as thousands separator.
_COUNT_PATTERN = r"^\d{1,3}(\.\d{3})*$|^\d+$"


def rename_source_columns(df: DataFrame) -> DataFrame:
    """Rename the original Spanish CSV headers to canonical snake_case names."""
    return df.select([F.col(f"`{src}`").alias(dst) for src, dst in SOURCE_TO_CANONICAL.items()])


def null_if_missing(column: Column) -> Column:
    """Trim a string column and turn known missing-value tokens into null."""
    trimmed = F.trim(column)
    return F.when(F.upper(trimmed).isin(*MISSING_TOKENS), F.lit(None)).otherwise(trimmed)


def normalize_category(column: Column) -> Column:
    """Clean a categorical column: missing tokens -> null, collapsed spaces, upper case."""
    return F.upper(F.regexp_replace(null_if_missing(column), r"\s+", " "))


def parse_flag(column: Column) -> Column:
    """Map ``SI``/``NO`` to True/False; anything else (incl. missing) to null."""
    value = normalize_category(column)
    return F.when(value == "SI", F.lit(True)).when(value == "NO", F.lit(False))


def parse_date(column: Column) -> Column:
    """Parse ``yyyy-MM-dd`` dates; placeholder (1900) and malformed dates become null."""
    parsed = F.to_date(null_if_missing(column), "yyyy-MM-dd")
    return F.when(F.year(parsed) > PLACEHOLDER_YEAR, parsed)


def parse_count(column: Column) -> Column:
    """Parse the beneficiary count, removing ``.`` thousands separators.

    Values that are not well-formed positive integers become null so they can be
    rejected explicitly by :func:`split_valid_records`.
    """
    value = F.trim(column)
    return F.when(value.rlike(_COUNT_PATTERN), F.regexp_replace(value, r"\.", "").cast("int"))


def parse_code(column: Column, width: int) -> Column:
    """Normalize a numeric DANE code to a fixed-width, zero-padded string."""
    value = null_if_missing(column)
    return F.when(value.rlike(r"^\d+$"), F.lpad(value, width, "0"))


def normalize_ethnicity(column: Column) -> Column:
    """Unify dash variants and known aliases of the ethnicity label."""
    value = F.regexp_replace(normalize_category(column), "–", "-")  # en dash -> hyphen
    value = F.regexp_replace(value, r"\s*-\s*", " - ")
    mapped = value
    for alias, label in ETHNICITY_ALIASES.items():
        mapped = F.when(value == alias, F.lit(label)).otherwise(mapped)
    return mapped


def clean_beneficiaries(df: DataFrame) -> DataFrame:
    """Type and normalize a DataFrame with canonical (string) columns.

    Works for both the batch CSV (after :func:`rename_source_columns`) and the
    parsed Kafka events. Extra columns (e.g. event metadata) are preserved.

    Returns:
        A DataFrame whose first columns follow :data:`CURATED_SCHEMA` exactly.
    """
    curated = {
        "department_code": parse_code(F.col("department_code"), 2),
        "department_name": normalize_category(F.col("department_name")),
        "municipality_code": parse_code(F.col("municipality_code"), 5),
        "municipality_name": normalize_category(F.col("municipality_name")),
        "is_banked": parse_flag(F.col("is_banked")),
        "has_disability": parse_flag(F.col("has_disability")),
        "is_household_head": parse_flag(F.col("is_household_head")),
        "beneficiary_status": normalize_category(F.col("beneficiary_status")),
        "ethnicity": normalize_ethnicity(F.col("ethnicity")),
        "gender": normalize_category(F.col("gender")),
        "age_range": normalize_category(F.col("age_range")),
        "education_level": normalize_category(F.col("education_level")),
        "population_type": normalize_category(F.col("population_type")),
        "document_type": normalize_category(F.col("document_type")),
        "benefit_assignment_type": normalize_category(F.col("benefit_assignment_type")),
        "benefit_type": normalize_category(F.col("benefit_type")),
        "total_benefit_range": null_if_missing(F.col("total_benefit_range")),
        "last_benefit_range": null_if_missing(F.col("last_benefit_range")),
        "enrollment_date": parse_date(F.col("enrollment_date")),
        "last_benefit_date": parse_date(F.col("last_benefit_date")),
        "beneficiary_count": parse_count(F.col("beneficiary_count")),
    }
    typed = [curated[field.name].cast(field.dataType).alias(field.name) for field in CURATED_SCHEMA]
    passthrough = [
        F.col(name)
        for name in df.columns
        if name not in curated and name not in SOURCE_TO_CANONICAL.values()
    ]
    return df.select(*typed, *passthrough)


def is_valid_record() -> Column:
    """Validity rule for a curated record (department present and positive count)."""
    return (
        F.col("department_code").isNotNull()
        & F.col("department_name").isNotNull()
        & F.col("beneficiary_count").isNotNull()
        & (F.col("beneficiary_count") > 0)
    )


def split_valid_records(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a curated DataFrame into (valid, rejected) records."""
    rule = is_valid_record()
    return df.filter(rule), df.filter(~rule)


def label_missing(column_name: str) -> Column:
    """Replace null categories with an explicit label for grouping and charts."""
    return F.coalesce(F.col(column_name), F.lit(NOT_REPORTED)).alias(column_name)
