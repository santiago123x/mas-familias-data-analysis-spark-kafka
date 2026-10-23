"""Tests for the shared cleaning and typing rules."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import SparkSession

from mfa_analytics.cleaning import clean_beneficiaries, split_valid_records
from mfa_analytics.schema import CANONICAL_COLUMNS, CURATED_SCHEMA

BASE_RECORD: dict[str, str] = {
    "is_banked": "SI",
    "department_code": "8",
    "municipality_code": "8421",
    "has_disability": "NO",
    "beneficiary_status": "ACTIVO",
    "ethnicity": "AFROCOLOMBIANO – NEGRO",
    "enrollment_date": "2012-12-01",
    "gender": "Mujer",
    "education_level": "ND",
    "department_name": " atlantico ",
    "municipality_name": "LURUACO",
    "country": "ND",
    "benefit_assignment_type": "MONETARIO",
    "benefit_type": "ND",
    "document_type": "CC",
    "population_type": "UNIDOS",
    "total_benefit_range": "4.500.001 - 6.000.000",
    "last_benefit_range": "0 - 1.300.000",
    "last_benefit_date": "1900-01-01",
    "age_range": "30-49",
    "is_household_head": "SI",
    "beneficiary_count": "1",
}


def _clean(spark: SparkSession, *overrides: dict[str, str]):
    rows = [{**BASE_RECORD, **override} for override in overrides]
    df = spark.createDataFrame(rows).select(*CANONICAL_COLUMNS)
    return clean_beneficiaries(df).collect()


def test_curated_schema_matches_contract(spark: SparkSession) -> None:
    df = spark.createDataFrame([BASE_RECORD]).select(*CANONICAL_COLUMNS)
    assert clean_beneficiaries(df).schema == CURATED_SCHEMA


def test_typing_and_normalization(spark: SparkSession) -> None:
    (row,) = _clean(spark, {})
    assert row.is_banked is True
    assert row.has_disability is False
    assert row.department_code == "08"  # leading zero restored
    assert row.municipality_code == "08421"
    assert row.department_name == "ATLANTICO"  # trimmed and upper-cased
    assert row.gender == "MUJER"
    assert row.education_level is None  # "ND" -> null
    assert row.enrollment_date == dt.date(2012, 12, 1)
    assert row.last_benefit_date is None  # 1900-01-01 placeholder -> null
    assert row.ethnicity == "AFROCOLOMBIANO - NEGRO"  # en dash unified


def test_beneficiary_count_parsing(spark: SparkSession) -> None:
    rows = _clean(
        spark,
        {"beneficiary_count": "1.414"},
        {"beneficiary_count": "14.701"},
        {"beneficiary_count": "21"},
        {"beneficiary_count": "1.2"},
        {"beneficiary_count": "abc"},
    )
    assert [r.beneficiary_count for r in rows] == [1414, 14701, 21, None, None]


def test_unknown_flags_and_ethnicity_aliases(spark: SparkSession) -> None:
    rows = _clean(
        spark,
        {"is_banked": "", "ethnicity": "ROM"},
        {"is_banked": "ND", "ethnicity": "AFROCOLOMBIANO"},
    )
    assert [r.is_banked for r in rows] == [None, None]
    assert [r.ethnicity for r in rows] == ["ROM - GITANO", "AFROCOLOMBIANO - NEGRO"]


def test_invalid_records_are_separated_not_lost(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            BASE_RECORD,
            {**BASE_RECORD, "beneficiary_count": "0"},
            {**BASE_RECORD, "beneficiary_count": "x"},
            {**BASE_RECORD, "department_code": "ND"},
        ]
    ).select(*CANONICAL_COLUMNS)
    valid, rejected = split_valid_records(clean_beneficiaries(df))
    assert (valid.count(), rejected.count()) == (1, 3)
