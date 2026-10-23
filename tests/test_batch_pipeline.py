"""End-to-end checks of the batch building blocks on the bundled sample."""

from __future__ import annotations

from pyspark.sql import SparkSession

from mfa_analytics import analysis
from mfa_analytics.cleaning import clean_beneficiaries, rename_source_columns, split_valid_records
from mfa_analytics.spark_io import read_source_csv


def _valid_sample(spark: SparkSession, sample_csv):
    raw = rename_source_columns(read_source_csv(spark, str(sample_csv)))
    valid, _ = split_valid_records(clean_beneficiaries(raw))
    return valid


def test_sample_is_read_with_expected_shape(spark: SparkSession, sample_csv) -> None:
    raw = read_source_csv(spark, str(sample_csv))
    assert len(raw.columns) == 22
    assert 0 < raw.count() <= 50


def test_rdd_and_dataframe_totals_match(spark: SparkSession, sample_csv) -> None:
    valid = _valid_sample(spark, sample_csv)
    by_department = {
        row.department_name: row.beneficiaries for row in analysis.by_department(valid).collect()
    }
    assert analysis.beneficiaries_by_department_rdd(valid) == by_department


def test_banking_rate_uses_known_status_only(spark: SparkSession, sample_csv) -> None:
    valid = _valid_sample(spark, sample_csv)
    row = analysis.national_summary(valid).first()
    assert row.banked + row.not_banked + row.banking_unknown == row.beneficiaries
    assert round(row.banked / (row.banked + row.not_banked), 4) == row.banking_rate
