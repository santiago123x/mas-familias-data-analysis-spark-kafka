"""Data-quality profiling of the raw and curated layers."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from mfa_analytics.cleaning import MISSING_TOKENS


@dataclass
class DataQualityReport:
    """Summary of the checks performed before the analysis."""

    raw_rows: int
    raw_columns: int
    exact_duplicate_rows: int
    valid_rows: int
    rejected_rows: int
    total_beneficiaries: int
    missing_by_column: dict[str, int] = field(default_factory=dict)
    placeholder_dates: dict[str, int] = field(default_factory=dict)
    thousands_separator_counts: int = 0
    municipality_department_mismatches: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Return the report as a JSON-serializable dictionary."""
        return asdict(self)


def count_missing_tokens(raw_df: DataFrame) -> dict[str, int]:
    """Count null or missing-token values per raw column in a single pass."""
    aggregations = [
        F.sum(
            F.when(
                F.col(name).isNull() | F.upper(F.trim(F.col(name))).isin(*MISSING_TOKENS), 1
            ).otherwise(0)
        ).alias(name)
        for name in raw_df.columns
    ]
    row = raw_df.agg(*aggregations).first()
    return {name: int(row[name] or 0) for name in raw_df.columns}


def build_quality_report(
    raw_df: DataFrame, valid_df: DataFrame, rejected_df: DataFrame
) -> DataQualityReport:
    """Profile the raw input and the result of cleaning.

    ``raw_df`` must have canonical column names (see ``rename_source_columns``).

    Note on duplicates: the source has no beneficiary identifier, and each row
    is a group of beneficiaries sharing the same attributes. Two identical rows
    can therefore describe different people, so duplicates are *reported* but
    not removed.
    """
    raw_rows = raw_df.count()
    distinct_rows = raw_df.distinct().count()

    placeholders = raw_df.agg(
        F.sum(F.when(F.col("enrollment_date").startswith("1900"), 1).otherwise(0)).alias(
            "enrollment_date"
        ),
        F.sum(F.when(F.col("last_benefit_date").startswith("1900"), 1).otherwise(0)).alias(
            "last_benefit_date"
        ),
        F.sum(F.when(F.col("beneficiary_count").contains("."), 1).otherwise(0)).alias("dotted"),
    ).first()

    mismatches = valid_df.filter(
        F.col("municipality_code").isNotNull()
        & (F.substring("municipality_code", 1, 2) != F.col("department_code"))
    ).count()

    total = valid_df.agg(F.sum("beneficiary_count")).first()[0] or 0

    return DataQualityReport(
        raw_rows=raw_rows,
        raw_columns=len(raw_df.columns),
        exact_duplicate_rows=raw_rows - distinct_rows,
        valid_rows=valid_df.count(),
        rejected_rows=rejected_df.count(),
        total_beneficiaries=int(total),
        missing_by_column=count_missing_tokens(raw_df),
        placeholder_dates={
            "enrollment_date": int(placeholders["enrollment_date"] or 0),
            "last_benefit_date": int(placeholders["last_benefit_date"] or 0),
        },
        thousands_separator_counts=int(placeholders["dotted"] or 0),
        municipality_department_mismatches=mismatches,
    )
