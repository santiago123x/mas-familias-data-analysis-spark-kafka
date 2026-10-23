"""Exploratory analysis of the curated beneficiaries data.

Business question:
    *Where are the program's beneficiaries concentrated, and in which territories
    and population groups is financial inclusion (being "bancarizado") lowest?*

Important modelling note: each row is a **group** of beneficiaries, so every
metric is weighted by ``beneficiary_count`` (a row count would be misleading).
The banking rate is computed only over beneficiaries with a known banking
status: ``banked / (banked + not banked)``. Unknown status is reported apart.
"""

from __future__ import annotations

from operator import add

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from mfa_analytics.cleaning import label_missing

BENEFICIARIES = "beneficiaries"


def _weighted_metrics() -> list[Column]:
    """Aggregations shared by every breakdown (all weighted by group size)."""
    count = F.col("beneficiary_count")
    banked = F.sum(F.when(F.col("is_banked"), count).otherwise(0))
    unbanked = F.sum(F.when(~F.col("is_banked"), count).otherwise(0))
    return [
        F.count(F.lit(1)).alias("records"),
        F.sum(count).alias(BENEFICIARIES),
        banked.alias("banked"),
        unbanked.alias("not_banked"),
        F.sum(F.when(F.col("is_banked").isNull(), count).otherwise(0)).alias("banking_unknown"),
        F.round(banked / (banked + unbanked), 4).alias("banking_rate"),
        F.round(
            F.sum(F.when(F.col("beneficiary_status") == "ACTIVO", count).otherwise(0))
            / F.sum(count),
            4,
        ).alias("active_share"),
    ]


def national_summary(df: DataFrame) -> DataFrame:
    """One-row summary with national totals and rates."""
    count = F.col("beneficiary_count")
    female = F.sum(F.when(F.col("gender") == "MUJER", count).otherwise(0))
    return df.agg(
        *_weighted_metrics(),
        F.round(female / F.sum(count), 4).alias("female_share"),
        F.countDistinct("department_code").alias("departments"),
        F.countDistinct("municipality_code").alias("municipalities"),
    )


def by_department(df: DataFrame) -> DataFrame:
    """Beneficiaries and banking rate per department, largest first."""
    return (
        df.groupBy("department_code", "department_name")
        .agg(*_weighted_metrics())
        .withColumn(
            "share_of_total",  # window over the small aggregated result (33 rows)
            F.round(F.col(BENEFICIARIES) / F.sum(BENEFICIARIES).over(Window.partitionBy()), 4),
        )
        .orderBy(F.desc(BENEFICIARIES))
    )


def by_category(df: DataFrame, column: str) -> DataFrame:
    """Weighted metrics for one categorical column (nulls labelled NOT REPORTED)."""
    return (
        df.groupBy(label_missing(column)).agg(*_weighted_metrics()).orderBy(F.desc(BENEFICIARIES))
    )


def by_age_and_gender(df: DataFrame) -> DataFrame:
    """Beneficiaries per age range and gender (long format, for grouped bars)."""
    return (
        df.groupBy(label_missing("age_range"), label_missing("gender"))
        .agg(F.sum("beneficiary_count").alias(BENEFICIARIES))
        .orderBy("age_range", "gender")
    )


def by_enrollment_year(df: DataFrame) -> DataFrame:
    """Beneficiaries per enrollment year (rows without a valid date are excluded)."""
    return (
        df.filter(F.col("enrollment_date").isNotNull())
        .groupBy(F.year("enrollment_date").alias("enrollment_year"))
        .agg(F.sum("beneficiary_count").alias(BENEFICIARIES))
        .orderBy("enrollment_year")
    )


def beneficiaries_by_department_rdd(df: DataFrame) -> dict[str, int]:
    """Compute beneficiaries per department with the RDD API.

    Included on purpose to apply the RDD transformations studied in the course
    (``map`` + ``reduceByKey``) and to cross-check the DataFrame result.
    ``reduceByKey`` combines values inside each partition before the shuffle,
    which is why it is preferred over ``groupByKey`` for aggregations.
    """
    pairs = df.select("department_name", "beneficiary_count").rdd.map(
        lambda row: (row["department_name"], row["beneficiary_count"])
    )
    return dict(pairs.reduceByKey(add).collect())  # small result: one row per department
