"""Spark transformations for the Kafka stream.

These functions only depend on a DataFrame with a binary ``value`` column, so
they work on the Kafka source in production and on a static DataFrame in tests.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from mfa_analytics.cleaning import clean_beneficiaries, is_valid_record
from mfa_analytics.schema import EVENT_SCHEMA


def parse_events(kafka_df: DataFrame) -> DataFrame:
    """Decode the JSON payload, type the event and apply the shared cleaning rules.

    Malformed JSON does not stop the stream: ``from_json`` returns null for it
    and the record is flagged as invalid. A ``is_valid`` column and the original
    ``raw_value`` are kept to route and inspect rejected messages.
    """
    raw_value = F.col("value").cast("string")
    decoded = kafka_df.select(
        raw_value.alias("raw_value"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(raw_value, EVENT_SCHEMA).alias("event"),
    ).select("raw_value", "kafka_timestamp", "event.*")

    events = decoded.withColumn("event_time", F.to_timestamp("event_time"))
    curated = clean_beneficiaries(events)
    return curated.withColumn(
        "is_valid",
        F.col("event_id").isNotNull() & F.col("event_time").isNotNull() & is_valid_record(),
    ).withColumn("rejection_reason", rejection_reason())


def rejection_reason() -> Column:
    """Human-readable reason why an event is invalid (null for valid events)."""
    return (
        F.when(F.col("is_valid"), F.lit(None))
        .when(F.col("event_id").isNull(), "malformed JSON or missing metadata")
        .when(F.col("event_time").isNull(), "invalid event_time")
        .when(F.col("department_code").isNull(), "missing department_code")
        .when(F.col("beneficiary_count").isNull(), "invalid beneficiary_count")
        .otherwise("failed validation rules")
    )


def window_metrics(valid_events: DataFrame, window: str, watermark: str) -> DataFrame:
    """Per-department metrics over tumbling event-time windows.

    The watermark bounds the state Spark keeps: events arriving later than
    ``watermark`` behind the latest event time are ignored for closed windows.
    """
    count = F.col("beneficiary_count")
    banked = F.sum(F.when(F.col("is_banked"), count).otherwise(0))
    known = F.sum(F.when(F.col("is_banked").isNotNull(), count).otherwise(0))
    return (
        valid_events.withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window).alias("window"), "department_name")
        .agg(
            F.count(F.lit(1)).alias("events"),
            F.sum(count).alias("beneficiaries"),
            F.round(banked / known, 3).alias("banking_rate"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "department_name",
            "events",
            "beneficiaries",
            "banking_rate",
        )
    )
