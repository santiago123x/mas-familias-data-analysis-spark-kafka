"""Tests for event generation (pure Python) and stream parsing (static DataFrame)."""

from __future__ import annotations

import json
import random
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from mfa_analytics.streaming.events import (
    RandomRowSampler,
    build_event,
    corrupt_event,
    event_key,
    serialize_event,
    to_canonical_record,
)
from mfa_analytics.streaming.transform import parse_events, window_metrics


def _sample_events(sample_csv, n: int, seed: int = 7) -> list[dict]:
    with RandomRowSampler(sample_csv, seed=seed) as sampler:
        return [build_event(to_canonical_record(sampler.sample())) for _ in range(n)]


def test_sampler_is_reproducible(sample_csv) -> None:
    first = [e["municipality_code"] for e in _sample_events(sample_csv, 10)]
    second = [e["municipality_code"] for e in _sample_events(sample_csv, 10)]
    assert first == second


def test_event_is_flagged_as_simulated_and_keyed(sample_csv) -> None:
    event = _sample_events(sample_csv, 1)[0]
    assert event["simulated"] is True
    assert event["source"] == "mfa_csv_replay"
    assert "country" not in event  # dropped column is not sent
    assert event_key(event) == event["department_code"].encode()
    assert json.loads(serialize_event(event)) == event


def test_parse_events_validates_messages(spark: SparkSession, sample_csv) -> None:
    good = _sample_events(sample_csv, 3)
    rng = random.Random(1)
    messages = [serialize_event(e) for e in good] + [
        b"{not json",
        serialize_event({**good[0], "beneficiary_count": "not-a-number"}),
        corrupt_event(good[1], rng),
    ]
    now = datetime.now(timezone.utc)
    kafka_like = spark.createDataFrame(
        [(bytearray(m), now) for m in messages], "value binary, timestamp timestamp"
    )
    parsed = parse_events(kafka_like)
    flags = [row.is_valid for row in parsed.collect()]
    assert flags[:3] == [True, True, True]
    assert flags[3:] == [False, False, False]
    reasons = [row.rejection_reason for row in parsed.collect()]
    assert reasons[:3] == [None, None, None]
    assert reasons[3] == "malformed JSON or missing metadata"
    assert reasons[4] == "invalid beneficiary_count"

    metrics = window_metrics(parsed.filter("is_valid"), "1 minute", "2 minutes")
    total = sum(row.beneficiaries for row in metrics.collect())
    assert total == sum(int(e["beneficiary_count"].replace(".", "")) for e in good)
