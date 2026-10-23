"""Shared pytest fixtures."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_CSV = REPO_ROOT / "data" / "sample" / "mfa_sample.csv"


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """A small local SparkSession shared by all tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("mfa-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_csv() -> Path:
    """Path to the bundled 49-row sample of the source dataset."""
    return SAMPLE_CSV
