"""Spark session and input/output helpers."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from mfa_analytics.schema import RAW_SCHEMA

logger = logging.getLogger(__name__)


def build_spark_session(app_name: str, shuffle_partitions: int = 8) -> SparkSession:
    """Create (or reuse) a SparkSession.

    The master URL and memory settings are intentionally *not* hard-coded: they
    are passed through ``spark-submit`` so the same code runs locally or on a
    cluster. A small number of shuffle partitions avoids hundreds of tiny tasks
    on a single machine (Spark's default is 200).
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def path_exists(spark: SparkSession, path: str) -> bool:
    """Check whether a path exists on any Hadoop-compatible file system.

    Works for local paths (``file:///``), HDFS (``hdfs://``) and paths relative
    to the configured default file system.
    """
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    file_system = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return bool(file_system.exists(hadoop_path))


def read_source_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read the source CSV with the explicit all-string raw schema.

    ``enforceSchema=False`` makes Spark validate the file header against the
    expected column names, so a wrong or modified file fails fast with a clear
    error instead of producing misaligned columns.

    Raises:
        FileNotFoundError: if ``path`` does not exist.
    """
    if not path_exists(spark, path):
        raise FileNotFoundError(f"Input file not found: {path}")
    return (
        spark.read.option("header", True)
        .option("encoding", "UTF-8")
        .option("enforceSchema", False)
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .csv(path)
    )


def write_table(pdf: pd.DataFrame, output_dir: Path, name: str) -> Path:
    """Write a small, already-aggregated table as a single CSV file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{name}.csv"
    pdf.to_csv(target, index=False)
    logger.info("Saved table %s", target)
    return target
