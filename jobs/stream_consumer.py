"""Spark Structured Streaming consumer for the MFA Kafka topic.

Reads JSON events from Kafka, validates and cleans them with the same rules as
the batch pipeline, and computes per-department metrics over event-time windows.

Usage (from the repository root; connector version as in Anexo 3):

    spark-submit --master "local[2]" \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        jobs/stream_consumer.py --window "1 minute"

Console output:
    * "Window metrics" tables: events, beneficiaries and banking rate per window.
    * "Rejected events" tables: invalid messages, the reason and a payload preview.
Files:
    outputs/streaming/window_metrics.csv  (input for jobs/plot_stream_results.py)
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
import tempfile
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from mfa_analytics.config import (
    DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    DEFAULT_KAFKA_TOPIC,
    DEFAULT_STREAM_OUTPUT_DIR,
)
from mfa_analytics.spark_io import build_spark_session
from mfa_analytics.streaming.transform import parse_events, window_metrics

logger = logging.getLogger("stream_consumer")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Consume MFA events from Kafka with Spark.")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_KAFKA_TOPIC)
    parser.add_argument("--window", default="1 minute", help="Tumbling window size.")
    parser.add_argument("--watermark", default="2 minutes", help="Allowed event lateness.")
    parser.add_argument("--trigger", default="10 seconds", help="Micro-batch interval.")
    parser.add_argument(
        "--starting-offsets",
        choices=("latest", "earliest"),
        default="latest",
        help="'latest' reads only new events; 'earliest' replays the whole topic.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path(DEFAULT_STREAM_OUTPUT_DIR))
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=None,
        help="Checkpoint directory. If omitted, a temporary one is used so every run starts clean.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=0,
        help="Stop automatically after N seconds (0 = run until Ctrl+C).",
    )
    return parser.parse_args(argv)


def make_metrics_writer(output_file: Path):
    """Build the ``foreachBatch`` function that prints and stores window metrics.

    Each micro-batch contains only the windows updated in that batch (update
    mode). Rows are appended with the batch id, so the latest value per window
    can be recovered later for plotting.
    """

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        ordered = batch_df.orderBy(F.desc("window_start"), F.desc("beneficiaries"))
        print(f"\n=== Window metrics | micro-batch {batch_id} ===")
        ordered.show(40, truncate=False)
        # The updated windows are few rows per batch, so collecting them is safe.
        pdf = ordered.withColumn("batch_id", F.lit(batch_id)).toPandas()
        output_file.parent.mkdir(parents=True, exist_ok=True)
        pdf.to_csv(output_file, mode="a", header=not output_file.exists(), index=False)

    return write_batch


def run(args: argparse.Namespace) -> None:
    """Start the streaming queries and wait for termination."""
    spark = build_spark_session("MFA-StreamConsumer", shuffle_partitions=4)
    checkpoint_root = args.checkpoint_dir or Path(tempfile.mkdtemp(prefix="mfa-checkpoints-"))
    metrics_file = args.output_dir / "window_metrics.csv"
    if metrics_file.exists():
        metrics_file.unlink()  # each run produces a fresh results file

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")  # tolerate topic re-creation between demo runs
        .load()
    )
    events = parse_events(kafka_stream)

    metrics_query = (
        window_metrics(events.filter("is_valid"), args.window, args.watermark)
        .writeStream.outputMode("update")
        .foreachBatch(make_metrics_writer(metrics_file))
        .option("checkpointLocation", str(checkpoint_root / "metrics"))
        .trigger(processingTime=args.trigger)
        .queryName("window_metrics")
        .start()
    )
    rejected_query = (
        events.filter(~F.col("is_valid"))
        .select(
            "kafka_timestamp",
            "rejection_reason",
            F.substring("raw_value", 1, 80).alias("raw_value_preview"),
        )
        .writeStream.outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", str(checkpoint_root / "rejected"))
        .trigger(processingTime=args.trigger)
        .queryName("rejected_events")
        .start()
    )
    logger.info(
        "Listening on topic '%s' (%s). Spark UI: http://localhost:4040. Press Ctrl+C to stop.",
        args.topic,
        args.bootstrap_servers,
    )

    try:
        spark.streams.awaitAnyTermination(args.timeout_seconds or None)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        for query in (metrics_query, rejected_query):
            query.stop()
        spark.stop()
        if args.checkpoint_dir is None:
            shutil.rmtree(checkpoint_root, ignore_errors=True)
        logger.info("Stopped. Window metrics saved to %s", metrics_file)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns a process exit code."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(parse_args(argv))
    return 0


if __name__ == "__main__":
    sys.exit(main())
