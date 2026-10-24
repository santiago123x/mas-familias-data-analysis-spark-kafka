"""Kafka producer: replays MFA records as JSON events at a controlled rate.

The events are a **simulation**: real historical rows sampled from the CSV, sent
with the current timestamp and flagged with ``"simulated": true``.

Usage (from the repository root):

    python3 jobs/event_producer.py --input data/raw/beneficiarios_mfa.csv \
        --rate 5 --max-events 600
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

from mfa_analytics.config import (
    DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    DEFAULT_KAFKA_TOPIC,
    DEFAULT_SAMPLE_INPUT,
)
from mfa_analytics.streaming.events import (
    RandomRowSampler,
    build_event,
    corrupt_event,
    event_key,
    serialize_event,
    to_canonical_record,
)

logger = logging.getLogger("event_producer")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Replay MFA records as Kafka events.")
    parser.add_argument("--input", type=Path, default=Path(DEFAULT_SAMPLE_INPUT))
    parser.add_argument("--bootstrap-servers", default=DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_KAFKA_TOPIC)
    parser.add_argument("--rate", type=float, default=2.0, help="Events per second.")
    parser.add_argument(
        "--max-events", type=int, default=0, help="Stop after N events (0 = run until Ctrl+C)."
    )
    parser.add_argument(
        "--invalid-ratio",
        type=float,
        default=0.0,
        help="Share of deliberately invalid messages (0-1) to demonstrate validation.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
    args = parser.parse_args(argv)
    if args.rate <= 0:
        parser.error("--rate must be positive")
    if not 0 <= args.invalid_ratio <= 1:
        parser.error("--invalid-ratio must be between 0 and 1")
    return args


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create a Kafka producer that waits for the broker acknowledgement."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        acks="all",
        retries=3,
        linger_ms=10,
    )


def run(args: argparse.Namespace) -> int:
    """Send events until ``--max-events`` is reached or the user presses Ctrl+C."""
    rng = random.Random(args.seed)
    interval = 1.0 / args.rate
    sent = invalid = 0
    delivery_errors: list[Exception] = []  # filled asynchronously by the Kafka client

    with RandomRowSampler(args.input, seed=args.seed) as sampler:
        producer = create_producer(args.bootstrap_servers)
        logger.info(
            "Producing SIMULATED events to topic '%s' at %.1f events/s (source: %s)",
            args.topic,
            args.rate,
            args.input,
        )
        try:
            while args.max_events == 0 or sent < args.max_events:
                started = time.monotonic()
                event = build_event(to_canonical_record(sampler.sample()))
                if rng.random() < args.invalid_ratio:
                    payload = corrupt_event(event, rng)
                    invalid += 1
                    label = "INVALID"
                else:
                    payload = serialize_event(event)
                    label = "ok"
                # send() is asynchronous; delivery failures are collected by the errback.
                producer.send(args.topic, key=event_key(event), value=payload).add_errback(
                    delivery_errors.append
                )
                sent += 1
                logger.info(
                    "#%d [%s] dept=%s count=%s banked=%s",
                    sent,
                    label,
                    event["department_name"],
                    event["beneficiary_count"],
                    event["is_banked"],
                )
                # Keep a steady rate regardless of how long the send took.
                time.sleep(max(0.0, interval - (time.monotonic() - started)))
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            producer.flush(timeout=10)
            producer.close(timeout=10)
            for error in delivery_errors[:5]:
                logger.warning("Delivery failed: %s", error)
            logger.info(
                "Done: %d sent (%d invalid on purpose), %d delivery failures",
                sent,
                invalid,
                len(delivery_errors),
            )
    return 0 if not delivery_errors else 1


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns a process exit code."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)  # keep the client's internal logs quiet
    args = parse_args(argv)
    try:
        return run(args)
    except KafkaError as error:
        # kafka-python 2.x raises NoBrokersAvailable and 3.x raises KafkaTimeoutError when the
        # broker is unreachable; both derive from KafkaError.
        logger.error(
            "Kafka error (%s). Is the broker running at %s?", error, args.bootstrap_servers
        )
    except (FileNotFoundError, ValueError) as error:
        logger.error("%s", error)
    return 2


if __name__ == "__main__":
    sys.exit(main())
