"""Event generation for the streaming demo (pure Python, no Spark dependency).

The producer **replays real historical records** from the source CSV as Kafka
events. Records are not invented, but their arrival is simulated: each event
gets the current timestamp and is explicitly flagged with ``"simulated": true``
and ``"source": "mfa_csv_replay"``. They must not be read as live enrollments.
"""

from __future__ import annotations

import csv
import io
import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mfa_analytics.schema import DROPPED_COLUMNS, SOURCE_COLUMNS, SOURCE_TO_CANONICAL

EVENT_SOURCE = "mfa_csv_replay"
REQUIRED_EVENT_FIELDS: tuple[str, ...] = ("event_id", "event_time", "department_code")


class RandomRowSampler:
    """Sample approximately random rows from a large CSV without loading it.

    Each draw seeks to a random byte offset, discards the partial line and
    parses the next complete one. Memory use is constant regardless of file
    size, and a fixed ``seed`` makes the sequence of events reproducible.
    The sampling is approximately uniform (rows after longer rows are slightly
    more likely), which is adequate for a demo.
    """

    def __init__(self, path: Path, seed: int | None = None) -> None:
        self._path = Path(path)
        if not self._path.is_file():
            raise FileNotFoundError(f"Input file not found: {self._path}")
        self._rng = random.Random(seed)
        self._file = self._path.open("rb")
        header = next(csv.reader([self._file.readline().decode("utf-8")]))
        if header != SOURCE_COLUMNS:
            self.close()
            raise ValueError(f"Unexpected CSV header in {self._path}")
        self._data_start = self._file.tell()
        self._size = self._path.stat().st_size
        if self._size <= self._data_start:
            self.close()
            raise ValueError(f"CSV file has no data rows: {self._path}")

    def sample(self) -> dict[str, str]:
        """Return one row as a dict keyed by the original source column names."""
        while True:
            self._file.seek(self._rng.randrange(self._data_start, self._size))
            self._file.readline()  # skip the (probably partial) current line
            line = self._file.readline()
            if not line:  # reached EOF: wrap around to the first data row
                self._file.seek(self._data_start)
                line = self._file.readline()
            values = next(csv.reader(io.StringIO(line.decode("utf-8"))), [])
            if len(values) == len(SOURCE_COLUMNS):
                return dict(zip(SOURCE_COLUMNS, values, strict=True))

    def close(self) -> None:
        """Release the file handle."""
        self._file.close()

    def __enter__(self) -> RandomRowSampler:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


def to_canonical_record(source_row: dict[str, str]) -> dict[str, str]:
    """Rename source columns to canonical names and drop unused columns."""
    record = {SOURCE_TO_CANONICAL[name]: value for name, value in source_row.items()}
    for name in DROPPED_COLUMNS:
        record.pop(name, None)
    return record


def build_event(record: dict[str, str], event_time: datetime | None = None) -> dict[str, Any]:
    """Wrap a canonical record with event metadata (id, UTC time, origin flags)."""
    timestamp = (event_time or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "source": EVENT_SOURCE,
        "simulated": True,
        **record,
    }


def corrupt_event(event: dict[str, Any], rng: random.Random) -> bytes:
    """Produce an invalid message to exercise the consumer's validation.

    Three failure modes are simulated: malformed JSON, a missing required
    field, and a non-numeric beneficiary count.
    """
    mode = rng.choice(("malformed_json", "missing_field", "bad_count"))
    if mode == "malformed_json":
        return serialize_event(event)[:-5]  # truncated JSON document
    broken = dict(event)
    if mode == "missing_field":
        broken.pop("department_code", None)
    else:
        broken["beneficiary_count"] = "not-a-number"
    return serialize_event(broken)


def serialize_event(event: dict[str, Any]) -> bytes:
    """Serialize an event as compact UTF-8 JSON."""
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def event_key(event: dict[str, Any]) -> bytes | None:
    """Kafka message key: the department code.

    Keying by department sends all events of a department to the same
    partition, preserving their relative order.
    """
    code = event.get("department_code")
    return code.encode("utf-8") if code else None
