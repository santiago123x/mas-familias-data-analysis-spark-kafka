"""Default settings shared by the jobs. All of them can be overridden from the CLI."""

from __future__ import annotations

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_KAFKA_TOPIC = "mfa_beneficiaries"
DEFAULT_SAMPLE_INPUT = "data/sample/mfa_sample.csv"
DEFAULT_BATCH_OUTPUT_DIR = "outputs/batch"
DEFAULT_STREAM_OUTPUT_DIR = "outputs/streaming"
DEFAULT_FIGURES_DIR = "reports/figures"
SOURCE_NOTE = "Source: datos.gov.co - Beneficiarios Más Familias en Acción (dataset xfif-myr2)."
