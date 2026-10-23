"""Batch pipeline: load, profile, clean, analyse and visualise the MFA dataset.

Usage (from the repository root):

    spark-submit --master "local[2]" --driver-memory 1g jobs/batch_analysis.py \
        --input hdfs://localhost:9000/Tarea3/beneficiarios_mfa.csv

Outputs:
    outputs/batch/tables/*.csv          aggregated tables
    outputs/batch/data_quality.json     data-quality report
    reports/figures/*.png               charts used in the report
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame

from mfa_analytics import analysis, plotting
from mfa_analytics.cleaning import clean_beneficiaries, rename_source_columns, split_valid_records
from mfa_analytics.config import (
    DEFAULT_BATCH_OUTPUT_DIR,
    DEFAULT_FIGURES_DIR,
    DEFAULT_SAMPLE_INPUT,
    SOURCE_NOTE,
)
from mfa_analytics.quality import build_quality_report
from mfa_analytics.spark_io import build_spark_session, read_source_csv, write_table

logger = logging.getLogger("batch_analysis")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--input",
        default=DEFAULT_SAMPLE_INPUT,
        help="CSV path or URI (file:///..., hdfs://...). Defaults to the bundled sample.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path(DEFAULT_BATCH_OUTPUT_DIR))
    parser.add_argument("--figures-dir", type=Path, default=Path(DEFAULT_FIGURES_DIR))
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    return parser.parse_args(argv)


def show(title: str, df: DataFrame, rows: int = 40) -> None:
    """Print a titled Spark table to the console (useful evidence for the video)."""
    print(f"\n=== {title} ===")
    df.show(rows, truncate=False)


def run(args: argparse.Namespace) -> None:
    """Execute the batch pipeline end to end."""
    spark = build_spark_session("MFA-BatchAnalysis", args.shuffle_partitions)
    try:
        # 1. Load: explicit all-string schema, header validated against the contract.
        raw = rename_source_columns(read_source_csv(spark, args.input))
        print("\n=== Raw schema (all strings by design) ===")
        raw.printSchema()

        # 2. Clean and type, then separate invalid records instead of dropping them silently.
        curated = clean_beneficiaries(raw)
        valid, rejected = split_valid_records(curated)
        # The valid set is reused by every aggregation: persist it once.
        valid = valid.persist(StorageLevel.MEMORY_AND_DISK)
        print("\n=== Curated schema ===")
        valid.printSchema()

        # 3. Data-quality profile.
        report = build_quality_report(raw, valid, rejected)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        report_path = args.output_dir / "data_quality.json"
        report_path.write_text(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
        print("\n=== Data quality report ===")
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
        if report.rejected_rows:
            show("Sample of rejected records", rejected, rows=10)

        # 4. Exploratory analysis (all metrics weighted by beneficiary_count).
        tables = {
            "national_summary": analysis.national_summary(valid),
            "by_department": analysis.by_department(valid),
            "by_population_type": analysis.by_category(valid, "population_type"),
            "by_beneficiary_status": analysis.by_category(valid, "beneficiary_status"),
            "by_ethnicity": analysis.by_category(valid, "ethnicity"),
            "by_age_and_gender": analysis.by_age_and_gender(valid),
            "by_enrollment_year": analysis.by_enrollment_year(valid),
        }
        # Aggregates are tiny (<= a few dozen rows), so converting them to pandas is safe.
        results = {name: df.toPandas() for name, df in tables.items()}
        for name, pdf in results.items():
            print(f"\n=== {name} ===")
            print(pdf.to_string(index=False))
            write_table(pdf, args.output_dir / "tables", name)

        # 5. RDD cross-check (map + reduceByKey) against the DataFrame result.
        rdd_totals = analysis.beneficiaries_by_department_rdd(valid)
        df_totals = dict(
            zip(
                results["by_department"]["department_name"],
                results["by_department"]["beneficiaries"],
                strict=True,
            )
        )
        if rdd_totals != {k: int(v) for k, v in df_totals.items()}:
            raise RuntimeError("RDD and DataFrame department totals do not match")
        print("\nRDD (map + reduceByKey) totals match the DataFrame aggregation.")

        # 6. Figures.
        create_figures(results, args.figures_dir)
        logger.info("Batch pipeline finished. Outputs in %s", args.output_dir)
    finally:
        spark.stop()


def create_figures(results: dict, figures_dir: Path) -> None:
    """Render the report figures from the aggregated pandas tables."""
    national_rate = float(results["national_summary"]["banking_rate"].iloc[0])
    departments = results["by_department"]

    plotting.plot_horizontal_bars(
        departments,
        "department_name",
        "beneficiaries",
        title="Beneficiaries by department",
        xlabel="Beneficiaries",
        output_dir=figures_dir,
        name="01_beneficiaries_by_department",
        source_note=SOURCE_NOTE,
    )
    plotting.plot_horizontal_bars(
        departments,
        "department_name",
        "banking_rate",
        title="Banking rate by department (beneficiaries with known status)",
        xlabel="Share of beneficiaries with a bank product",
        output_dir=figures_dir,
        name="02_banking_rate_by_department",
        source_note=SOURCE_NOTE,
        as_percent=True,
        reference_value=national_rate,
        reference_label=f"National {national_rate:.1%}",
    )
    plotting.plot_horizontal_bars(
        results["by_population_type"],
        "population_type",
        "banking_rate",
        title="Banking rate by population type",
        xlabel="Share of beneficiaries with a bank product",
        output_dir=figures_dir,
        name="03_banking_rate_by_population_type",
        source_note=SOURCE_NOTE,
        as_percent=True,
        reference_value=national_rate,
        reference_label=f"National {national_rate:.1%}",
    )
    plotting.plot_grouped_bars(
        results["by_age_and_gender"],
        "age_range",
        "gender",
        "beneficiaries",
        title="Beneficiaries by age range and gender",
        ylabel="Beneficiaries",
        output_dir=figures_dir,
        name="04_beneficiaries_by_age_and_gender",
        source_note=SOURCE_NOTE,
        series_order=["MUJER", "HOMBRE"],
    )
    plotting.plot_vertical_bars(
        results["by_enrollment_year"],
        "enrollment_year",
        "beneficiaries",
        title="Beneficiaries by enrollment year",
        ylabel="Beneficiaries",
        output_dir=figures_dir,
        name="05_beneficiaries_by_enrollment_year",
        source_note=SOURCE_NOTE,
    )


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns a process exit code."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = parse_args(argv)
    try:
        run(args)
    except FileNotFoundError as error:
        logger.error("%s", error)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
