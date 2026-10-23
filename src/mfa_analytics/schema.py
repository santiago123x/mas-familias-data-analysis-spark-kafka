"""Data contracts for the *Más Familias en Acción* beneficiaries dataset.

The module defines three layers of the data model:

1. **Raw layer** - the CSV exactly as published by datos.gov.co. Every field is
   read as a string because the source mixes sentinel tokens (``"ND"``),
   thousands separators (``"1.414"``) and placeholder dates (``1900-01-01``).
   Typing is applied explicitly during cleaning instead of relying on schema
   inference, which would silently produce wrong types or nulls.
2. **Canonical raw layer** - the same string values with stable, English,
   snake_case column names. This is also the payload contract of the Kafka
   events, so batch and streaming share a single set of cleaning rules.
3. **Curated layer** - typed and normalized columns used for analysis.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Source column name -> canonical column name, in the order of the CSV header.
SOURCE_TO_CANONICAL: dict[str, str] = {
    "Bancarizado": "is_banked",
    "CodigoDepartamentoAtencion": "department_code",
    "CodigoMunicipioAtencion": "municipality_code",
    "Discapacidad": "has_disability",
    "EstadoBeneficiario": "beneficiary_status",
    "Etnia": "ethnicity",
    "FechaInscripcionBeneficiario": "enrollment_date",
    "Genero": "gender",
    "NivelEscolaridad": "education_level",
    "NombreDepartamentoAtencion": "department_name",
    "NombreMunicipioAtencion": "municipality_name",
    "Pais": "country",
    "TipoAsignacionBeneficio": "benefit_assignment_type",
    "TipoBeneficio": "benefit_type",
    "TipoDocumento": "document_type",
    "TipoPoblacion": "population_type",
    "RangoBeneficioConsolidadoAsignado": "total_benefit_range",
    "RangoUltimoBeneficioAsignado": "last_benefit_range",
    "FechaUltimoBeneficioAsignado": "last_benefit_date",
    "RangoEdad": "age_range",
    "Titular": "is_household_head",
    "CantidadDeBeneficiarios": "beneficiary_count",
}

SOURCE_COLUMNS: list[str] = list(SOURCE_TO_CANONICAL)
CANONICAL_COLUMNS: list[str] = list(SOURCE_TO_CANONICAL.values())

# Columns excluded from the curated layer, with the reason documented in the README.
# `country` is ~95% "ND" and mixes names with numeric codes ("57", "169").
DROPPED_COLUMNS: tuple[str, ...] = ("country",)

# The raw CSV is read with an explicit all-string schema (see module docstring).
RAW_SCHEMA = StructType([StructField(name, StringType(), nullable=True) for name in SOURCE_COLUMNS])

# Typed contract of the curated layer. Tests assert that cleaning produces exactly this.
CURATED_SCHEMA = StructType(
    [
        StructField("department_code", StringType()),
        StructField("department_name", StringType()),
        StructField("municipality_code", StringType()),
        StructField("municipality_name", StringType()),
        StructField("is_banked", BooleanType()),
        StructField("has_disability", BooleanType()),
        StructField("is_household_head", BooleanType()),
        StructField("beneficiary_status", StringType()),
        StructField("ethnicity", StringType()),
        StructField("gender", StringType()),
        StructField("age_range", StringType()),
        StructField("education_level", StringType()),
        StructField("population_type", StringType()),
        StructField("document_type", StringType()),
        StructField("benefit_assignment_type", StringType()),
        StructField("benefit_type", StringType()),
        StructField("total_benefit_range", StringType()),
        StructField("last_benefit_range", StringType()),
        StructField("enrollment_date", DateType()),
        StructField("last_benefit_date", DateType()),
        StructField("beneficiary_count", IntegerType()),
    ]
)
CURATED_COLUMNS: list[str] = CURATED_SCHEMA.fieldNames()

# Kafka event contract: event metadata + the canonical raw payload (strings).
EVENT_METADATA_FIELDS: tuple[str, ...] = ("event_id", "event_time", "source", "simulated")
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_time", StringType()),
        StructField("source", StringType()),
        StructField("simulated", BooleanType()),
    ]
    + [StructField(name, StringType()) for name in CANONICAL_COLUMNS if name not in DROPPED_COLUMNS]
)
