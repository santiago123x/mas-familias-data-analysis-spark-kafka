# Data

## Source

| Item | Value |
|---|---|
| Dataset | **Beneficiarios Más Familias en Acción** |
| Publisher portal | Portal Nacional de Datos Abiertos de Colombia (datos.gov.co) |
| Dataset page | https://www.datos.gov.co/Inclusi-n-Social-y-Reconciliaci-n/Beneficiarios-M-s-Familias-en-Acci-n/xfif-myr2/about_data |
| Dataset id | `xfif-myr2` |
| Snapshot used | Exported on 2026-10-23 |
| Size of the snapshot | 735 MB, 3,958,594 data rows, 22 columns, UTF-8, comma-separated |

## Why the full file is not in the repository

The CSV is 735 MB, well above GitHub's 100 MB per-file limit, and it can be
downloaded again from its official source. Only a small sample is versioned.

## How to obtain it

1. Open the dataset page above and choose **Export → CSV**, or download it
   from the command line (Socrata export endpoint):

   ```bash
   wget -O data/raw/beneficiarios_mfa.csv \
     "https://www.datos.gov.co/api/views/xfif-myr2/rows.csv?accessType=DOWNLOAD"
   ```

2. Place it at `data/raw/beneficiarios_mfa.csv` (this folder is git-ignored).
3. For the batch job on HDFS, upload it as the `hadoop` user:

   ```bash
   hdfs dfs -mkdir -p /Tarea3
   hdfs dfs -put data/raw/beneficiarios_mfa.csv /Tarea3/
   ```

## Sample

`sample/mfa_sample.csv` contains **49 real rows** of the snapshot (same header,
untouched values). 44 rows were drawn at random (seed 42) and 5 were added on
purpose to cover edge cases: counts with thousands separators (`1.414`), an
empty banking flag, an inactive beneficiary and a missing gender. It is used by
the tests and lets anyone run the pipelines without downloading the full file.

## Columns

Each row is a **group of beneficiaries** sharing the same attributes;
`CantidadDeBeneficiarios` is the group size. There is no beneficiary identifier.

| Source column | Canonical name | Curated type | Notes |
|---|---|---|---|
| Bancarizado | `is_banked` | boolean | `SI`/`NO`; `ND` and empty → null |
| CodigoDepartamentoAtencion | `department_code` | string(2) | DANE code, leading zeros kept |
| CodigoMunicipioAtencion | `municipality_code` | string(5) | DANE code, leading zeros kept |
| Discapacidad | `has_disability` | boolean | |
| EstadoBeneficiario | `beneficiary_status` | string | `ACTIVO` / `NO ACTIVO` |
| Etnia | `ethnicity` | string | Spelling variants unified |
| FechaInscripcionBeneficiario | `enrollment_date` | date | Monthly granularity (day = 01) |
| Genero | `gender` | string | `MUJER` / `HOMBRE` |
| NivelEscolaridad | `education_level` | string | Mostly `ND` |
| NombreDepartamentoAtencion | `department_name` | string | |
| NombreMunicipioAtencion | `municipality_name` | string | |
| Pais | `country` | *dropped* | ~95% `ND`, mixes names and codes (`57`, `169`) |
| TipoAsignacionBeneficio | `benefit_assignment_type` | string | |
| TipoBeneficio | `benefit_type` | string | Some values concatenate two benefits |
| TipoDocumento | `document_type` | string | |
| TipoPoblacion | `population_type` | string | `SISBEN`, `DESPLAZADOS`, `UNIDOS`, `INDIGENAS` |
| RangoBeneficioConsolidadoAsignado | `total_benefit_range` | string | Range label (COP) |
| RangoUltimoBeneficioAsignado | `last_benefit_range` | string | Range label (COP) |
| FechaUltimoBeneficioAsignado | `last_benefit_date` | date | `1900-01-01` = no date → null |
| RangoEdad | `age_range` | string | |
| Titular | `is_household_head` | boolean | Holder of the benefit |
| CantidadDeBeneficiarios | `beneficiary_count` | integer | `.` is a thousands separator |
