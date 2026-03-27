# Medigy Market Intelligence (MMI)

A CMS Medigy Part B analytics pipeline built with `surveilr` and `spry` that transforms raw public datasets into a navigable SQLPage business intelligence application. Designed to identify high-opportunity disease-specialty clusters through evidence-based commercial validation.

> **Note:** Source code is on the main branch. `cms_provider.csv` is used by the pipeline but excluded from the repository due to file size.

---

## Table of Contents

- [Medigy Market Intelligence (MMI)](#medigy-market-intelligence-mmi)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Architecture](#architecture)
  - [Project Structure](#project-structure)
  - [Prerequisites](#prerequisites)
  - [Data Assets](#data-assets)
    - [SQLite Table Mapping](#sqlite-table-mapping)
  - [Pipeline Stages](#pipeline-stages)
  - [Data Model](#data-model)
    - [Dimension Tables](#dimension-tables)
      - [`dim_procedure` — Procedure Signal Classification](#dim_procedure--procedure-signal-classification)
      - [`dim_diagnosis` — Disease State Tiers](#dim_diagnosis--disease-state-tiers)
    - [Fact Tables](#fact-tables)
    - [Analytical Views](#analytical-views)
      - [Section 4 Evidence Tables](#section-4-evidence-tables)
  - [Opportunity Scoring](#opportunity-scoring)
  - [SQLPage Dashboard](#sqlpage-dashboard)
    - [Navigation Menu](#navigation-menu)
    - [Reference Pages (linked from Home)](#reference-pages-linked-from-home)
  - [Deploy](#deploy)
  - [Key Design Decisions](#key-design-decisions)
    - [`fact_utilization` sources from `bygeography` + `specialty_by_state`](#fact_utilization-sources-from-bygeography--specialty_by_state)
    - [Specialty normalization is inline CASE, not a lookup join](#specialty-normalization-is-inline-case-not-a-lookup-join)
    - [Opportunity scoring bridges disease → specialty via body system](#opportunity-scoring-bridges-disease--specialty-via-body-system)
    - [`condition_monitoring_proxy` uses disease-specific HCPCS filters](#condition_monitoring_proxy-uses-disease-specific-hcpcs-filters)

---

## Overview

The pipeline ingests CMS public datasets, builds a normalized star schema in SQLite, runs 13 analytical views and an opportunity scoring engine, and packages everything into a multi-page SQLPage UI — all from a single Executable Markdown file (`mmi-dashboard.md`) and one SQL script (`medicare-analytics.sql`).

The primary deliverable is `opportunity_scoring_view`: a composite Tier 1/2/3 ranking of every disease-state × specialty combination, scored on patient volume, interaction intensity, and economic weight.

**Dataset vintage:** CMS 2023  
**Database engine:** SQLite (surveilr RSSD)

---

## Architecture

```
CMS Public Datasets (CSV) + master reference data (ICD, CPT, HCPCS, GPCI, etc.)
         │
         ▼
 surveilr ingest          ← extracts and loads raw CSVs into SQLite RSSD
         │
         ▼
 surveilr orchestrate     ← transforms raw CSV ingestion into typed tables
         │
         ▼
 medigy-analytics.sql   ← ELT pipeline: indexes → dims → facts → views → scoring
         │
         ▼
 spry + SQLPage           ← packages Executable Markdown into browser UI
         │
         ▼
 resource-surveillance.sqlite.db   ← single-file output (RSSD)
```

---

## Project Structure

```text
.
├── medicare-ds/                         # CMS source CSV files (Git-ignored)
├── sql/
│   └── medigy-analytics.sql           # Full ELT pipeline — run after ingestion
│   └── medigy-ddl.sql                  # Data Provenance and other schema objects
├── mmi-dashboard.md                     # Executable Markdown — UI definition + deploy script
└── resource-surveillance.sqlite.db      # Output RSSD (SQLite, generated at runtime)
```

---

## Prerequisites

The following tools must be installed and available on your `PATH`:

- [`surveilr`](https://www.surveilr.com)
- [`spry`](https://github.com/netspective-labs/spry)

---

## Data Assets

All datasets are publicly available from CMS — no login required.

| Local Filename | Description |
|---|---|
| `cms_provider.csv` | Provider-level Medicare utilization and payment metrics |
| `cms_bygeography.csv` | HCPCS/CPT utilization by geography, including national totals |
| `ref_icd10_diagnosis.csv` | ICD-10 diagnosis reference data |
| `ref_condition_icd_mapping.csv` | Manual disease-state to ICD prefix mapping seed |
| `ref_hcpcs_level_two_procedures.csv` | HCPCS Level II reference data |
| `ref_procedure_code.csv` | Procedure code reference data |
| `ref_rvu_qpp.csv` | RVU/QPP reference data |
| `ref_geo_adjustment.csv` | CMS GPCI locality adjustment factors |
| `ref_medicare_localities.csv` | Medicare locality definitions |
| `ref_opps_price_cap.csv` | OPPS price cap reference |
| `ref_anes_conversion_factor.csv` | CMS anesthesia conversion factors |

**Source:** [CMS Physician & Other Practitioners](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners)

> **Download tip:** Start with By Geography & Service (smallest file). By Provider & Service is the largest — begin that download first.

### SQLite Table Mapping

| SQLite Table | CMS Dataset | Key Columns Used |
|---|---|---|
| `uniform_resource_cms_bygeography` | Physician & Other Practitioners — By Geography & Service | `HCPCS_Cd`, `Tot_Srvcs`, `Tot_Benes`, `Avg_Mdcr_Pymt_Amt`, `Rndrng_Prvdr_Geo_Cd`, `Place_Of_Srvc` |
| `uniform_resource_cms_provider` | Physician & Other Practitioners — By Provider | `Rndrng_NPI`, `Rndrng_Prvdr_Type`, `Rndrng_Prvdr_State_Abrvtn` |
| `uniform_resource_ref_icd10_diagnosis` | ICD-10-CM (CDC/NCHS) | `icd10_code`, `description_long` |
| `uniform_resource_ref_procedure_code` | CMS Physician Fee Schedule | `HCPCS`, `WORK RVU`, `MEDICARE PAYMENT` |
| `uniform_resource_ref_hcpcs_level_two_procedures` | CMS HCPCS Level II | `hcpcs_code`, `short_description` |
| `uniform_resource_ref_geo_adjustment` | CMS GPCI 2026 | `State`, `PW GPCI` |
| `uniform_resource_ref_anes_conversion_factor` | CMS Anesthesia Conversion Factor | `Contractor`, `Locality`, `Conversion Factor` |

---

## Pipeline Stages

`medicare-analytics.sql` executes end-to-end in five sequential sections.

| Section | Objects Created | Purpose |
|---|---|---|
| **Section 0** | 14 indexes | Query performance on raw CMS source tables (million-row joins) |
| **Section 1** | `dim_procedure`, `dim_diagnosis`, `dim_specialty`, `dim_geography`, `fips_state_map` | Star schema normalization — one clean lookup per domain |
| **Section 2A** | `specialty_by_state`, `fact_utilization` | Core fact table: volume + economics per HCPCS × state × specialty |
| **Section 2B** | `specialty_market_dynamics`, `condition_monitoring_proxy_table` | Specialty dominance ratio per HCPCS code nationally; condition-level repeat-interaction proxy |
| **Section 3** | Views 1–12 | Business question layer (see Analytical Views below) |
| **Section 4** | `opportunity_scoring_view`, `mdsd_global_opportunity_matrix`, `mdsd_economic_intensity_proof`, `mdsd_interaction_model_fit`, `mdsd_specialty_gatekeepers` | Primary deliverable — composite Tier 1/2/3 ranking + evidence tables |
| **Section 5** | Queries A–I | Reference analyst queries (commented out, ready to run) |

---

## Data Model

### Dimension Tables

| Table | Key Columns | Purpose |
|---|---|---|
| `dim_procedure` | `hcpcs_code`, `procedure_category`, `procedure_signal`, `is_monitoring_flag` | CPT + HCPCS Level II lookup with clinical categorization, commercial signal classification, and repeat-visit flag |
| `dim_diagnosis` | `icd10_code`, `disease_state`, `body_system` | 17+ named disease clusters mapped from ICD-10-CM; `body_system` bridges to specialty domain in scoring |
| `dim_specialty` | `raw_specialty_name`, `specialty_name`, `specialty_domain` | Canonical specialty names normalized from CMS `Rndrng_Prvdr_Type` strings |
| `dim_geography` | `state_abbr`, `pw_gpci`, `cost_tier` | 2026 CMS GPCI factors for cost-adjusted market sizing |
| `fips_state_map` | `fips_code`, `state_abbr` | Static 52-row FIPS-to-abbreviation bridge — required to join `bygeography` → `provider` |

#### `dim_procedure` — Procedure Signal Classification

The `procedure_signal` column classifies each HCPCS code into a commercial model context:

| Signal Value | Example Codes | Commercial Implication |
|---|---|---|
| `Sleep Lab (High Intensity)` | `95810`, `95811` | Diagnostic trigger — Model B |
| `Neuro Assessment` | `95812`, `95819` | Diagnostic trigger |
| `Cognitive Assessment` | `99483` | Diagnostic trigger — Alzheimer's |
| `Respiratory Rehab (High Freq)` | `G0238` | SaaS / monitoring — Model C |
| `Chronic Care Management` | `99490` | SaaS / monitoring |
| `SUD/Opioid Treatment` | `G2086`, `G2087` | SaaS / monitoring |
| `Standard E/M Visit` | `99214` | Low-margin baseline |
| `Lab Screening (A1C)` | `83036` | Low-margin baseline |

#### `dim_diagnosis` — Disease State Tiers

| Priority | Disease States | Body System |
|---|---|---|
| Tier 1–2 (Core Targets) | Sleep Apnea, COPD, Parkinson's Disease, Heart Failure, Opioid Use Disorder | Respiratory & Sleep, Cardiovascular, Neurological & Mental Health |
| Tier 3 (Baselines) | Hypertension, Type 2 Diabetes, Hypothyroidism, Asthma | Cardiovascular, Endocrine & Metabolic, Respiratory & Sleep |
| Tier 4 (Mental Health) | Major Depression, Anxiety (GAD), PTSD, Bipolar Disorder | Neurological & Mental Health |
| Tier 4 (Specialty/Niche) | Alzheimer's, Multiple Sclerosis, Oncology, Frailty/Vocal Disorders | Neurological & Mental Health, Oncology |

### Fact Tables

| Table | Key Columns | Purpose |
|---|---|---|
| `specialty_by_state` | `state_abbr`, `specialty_name`, `specialty_domain` | Deduplicated provider specialty lookup per state — intermediate bridge table |
| `fact_utilization` | `specialty_name`, `specialty_domain`, `hcpcs_code`, `state_abbr`, `place_of_service`, `total_beneficiaries`, `total_services`, `total_medicare_payment` | Central fact table: volume, frequency, and economics per HCPCS × state × specialty |
| `specialty_market_dynamics` | `specialty_name`, `hcpcs_code`, `specialty_dominance_ratio` | Each specialty's share of national patient volume per HCPCS code (0.0–1.0) |
| `condition_monitoring_proxy_table` | `disease_state`, `body_system`, `monitoring_services_per_beneficiary`, `interaction_rank` | Condition-level repeat-interaction proxy using monitoring-flagged HCPCS only |

### Analytical Views

| # | View | Business Purpose |
|---|---|---|
| 1 | `specialty_activity_summary` | Top-line KPIs: total spend, patient reach, provider count, spend/patient, services/patient |
| 2 | `specialty_economic_intensity` | Intensity index = spend/patient × services/patient, ranked within clinical domain |
| 3 | `specialty_top_procedures` | Top 10 HCPCS codes per specialty by volume with spend rank alongside |
| 4 | `specialty_market_concentration` | Specialty ownership share of national volume per code — market dominance signal |
| 5 | `chronic_interaction_density` | Services per patient per code; tiered by business model fit (SaaS / Diagnostic / Maintenance) |
| 6 | `monitoring_procedure_intensity` | % of specialty volume from repeat-monitoring codes; monitoring vs total spend |
| 6B | `condition_monitoring_proxy` | Disease-specific monitoring interaction density; `interaction_rank` used in scoring |
| 7 | `dme_supply_refill_metrics` | DME/supply refill velocity (units per patient) — chronic disease management signal |
| 8 | `surgical_economic_metrics` | Anesthesia CPT codes scored by conversion factor — surgical economic load proxy |
| 9 | `part_b_drug_intensity` | Part B drug administrations, patients, and spend per specialty by HCPCS code |
| 10 | `geographic_market_opportunity` | State-level volume and GPCI cost-adjusted spend per specialty |
| 11 | `facility_vs_office_split` | Office vs facility care setting mix — ambulatory ownership signal |
| 12 | `disease_state_icd_coverage` | ICD-10 code count per disease cluster — mapping completeness validation |
| 13 | `opportunity_scoring_view` | **Primary deliverable** — composite Tier 1/2/3 ranking of disease × specialty clusters |

#### Section 4 Evidence Tables

These materialized tables are produced from the scoring views for use in the dashboard and final report:

| Table | Sourced From | Purpose |
|---|---|---|
| `mdsd_global_opportunity_matrix` | `opportunity_scoring_view` | High-level scoring for all mapped disease states, ordered by composite score |
| `mdsd_economic_intensity_proof` | `specialty_economic_intensity` | Full specialty economic intensity ranking — all baselines included |
| `mdsd_interaction_model_fit` | `condition_monitoring_proxy_table` | Per-condition model fit: SaaS (≥12 interactions/yr) vs Diagnostic (<4) |
| `mdsd_specialty_gatekeepers` | `specialty_market_dynamics` + `dim_procedure` + `dim_diagnosis` | Primary specialty "owner" per disease-state × procedure signal pair — proves B2B sales targeting |

---

## Opportunity Scoring

The `opportunity_scoring_view` produces the composite Tier 1/2/3 ranking across every disease × specialty intersection.

```
Composite Score = (0.35 × volume_percentile)
                + (0.35 × intensity_percentile)
                + (0.30 × economics_percentile)
                + dominance_bonus (up to +10 pts)

Tier 1 — High Opportunity  : score ≥ 75
Tier 2 — Moderate           : score ≥ 50
Tier 3 — Low Priority       : score < 50
```

All three dimensions are normalized to `NTILE(100)` percentile ranks before combining. The dominance bonus is derived from `specialty_market_dynamics.specialty_dominance_ratio × 10`.

**Scoring bridge:** `dim_diagnosis.body_system` (e.g., `'Cardiovascular'`) maps to `fact_utilization.specialty_domain` (e.g., `'Cardiovascular'`). The `opportunity_scoring_view` joins on this bridge to score every disease × specialty intersection.

---

## SQLPage Dashboard

`mmi-dashboard.md` is the Executable Markdown file that defines both the deploy script and the full SQLPage UI. It runs on port `9227` against `resource-surveillance.sqlite.db`.

### Navigation Menu

| Page | Route | Description |
|---|---|---|
| Home | `/` | Pipeline health check: specialties indexed, ICD clusters mapped, procedure codes, states in scope |
| Executive Dashboard | `/mmi/executive-dashboard.sql` | Top-line KPIs across all specialties |
| Opportunity Scores | `/mmi/opportunity-scoring.sql` | Full Tier 1/2/3 ranked output |
| Evidence | `/mmi/sleep-apnea-evidence.sql` | Disease-specific evidence deep-dives |
| Disease Mapping | `/mmi/disease-mapping.sql` | ICD-10 cluster browser |
| Procedure Drilldown | `/mmi/procedure-drilldown.sql` | Per-HCPCS code analysis |
| Data Dictionary | `/mmi/data-dictionary.sql` | Schema reference: all views, tables, and CMS source datasets |

### Reference Pages (linked from Home)

| Page | Route | Content |
|---|---|---|
| Medical Specialties | `/mmi/medical-specialities.sql` | Searchable table of all indexed specialties and domains |
| Procedure Inventory | `/mmi/procedures.sql` | Full HCPCS/CPT code inventory |
| Geographic Scope | `/mmi/geography.sql` | States and GPCI locality cards |
| Disease Clusters | `/mmi/disease-clusters.sql` | ICD-10 mapped disease cluster list |

---

## Deploy

```bash
# 1. Clean slate — remove any previous database
rm -f resource-surveillance.sqlite.*

# 2. Ingest raw CMS CSV files into SQLite RSSD
surveilr ingest files -r medicare-ds/

# 3. Transform raw ingestion into typed reference tables
surveilr orchestrate transform-csv

# 4. Run the ELT pipeline — builds all dims, facts, views, and scoring
surveilr shell sql/medigy-ddl.sql
surveilr shell sql/medigy-analytics.sql

# 5. Package the SQLPage UI and load it into the database
spry sp spc --package --conf sqlpage/sqlpage.json -m mmi-dashboard.md \
  | sqlite3 resource-surveillance.sqlite.db

echo "Medigy Market Intelligence database and SQLPage UI are ready."
```

The SQLPage application will be served at `http://localhost:9227`.

---


## Key Design Decisions

### `fact_utilization` sources from `bygeography` + `specialty_by_state`

The two core CMS datasets use incompatible state identifiers: `bygeography` uses numeric FIPS codes (`01`, `06`, `48`) while `cms_provider` uses state abbreviations (`AL`, `CA`, `TX`). A hardcoded `fips_state_map` table bridges them. Specialty is derived from `cms_provider.Rndrng_Prvdr_Type` via a deduplicated `specialty_by_state` materialized table (one row per state × specialty), joined to `bygeography` on the translated state abbreviation. This avoids both the Cartesian explosion (joining on state alone) and NULL specialty (joining on a mismatched key).

### Specialty normalization is inline CASE, not a lookup join

`dim_specialty` exists as a reference table, but `fact_utilization` and `specialty_by_state` derive `specialty_name` and `specialty_domain` directly from `Rndrng_Prvdr_Type` using CASE expressions. This eliminates join failures caused by whitespace or case differences between raw CMS strings.

### Opportunity scoring bridges disease → specialty via body system

`dim_diagnosis.body_system` (e.g., `'Cardiovascular'`) maps to `fact_utilization.specialty_domain` (e.g., `'Cardiovascular'`). The `opportunity_scoring_view` joins on this bridge to score every disease × specialty intersection. Scores are normalized via `NTILE` percentile ranking across three dimensions — volume (35%), interaction intensity (35%), and economic weight (30%) — with a market concentration bonus of up to +10 points from `specialty_dominance_ratio`.

### `condition_monitoring_proxy` uses disease-specific HCPCS filters

Rather than joining all monitoring codes to all disease states via body system (which collapses conditions to a uniform value), the view applies explicit per-disease filters (e.g., Sleep Apnea → `95810`/`95811`, COPD → `G0238`, Hypertension → `99214`). This ensures `interaction_rank` is meaningfully differentiated across conditions.

---
