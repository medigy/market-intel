# Medigy Market Intelligence (MMI) — Unified v2

A CMS Medicare analytics pipeline built with `surveilr` and `spry` that transforms raw public datasets into a navigable SQLPage business intelligence application. Designed to identify high-opportunity disease-specialty clusters through evidence-based commercial validation.

The v2 pipeline introduces a **unified extensible ELT architecture**: adding a new disease condition requires inserting exactly one row into `dim_condition_registry`. All downstream dimensions, facts, analytics views, and every page of the UI update automatically — no SQL or page logic changes needed.

> **Note:** Source code is on the `main` branch. `cms_provider.csv` is used by the pipeline but excluded from the repository due to file size.

---

## Table of Contents

- [Medigy Market Intelligence (MMI) — Unified v2](#medigy-market-intelligence-mmi--unified-v2)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Architecture](#architecture)
  - [Project Structure](#project-structure)
  - [Prerequisites](#prerequisites)
  - [Data Assets](#data-assets)
    - [SQLite Table Mapping](#sqlite-table-mapping)
  - [Pipeline Layers](#pipeline-layers)
  - [Data Model](#data-model)
    - [Disease Condition Registry](#disease-condition-registry)
    - [Dimension Tables](#dimension-tables)
    - [Fact Table](#fact-table)
    - [Analytics Views](#analytics-views)
  - [Opportunity Scoring](#opportunity-scoring)
  - [SQLPage Dashboard](#sqlpage-dashboard)
    - [Navigation Menu](#navigation-menu)
  - [Deploy](#deploy)
  - [Adding a New Disease Condition](#adding-a-new-disease-condition)
  - [Key Design Decisions](#key-design-decisions)
    - [Single registry drives everything](#single-registry-drives-everything)
    - [Multi-source fact table with heterogeneous procedure codes](#multi-source-fact-table-with-heterogeneous-procedure-codes)
    - [URL parameter routing for the Condition Hub](#url-parameter-routing-for-the-condition-hub)
    - [Opportunity scoring uses tier weights, not percentile rank, for strategic tiers](#opportunity-scoring-uses-tier-weights-not-percentile-rank-for-strategic-tiers)
    - [GPCI cost-tier enrichment in the geography layer](#gpci-cost-tier-enrichment-in-the-geography-layer)

---

## Overview

The pipeline ingests CMS public datasets, builds a normalized star schema in SQLite, runs a suite of condition-agnostic analytics views and an opportunity scoring engine, then packages everything into a multi-page SQLPage UI — all from a single Executable Markdown file (`mmi-unified-dashboard.md`) and one SQL script (`medigy-unified-v2.sql`).

The primary deliverable is `opportunity_score`: a composite ranking of every active disease condition, scored on patient volume, Medicare allowed spend, and strategic tier weighting.

**Dataset vintage:** CMS 2023  
**Database engine:** SQLite (surveilr RSSD)  
**UI server port:** 9227

---

## Architecture

```
CMS Public Datasets (CSV) + master reference data (ICD, CPT, HCPCS, GPCI, etc.)
         │
         ▼
 surveilr ingest          ← extracts and loads raw CSVs into SQLite RSSD
         │
         ▼
 surveilr orchestrate     ← transforms raw CSV ingestion into uniform_resource_* tables
         │
         ▼
 medigy-unified-v2.sql    ← ELT pipeline: indexes → registry → dims → fact → analytics
         │
         ▼
 spry + SQLPage            ← compiles Executable Markdown into browser UI
         │
         ▼
 resource-surveillance.sqlite.db   ← single-file output (RSSD)
```

---

## Project Structure

```text
.
├── medicare-ds/                        # CMS source CSV files (Git-ignored)
├── sql/
│   ├── medigy-unified-v2.sql           # Unified ELT pipeline — run after ingestion
│   └── medigy-ddl.sql                  # Data provenance and supplementary schema objects
├── mmi-unified-dashboard.md            # Executable Markdown — UI definition + deploy script
└── resource-surveillance.sqlite.db     # Output RSSD (SQLite, generated at runtime)
```

---

## Prerequisites

The following tools must be installed and available on your `PATH`:

| Tool | Purpose |
|------|---------|
| [`surveilr`](https://www.surveilr.com) | Ingest raw CSV files and execute SQL orchestration |
| [`spry`](https://github.com/netspective-labs/spry) | Compile Executable Markdown into SQLPage SQL files |
| `sqlite3` | Load compiled SQL into the RSSD database |
| [`sqlpage`](https://sql.page/) | Serve the web UI (configured via `sqlpage/sqlpage.json`) |

---

## Data Assets

All datasets are publicly available from CMS — no login required.

| Local Filename | Description |
| :--- | :--- |
| `cms_bygeo_place_of_service_mapping.csv` | Mapping file for Place of Service (POS) codes by geographic region |
| `cms_bygeography.csv` | HCPCS/CPT utilization by geography, including national totals |
| `cms_byproviderandservice.csv` | Granular Medicare data linked by specific provider NPI and service code |
| `cms_inpatienthospitals_byproviderandservice.csv` | Inpatient hospital utilization by provider and DRG service |
| `cms_outpatienthospitals_byproviderandservice.csv` | Outpatient hospital utilization by provider and APC service |
| `cms_provider.csv` | Provider-level Medicare utilization and payment metrics |
| `cms_providerandservice_pos.csv` | Provider service data categorized by Place of Service (e.g., Office vs. Facility) |
| `copd_oxygen.csv` | Specific Medicare utilization data for COPD and oxygen therapy |
| `diagnostics_data.csv` | Aggregate metrics for diagnostic testing and laboratory services |
| `DME_CPAP_E0601_E0470_E0471.csv` | Specialized DME data for Sleep Apnea devices (CPAP/BiPAP) |
| `dme_data.csv` | DMEPOS supplier utilization and payment data |
| `ref_anes_conversion_factor.csv` | Anesthesia-specific base units and geographic conversion factors |
| `ref_geo_adjustment.csv` | CMS GPCI locality adjustment factors (2026) |
| `ref_hcpcs_level_two_procedures.csv` | HCPCS Level II reference data (supplies, drugs, and DME codes) |
| `ref_icd10_diagnosis.csv` | ICD-10-CM diagnosis code reference data |
| `ref_medicare_localities.csv` | Crosswalk of counties/zip codes to Medicare Locality IDs |
| `ref_opps_price_cap.csv` | Outpatient Prospective Payment System (OPPS) price cap reference |
| `ref_procedure_code.csv` | Procedure code reference with RVU data |
| `ref_rvu_qpp.csv` | Relative Value Units (RVU) and Quality Payment Program (QPP) data |

**Source:** [CMS Physician & Other Practitioners](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners)

> **Download tip:** Start with By Geography & Service (smallest file). By Provider & Service is the largest — begin that download first.

### SQLite Table Mapping

| SQLite Table | Source | Key Columns |
|---|---|---|
| `uniform_resource_cms_bygeography` | Part B — By Geography & Service | `HCPCS_Cd`, `Rndrng_Prvdr_Geo_Lvl`, `Rndrng_Prvdr_Geo_Cd`, `Place_Of_Srvc`, `Tot_Benes`, `Tot_Srvcs`, `Avg_Mdcr_Alowd_Amt`, `Avg_Mdcr_Pymt_Amt` |
| `uniform_resource_cms_provider` | Part B — By Provider & Service | `Rndrng_NPI`, `Rndrng_Prvdr_Type`, `Rndrng_Prvdr_State_Abrvtn` |
| `uniform_resource_dme_data` | DMEPOS Supplier | `HCPCS_Cd`, `Rfrg_Prvdr_State_Abrvtn`, `Tot_Suplr_Benes`, `Tot_Suplr_Srvcs`, `Avg_Suplr_Mdcr_Alowd_Amt` |
| `uniform_resource_cms_outpatienthospitals_byproviderandservice` | Outpatient Hospital | `APC_Cd`, `Rndrng_Prvdr_State_Abrvtn`, `Rndrng_Prvdr_CCN`, `Tot_Benes`, `Tot_Srvcs`, `Avg_Mdcr_Alowd_Amt` |
| `uniform_resource_cms_inpatienthospitals_byproviderandservice` | Inpatient Hospital | `DRG_Cd`, `Rndrng_Prvdr_St`, `Rndrng_Prvdr_CCN` |
| `uniform_resource_ref_icd10_diagnosis` | ICD-10-CM (CDC/NCHS) | `icd10_code`, `description_long` |
| `uniform_resource_ref_procedure_code` | CMS Physician Fee Schedule | `HCPCS`, `WORK RVU`, `MEDICARE PAYMENT` |
| `uniform_resource_ref_hcpcs_level_two_procedures` | CMS HCPCS Level II | `hcpcs_code`, `short_description` |
| `uniform_resource_ref_geo_adjustment` | CMS GPCI 2026 | `State`, `Locality Name`, `2026 PW GPCI (with 1.0 Floor)` |
| `uniform_resource_ref_opps_price_cap` | CMS OPPS | `HCPCS` |

---

## Pipeline Layers

`medigy-unified-v2.sql` executes end-to-end across five layers.

| Layer | Objects Created | Purpose |
|---|---|---|
| **Layer 0** | ~20 indexes | Query performance on raw `uniform_resource_*` tables (million-row joins) |
| **Layer 1** | `dim_condition_registry` | Master disease catalog — the single place to add a new condition |
| **Layer 2** | `dim_diagnosis`, `dim_procedure`, `dim_specialty`, `dim_geography` | Derived star schema dimensions (auto-built from registry; never hand-edited) |
| **Layer 3** | `fact_utilization_unified` | Multi-source fact table combining GEO + DME + HOSPITAL data per condition |
| **Layer 4** | `condition_national_summary`, `condition_state_breakdown`, `condition_source_breakdown`, `condition_hcpcs_detail`, `executive_kpis`, `opportunity_score` | Condition-agnostic analytics views powering the dashboard |

---

## Data Model

### Disease Condition Registry

`dim_condition_registry` is the single seed table for the entire pipeline. All dimensions, the fact table, and every analytics view derive from it. Adding a row here and re-running the pipeline is all that is needed to bring a new disease into scope.

| Column | Type | Description |
|---|---|---|
| `condition_name` | TEXT | Display name and primary key for all downstream joins |
| `body_system` | TEXT | Clinical body system grouping |
| `tier` | INTEGER | 1 = Flagship, 2 = Core, 3 = Baseline |
| `icd10_prefix` | TEXT | Primary ICD-10 prefix (matched with `LIKE prefix || '%'`) |
| `icd10_prefix_2` | TEXT | Optional secondary ICD-10 prefix |
| `hcpcs_range_start` / `hcpcs_range_end` | TEXT | Inclusive CPT range for Part B geographic matching |
| `hcpcs_exact_list` | TEXT | JSON array of exact HCPCS codes (overrides range) |
| `dme_hcpcs_list` | TEXT | JSON array of HCPCS codes for DMEPOS matching |
| `use_bygeo` / `use_dmepos` / `use_hospital` | INTEGER | Flags controlling which data source layers are included |
| `specialty_domain` | TEXT | Clinical domain for opportunity scoring bridge |
| `b2b_tier_primary` | TEXT | Primary B2B sales target specialty |
| `em_share_pct` | REAL | Estimated fraction of E&M visits attributable to this condition |
| `dme_cap_months` | INTEGER | DMEPOS rental cap in months (0 = purchase only) |
| `icon` / `color` | TEXT | Tabler icon name and SQLPage color for the UI cards |
| `is_active` | INTEGER | 1 = included in pipeline, 0 = excluded |

**Current disease portfolio:**

| Condition | Tier | Body System | Data Sources |
|---|---|---|---|
| Sleep Apnea | 1 — Flagship | Respiratory & Sleep | GEO + DME + Hospital |
| COPD | 1 — Flagship | Respiratory & Sleep | GEO + DME + Hospital |
| Hypertriglyceridaemia | 2 — Core | Endocrine & Metabolic | GEO |
| Heart Failure | 2 — Core | Cardiovascular | GEO + Hospital |
| Type 2 Diabetes | 2 — Core | Endocrine & Metabolic | GEO |
| Parkinson's Disease | 2 — Core | Neurological & Mental Health | GEO |
| Hypertension | 3 — Baseline | Cardiovascular | GEO |

### Dimension Tables

| Table | Key Columns | Purpose |
|---|---|---|
| `dim_diagnosis` | `icd10_code`, `disease_state`, `body_system` | ICD-10 codes joined to condition registry via prefix matching |
| `dim_procedure` | `hcpcs_code`, `procedure_category`, `procedure_signal`, `linked_condition` | CPT/HCPCS codes classified by clinical category and commercial signal |
| `dim_specialty` | `specialty_domain`, `b2b_tier_primary` | Specialty domains and B2B targeting labels derived from the registry |
| `dim_geography` | `state_abbr`, `locality_name`, `pw_gpci`, `cost_tier`, `mac` | 2026 CMS GPCI factors and cost-tier classification per state |

### Fact Table

`fact_utilization_unified` is the central fact table, combining rows from up to three source layers per condition:

| Source Type | Source Table | Procedure Code Column |
|---|---|---|
| `GEO` | `uniform_resource_cms_bygeography` | `HCPCS_Cd` |
| `DME` | `uniform_resource_dme_data` and condition-specific DME tables | `HCPCS_Cd` |
| `HOSPITAL_OUTPATIENT` | `uniform_resource_cms_outpatienthospitals_byproviderandservice` | `APC_Cd` |
| `HOSPITAL_INPATIENT` | `uniform_resource_cms_inpatienthospitals_byproviderandservice` | `DRG_Cd` |

Key columns: `condition_name`, `source_type`, `hcpcs_code`, `state_abbr`, `total_beneficiaries`, `total_services`, `total_allowed_amt`, `total_medicare_payment`.

### Analytics Views

All views in Layer 4 are condition-agnostic — they query `fact_utilization_unified` and filter by `condition_name` where needed.

| View | Purpose |
|---|---|
| `condition_national_summary` | Per-condition national KPIs: beneficiaries, services, allowed spend, allowed/patient, opportunity score, data source count |
| `condition_state_breakdown` | State-level breakdown per condition with GPCI cost-tier enrichment |
| `condition_source_breakdown` | Allowed spend split by data source layer (GEO / DME / Hospital) per condition |
| `condition_hcpcs_detail` | Procedure-level analytics per condition with clinical descriptions |
| `executive_kpis` | Portfolio totals across all active conditions |
| `opportunity_score` | Composite ranked scoring across all conditions |

---

## Opportunity Scoring

`opportunity_score` produces the composite ranking across all active conditions.

```
Composite Score = (0.40 × beneficiary_volume_percentile)
                + (0.40 × medicare_allowed_percentile)
                + (0.20 × strategic_tier_weight)

Tier 1 — Flagship  : tier = 1  (weight = 1.0)
Tier 2 — Core      : tier = 2  (weight = 0.65)
Tier 3 — Baseline  : tier = 3  (weight = 0.35)
```

Volume and spend dimensions are normalized to `NTILE(100)` percentile ranks before combining. The tier weight is applied as a fixed multiplier, not a percentile, ensuring flagship conditions carry a persistent strategic premium regardless of data volume.

---

## SQLPage Dashboard

`mmi-unified-dashboard.md` is the Executable Markdown file that defines both the deploy script and the full SQLPage UI. It runs on port `9227` against `resource-surveillance.sqlite.db`.

### Navigation Menu

| Page | Route | Description |
|---|---|---|
| Home | `/` | Pipeline health KPIs + dynamic disease condition cards (auto-generated from registry) |
| Executive Dashboard | `/mmi/executive-dashboard.sql` | Portfolio totals, cross-condition comparison charts, full summary table |
| Disease Conditions | `/mmi/conditions.sql` | Full condition registry — cards and table view |
| Opportunity Scores | `/mmi/opportunity-scoring.sql` | Composite ranked opportunity matrix |
| Geography | `/mmi/geography.sql` | State-level market sizing, cost tiers, and GPCI factors |
| Procedure Drilldown | `/mmi/procedure-drilldown.sql` | HCPCS-level analytics with condition and code filters, paginated |
| Data Dictionary | `/mmi/data-dictionary.sql` | Schema reference: derived objects, source tables, and performance indexes |

Every condition card on the landing page links to a universal **Condition Hub** (`/mmi/condition-hub.sql?condition=<name>`) that shows national KPIs, data source breakdown, top procedures, and geographic breakdown for that condition — all driven by the `?condition=` URL parameter. No new page is needed when a condition is added.

---

## Deploy

```bash
# 1. Clean slate — remove any previous database
rm -f resource-surveillance.sqlite.*

# 2. Ingest raw CMS CSV files into SQLite RSSD
surveilr ingest files -r medicare-ds/

# 3. Transform raw ingestion into typed uniform_resource_* tables
surveilr orchestrate transform-csv

# 4. Apply base DDL (data provenance and supplementary schema objects)
surveilr shell sql/medigy-ddl.sql

# 5. Run the unified ELT pipeline — builds all dims, fact, and analytics views
surveilr shell sql/medigy-unified-v2.sql


# 6. Configure SMTP environment variables for registration welcome emails
export EMAIL_HOST="<your-host>"
export EMAIL_USERNAME="<your-user-name>"
export EMAIL_APP_PASSWORD="<your-mailgun-app-password>"
export EMAIL_FROM="<your-from-email>"
export EMAIL_PORT="<your-port>"

# 7. Compile the Executable Markdown UI and load it into the database
spry sp spc --package --conf sqlpage/sqlpage.json -m mmi-unified-dashboard.md \
  | sqlite3 resource-surveillance.sqlite.db

echo "Medigy Market Intelligence (Unified v2) is ready at http://localhost:9227"
```

---

## Adding a New Disease Condition

1. Open `sql/medigy-unified-v2.sql` and locate the seed block under **Layer 1**.
2. Insert one row into `dim_condition_registry` following the pattern of existing entries.
3. Re-run the pipeline from step 5 of the deploy sequence above.

The landing page card, Condition Hub drilldown, executive dashboard, opportunity score, geographic breakdown, and procedure table all update automatically.

Example (Heart Failure was added this way):

```sql
INSERT OR IGNORE INTO dim_condition_registry
(condition_name, body_system, tier, icd10_prefix,
 hcpcs_range_start, hcpcs_range_end, hcpcs_exact_list,
 use_bygeo, use_dmepos, use_hospital,
 specialty_domain, b2b_tier_primary, em_share_pct, dme_cap_months,
 icon, color)
VALUES
('Heart Failure', 'Cardiovascular', 2, 'I50',
 '93000', '93999', '["93000","93303","93306","93350","93351","99490","99439"]',
 1, 0, 1,
 'Cardiovascular', 'Cardiology', 0.07, 0,
 'heart', 'red');
```

---

## Key Design Decisions

### Single registry drives everything

All dimension tables, the unified fact table, and every analytics view derive from `dim_condition_registry`. This eliminates the need to modify SQL logic or page definitions when adding new conditions — the only file that ever changes is the registry seed block.

### Multi-source fact table with heterogeneous procedure codes

`fact_utilization_unified` combines three structurally different CMS datasets, each using a different procedure code column: `HCPCS_Cd` (Part B geographic and DME), `APC_Cd` (outpatient hospital), and `DRG_Cd` (inpatient hospital). All are unified under a single `hcpcs_code` column in the fact table, with `source_type` distinguishing origin. This allows cross-source aggregation at the condition level while preserving source-layer drilldown in `condition_source_breakdown`.

### URL parameter routing for the Condition Hub

Rather than generating one SQL page per disease, a single `condition-hub.sql` page accepts a `?condition=` URL parameter and drives all queries from it. Condition names with spaces are encoded using `REPLACE(condition_name, ' ', '%20')` for SQLite compatibility. All filtering uses `LOWER(TRIM(...))` on both sides to guard against case and whitespace mismatches in the registry seed data.

### Opportunity scoring uses tier weights, not percentile rank, for strategic tiers

The tier dimension (Flagship / Core / Baseline) is applied as a fixed multiplier (1.0 / 0.65 / 0.35) rather than being normalized into a percentile alongside volume and spend. This ensures that a Tier 1 condition with moderate data volume still outranks a Tier 3 condition with high volume — reflecting the intentional strategic prioritization in the registry, not just data size.

### GPCI cost-tier enrichment in the geography layer

`dim_geography` enriches each state with the 2026 CMS Physician Work GPCI factor and a derived cost tier (`High / Medium / Low`). This tier is surfaced in `condition_state_breakdown` and the Geography dashboard page, enabling cost-adjusted market sizing without post-processing.
