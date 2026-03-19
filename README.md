# Medicare Analytics — Combined Project README

This document consolidates three parallel approaches to building a Medicare analytics pipeline from CMS public datasets using `surveilr`, `spry`, and SQLPage.

---

## Table of Contents

1. [Approach 1 — Medicare Market Intelligence (MMI)](#approach-1--medicare-market-intelligence-mmi)
2. [Approach 2 — Medicare DSD Evidence (Spryfile-based)](#approach-2--medicare-dsd-evidence-spryfile-based)
3. [Approach 3 — Medicare DSD Evidence (cms_workflow-based)](#approach-3--medicare-dsd-evidence-cms_workflow-based)
4. [Shared Prerequisites](#shared-prerequisites)
5. [Shared Data Assets](#shared-data-assets)

---

## Approach 1 — Medicare Market Intelligence (MMI)

A CMS Medicare Part B analytics pipeline built with `surveilr` and `spry` that transforms raw public datasets into a navigable SQLPage business intelligence application. Designed to identify high-opportunity disease-specialty clusters for **Manos Health** and **CCIQ** through evidence-based commercial validation.

**Note:** The code for this approach is available on the main branch. One additional file, cms_provider.csv, is used but excluded from the repository due to its size.

### What This Does

The pipeline ingests CMS public datasets, builds a normalized star schema in SQLite, runs 13 analytical views and an opportunity scoring engine, and packages everything into a multi-page SQLPage UI — all from a single Executable Markdown file and one SQL script.

The primary deliverable is `opportunity_scoring_view`: a composite Tier 1/2/3 ranking of every disease-state × specialty combination, scored on patient volume, interaction intensity, and economic weight.

### Architecture

```
CMS Public Datasets (CSV) and master data (ICD, CPT, HCPCS etc.)
        │
        ▼
surveilr ingest          ← extracts and loads into SQLite RSSD
        │
        ▼
surveilr orchestrate     ← transforms raw CSV ingestion into typed tables
        │
        ▼
medicare-analytics.sql   ← ELT pipeline: indexes → dims → facts → views → scoring
        │
        ▼
spry + SQLPage           ← packages Executable MD into browser UI
        │
        ▼
resource-surveillance.sqlite.db   ← single-file output (RSSD)
```


#### Key Design Decisions

**`fact_utilization` sources from `bygeography` + `specialty_by_state`**

The two core CMS datasets use incompatible state identifiers — `bygeography` uses numeric FIPS codes (`01`, `06`, `48`) while `cms_provider` uses state abbreviations (`AL`, `CA`, `TX`). A hardcoded `fips_state_map` table bridges them. Specialty is derived from `cms_provider.Rndrng_Prvdr_Type` via a deduplicated `specialty_by_state` materialized table (one row per state × specialty), joined to `bygeography` on the translated state abbreviation. This avoids both the Cartesian explosion (joining on state alone) and NULL specialty (joining on a mismatched key).

**Specialty normalization is inline CASE, not a lookup join**

`dim_specialty` exists as a reference table, but `fact_utilization` and `specialty_by_state` derive `specialty_name` and `specialty_domain` directly from `Rndrng_Prvdr_Type` using CASE expressions. This eliminates join failures caused by whitespace or case differences between raw CMS strings.

**Opportunity scoring bridges disease → specialty via body system**

`dim_diagnosis.body_system` (e.g. `'Cardiovascular'`) maps to `fact_utilization.specialty_domain` (e.g. `'Cardiovascular'`). The `opportunity_scoring_view` joins on this bridge to score every disease × specialty intersection. Scores are normalized via `NTILE` percentile ranking across three dimensions: volume (35%), interaction intensity (35%), and economic weight (30%), with a market concentration bonus of up to +10 points from `specialty_dominance_ratio`.

### Project Structure

```text
.
├── medicare-ds/                        # CMS source CSV files (Git ignored)
├── sql/
│   └── medicare-analytics.sql          # Full ELT pipeline — run this after ingestion
├── mmi-dashboard.md            # Executable Markdown — UI definition + deploy script
└── resource-surveillance.sqlite.db     # Output RSSD (SQLite, generated)
```

### Pipeline Stages (`medicare-analytics.sql`)

| Section | Objects Created | Purpose |
|---|---|---|
| **Section 0** | 14 indexes | Query performance on raw CMS source tables |
| **Section 1** | `dim_procedure`, `dim_diagnosis`, `dim_specialty`, `dim_geography` | Star schema normalization |
| **Section 1B** | `fips_state_map` | Bridges FIPS numeric codes → state abbreviations |
| **Section 2A** | `specialty_by_state`, `fact_utilization` | Core fact table: volume + economics per HCPCS × state × specialty |
| **Section 2B** | `specialty_market_dynamics` | Specialty dominance ratio per HCPCS code nationally |
| **Section 3** | Views 1–12 | Business question layer (see Views below) |
| **Section 4** | `opportunity_scoring_view` | Primary deliverable — composite Tier 1/2/3 ranking |
| **Section 5** | Queries A–I | Reference analyst queries (commented out) |

### Data Model

#### Dimension Tables

| Table | Key Columns | Purpose |
|---|---|---|
| `dim_procedure` | `hcpcs_code`, `procedure_category`, `work_rvu`, `is_monitoring_flag` | CPT + HCPCS Level II lookup with clinical categorization and RVU weights |
| `dim_diagnosis` | `icd10_code`, `disease_state`, `body_system` | 25+ named disease clusters mapped from ICD-10-CM; `body_system` bridges to specialty domain |
| `dim_specialty` | `raw_specialty_name`, `specialty_name`, `specialty_domain` | Canonical specialty names normalized from CMS `Rndrng_Prvdr_Type` strings |
| `dim_geography` | `state_abbr`, `pw_gpci`, `cost_tier` | 2026 CMS GPCI factors for cost-adjusted market sizing |
| `fips_state_map` | `fips_code`, `state_abbr` | Static 52-row FIPS-to-abbreviation bridge (required to join bygeography → provider) |

#### Fact Tables

| Table | Key Columns | Purpose |
|---|---|---|
| `specialty_by_state` | `state_abbr`, `specialty_name`, `specialty_domain` | Deduplicated provider specialty lookup per state — intermediate bridge table |
| `fact_utilization` | `specialty_name`, `specialty_domain`, `hcpcs_code`, `state_abbr`, `place_of_service`, `total_beneficiaries`, `total_services`, `total_medicare_payment` | Central fact table: volume, frequency, and economics per HCPCS × state × specialty |
| `specialty_market_dynamics` | `specialty_name`, `hcpcs_code`, `specialty_dominance_ratio` | Each specialty's share of national patient volume per HCPCS code (0.0–1.0) |

#### Analytical Views

| # | View | Business Purpose |
|---|---|---|
| 1 | `specialty_activity_summary` | Top-line KPIs: total spend, patient reach, provider count, spend/patient, services/patient |
| 2 | `specialty_economic_intensity` | Intensity index = spend/patient × services/patient, ranked within clinical domain |
| 3 | `specialty_top_procedures` | Top 10 HCPCS codes per specialty by volume with spend rank alongside |
| 4 | `specialty_market_concentration` | Specialty ownership share of national volume per code — market dominance signal |
| 5 | `chronic_interaction_density` | Services per patient per code; tiered High / Moderate / Low interaction frequency |
| 6 | `monitoring_procedure_intensity` | % of specialty volume from repeat-monitoring codes; monitoring vs total spend |
| 7 | `dme_supply_refill_metrics` | DME/supply refill velocity (units per patient) — chronic disease management signal |
| 8 | `surgical_economic_metrics` | Anesthesia CPT codes scored by conversion factor — surgical economic load proxy |
| 9 | `part_b_drug_intensity` | Part B drug administrations, patients, and spend per specialty by HCPCS code |
| 10 | `geographic_market_opportunity` | State-level volume and GPCI cost-adjusted spend per specialty |
| 11 | `facility_vs_office_split` | Office vs facility care setting mix — ambulatory ownership signal |
| 12 | `disease_state_icd_coverage` | ICD-10 code count per disease cluster — mapping completeness validation |
| 13 | `opportunity_scoring_view` | **Primary deliverable** — composite Tier 1/2/3 ranking of disease × specialty clusters |

### Opportunity Scoring Formula

```
Composite Score = (0.35 × volume_percentile)
                + (0.35 × intensity_percentile)
                + (0.30 × economics_percentile)
                + dominance_bonus (up to +10 pts)

Tier 1 — High Opportunity  : score ≥ 75
Tier 2 — Moderate           : score ≥ 50
Tier 3 — Low Priority       : score < 50
```

All three dimensions are normalized to NTILE(100) percentile ranks before combining. The dominance bonus is derived from `specialty_market_dynamics.specialty_dominance_ratio × 10`.

### CMS Source Datasets

All datasets are publicly available — no login required.

| SQLite Table | CMS Dataset | Key Columns Used |
|---|---|---|
| `uniform_resource_cms_bygeography` | Physician & Other Practitioners — By Geography & Service | `HCPCS_Cd`, `Tot_Srvcs`, `Tot_Benes`, `Avg_Mdcr_Pymt_Amt`, `Rndrng_Prvdr_Geo_Cd`, `Place_Of_Srvc` |
| `uniform_resource_cms_provider` | Physician & Other Practitioners — By Provider | `Rndrng_NPI`, `Rndrng_Prvdr_Type`, `Rndrng_Prvdr_State_Abrvtn` |
| `uniform_resource_ref_icd10_diagnosis` | ICD-10-CM (CDC/NCHS) | `icd10_code`, `description_long` |
| `uniform_resource_ref_procedure_code` | CMS Physician Fee Schedule | `HCPCS`, `WORK RVU`, `MEDICARE PAYMENT` |
| `uniform_resource_ref_hcpcs_level_two_procedures` | CMS HCPCS Level II | `hcpcs_code`, `short_description` |
| `uniform_resource_ref_geo_adjustment` | CMS GPCI 2026 | `State`, `PW GPCI` |
| `uniform_resource_ref_anes_conversion_factor` | CMS Anesthesia Conversion Factor | `Contractor`, `Locality`, `Conversion Factor` |

> **Download order:** Start with By Geography & Service (smallest file). By Provider & Service is the largest — begin that download early.

Source: [CMS Physician & Other Practitioners](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners)

### Deploy

```bash
# 1. Clean slate — remove any previous database
rm -f resource-surveillance.sqlite.db

# 2. Ingest raw CMS CSV files into SQLite RSSD
surveilr ingest files -r medicare-ds/

# 3. Transform raw ingestion into typed reference tables
surveilr orchestrate transform-csv

# 4. Run the ELT pipeline — builds all dims, facts, views, and scoring
surveilr shell sql/medicare-analytics.sql

# 5. Package the SQLPage UI and load it into the database
spry sp spc --package --conf sqlpage/sqlpage.json -m mmi-dashboard.md \
  | sqlite3 resource-surveillance.sqlite.db

echo "MMI database and SQLPage UI are ready."
```

### Launch

```bash
# SQLPage serves on port 9227 by default (configured in sqlpage.json)
sqlpage --database resource-surveillance.sqlite.db
```

Then open [http://localhost:9227](http://localhost:9227) in your browser.

### SQLPage UI Modules

| Page | Route | Description |
|---|---|---|
| Home | `/` | Pipeline health KPIs + navigation to all modules |
| Executive Dashboard | `/mmi/executive-dashboard.sql` | Top-line spend, patient reach, and economic intensity by specialty |
| Opportunity Scoring | `/mmi/opportunity-scoring.sql` | Composite Tier 1/2/3 ranking — primary Manos Health deliverable |
| Specialty Explorer | `/mmi/specialty-explorer.sql` | Deep-dive into any specialty: top procedures, monitoring intensity, market dominance |
| Disease Mapping | `/mmi/disease-mapping.sql` | ICD-10 cluster coverage, interaction density, and repeat-visit tier classification |
| Geographic Markets | `/mmi/geographic-markets.sql` | State-level patient volume and GPCI cost-adjusted spend |
| Procedure Drilldown | `/mmi/procedure-drilldown.sql` | Part B drug spend, DME refill velocity, facility vs office split, surgical economics |
| Data Dictionary | `/mmi/data-dictionary.sql` | Complete schema reference for all tables, views, and scoring methodology |

### Known Constraints

**State-level specialty inference** — `bygeography` records HCPCS + spend at the state level with no specialty column. `cms_provider` records specialty at the NPI level with no HCPCS or spend columns. There is no shared key between them at the HCPCS level. Specialty is inferred by joining on state: "Cardiology operated in Texas → Cardiology is attributed to all Cardiology HCPCS rows in Texas." This is standard practice for CMS geography-level analysis but means individual provider-level attribution is not possible from these two datasets alone.

**FIPS code bridging** — `bygeography` uses numeric FIPS state codes; `cms_provider` uses state abbreviations. The `fips_state_map` table (52 rows, hardcoded) bridges them. Territories (PR `72`, VI `78`) are included.

**NTILE bucket size** — `opportunity_scoring_view` uses `NTILE(10)` by default. Switch to `NTILE(100)` once you confirm the disease × specialty bridge produces at least several hundred rows, and adjust tier thresholds to `≥ 75` / `≥ 50` accordingly.

---

---

## Approach 2 — Medicare DSD Evidence (Spryfile-based)

This approach builds a Spry-managed SQLPage dashboard for exploring Medicare data from CMS public-use files. The dashboard focuses on specialty proxy utilization, top HCPCS/CPT procedures, condition opportunity scoring, and classification QA for the HCPCS-to-specialty heuristic mapping.

`Spryfile.md` is the source of truth for the SQLPage routes. Spry materializes the executable SQL files into `dev-src.auto/`, and SQLPage serves those routes against `resource-surveillance.sqlite.db`.

### Repository Layout

- `Spryfile.md` — Spry playbook and embedded SQLPage routes
- `business_question_views.sql` — derived analytical SQLite views
- `medicare-ds/` — CMS source/reference CSV files used for ingestion
- `dev-src.auto/` — generated SQLPage web root for local development
- `sqlpage/sqlpage.json` — SQLPage runtime config
- `resource-surveillance.sqlite.db` — local SQLite database

### Dashboard Routes

The Spry playbook generates these SQLPage pages:

- `index.sql` — overview / executive summary
- `specialties.sql` — specialty proxy utilization and economic intensity
- `procedures.sql` — top procedures and drilldowns by specialty proxy
- `conditions.sql` — condition opportunity scoring
- `classification.sql` — mapping coverage and unclassified code QA

### Derived Views

Running `business_question_views.sql` creates the following views used by the dashboard:

- `procedure_specialty_proxy_map`
- `procedure_volume_by_specialty`
- `top_procedures_per_specialty`
- `condition_to_icd_mapping`
- `proxy_condition_activity`
- `economic_intensity_index`
- `opportunity_scoring_view`

### Build or Refresh the SQLite Database

Run from the repository root:

```bash
rm -f resource-surveillance.sqlite.db
surveilr ingest files -r medicare-ds/
surveilr orchestrate transform-csv
surveilr shell --engine rusqlite business_question_views.sql -d resource-surveillance.sqlite.db
```

### Generate the SQLPage App from Spry

Materialize the SQLPage routes into `dev-src.auto/`:

```bash
spry sp spc -m Spryfile.md --fs dev-src.auto --destroy-first --conf sqlpage/sqlpage.json
```

To confirm the playbook parses and exposes the expected routes:

```bash
spry sp spc ls -m Spryfile.md
```

### Run SQLPage

```bash
sqlpage
```

Then open: `http://127.0.0.1:8080/index.sql`

### Development Workflow

Regenerate the app whenever you change `Spryfile.md`:

```bash
spry sp spc -m Spryfile.md --fs dev-src.auto --destroy-first --conf sqlpage/sqlpage.json
```

For watch mode:

```bash
spry sp spc -m Spryfile.md --fs dev-src.auto --destroy-first --conf sqlpage/sqlpage.json --watch
```

If you want Spry to restart SQLPage automatically after rebuilds:

```bash
spry sp spc -m Spryfile.md --fs dev-src.auto --destroy-first --conf sqlpage/sqlpage.json --watch --with-sqlpage
```

### Cleaning Generated Artifacts

```bash
rm -rf dev-src.auto
```

### Notes

- The analytics are built against national aggregates (`Rndrng_Prvdr_Geo_Lvl = 'National'`).
- `dev-src.auto/` is generated output and can be recreated from `Spryfile.md`.
- `spry.d/` content inside `dev-src.auto/` is generated metadata used by Spry/SQLPage.

### Typical Local Run

```bash
rm -f resource-surveillance.sqlite.db
surveilr ingest files -r medicare-ds/
surveilr orchestrate transform-csv
surveilr shell --engine rusqlite business_question_views.sql -d resource-surveillance.sqlite.db
spry sp spc -m Spryfile.md --fs dev-src.auto --destroy-first --conf sqlpage/sqlpage.json
sqlpage
```

---

---

## Approach 3 — Medicare DSD Evidence (cms_workflow-based)

This approach builds a Medicare evidence database from CMS provider, geography, procedure, and diagnosis reference files. It uses `surveilr` to ingest CSV assets into a SQLite Resource Surveillance State Database (RSSD), applies SQL business views for specialty and condition analytics, and packages a SQLPage UI with `spry`.

### Purpose

The project is designed to answer business questions such as:

- which specialties have the highest procedure volume
- which CPT/HCPCS codes rank highest within a specialty
- how disease-state mappings connect to ICD-10 code families
- which conditions appear commercially attractive based on prevalence and proxy utilization
- which specialties show the highest economic intensity

### Repository Contents

```text
.
├── README.md
├── cms_workflow.md                    # Executable workflow and SQLPage definitions
├── convert-hcps-tocsv.py              # Fixed-width HCPCS to CSV helper
├── convert-icd-to-csv.py              # Fixed-width ICD to CSV helper
├── medicare-ds/                       # Source Medicare and reference CSV datasets
│   ├── cms_bygeography.csv
│   ├── cms_provider.csv
│   ├── ref_anes_conversion_factor.csv
│   ├── ref_condition_icd_mapping.csv
│   ├── ref_geo_adjustment.csv
│   ├── ref_hcpcs_level_two_procedures.csv
│   ├── ref_icd10_diagnosis.csv
│   ├── ref_medicare_localities.csv
│   ├── ref_opps_price_cap.csv
│   ├── ref_procedure_code.csv
│   └── ref_rvu_qpp.csv
├── sql/
│   └── medicare_business_views.sql    # Derived analytics views
└── sqlpage/
    └── sqlpage.json                   # SQLPage packaging/runtime settings
```

### Core Tooling

- `surveilr` — ingests files and exposes the RSSD through MCP
- `spry` — packages executable markdown into SQLPage content
- `sqlite3` — applies SQL views and stores generated pages
- SQLPage — renders the database-backed UI
- VS Code MCP — enables query access to the SQLite RSSD from chat

### Workflow

The end-to-end workflow is defined in [cms_workflow.md](cms_workflow.md) and can be executed with a single command:

```bash
spry rb task prepare-db-deploy-server cms_workflow.md
```

That task performs the following steps:

1. remove any previous SQLite database
2. ingest CSV files from `medicare-ds/`
3. run `surveilr orchestrate transform-csv`
4. load business views from `sql/medicare_business_views.sql`
5. package SQLPage pages defined in `cms_workflow.md`

In practical terms, the workflow builds `resource-surveillance.sqlite.db` in the project root.

### Setup

Install these tools and make sure they are available on your `PATH`:

- `surveilr`
- `spry`
- `sqlite3`

### Build the Database and UI

```bash
spry rb task prepare-db-deploy-server cms_workflow.md
```

### View SQLPage

After the database and packaged pages are ready, start SQLPage from the terminal:

```bash
sqlpage
```

### SQLPage Configuration

SQLPage runtime settings are stored in `sqlpage/sqlpage.json`:

- `database_url`: `sqlite://resource-surveillance.sqlite.db?mode=rwc`
- `port`: `9227`
- `allow_exec`: `true`

### Analytics Views

The main derived views are defined in `sql/medicare_business_views.sql`.

#### `procedure_volume_by_specialty`

Aggregates provider data by `Rndrng_Prvdr_Type` and exposes:

- `specialty`
- `provider_count`
- `total_hcpcs_codes`
- `total_services`
- `total_beneficiaries`
- `total_allowed_amount`
- `avg_allowed_per_service`
- `avg_services_per_provider`

#### `top_procedures_per_specialty`

Ranks procedure codes within each specialty and exposes:

- `specialty`
- `procedure_rank`
- `hcpcs_code`
- `hcpcs_desc`
- `estimated_services`
- `estimated_beneficiaries`
- `estimated_allowed_amount`

> **Note:** This view is an **estimated** ranking. It allocates national HCPCS volume to each specialty based on the specialty's share of overall services, then keeps the top 25 ranked codes per specialty.

#### `condition_to_icd_mapping`

Combines the manual condition mapping seed with ICD-10 reference data and exposes:

- `condition_name`
- `priority_tier`
- `prevalence_weight`
- `proxy_specialty_pattern`
- `icd_prefix`
- `icd10_code`
- `is_billable`
- `description_short`
- `description_long`

#### `proxy_condition_activity`

Approximates condition activity by linking mapped ICD families to CPT-heavy specialties. Exposed fields:

- `condition_name`
- `priority_tier`
- `prevalence_weight`
- `mapped_icd_prefix_count`
- `mapped_icd_code_count`
- `linked_cpt_heavy_specialties`
- `proxy_total_services`
- `proxy_total_beneficiaries`
- `proxy_total_allowed_amount`
- `services_per_mapped_icd`

#### `economic_intensity_index`

Calculates a normalized specialty score from services, beneficiaries, and allowed amount:

- `specialty`
- `total_services`
- `total_beneficiaries`
- `total_allowed_amount`
- `services_score`
- `beneficiaries_score`
- `allowed_amount_score`
- `economic_intensity_index`

#### `opportunity_scoring_view`

Builds a condition-level opportunity ranking using prevalence, procedure density, and specialty concentration:

- `opportunity_rank`
- `condition_name`
- `opportunity_score`
- `prevalence_score`
- `procedure_density_score`
- `specialty_concentration_score`
- `total_services`
- `total_beneficiaries`
- `linked_specialties`

### SQLPage Pages

The packaged SQLPage UI currently includes pages for:

- Home
- Procedure Volume by Specialty
- Top Procedures per Specialty
- Condition to ICD Mapping
- Proxy Condition Activity
- Economic Intensity Index
- Opportunity Scoring View

These page definitions are embedded in `cms_workflow.md`.

### MCP Configuration in VS Code

This workspace includes MCP server configuration in `.vscode/mcp.json`:

```json
{
  "servers": {
    "surveilr": {
      "command": "surveilr",
      "args": ["mcp", "server", "-d", "resource-surveillance.sqlite.db"],
      "env": {}
    }
  }
}
```

This lets VS Code chat query the project database through the `surveilr` MCP server.

### Helper Conversion Scripts

- `convert-icd-to-csv.py` — converts fixed-width ICD source files into CSV
- `convert-hcps-tocsv.py` — converts fixed-width HCPCS source files into CSV

These helpers are useful when refreshing reference datasets from raw CMS text distributions.

### Typical Questions This Project Can Answer

- How many specialties are present in the provider data?
- What are the top ranked HCPCS/CPT codes within each specialty?
- What ICD-10 codes map to a specific condition such as Alzheimer Disease and Related Dementias?
- Which specialties have the highest volume or allowed amount?
- Which conditions rank highest in the opportunity scoring model?

### Notes and Assumptions

- The SQLite database file is generated locally and may not exist until the workflow has been run.
- Specialty-to-procedure ranking in `top_procedures_per_specialty` is derived from proportional allocation and should be interpreted as directional evidence rather than direct claim-level attribution.
- The disease-state mapping depends on `ref_condition_icd_mapping.csv`, which acts as a curated seed table.

### License / Data Usage

Ensure downstream use complies with CMS data usage terms and any applicable licensing or redistribution rules for the included reference files.

---

---

## Shared Prerequisites

All three approaches require the following tools installed and available on your `PATH`:

- [`surveilr`](https://www.surveilr.com)
- [`spry`](https://github.com/netspective-labs/spry)
- `sqlite3`
- `sqlpage`

---

## Shared Data Assets

All three approaches ingest from CMS public datasets, predominantly:

| File | Description |
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

Source: [CMS Physician & Other Practitioners](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners)
