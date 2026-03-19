# Medicare Market Intelligence (MMI)

A CMS Medicare Part B analytics pipeline built with `surveilr` and `spry` that transforms raw public datasets into a navigable SQLPage business intelligence application. Designed to identify high-opportunity disease-specialty clusters for **Manos Health** and **CCIQ** through evidence-based commercial validation.

---

## What This Does

The pipeline ingests CMS public datasets, builds a normalized star schema in SQLite, runs 13 analytical views and an opportunity scoring engine, and packages everything into a multi-page SQLPage UI — all from a single Executable Markdown file and one SQL script.

The primary deliverable is `opportunity_scoring_view`: a composite Tier 1/2/3 ranking of every disease-state × specialty combination, scored on patient volume, interaction intensity, and economic weight.

---

## Architecture

```
CMS Public Datasets (CSV) and master data(ICT,CPT,HCPS etc)
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

### Key Design Decisions

**`fact_utilization` sources from `bygeography` + `specialty_by_state`**

The two core CMS datasets use incompatible state identifiers — `bygeography` uses numeric FIPS codes (`01`, `06`, `48`) while `cms_provider` uses state abbreviations (`AL`, `CA`, `TX`). A hardcoded `fips_state_map` table bridges them. Specialty is derived from `cms_provider.Rndrng_Prvdr_Type` via a deduplicated `specialty_by_state` materialized table (one row per state × specialty), joined to `bygeography` on the translated state abbreviation. This avoids both the Cartesian explosion (joining on state alone) and NULL specialty (joining on a mismatched key).

**Specialty normalization is inline CASE, not a lookup join**

`dim_specialty` exists as a reference table, but `fact_utilization` and `specialty_by_state` derive `specialty_name` and `specialty_domain` directly from `Rndrng_Prvdr_Type` using CASE expressions. This eliminates join failures caused by whitespace or case differences between raw CMS strings.

**Opportunity scoring bridges disease → specialty via body system**

`dim_diagnosis.body_system` (e.g. `'Cardiovascular'`) maps to `fact_utilization.specialty_domain` (e.g. `'Cardiovascular'`). The `opportunity_scoring_view` joins on this bridge to score every disease × specialty intersection. Scores are normalized via `NTILE` percentile ranking across three dimensions: volume (35%), interaction intensity (35%), and economic weight (30%), with a market concentration bonus of up to +10 points from `specialty_dominance_ratio`.

---

## Project Structure

```text
.
├── medicare-ds/                        # CMS source CSV files (Git ignored)
├── sql/
│   └── medicare-analytics.sql          # Full ELT pipeline — run this after ingestion
├── sqlpage/
│   └── sqlpage.json                    # SQLPage server configuration
├── mmi-sqlpage-dashboard.md            # Executable Markdown — UI definition + deploy script
└── resource-surveillance.sqlite.db     # Output RSSD (SQLite, generated)
```

---

## Pipeline Stages (`medicare-analytics.sql`)

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

---

## Data Model

### Dimension Tables

| Table | Key Columns | Purpose |
|---|---|---|
| `dim_procedure` | `hcpcs_code`, `procedure_category`, `work_rvu`, `is_monitoring_flag` | CPT + HCPCS Level II lookup with clinical categorization and RVU weights |
| `dim_diagnosis` | `icd10_code`, `disease_state`, `body_system` | 25+ named disease clusters mapped from ICD-10-CM; `body_system` bridges to specialty domain |
| `dim_specialty` | `raw_specialty_name`, `specialty_name`, `specialty_domain` | Canonical specialty names normalized from CMS `Rndrng_Prvdr_Type` strings |
| `dim_geography` | `state_abbr`, `pw_gpci`, `cost_tier` | 2026 CMS GPCI factors for cost-adjusted market sizing |
| `fips_state_map` | `fips_code`, `state_abbr` | Static 52-row FIPS-to-abbreviation bridge (required to join bygeography → provider) |

### Fact Tables

| Table | Key Columns | Purpose |
|---|---|---|
| `specialty_by_state` | `state_abbr`, `specialty_name`, `specialty_domain` | Deduplicated provider specialty lookup per state — intermediate bridge table |
| `fact_utilization` | `specialty_name`, `specialty_domain`, `hcpcs_code`, `state_abbr`, `place_of_service`, `total_beneficiaries`, `total_services`, `total_medicare_payment` | Central fact table: volume, frequency, and economics per HCPCS × state × specialty |
| `specialty_market_dynamics` | `specialty_name`, `hcpcs_code`, `specialty_dominance_ratio` | Each specialty's share of national patient volume per HCPCS code (0.0–1.0) |

### Analytical Views

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

---

## CMS Source Datasets

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

---

## Getting Started

### Prerequisites

- [`surveilr`](https://www.surveilr.com) — installed and on PATH
- [`spry`](https://github.com/netspective-labs/spry) — installed and on PATH
- `sqlite3` — available in shell
- CMS source CSV files downloaded into `medicare-ds/`

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

---

## SQLPage UI Modules

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

---

## Known Constraints

**State-level specialty inference** — `bygeography` records HCPCS + spend at the state level with no specialty column. `cms_provider` records specialty at the NPI level with no HCPCS or spend columns. There is no shared key between them at the HCPCS level. Specialty is inferred by joining on state: "Cardiology operated in Texas → Cardiology is attributed to all Cardiology HCPCS rows in Texas." This is standard practice for CMS geography-level analysis but means individual provider-level attribution is not possible from these two datasets alone.

**FIPS code bridging** — `bygeography` uses numeric FIPS state codes; `cms_provider` uses state abbreviations. The `fips_state_map` table (52 rows, hardcoded) bridges them. Territories (PR `72`, VI `78`) are included.

**NTILE bucket size** — `opportunity_scoring_view` uses `NTILE(10)` by default. Switch to `NTILE(100)` once you confirm the disease × specialty bridge produces at least several hundred rows, and adjust tier thresholds to `≥ 75` / `≥ 50` accordingly.
