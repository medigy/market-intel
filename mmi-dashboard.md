---
sqlpage-conf:
  database_url: "sqlite://resource-surveillance.sqlite.db?mode=rwc"
  web_root: "./dev-src.auto"
  allow_exec: true
  port: 9227
---
# Medicare Market Intelligence — SQLPage Application

This application surfaces the Medicare CMS analytics pipeline built from
`medicare_analytics_final.sql` through a navigable SQLPage UI.

- Data ingested from CMS public datasets via surveilr into an RSSD (SQLite)
- Star schema: `dim_procedure`, `dim_diagnosis`, `dim_specialty`, `dim_geography`
- Core fact: `fact_utilization` + `specialty_market_dynamics`
- 13 analytical views + opportunity scoring engine
- Clients: **Manos Health** & **CCIQ**

---

```bash prepare-db-deploy-server --descr "Ingest Medicare raw files, build normalized analytics tables, and package SQLPage UI."
#!/bin/bash
set -euo pipefail

# Start from a clean database to avoid previously ingested malformed resources.
rm -f resource-surveillance.sqlite.db

surveilr ingest files -r medicare-ds/ && surveilr orchestrate transform-csv 
surveilr shell sql/medicare-analytics.sql 
spry sp spc --package --conf sqlpage/sqlpage.json -m mmi-dashboard.md | sqlite3 resource-surveillance.sqlite.db
echo "Medicare patient analytics database and SQLPage UI are ready."
```

---

## Layout

Global shell injected into every page.

```sql PARTIAL global-layout.sql --inject *.sql --inject mmi/*.sql

-- BEGIN: PARTIAL global-layout.sql
SELECT 'shell' AS component,
       'Medicare Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
       '/' AS link,
       '{"link":"/","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/specialty-explorer.sql","title":"Specialty Explorer"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/geographic-markets.sql","title":"Geographic Markets"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;
-- END: PARTIAL global-layout.sql
```

---

## Home Page

```sql index.sql { route: { caption: "Home" } }
-- @route.description "Medicare Market Intelligence — Landing Page"

-- HERO
SELECT 'hero' AS component,
    'Medicare Market Intelligence' AS title,
    'CMS Part B analytics for identifying high-opportunity disease-specialty clusters. Built for Manos Health & CCIQ.' AS description,
    'azure' AS color;

-- PIPELINE HEALTH CHECK
SELECT 'big_number' AS component, 4 AS columns;

SELECT
    'Specialties Indexed' AS title,
    COUNT(DISTINCT specialty_name) AS value,
    'stethoscope' AS icon,
    'teal' AS color
FROM dim_specialty;

SELECT
    'ICD-10 Clusters Mapped' AS title,
    COUNT(DISTINCT disease_state) AS value,
    'virus' AS icon,
    'azure' AS color
FROM dim_diagnosis
WHERE disease_state != 'General / Other';

SELECT
    'Procedure Codes' AS title,
    COUNT(*) AS value,
    'clipboard-list' AS icon,
    'indigo' AS color
FROM dim_procedure;

SELECT
    'States in Scope' AS title,
    COUNT(DISTINCT state_abbr) AS value,
    'map-pin' AS icon,
    'teal' AS color
FROM dim_geography;

SELECT 'divider' AS component;

-- CORE NAVIGATION CARDS
SELECT 'card' AS component, 'Analytics Modules' AS title, 3 AS columns;

SELECT
    'Executive Dashboard' AS title,
    'Top-line KPIs: total spend, patient reach, spend per patient, and service intensity across all specialties.' AS description,
    '/mmi/executive-dashboard.sql' AS link,
    'layout-dashboard' AS icon,
    'teal' AS color;

SELECT
    'Opportunity Scoring' AS title,
    'Composite Tier 1/2/3 ranking of disease × specialty clusters. The primary Manos Health deliverable.' AS description,
    '/mmi/opportunity-scoring.sql' AS link,
    'trophy' AS icon,
    'azure' AS color;

SELECT
    'Specialty Explorer' AS title,
    'Deep-dive into any specialty: top procedures by volume, spend rank, monitoring intensity, and market dominance.' AS description,
    '/mmi/specialty-explorer.sql' AS link,
    'stethoscope' AS icon,
    'indigo' AS color;

SELECT
    'Disease Mapping' AS title,
    'ICD-10 cluster coverage, interaction density by disease, and repeat-visit tier classification.' AS description,
    '/mmi/disease-mapping.sql' AS link,
    'virus' AS icon,
    'teal' AS color;

SELECT
    'Geographic Markets' AS title,
    'State-level patient volume and GPCI-adjusted spend. Identifies the strongest markets per specialty.' AS description,
    '/mmi/geographic-markets.sql' AS link,
    'map' AS icon,
    'cyan' AS color;

SELECT
    'Procedure Drilldown' AS title,
    'Part B drug spend, facility vs office split, DME/supply refill velocity, and surgical anesthesia metrics.' AS description,
    '/mmi/procedure-drilldown.sql' AS link,
    'pill' AS icon,
    'azure' AS color;

SELECT 'divider' AS component;

-- DATA SOURCE REFERENCE
SELECT 'card' AS component, 'Pipeline Reference' AS title, 2 AS columns;

SELECT
    'Data Dictionary' AS title,
    'Schema reference for all dimension and fact tables, views, and scoring methodology.' AS description,
    '/mmi/data-dictionary.sql' AS link,
    'database' AS icon,
    'gray' AS color;

SELECT
    'CMS Source Data' AS title,
    'Direct link to CMS Physician & Other Practitioners public datasets used in this pipeline.' AS description,
    'https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners' AS link,
    'external-link' AS icon,
    'gray' AS color;
```

---

## Executive Dashboard

```sql mmi/executive-dashboard.sql { route: { caption: "Executive Dashboard" } }
-- @route.description "Top-line Medicare market KPIs: spend, volume, and intensity by specialty"

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '/' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
    'Executive Dashboard' AS title,
    'Aggregate Medicare Part B performance metrics across all specialties and geographies.' AS description,
    'teal' AS color;

-- TOP-LINE KPIs
SELECT 'big_number' AS component, 4 AS columns;

SELECT
    'Total Medicare Spend' AS title,
    '$' || ROUND(SUM(total_medicare_spend) / 1e9, 1) || 'B' AS value,
    'currency-dollar' AS icon,
    'teal' AS color
FROM specialty_activity_summary;

SELECT
    'Total Patient Reach' AS title,
    ROUND(SUM(patient_reach) / 1e6, 1) || 'M' AS value,
    'users' AS icon,
    'azure' AS color
FROM specialty_activity_summary;

SELECT
    'Avg Spend / Patient' AS title,
    '$' || ROUND(SUM(total_medicare_spend) / NULLIF(SUM(patient_reach), 0), 0) AS value,
    'calculator' AS icon,
    'indigo' AS color
FROM specialty_activity_summary;

SELECT
    'Avg Services / Patient' AS title,
    ROUND(SUM(service_volume) * 1.0 / NULLIF(SUM(patient_reach), 0), 1) AS value,
    'activity' AS icon,
    'teal' AS color
FROM specialty_activity_summary;

SELECT 'divider' AS component;

-- TOP SPECIALTIES BY SPEND
SELECT 'text' AS component,
    'Top Specialties by Medicare Spend' AS title,
    'Ranked by total Medicare payment across all states and procedure codes.' AS contents;

SELECT 'table' AS component,
    TRUE AS sort,
    TRUE AS search,
    TRUE AS hover,
    TRUE AS striped_rows;

SELECT
    specialty_name      AS "Specialty",
    specialty_domain    AS "Domain",
    ROUND(total_medicare_spend / 1e9, 2) AS "Spend ($B)",
    patient_reach       AS "Patients",
    ROUND(spend_per_patient, 2) AS "$/Patient",
    ROUND(avg_srvcs_per_patient, 2) AS "Srvcs/Patient"
FROM specialty_activity_summary
ORDER BY total_medicare_spend DESC
LIMIT 20;

SELECT 'divider' AS component;

-- ECONOMIC INTENSITY LEADERS
SELECT 'text' AS component,
    'Highest Economic Intensity by Specialty' AS title,
    'Intensity index = spend per patient × services per patient. High values indicate specialties with both high spend AND high visit frequency — the most valuable ongoing care relationships.' AS contents;

SELECT 'table' AS component,
    TRUE AS sort,
    TRUE AS hover,
    TRUE AS striped_rows;

SELECT
    specialty_name              AS "Specialty",
    specialty_domain            AS "Domain",
    ROUND(economic_intensity_index, 1) AS "Intensity Index",
    ROUND(spend_per_patient, 2) AS "$/Patient",
    ROUND(avg_srvcs_per_patient, 2) AS "Srvcs/Patient",
    rank_within_domain          AS "Rank in Domain"
FROM specialty_economic_intensity
ORDER BY economic_intensity_index DESC
LIMIT 20;
```

---

## Opportunity Scoring

```sql mmi/opportunity-scoring.sql { route: { caption: "Opportunity Scores" } }
-- @route.description "Composite Tier 1/2/3 ranking of disease × specialty clusters"

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '/' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
    'Opportunity Scoring Engine' AS title,
    'Composite ranking of disease-state × specialty clusters. Formula: 35% Volume + 35% Interaction Intensity + 30% Economic Impact. Market concentration bonus up to +10 points.' AS description,
    'azure' AS color;

SELECT 'big_number' AS component, 3 AS columns;

SELECT 'Tier 1 — High Opportunity' AS title,
    COUNT(*) AS value, 'trophy' AS icon, 'teal' AS color
FROM opportunity_scoring_view WHERE opportunity_tier = 'Tier 1 — High Opportunity';

SELECT 'Tier 2 — Moderate Opportunity' AS title,
    COUNT(*) AS value, 'star' AS icon, 'azure' AS color
FROM opportunity_scoring_view WHERE opportunity_tier = 'Tier 2 — Moderate Opportunity';

SELECT 'Tier 3 — Low Priority' AS title,
    COUNT(*) AS value, 'minus' AS icon, 'gray' AS color
FROM opportunity_scoring_view WHERE opportunity_tier = 'Tier 3 — Low Priority';

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Tier 1 — High Opportunity Clusters' AS title,
    'Disease × specialty combinations scoring ≥ 75. These represent the highest-value targets for Manos Health engagement programs.' AS contents;

SELECT 'table' AS component,
    TRUE AS sort,
    TRUE AS search,
    TRUE AS hover,
    TRUE AS striped_rows;

SELECT
    disease_state               AS "Disease State",
    specialty_name              AS "Specialty",
    composite_opportunity_score AS "Score",
    opportunity_tier            AS "Tier",
    patient_volume              AS "Patients",
    ROUND(avg_srvcs_per_patient, 2) AS "Srvcs/Patient",
    total_spend_millions        AS "Spend ($M)",
    ROUND(market_concentration_pct, 1) AS "Market Conc. (%)"
FROM opportunity_scoring_view
ORDER BY composite_opportunity_score DESC;
```

---

## Specialty Explorer

```sql mmi/specialty-explorer.sql { route: { caption: "Specialty Explorer" } }
-- @route.description "Deep-dive into any specialty: top procedures, monitoring intensity, market dominance"

-- KPIs and Deep Dive for Selected Specialty

SELECT 'hero' AS component,
    'Specialty Explorer' AS title,
    'Select a specialty to analyze procedure volume, spend, and site-of-service mix.' AS description;

-- 1. Selection Form
SELECT 'form' AS component, 'Get' AS method;
SELECT 
    'specialty' AS name, 
    'Select Specialty' AS label, 
    'select' AS type,
    specialty_name AS value,
    specialty_name AS label,
    (specialty_name = $specialty) AS selected
FROM (SELECT DISTINCT specialty_name FROM fact_utilization ORDER BY 1);

-- 2. Empty State Alert
SELECT 'alert' AS component,
    'Please select a specialty above to begin analysis.' AS title,
    'info' AS color
WHERE $specialty IS NULL;

-- 3. Top-Level KPIs
SELECT 'big_number' AS component, 4 AS columns WHERE $specialty IS NOT NULL;

SELECT 'Total Spend' AS title, '$' || ROUND(SUM(total_allowed_amt)/1e6, 1) || 'M' AS value, 'currency-dollar' AS icon
FROM fact_utilization WHERE specialty_name = $specialty;

SELECT 'Patients' AS title, SUM(total_beneficiaries) AS value, 'users' AS icon
FROM fact_utilization WHERE specialty_name = $specialty;

SELECT 'Intensity' AS title, ROUND(CAST(SUM(total_services) AS REAL)/SUM(total_beneficiaries), 1) AS value, 'activity' AS icon
FROM fact_utilization WHERE specialty_name = $specialty;

-- 4. Procedure Breakdown Table
SELECT 'table' AS component, 'Top Procedures' AS title, TRUE AS sort WHERE $specialty IS NOT NULL;
SELECT 
    hcpcs_code AS "HCPCS",
    total_services AS "Volume",
    total_beneficiaries AS "Patients",
    ROUND(total_allowed_amt/1e6, 2) AS "Spend ($M)"
FROM fact_utilization
WHERE specialty_name = $specialty
ORDER BY total_services DESC LIMIT 20;
```

---

## Disease Mapping

```sql mmi/disease-mapping.sql { route: { caption: "Disease Mapping" } }
-- @route.description "ICD-10 cluster coverage, interaction density, and repeat-visit tiers"

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '/' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
    'Disease Mapping' AS title,
    'ICD-10 cluster coverage, interaction density by disease state, and repeat-visit tier classification.' AS description,
    'teal' AS color;

-- ICD-10 COVERAGE
SELECT 'text' AS component,
    'ICD-10 Coverage by Disease Cluster' AS title,
    'Code count per disease state. Use to validate mapping completeness — gaps here mean missed patient populations.' AS contents;

SELECT 'table' AS component, TRUE AS sort, TRUE AS search, TRUE AS hover, TRUE AS striped_rows;

SELECT
    disease_state   AS "Disease State",
    body_system     AS "Body System",
    icd_code_count  AS "ICD-10 Codes Mapped",
    sample_icd_codes AS "Sample Codes"
FROM disease_state_icd_coverage
ORDER BY icd_code_count DESC;

SELECT 'divider' AS component;

-- CHRONIC INTERACTION DENSITY
SELECT 'text' AS component,
    'Chronic Interaction Density — Repeat-Visit Analysis' AS title,
    'High interaction tier (12+ sessions/year) codes are the strongest signals for ongoing care relationships — dialysis, glucose monitoring, cardiac rhythm management.' AS contents;

SELECT 'table' AS component, TRUE AS sort, TRUE AS search, TRUE AS hover, TRUE AS striped_rows;

SELECT
    specialty_name      AS "Specialty",
    hcpcs_code          AS "HCPCS",
    procedure_description AS "Procedure",
    procedure_category  AS "Category",
    total_benes         AS "Patients",
    interaction_tier    AS "Interaction Tier"
FROM chronic_interaction_density
ORDER BY
    CASE interaction_tier
        WHEN 'High (12+ sessions/yr)' THEN 1
        WHEN 'Moderate (4-11 sessions/yr)' THEN 2
        ELSE 3
    END,
    total_benes DESC
LIMIT 50;

SELECT 'divider' AS component;

-- MONITORING PROCEDURE INTENSITY
SELECT 'text' AS component,
    'Monitoring Procedure Intensity by Specialty' AS title,
    'Specialties where a high percentage of activity is repeat-monitoring procedures have the strongest case for care coordination programs.' AS contents;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    specialty_name  AS "Specialty",
    monitoring_volume AS "Monitoring Volume",
    total_volume    AS "Total Volume",
    monitoring_pct  AS "Monitoring (%)",
    ROUND(monitoring_spend / 1e6, 2) AS "Monitoring Spend ($M)",
    ROUND(total_spend / 1e6, 2) AS "Total Spend ($M)"
FROM monitoring_procedure_intensity
ORDER BY monitoring_pct DESC
LIMIT 25;
```

---

## Geographic Markets

```sql mmi/geographic-markets.sql { route: { caption: "Geographic Markets" } }
-- @route.description "State-level patient volume and GPCI-adjusted spend by specialty"

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '/' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
    'Geographic Markets' AS title,
    'State-level patient volume and GPCI cost-adjusted spend. Identifies the highest-value markets per specialty for geographic expansion decisions.' AS description,
    'cyan' AS color;

-- SPECIALTY FILTER
-- Drive the dropdown with one row per option (GROUP_CONCAT in 'options' causes a parse error in SQLPage)
SELECT 'form' AS component, 'Get' AS method;
SELECT
    'specialty'            AS name,
    'Filter by Specialty'  AS label,
    'select'               AS type,
    specialty_name         AS value,
    specialty_name         AS label,
    (specialty_name = $specialty) AS selected
FROM (
    SELECT 'All' AS specialty_name
    UNION ALL
    SELECT DISTINCT specialty_name
    FROM geographic_market_opportunity
    WHERE specialty_name IS NOT NULL
    ORDER BY specialty_name
);

SELECT 'table' AS component, TRUE AS sort, TRUE AS search, TRUE AS hover, TRUE AS striped_rows;

SELECT
    state_abbr          AS "State",
    cost_tier           AS "Market Tier",
    mac_name            AS "MAC Region",
    specialty_name      AS "Specialty",
    state_patient_volume AS "Patients",
    ROUND(state_total_spend / 1e6, 2) AS "Spend ($M)",
    ROUND(state_spend_per_patient, 2) AS "$/Patient",
    ROUND(gpci_adjusted_spend / 1e6, 2) AS "GPCI-Adj. Spend ($M)"
FROM geographic_market_opportunity
WHERE ($specialty = 'All' OR specialty_name = $specialty)
ORDER BY gpci_adjusted_spend DESC
LIMIT 50;
```

---

## Procedure Drilldown

```sql mmi/procedure-drilldown.sql { route: { caption: "Procedure Drilldown" } }
-- @route.description "Part B drug spend, facility vs office split, DME refill velocity, surgical economics"

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '/' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
    'Procedure Drilldown' AS title,
    'Part B drug spend by specialty, DME/supply refill velocity, facility vs. office care setting mix, and surgical anesthesia economics.' AS description,
    'azure' AS color;

SELECT 'divider' AS component;

-- SECTION: FACILITY VS OFFICE SPLIT
SELECT 'text' AS component,
    'Care Setting Mix — Facility vs. Office' AS title,
    'Office-dominant specialties indicate higher ambulatory ownership and patient control. Key signal for practice acquisition and care coordination strategy.' AS contents;

SELECT 'table' AS component, TRUE AS sort, TRUE AS search, TRUE AS hover, TRUE AS striped_rows;

SELECT
    specialty_name      AS "Specialty",
    office_pct          AS "Office (%)",
    office_services     AS "Office Services",
    facility_services   AS "Facility Services",
    total_services      AS "Total Services",
    ROUND(office_spend / 1e6, 2) AS "Office Spend ($M)",
    ROUND(facility_spend / 1e6, 2) AS "Facility Spend ($M)"
FROM facility_vs_office_split
WHERE total_services > 50000
ORDER BY office_pct DESC
LIMIT 25;

SELECT 'divider' AS component;

-- SECTION: PART B DRUG INTENSITY
SELECT 'text' AS component,
    'Part B Drug Intensity by Specialty' AS title,
    'Drug administration spend and patient reach per specialty and HCPCS code. High drug spend per patient is a key indicator for oncology and nephrology market sizing.' AS contents;

SELECT 'table' AS component, TRUE AS sort, TRUE AS search, TRUE AS hover, TRUE AS striped_rows;

SELECT
    specialty_name      AS "Specialty",
    hcpcs_code          AS "HCPCS",
    procedure_description AS "Drug Description",
    total_drug_administrations AS "Administrations",
    patients_receiving_drug AS "Patients",
    ROUND(total_drug_spend / 1e6, 2) AS "Spend ($M)",
    drug_spend_per_patient AS "$/Patient"
FROM part_b_drug_intensity
ORDER BY total_drug_spend DESC
LIMIT 20;

SELECT 'divider' AS component;

-- SECTION: DME / SUPPLY REFILL VELOCITY
SELECT 'text' AS component,
    'DME & Supply Refill Velocity' AS title,
    'Units dispensed per patient. High refill velocity = chronic disease management (e.g. CGM supplies, insulin pumps, dialysis supplies). Key metric for Manos Health engagement model.' AS contents;

SELECT 'table' AS component, TRUE AS sort, TRUE AS search, TRUE AS hover, TRUE AS striped_rows;

SELECT
    supply_item         AS "Supply Item",
    hcpcs_code          AS "HCPCS",
    supply_category     AS "Category",
    total_units         AS "Units Dispensed",
    total_patients      AS "Patients",
    refill_velocity     AS "Units/Patient (Refill Velocity)"
FROM dme_supply_refill_metrics
ORDER BY refill_velocity DESC
LIMIT 25;

SELECT 'divider' AS component;

-- SECTION: SURGICAL ECONOMICS
SELECT 'text' AS component,
    'Surgical Procedure Economics (Anesthesia Proxy)' AS title,
    'Anesthesia CPT codes (00100–01999) combined with geographic conversion factors estimate total surgical economic load. High estimated cost = high-complexity procedure cluster.' AS contents;

SELECT 'table' AS component, TRUE AS sort, TRUE AS search, TRUE AS hover, TRUE AS striped_rows;

SELECT
    hcpcs_code          AS "HCPCS",
    procedure_name      AS "Procedure",
    volume              AS "Volume",
    patients            AS "Patients",
    ROUND(avg_anesthesia_cf, 4) AS "Avg Anes CF",
    ROUND(estimated_total_anesthesia_cost / 1e6, 2) AS "Est. Anes Cost ($M)"
FROM surgical_economic_metrics
ORDER BY estimated_total_anesthesia_cost DESC
LIMIT 20;
```

---

## Data Dictionary

```sql mmi/data-dictionary.sql { route: { caption: "Data Dictionary" } }
-- @route.description "Schema reference for all tables, views, and the scoring methodology"

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '/' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
    'Data Dictionary & Pipeline Reference' AS title,
    'Complete schema reference for dimension tables, fact tables, analytical views, and the scoring engine.' AS description,
    'gray' AS color;

-- PIPELINE OVERVIEW
SELECT 'text' AS component,
    'Pipeline Architecture' AS title,
    '
**Pattern:** ELT — Raw ingestion → Normalization → Analytical layer → Scoring

| Section | Objects | Purpose |
|---|---|---|
| Section 0 | 14 indexes | Query performance on raw CMS tables |
| Section 1 | `dim_procedure`, `dim_diagnosis`, `dim_specialty`, `dim_geography` | Star schema normalization |
| Section 2A | `fact_utilization` | Core volume + economic fact table |
| Section 2B | `specialty_market_dynamics` | Market concentration (dominance ratio) |
| Section 3 | Views 1–12 | Business question layer |
| Section 4 | `opportunity_scoring_view` | Primary deliverable (Manos / CCIQ) |
| Section 5 | Queries A–I | Starter analyst queries |
    ' AS contents_md;

SELECT 'divider' AS component;

-- DIMENSION TABLES
SELECT 'text' AS component, 'Dimension Tables' AS title, '' AS contents;

SELECT 'datagrid' AS component, 'dim_procedure' AS title;
SELECT 'hcpcs_code' AS title, 'Primary key — CPT or HCPCS Level II code' AS description;
SELECT 'procedure_description' AS title, 'Human-readable procedure name (TRIM cleaned)' AS description;
SELECT 'procedure_category' AS title, 'Clinical range bucket: E&M / Radiology / Surgery / Anesthesia / Medicine / HCPCS L2' AS description;
SELECT 'work_rvu' AS title, 'Work Relative Value Unit — physician effort weight' AS description;
SELECT 'medicare_fee_schedule_payment' AS title, 'Published CMS fee schedule payment amount' AS description;
SELECT 'is_monitoring_flag' AS title, '1 = repeat-visit / monitoring procedure (dialysis, glucose, EKG, E&M)' AS description;

SELECT 'datagrid' AS component, 'dim_diagnosis' AS title;
SELECT 'icd10_code' AS title, 'ICD-10-CM diagnosis code' AS description;
SELECT 'disease_state' AS title, '25+ named clusters (Type 2 Diabetes, ESRD, AFib, COPD, etc.)' AS description;
SELECT 'body_system' AS title, 'Rollup system (Cardiovascular, Renal, Oncology, etc.) — bridge to specialty_domain' AS description;

SELECT 'datagrid' AS component, 'dim_specialty' AS title;
SELECT 'raw_specialty_name' AS title, 'CMS raw Rndrng_Prvdr_Type string — join key' AS description;
SELECT 'specialty_name' AS title, 'Canonical normalized specialty (e.g. "Cardiology", "Internal Medicine")' AS description;
SELECT 'specialty_domain' AS title, 'Domain group — maps to body_system for disease bridge scoring' AS description;

SELECT 'datagrid' AS component, 'dim_geography' AS title;
SELECT 'state_abbr' AS title, 'State abbreviation — join key to fact_utilization' AS description;
SELECT 'pw_gpci' AS title, '2026 Physician Work GPCI factor (1.0 = national average)' AS description;
SELECT 'cost_tier' AS title, 'High / Average / Low cost market classification' AS description;

SELECT 'divider' AS component;

-- FACT TABLES
SELECT 'text' AS component, 'Fact Tables' AS title, '' AS contents;

SELECT 'datagrid' AS component, 'fact_utilization' AS title;
SELECT 'total_beneficiaries' AS title, 'Distinct Medicare patients — primary volume signal' AS description;
SELECT 'total_services' AS title, 'Total procedure occurrences — repeat interaction signal' AS description;
SELECT 'total_allowed_amt' AS title, 'Avg_Mdcr_Alowd_Amt × Tot_Srvcs — fair market proxy' AS description;
SELECT 'total_medicare_payment' AS title, 'Avg_Mdcr_Pymt_Amt × Tot_Srvcs — actual economic activity' AS description;
SELECT 'srvcs_per_patient' AS title, 'total_services / total_beneficiaries — visit density ratio' AS description;
SELECT 'is_drug_service' AS title, '1 = HCPCS_Drug_Ind = Y — Part B drug administration code' AS description;
SELECT 'place_of_service' AS title, 'F = Facility (hospital), O = Office/Non-Facility' AS description;

SELECT 'datagrid' AS component, 'specialty_market_dynamics' AS title;
SELECT 'specialty_dominance_ratio' AS title, 'Specialty share of national HCPCS volume (0.0–1.0)' AS description;
SELECT 'Use' AS title, 'Powers specialty_market_concentration view and dominance_bonus in opportunity score' AS description;

SELECT 'divider' AS component;

-- VIEWS INDEX
SELECT 'text' AS component, 'Analytical Views Index' AS title, '' AS contents;

SELECT 'table' AS component, TRUE AS hover, TRUE AS striped_rows;

SELECT * FROM (VALUES
    ('specialty_activity_summary',    'Executive KPIs per specialty: spend, reach, provider count, spend/patient, srvcs/patient'),
    ('specialty_economic_intensity',  'Intensity index = spend/pt × srvcs/pt; ranked within domain'),
    ('specialty_top_procedures',      'Top 10 HCPCS codes per specialty by volume; includes spend_rank alongside'),
    ('specialty_market_concentration','Which codes does each specialty dominate? pct_of_national_volume'),
    ('chronic_interaction_density',   'Services-per-patient per HCPCS code; High / Moderate / Low interaction tier'),
    ('monitoring_procedure_intensity','% of specialty volume from monitoring codes; monitoring spend vs total'),
    ('dme_supply_refill_metrics',     'DME/supply refill_velocity (units per patient); supply_category classification'),
    ('surgical_economic_metrics',     'Anesthesia-range CPT codes; estimated_total_anesthesia_cost by procedure'),
    ('part_b_drug_intensity',         'Drug administrations, patients, and spend per specialty; drug_spend_per_patient'),
    ('geographic_market_opportunity', 'State-level volume, spend, and GPCI-adjusted spend per specialty'),
    ('facility_vs_office_split',      'Office vs facility service and spend mix per specialty; office_pct'),
    ('disease_state_icd_coverage',    'ICD code count per disease cluster; validates mapping completeness'),
    ('opportunity_scoring_view',      'PRIMARY DELIVERABLE: Composite Tier 1/2/3 ranking of disease × specialty clusters')
) AS t(view_name, purpose)
ORDER BY 1;

SELECT 'divider' AS component;

-- CMS SOURCE DATASETS
SELECT 'text' AS component,
    'CMS Source Dataset Reference' AS title,
    '
| Table | CMS Dataset | Key Fields |
|---|---|---|
| `uniform_resource_cms_bygeography` | By Geography & Service | HCPCS_Cd, Tot_Srvcs, Tot_Benes, Avg_Mdcr_Pymt_Amt |
| `uniform_resource_cms_provider` | By Provider | Rndrng_NPI, Rndrng_Prvdr_Type, Rndrng_Prvdr_State_Abrvtn |
| `uniform_resource_ref_icd10_diagnosis` | ICD-10-CM CDC/NCHS | icd10_code, description_long |
| `uniform_resource_ref_procedure_code` | CMS Physician Fee Schedule | HCPCS, WORK RVU, MEDICARE PAYMENT |
| `uniform_resource_ref_hcpcs_level_two_procedures` | CMS HCPCS Level II | hcpcs_code, short_description |
| `uniform_resource_ref_geo_adjustment` | CMS GPCI 2026 | State, PW GPCI |
| `uniform_resource_ref_anes_conversion_factor` | CMS Anesthesia CF | Contractor, Locality, Conversion Factor |

All datasets are publicly available, no login required. Download priority: Geography & Service first (smallest), then Provider & Service (largest, start early).
    ' AS contents_md;
```
