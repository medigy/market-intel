---
sqlpage-conf:
  database_url: "sqlite://resource-surveillance.sqlite.db?mode=rwc"
  web_root: "./dev-src.auto"
  allow_exec: true
  port: 9227
---
# Medigy Market Intelligence — SQLPage Application

This application surfaces the Medigy CMS analytics pipeline built from
`medigy-analytics` through a navigable SQLPage UI.

- Data ingested from CMS public datasets via surveilr into an RSSD (SQLite)
- Star schema: `dim_procedure`, `dim_diagnosis`, `dim_specialty`, `dim_geography`
- Core fact: `fact_utilization` + `specialty_market_dynamics`
- 13 analytical views + opportunity scoring engine

---

```bash prepare-db-deploy-server --descr "Ingest Medigy raw files, build normalized analytics tables, and package SQLPage UI."
#!/bin/bash
set -euo pipefail

# Start from a clean database to avoid previously ingested malformed resources.
rm -f resource-surveillance.sqlite.*   

surveilr ingest files -r medicare-ds/ && surveilr orchestrate transform-csv 
surveilr shell sql/medigy-ddl.sql
surveilr shell sql/medigy-analytics.sql 
spry sp spc --package --conf sqlpage/sqlpage.json -m mmi-dashboard.md | sqlite3 resource-surveillance.sqlite.db
echo "Medigy Market Intellignece database and SQLPage UI are ready."
```

---

## Layout

Global shell injected into every page.

```sql PARTIAL global-layout.sql --inject *.sql --inject mmi/*.sql

-- BEGIN: PARTIAL global-layout.sql
SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
       '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;
-- END: PARTIAL global-layout.sql
```

---

## Home Page

```sql index.sql { route: { caption: "Home" } }
-- @route.description "Medigy Market Intelligence — Landing Page"

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
       '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

-- HERO
SELECT 'hero' AS component,
    'Medigy Market Intelligence' AS title,
    'CMS Part B analytics for identifying high-opportunity disease-specialty clusters.' AS description,
    'azure' AS color;

-- PIPELINE HEALTH CHECK
-- SELECT 'big_number' AS component, 4 AS columns;

SELECT 'card' AS component, 4 AS columns;

SELECT
    'Specialties Indexed' AS title,
    CAST(COUNT(DISTINCT specialty_name) AS TEXT) AS description,
    '/mmi/medical-specialities.sql' AS link,
    'stethoscope' AS icon,
    'teal' AS color
FROM dim_specialty;

SELECT
    'ICD-10 Clusters Mapped' AS title,
    CAST(COUNT(DISTINCT disease_state) AS TEXT) AS description,
    '/mmi/disease-clusters.sql' AS link,
    'virus' AS icon,
    'azure' AS color
FROM dim_diagnosis
WHERE disease_state != 'General / Other';

SELECT
    'Procedure Codes' AS title,
    CAST(COUNT(*) AS TEXT) AS description,
    '/mmi/procedures.sql' AS link,
    'clipboard-list' AS icon,
    'indigo' AS color
FROM dim_procedure;

SELECT
    'States in Scope' AS title,
    CAST(COUNT(DISTINCT state_abbr) AS TEXT) AS description,
    '/mmi/geography.sql' AS link,
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
    'Composite Tier 1/2/3 ranking of disease × specialty clusters.' AS description,
    '/mmi/opportunity-scoring.sql' AS link,
    'trophy' AS icon,
    'azure' AS color;

SELECT
    'Evidence' AS title,
    'Consolidated evidence views and charts mapped to the Voxia prioritization report references, including opportunity matrix, gatekeepers, Pareto drug spend, and geographic concentration.' AS description,
    '/mmi/sleep-apnea-evidence.sql' AS link,
    'moon-stars' AS icon,
    'cyan' AS color;

SELECT
    'Sleep Apnea Market' AS title,
    'Dedicated CMS sleep apnea diagnostic and DME market analysis with national totals, test mix, geography, device billing, and system friction.' AS description,
    '/mmi/cms-sleep-apnea-market-analysis.sql' AS link,
    'bed-flat' AS icon,
    'blue' AS color;

SELECT
    'Disease Mapping' AS title,
    'ICD-10 cluster coverage, interaction density by disease, and repeat-visit tier classification.' AS description,
    '/mmi/disease-mapping.sql' AS link,
    'virus' AS icon,
    'teal' AS color;

SELECT
    'Procedure Drilldown' AS title,
    'Part B drug spend, facility vs office split, DME/supply refill velocity, and surgical anesthesia metrics.' AS description,
    '/mmi/procedure-drilldown.sql' AS link,
    'pill' AS icon,
    'azure' AS color;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Sleep Apnea Snapshot' AS title,
    'Home-page summary derived from the national diagnostic and DME queries in `sql/cms_sleep_apnea_market_analysis.sql`.' AS contents;

SELECT 'card' AS component, 4 AS columns;

SELECT
    'Diagnostic Allowed' AS title,
    '$' || printf('%,.1f', SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt) / 1000000.0) || 'M' AS description,
    '/mmi/cms-sleep-apnea-market-analysis.sql' AS link,
    'activity-heartbeat' AS icon,
    'teal' AS color
FROM uniform_resource_diagnostics_data
WHERE Rndrng_Prvdr_Geo_Lvl = 'National';

SELECT
    'Diagnostic Beneficiaries' AS title,
    printf('%,.0f', SUM(Tot_Benes)) AS description,
    '/mmi/cms-sleep-apnea-market-analysis.sql' AS link,
    'users' AS icon,
    'azure' AS color
FROM uniform_resource_diagnostics_data
WHERE Rndrng_Prvdr_Geo_Lvl = 'National';

SELECT
    'DME Allowed' AS title,
    '$' || printf('%,.1f', SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt) / 1000000.0) || 'M' AS description,
    '/mmi/cms-sleep-apnea-market-analysis.sql' AS link,
    'device-desktop' AS icon,
    'cyan' AS color
FROM uniform_resource_dme_data
WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471');

SELECT
    'Allowed vs Submitted' AS title,
    printf('%.1f%%',
        (SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt) * 100.0)
        / NULLIF(SUM(Tot_Suplr_Srvcs * Avg_Suplr_Sbmtd_Chrg), 0)
    ) AS description,
    '/mmi/cms-sleep-apnea-market-analysis.sql' AS link,
    'percentage' AS icon,
    'indigo' AS color
FROM uniform_resource_dme_data
WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471');

SELECT 'divider' AS component;

-- DATA SOURCE REFERENCE
SELECT 'card' AS component, 'Pipeline Reference' AS title, 1 AS columns;

SELECT
    'Data Dictionary' AS title,
    'Schema reference for all dimension and fact tables, views, and scoring methodology.' AS description,
    '/mmi/data-dictionary.sql' AS link,
    'database' AS icon,
    'gray' AS color;

```

---

## Executive Dashboard

```sql mmi/executive-dashboard.sql { route: { caption: "Executive Dashboard" } }
-- @route.description "Top-line Medicare market KPIs: spend, volume, and intensity by specialty"

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
    '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

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

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
    '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

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
    'Disease × specialty combinations scoring ≥ 75. These represent the highest-value targets for  Health engagement programs.' AS contents;

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

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
    '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

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

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
    '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

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
    -- procedure_category  AS "Category",  -- Removed because not present in view
    total_benes         AS "Patients"
    -- interaction_tier    AS "Interaction Tier"
FROM chronic_interaction_density
ORDER BY
    -- CASE interaction_tier
    --     WHEN 'High (12+ sessions/yr)' THEN 1
    --     WHEN 'Moderate (4-11 sessions/yr)' THEN 2
    --     ELSE 3
    -- END,
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

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
    '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

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

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
    '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

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
    'Units dispensed per patient. High refill velocity = chronic disease management (e.g. CGM supplies, insulin pumps, dialysis supplies). Key metric for  Health engagement model.' AS contents;

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

## CMS Sleep Apnea Market

```sql mmi/cms-sleep-apnea-market-analysis.sql { route: { caption: "Sleep Apnea Market" } }
-- @route.description "CMS sleep apnea diagnostic and DME market analysis from the dedicated SQL report"

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
    '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
    'CMS Sleep Apnea Market Analysis' AS title,
    'blue' AS color;

WITH diagnostics AS (
    SELECT
        SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt) AS total_allowed,
        SUM(Tot_Srvcs) AS total_services,
        SUM(Tot_Benes) AS total_beneficiaries,
        SUM(Tot_Srvcs * Avg_Sbmtd_Chrg) AS total_submitted
    FROM uniform_resource_diagnostics_data
    WHERE Rndrng_Prvdr_Geo_Lvl = 'National'
)
SELECT 'text' AS component,
    'The national diagnostic sleep-apnea market captures about $'
    || printf('%,.1f', total_allowed / 1000000.0)
    || 'M in Medicare allowed payments across '
    || printf('%,.0f', total_services)
    || ' billed services and '
    || printf('%,.0f', total_beneficiaries)
    || ' beneficiaries, against approximately $'
    || printf('%,.1f', total_submitted / 1000000.0)
    || 'M in submitted charges.' AS contents
FROM diagnostics;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Part B — Diagnostic (PSG + home sleep tests)' AS title,
    'National diagnostic query results for polysomnography (PSG) and home sleep testing.' AS contents;

SELECT 'table' AS component, TRUE AS hover, TRUE AS striped_rows;

WITH diagnostics AS (
    SELECT
        SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt) AS total_allowed,
        SUM(Tot_Srvcs) AS total_services,
        SUM(Tot_Benes) AS total_beneficiaries,
        SUM(Tot_Srvcs * Avg_Sbmtd_Chrg) AS total_submitted
    FROM uniform_resource_diagnostics_data
    WHERE Rndrng_Prvdr_Geo_Lvl = 'National'
)
SELECT
    ROUND(total_allowed, 2) AS "Total Medicare Allowed Payments",
    total_services AS "Total Procedures Billed",
    total_beneficiaries AS "Unique Medicare Beneficiaries",
    ROUND(total_submitted, 2) AS "Total Submitted Charges"
FROM diagnostics;

SELECT 'card' AS component, 4 AS columns;

SELECT
    'Allowed Payments' AS title,
    '$' || printf('%,.1f', SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt) / 1000000.0) || 'M' AS description,
    'currency-dollar' AS icon,
    'teal' AS color
FROM uniform_resource_diagnostics_data
WHERE Rndrng_Prvdr_Geo_Lvl = 'National';

SELECT
    'Procedures Billed' AS title,
    printf('%,.0f', SUM(Tot_Srvcs)) AS description,
    'clipboard-list' AS icon,
    'azure' AS color
FROM uniform_resource_diagnostics_data
WHERE Rndrng_Prvdr_Geo_Lvl = 'National';

SELECT
    'Beneficiaries' AS title,
    printf('%,.0f', SUM(Tot_Benes)) AS description,
    'users' AS icon,
    'cyan' AS color
FROM uniform_resource_diagnostics_data
WHERE Rndrng_Prvdr_Geo_Lvl = 'National';

SELECT
    'Submitted Charges' AS title,
    '$' || printf('%,.1f', SUM(Tot_Srvcs * Avg_Sbmtd_Chrg) / 1000000.0) || 'M' AS description,
    'receipt-2' AS icon,
    'indigo' AS color
FROM uniform_resource_diagnostics_data
WHERE Rndrng_Prvdr_Geo_Lvl = 'National';

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Diagnostic Procedure Mix' AS title,
    'National procedure-level breakdown by total allowed amount and weighted allowed per test.' AS contents;

SELECT 'chart' AS component,
    'Top Diagnostic Procedures by Allowed Amount' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Allowed Amount ($)' AS xtitle;

SELECT
    HCPCS_Cd || ' — ' || HCPCS_Desc AS label,
    SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt) AS value
FROM uniform_resource_diagnostics_data
WHERE Rndrng_Prvdr_Geo_Lvl = 'National'
GROUP BY HCPCS_Cd, HCPCS_Desc
ORDER BY value DESC
LIMIT 8;

SELECT 'table' AS component, TRUE AS sort, TRUE AS search, TRUE AS hover, TRUE AS striped_rows;

SELECT
    HCPCS_Cd AS "HCPCS",
    HCPCS_Desc AS "Description",
    SUM(Tot_Srvcs) AS "Services",
    SUM(Tot_Benes) AS "Beneficiaries",
    ROUND(SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt) / NULLIF(SUM(Tot_Srvcs), 0), 2) AS "Weighted Allowed/Test",
    ROUND(SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt), 2) AS "Total Allowed"
FROM uniform_resource_diagnostics_data
WHERE Rndrng_Prvdr_Geo_Lvl = 'National'
GROUP BY HCPCS_Cd, HCPCS_Desc
ORDER BY 6 DESC;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Test Setting Split' AS title,
    'Compares in-lab sleep studies with home sleep testing (HST) using total allowed amount.' AS contents;

SELECT 'chart' AS component,
    'In-Lab vs Home Sleep Test Market Split' AS title,
    'bar' AS type,
    TRUE AS labels,
    'Allowed Amount ($)' AS ytitle;

SELECT
    CASE
        WHEN HCPCS_Cd IN ('95810', '95811') THEN 'In-Lab'
        WHEN HCPCS_Cd IN ('G0398', 'G0399', 'G0400', '95800', '95806') THEN 'HST'
        ELSE 'Other'
    END AS label,
    SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt) AS value
FROM uniform_resource_diagnostics_data
WHERE Rndrng_Prvdr_Geo_Lvl = 'National'
GROUP BY 1
ORDER BY value DESC;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    HCPCS_Cd AS "HCPCS",
    SUM(Tot_Srvcs) AS "Services",
    SUM(Tot_Benes) AS "Beneficiaries",
    ROUND(SUM(Tot_Srvcs) * 1.0 / NULLIF(SUM(Tot_Benes), 0), 2) AS "Interaction Density"
FROM uniform_resource_diagnostics_data
WHERE Rndrng_Prvdr_Geo_Lvl = 'National'
GROUP BY HCPCS_Cd
ORDER BY HCPCS_Cd;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Geographic Demand' AS title,
    'State-by-state diagnostic volumes and Medicare allowed amounts.' AS contents;

SELECT 'chart' AS component,
    'Top States by Diagnostic Allowed Amount' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Allowed Amount ($)' AS xtitle;

SELECT
    Rndrng_Prvdr_Geo_Desc AS label,
    SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt) AS value
FROM uniform_resource_diagnostics_data
WHERE Rndrng_Prvdr_Geo_Lvl = 'State'
GROUP BY Rndrng_Prvdr_Geo_Desc
ORDER BY value DESC
LIMIT 15;

SELECT 'table' AS component, TRUE AS sort, TRUE AS search, TRUE AS hover, TRUE AS striped_rows;

SELECT
    Rndrng_Prvdr_Geo_Desc AS "State",
    SUM(Tot_Srvcs) AS "Services",
    SUM(Tot_Benes) AS "Beneficiaries",
    ROUND(SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt), 2) AS "Total Allowed"
FROM uniform_resource_diagnostics_data
WHERE Rndrng_Prvdr_Geo_Lvl = 'State'
GROUP BY Rndrng_Prvdr_Geo_Desc
ORDER BY 4 DESC;

SELECT 'divider' AS component;

WITH dme_totals AS (
    SELECT
        SUM(Tot_Suplr_Srvcs) AS grand_total_services,
        SUM(Tot_Suplr_Benes) AS grand_total_beneficiaries,
        SUM(Tot_Suplr_Srvcs * Avg_Suplr_Sbmtd_Chrg) AS grand_total_submitted,
        SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt) AS grand_total_allowed,
        SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Pymt_Amt) AS grand_total_payment
    FROM uniform_resource_dme_data
    WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471')
)
SELECT 'text' AS component,
    'DME Treatment Economics' AS title,
    'Across CPAP/BiPAP device billing, Medicare allowed about $'
    || printf('%,.1f', grand_total_allowed / 1000000.0)
    || 'M from $'
    || printf('%,.1f', grand_total_submitted / 1000000.0)
    || 'M in submitted charges, paying roughly $'
    || printf('%,.1f', grand_total_payment / 1000000.0)
    || 'M across '
    || printf('%,.0f', grand_total_services)
    || ' rental/service events.' AS contents
FROM dme_totals;

SELECT 'card' AS component, 4 AS columns;

SELECT
    'DME Allowed' AS title,
    '$' || printf('%,.1f', SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt) / 1000000.0) || 'M' AS description,
    'device-desktop' AS icon,
    'teal' AS color
FROM uniform_resource_dme_data
WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471');

SELECT
    'Medicare Payment' AS title,
    '$' || printf('%,.1f', SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Pymt_Amt) / 1000000.0) || 'M' AS description,
    'cash' AS icon,
    'azure' AS color
FROM uniform_resource_dme_data
WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471');

SELECT
    'System Friction' AS title,
    '$' || printf('%,.1f',
        (SUM(Tot_Suplr_Srvcs * Avg_Suplr_Sbmtd_Chrg) - SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt)) / 1000000.0
    ) || 'M' AS description,
    'arrows-diff' AS icon,
    'cyan' AS color
FROM uniform_resource_dme_data
WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471');

SELECT
    'Allowed / Submitted' AS title,
    printf('%.1f%%',
        (SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt) * 100.0)
        / NULLIF(SUM(Tot_Suplr_Srvcs * Avg_Suplr_Sbmtd_Chrg), 0)
    ) AS description,
    'percentage' AS icon,
    'indigo' AS color
FROM uniform_resource_dme_data
WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471');

SELECT 'chart' AS component,
    'Device Billing by Allowed Amount' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Allowed Amount ($)' AS xtitle;

SELECT
    HCPCS_CD || ' — ' || HCPCS_Desc AS label,
    SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt) AS value
FROM uniform_resource_dme_data
WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471')
GROUP BY HCPCS_CD, HCPCS_Desc
ORDER BY value DESC;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    HCPCS_CD AS "Device Code",
    HCPCS_Desc AS "Device Description",
    SUM(Tot_Suplr_Srvcs) AS "Services/Rentals",
    SUM(Tot_Suplr_Benes) AS "Beneficiaries",
    ROUND(SUM(Tot_Suplr_Srvcs * Avg_Suplr_Sbmtd_Chrg), 2) AS "Submitted Charges",
    ROUND(SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt), 2) AS "Allowed Amount",
    ROUND(SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Pymt_Amt), 2) AS "Medicare Payment"
FROM uniform_resource_dme_data
WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471')
GROUP BY HCPCS_CD, HCPCS_Desc
ORDER BY 6 DESC;
```

---

## Evidence

```sql mmi/sleep-apnea-evidence.sql { route: { caption: "Evidence" } }
-- @route.description "Evidence dashboard mapped to Voxia report references and Medicare analytical tables"

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
    '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
    'Evidence & Market Prioritization' AS title,
    'cyan' AS color;

SELECT 'text' AS component,
    'Global Opportunity Matrix & Scoring' AS title,
    '' AS contents;

WITH tier_summary AS (
    SELECT
        opportunity_tier,
        COUNT(*) AS tier_count
    FROM mdsd_global_opportunity_matrix
    GROUP BY opportunity_tier
),
top_tier AS (
    SELECT opportunity_tier, tier_count
    FROM tier_summary
    ORDER BY tier_count DESC
    LIMIT 1
),
hypertension AS (
    SELECT composite_opportunity_score, patient_volume, total_spend_millions, interaction_density
    FROM mdsd_global_opportunity_matrix
    WHERE disease_state = 'Hypertension'
    LIMIT 1
),
copd AS (
    SELECT composite_opportunity_score, patient_volume, total_spend_millions, interaction_density
    FROM mdsd_global_opportunity_matrix
    WHERE disease_state = 'COPD'
    LIMIT 1
),
hf AS (
    SELECT composite_opportunity_score, patient_volume, total_spend_millions, interaction_density
    FROM mdsd_global_opportunity_matrix
    WHERE disease_state = 'Heart Failure'
    LIMIT 1
),
copd_model AS (
    SELECT business_model_fit
    FROM mdsd_interaction_model_fit
    WHERE disease_state = 'COPD'
    LIMIT 1
)
SELECT 'text' AS component,
    'The market is currently categorized mainly into '
    || COALESCE((SELECT opportunity_tier FROM top_tier), 'a single tier')
    || ' opportunities ('
    || COALESCE((SELECT tier_count FROM top_tier), 0)
    || ' disease states), with meaningful variation in the underlying drivers. '
    || 'Hypertension holds the highest composite opportunity score at '
    || COALESCE((SELECT composite_opportunity_score FROM hypertension), 0)
    || ', driven by scale: '
    || printf('%,.0f', COALESCE((SELECT patient_volume FROM hypertension), 0))
    || ' patients and about $'
    || printf('%,.1f', COALESCE((SELECT total_spend_millions FROM hypertension), 0) / 1000.0)
    || 'B total spend. '
    || 'Conversely, COPD scores '
    || COALESCE((SELECT composite_opportunity_score FROM copd), 0)
    || ' with a much smaller patient base ('
    || printf('%,.0f', COALESCE((SELECT patient_volume FROM copd), 0))
    || ') but the highest interaction density ('
    || COALESCE((SELECT ROUND(interaction_density, 1) FROM copd), 0)
    || '), aligning to '
    || COALESCE((SELECT business_model_fit FROM copd_model), 'its model-fit')
    || '. '
    || 'Heart Failure remains a balanced profile at score '
    || COALESCE((SELECT composite_opportunity_score FROM hf), 0)
    || ', with '
    || printf('%,.0f', COALESCE((SELECT patient_volume FROM hf), 0))
    || ' patients and interaction density '
    || COALESCE((SELECT ROUND(interaction_density, 2) FROM hf), 0)
    || '.' AS contents;

SELECT 'chart' AS component,
    'Market Opportunity Analysis' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Composite Opportunity Score' AS xtitle;

SELECT
    disease_state AS label,
    ROUND(composite_opportunity_score, 2) AS value
FROM mdsd_global_opportunity_matrix
ORDER BY composite_opportunity_score DESC
LIMIT 10;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    disease_state AS "Disease State",
    opportunity_tier AS "Opportunity Tier",
    ROUND(composite_opportunity_score, 2) AS "Composite Score",
    patient_volume AS "Patients",
    ROUND(interaction_density, 2) AS "Interaction Density",
    ROUND(total_spend_millions, 2) AS "Spend ($M)",
    ROUND(market_concentration_pct, 1) AS "Market Concentration (%)"
FROM mdsd_global_opportunity_matrix
ORDER BY composite_opportunity_score DESC;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Specialty Economic Intensity & Efficiency' AS title,
    '' AS contents;

WITH top_intensity AS (
    SELECT
        specialty_name,
        specialty_domain,
        patient_reach,
        avg_allowed_per_patient,
        interaction_frequency,
        economic_intensity_index
    FROM mdsd_economic_intensity_proof
    ORDER BY economic_intensity_index DESC
    LIMIT 1
),
internal_med AS (
    SELECT
        specialty_name,
        patient_reach,
        total_medicare_spend
    FROM specialty_activity_summary
    WHERE specialty_name = 'Internal Medicine / PCP'
    LIMIT 1
),
pulmonology AS (
    SELECT
        specialty_name,
        specialty_domain,
        avg_cost_per_service,
        economic_intensity_index
    FROM specialty_economic_intensity
    WHERE specialty_name = 'Pulmonology'
    ORDER BY avg_cost_per_service ASC
    LIMIT 1
)
SELECT 'text' AS component,
    'Economic intensity provides a metric for the financial weight of clinical interventions relative to patient reach. '
    || 'The highest current intensity is '
    || COALESCE((SELECT specialty_name FROM top_intensity), 'N/A')
    || ' ('
    || COALESCE((SELECT specialty_domain FROM top_intensity), 'N/A')
    || ') with an Economic Intensity Index of '
    || COALESCE((SELECT ROUND(economic_intensity_index, 2) FROM top_intensity), 0)
    || ', supported by '
    || COALESCE((SELECT ROUND(interaction_frequency, 2) FROM top_intensity), 0)
    || ' services per patient and $'
    || COALESCE((SELECT printf('%,.2f', avg_allowed_per_patient) FROM top_intensity), '0.00')
    || ' spend per patient. '
    || 'Internal Medicine / PCP carries major scale with '
    || COALESCE((SELECT printf('%,.0f', patient_reach) FROM internal_med), '0')
    || ' patients and about $'
    || COALESCE((SELECT printf('%,.1f', total_medicare_spend / 1000000000.0) FROM internal_med), '0.0')
    || 'B total Medicare spend. '
    || 'Pulmonology reflects the leanest economic model in its profile, with average cost per service of $'
    || COALESCE((SELECT printf('%,.2f', avg_cost_per_service) FROM pulmonology), '0.00')
    || ' and economic intensity '
    || COALESCE((SELECT ROUND(economic_intensity_index, 2) FROM pulmonology), 0)
    || '.' AS contents;

WITH top_intensity AS (
    SELECT
        specialty_name,
        economic_intensity_index,
        interaction_frequency,
        avg_allowed_per_patient
    FROM mdsd_economic_intensity_proof
    ORDER BY economic_intensity_index DESC
    LIMIT 1
),
top_spend AS (
    SELECT
        specialty_name,
        total_medicare_spend
    FROM specialty_activity_summary
    ORDER BY total_medicare_spend DESC
    LIMIT 1
)
SELECT 'text' AS component,
    COALESCE((SELECT specialty_name FROM top_intensity), 'The top specialty')
    || ' exhibits the highest economic intensity at '
    || COALESCE((SELECT ROUND(economic_intensity_index, 2) FROM top_intensity), 0)
    || ', indicating a model driven by frequent ('
    || COALESCE((SELECT ROUND(interaction_frequency, 2) FROM top_intensity), 0)
    || ' services per patient) and high-value ($'
    || COALESCE((SELECT printf('%,.2f', avg_allowed_per_patient) FROM top_intensity), '0.00')
    || ' per patient) interactions. '
    || COALESCE((SELECT specialty_name FROM top_spend), 'The leading specialty')
    || ' manages the largest absolute Medicare spend at about $'
    || COALESCE((SELECT printf('%,.1f', total_medicare_spend / 1000000000.0) FROM top_spend), '0.0')
    || 'B.' AS contents;

SELECT 'chart' AS component,
    'Economic Intensity Index by Specialty' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Economic Intensity Index' AS ytitle;

SELECT
    specialty_name AS label,
    ROUND(economic_intensity_index, 2) AS value
FROM mdsd_economic_intensity_proof
ORDER BY economic_intensity_index DESC
LIMIT 12;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    specialty_name AS "Specialty",
    specialty_domain AS "Domain",
    patient_reach AS "Patients",
    ROUND(avg_allowed_per_patient, 2) AS "$/Patient",
    ROUND(interaction_frequency, 2) AS "Interactions/Patient",
    ROUND(economic_intensity_index, 2) AS "Intensity Index"
FROM mdsd_economic_intensity_proof
ORDER BY economic_intensity_index DESC
LIMIT 20;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Interaction Models & Clinical Gatekeepers' AS title,
    '' AS contents;

WITH ranked_models AS (
    SELECT
        disease_state,
        interaction_ratio,
        business_model_fit,
        ROW_NUMBER() OVER (ORDER BY interaction_ratio DESC) AS high_rank,
        ROW_NUMBER() OVER (ORDER BY interaction_ratio ASC) AS low_rank
    FROM mdsd_interaction_model_fit
),
high_model AS (
    SELECT disease_state, interaction_ratio, business_model_fit
    FROM ranked_models
    WHERE high_rank = 1
),
low_model AS (
    SELECT disease_state, interaction_ratio, business_model_fit
    FROM ranked_models
    WHERE low_rank = 1
),
hf_gatekeeper AS (
    SELECT specialty_name, market_share_percentage
    FROM mdsd_specialty_gatekeepers
    WHERE disease_state = 'Heart Failure'
    ORDER BY market_share_percentage DESC
    LIMIT 1
),
copd_gatekeeper AS (
    SELECT specialty_name, market_share_percentage
    FROM mdsd_specialty_gatekeepers
    WHERE disease_state = 'COPD'
    ORDER BY market_share_percentage DESC
    LIMIT 1
)
SELECT 'text' AS component,
    'Interaction model fit indicates distinct business patterns based on provider-patient engagement frequency. '
    || COALESCE((SELECT disease_state FROM high_model), 'The highest-intensity condition')
    || ' aligns to '
    || COALESCE((SELECT business_model_fit FROM high_model), 'a high-frequency model')
    || ' with interaction ratio '
    || COALESCE((SELECT ROUND(interaction_ratio, 2) FROM high_model), 0)
    || ', signaling suitability for continuous or high-frequency digital monitoring. '
    || COALESCE((SELECT disease_state FROM low_model), 'The lower-intensity condition')
    || ' aligns to '
    || COALESCE((SELECT business_model_fit FROM low_model), 'a diagnostic model')
    || ' with interaction ratio '
    || COALESCE((SELECT ROUND(interaction_ratio, 2) FROM low_model), 0)
    || ', favoring periodic assessment. '
    || 'Gatekeeper dominance remains concentrated: '
    || COALESCE((SELECT specialty_name FROM hf_gatekeeper), 'Top specialty')
    || ' controls '
    || COALESCE((SELECT ROUND(market_share_percentage, 1) FROM hf_gatekeeper), 0)
    || '% of Heart Failure gatekeeper activity, while '
    || COALESCE((SELECT specialty_name FROM copd_gatekeeper), 'the COPD leader')
    || ' controls '
    || COALESCE((SELECT ROUND(market_share_percentage, 1) FROM copd_gatekeeper), 0)
    || '% of COPD gatekeeper activity.' AS contents;

SELECT 'chart' AS component,
    'Interaction Model Fit by Condition' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels;

SELECT
    disease_state AS series,
    business_model_fit AS label,
    ROUND(interaction_ratio, 2) AS value
FROM mdsd_interaction_model_fit
ORDER BY interaction_ratio DESC;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    disease_state AS "Disease State",
    specialty_name AS "Gatekeeper Specialty",
    procedure_description AS "Procedure",
    ROUND(market_share_percentage, 1) AS "Market Share (%)",
    specialized_patient_reach AS "Specialized Reach",
    dominance_rank AS "Dominance Rank"
FROM mdsd_specialty_gatekeepers
ORDER BY market_share_percentage DESC, specialized_patient_reach DESC;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Facility vs. Office Service Distribution' AS title,
    '' AS contents;

WITH site_mix AS (
    SELECT
        SUM(facility_services) AS facility_services,
        SUM(office_services) AS office_services,
        SUM(total_services) AS total_services,
        SUM(facility_spend) AS facility_spend,
        SUM(office_spend) AS office_spend
    FROM facility_vs_office_split
    WHERE specialty_name LIKE '%Internal Medicine%'
       OR specialty_name LIKE '%PCP%'
),
metrics AS (
    SELECT
        facility_services,
        office_services,
        total_services,
        facility_spend,
        office_spend,
        CASE WHEN total_services > 0 THEN (facility_services * 100.0) / total_services ELSE 0 END AS facility_share_pct,
        CASE WHEN total_services > 0 THEN (office_services * 100.0) / total_services ELSE 0 END AS office_share_pct,
        CASE WHEN facility_services > 0 THEN facility_spend / facility_services ELSE 0 END AS facility_cost_per_service,
        CASE WHEN office_services > 0 THEN office_spend / office_services ELSE 0 END AS office_cost_per_service
    FROM site_mix
)
SELECT 'text' AS component,
    'Comparing Internal Medicine / PCP economics across care settings shows that '
    || CASE
        WHEN COALESCE((SELECT facility_cost_per_service FROM metrics), 0) > COALESCE((SELECT office_cost_per_service FROM metrics), 0)
            THEN 'facility-based services carry the higher cost-per-service burden'
        WHEN COALESCE((SELECT facility_cost_per_service FROM metrics), 0) < COALESCE((SELECT office_cost_per_service FROM metrics), 0)
            THEN 'office-based services carry the higher cost-per-service burden'
        ELSE 'both settings are currently at similar cost-per-service levels'
       END
    || '. Facility services represent '
    || COALESCE((SELECT ROUND(facility_share_pct, 1) FROM metrics), 0)
    || '% of total volume and account for about $'
    || COALESCE((SELECT printf('%,.2f', facility_spend / 1000000000.0) FROM metrics), '0.00')
    || 'B in spend, while office services represent '
    || COALESCE((SELECT ROUND(office_share_pct, 1) FROM metrics), 0)
    || '% of volume with about $'
    || COALESCE((SELECT printf('%,.2f', office_spend / 1000000000.0) FROM metrics), '0.00')
    || 'B in spend. Cost-per-service is approximately $'
    || COALESCE((SELECT printf('%,.2f', facility_cost_per_service) FROM metrics), '0.00')
    || ' in facility settings versus $'
    || COALESCE((SELECT printf('%,.2f', office_cost_per_service) FROM metrics), '0.00')
    || ' in office settings.' AS contents;

SELECT 'text' AS component,
    'Comparative Specialty Benchmarks' AS title,
    'Internal Medicine / PCP is shown alongside office-centric comparators to contextualize the relative facility footprint.' AS contents;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

WITH benchmark AS (
    SELECT
        CASE
            WHEN specialty_name = 'Internal Medicine / PCP' THEN 'Internal Medicine / PCP'
            WHEN specialty_name = 'Cardiology' AND specialty_domain = 'Primary Care' THEN 'Cardiology (Primary Care)'
            WHEN specialty_name = 'Neurology' AND specialty_domain = 'Primary Care' THEN 'Neurology (Primary Care)'
            WHEN specialty_name = 'Pulmonology' THEN 'Pulmonology'
        END AS benchmark_specialty,
        facility_spend,
        office_spend,
        office_pct,
        total_services,
        CASE
            WHEN specialty_name = 'Internal Medicine / PCP' THEN 1
            WHEN specialty_name = 'Cardiology' AND specialty_domain = 'Primary Care' THEN 2
            WHEN specialty_name = 'Neurology' AND specialty_domain = 'Primary Care' THEN 3
            WHEN specialty_name = 'Pulmonology' THEN 4
        END AS display_order
    FROM facility_vs_office_split
    WHERE specialty_name = 'Internal Medicine / PCP'
       OR (specialty_name = 'Cardiology' AND specialty_domain = 'Primary Care')
       OR (specialty_name = 'Neurology' AND specialty_domain = 'Primary Care')
       OR specialty_name = 'Pulmonology'
)
SELECT
    benchmark_specialty AS "Specialty Name",
    '$' || printf('%,.0f', ROUND(facility_spend, 0)) AS "Facility Spend",
    '$' || printf('%,.0f', ROUND(office_spend, 0)) AS "Office Spend",
    ROUND(office_pct, 1) || '%' AS "Office % of Services"
FROM benchmark
WHERE benchmark_specialty IS NOT NULL
ORDER BY display_order;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

WITH benchmark AS (
    SELECT
        CASE
            WHEN specialty_name = 'Internal Medicine / PCP' THEN 'Internal Medicine / PCP'
            WHEN specialty_name = 'Cardiology' AND specialty_domain = 'Primary Care' THEN 'Cardiology (Primary Care)'
            WHEN specialty_name = 'Neurology' AND specialty_domain = 'Primary Care' THEN 'Neurology (Primary Care)'
            WHEN specialty_name = 'Pulmonology' THEN 'Pulmonology'
        END AS benchmark_specialty,
        total_services,
        CASE
            WHEN specialty_name = 'Internal Medicine / PCP' THEN 1
            WHEN specialty_name = 'Cardiology' AND specialty_domain = 'Primary Care' THEN 2
            WHEN specialty_name = 'Neurology' AND specialty_domain = 'Primary Care' THEN 3
            WHEN specialty_name = 'Pulmonology' THEN 4
        END AS display_order
    FROM facility_vs_office_split
    WHERE specialty_name = 'Internal Medicine / PCP'
       OR (specialty_name = 'Cardiology' AND specialty_domain = 'Primary Care')
       OR (specialty_name = 'Neurology' AND specialty_domain = 'Primary Care')
       OR specialty_name = 'Pulmonology'
)
SELECT
    benchmark_specialty AS "Specialty Name",
    ROUND(total_services, 0) AS "Total Services"
FROM benchmark
WHERE benchmark_specialty IS NOT NULL
ORDER BY display_order;

WITH im AS (
    SELECT
        facility_services,
        total_services,
        facility_spend + office_spend AS total_spend
    FROM facility_vs_office_split
    WHERE specialty_name = 'Internal Medicine / PCP'
    LIMIT 1
)
SELECT 'text' AS component,
    'Key takeaway: Internal Medicine / PCP remains a major anchor for facility-based coordination, with about '
    || COALESCE((SELECT printf('%,.0f', facility_services) FROM im), '0')
    || ' services delivered in facility settings. Across both settings, this specialty accounts for approximately $'
    || COALESCE((SELECT printf('%,.1f', total_spend / 1000000000.0) FROM im), '0.0')
    || 'B in Medicare spend and '
    || COALESCE((SELECT printf('%,.0f', total_services) FROM im), '0')
    || ' total services.' AS contents;

SELECT 'chart' AS component,
    'Internal Medicine Service Distribution' AS title,
    'pie' AS type,
    TRUE AS labels;

SELECT
    'Office Services' AS label,
    ROUND(SUM(office_services), 0) AS value
FROM facility_vs_office_split
WHERE specialty_name LIKE '%Internal Medicine%'
   OR specialty_name LIKE '%PCP%'
UNION ALL
SELECT
    'Facility Services' AS label,
    ROUND(SUM(facility_services), 0) AS value
FROM facility_vs_office_split
WHERE specialty_name LIKE '%Internal Medicine%'
   OR specialty_name LIKE '%PCP%';

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    specialty_name AS "Specialty",
    specialty_domain AS "Domain",
    ROUND(facility_services, 0) AS "Facility Services",
    ROUND(office_services, 0) AS "Office Services",
    ROUND(total_services, 0) AS "Total Services",
    ROUND(office_pct, 1) AS "Office %",
    ROUND(facility_spend / 1e9, 2) AS "Facility Spend ($B)",
    ROUND(office_spend / 1e9, 2) AS "Office Spend ($B)",
    ROUND(facility_spend / NULLIF(facility_services, 0), 2) AS "Facility $/Service",
    ROUND(office_spend / NULLIF(office_services, 0), 2) AS "Office $/Service"
FROM facility_vs_office_split
WHERE specialty_name LIKE '%Internal Medicine%'
   OR specialty_name LIKE '%PCP%';

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Condition Monitoring & Intensity Proof' AS title,
    '' AS contents;

WITH sleep_med AS (
    SELECT specialty_name, monitoring_pct
    FROM monitoring_procedure_intensity
    WHERE specialty_name = 'Sleep Medicine'
    ORDER BY total_volume DESC
    LIMIT 1
),
card_pc AS (
    SELECT specialty_name, specialty_domain, monitoring_pct
    FROM monitoring_procedure_intensity
    WHERE specialty_name = 'Cardiology'
      AND specialty_domain = 'Primary Care'
    LIMIT 1
),
neuro_pc AS (
    SELECT specialty_name, specialty_domain, monitoring_pct
    FROM monitoring_procedure_intensity
    WHERE specialty_name = 'Neurology'
      AND specialty_domain = 'Primary Care'
    LIMIT 1
),
hypertension AS (
    SELECT monitoring_services, monitoring_spend_per_beneficiary
    FROM condition_monitoring_proxy_table
    WHERE disease_state = 'Hypertension'
    LIMIT 1
),
copd AS (
    SELECT monitoring_spend_per_beneficiary
    FROM condition_monitoring_proxy_table
    WHERE disease_state = 'COPD'
    LIMIT 1
)
SELECT 'text' AS component,
    'Monitoring intensity identifies which diseases and specialties require the most longitudinal oversight. '
    || COALESCE((SELECT specialty_name FROM sleep_med), 'Sleep-focused care')
    || ' shows a high monitoring share at '
    || COALESCE((SELECT ROUND(monitoring_pct, 1) FROM sleep_med), 0)
    || '%, indicating that a substantial portion of service volume is tied to ongoing tracking. '
    || COALESCE((SELECT specialty_name FROM card_pc), 'Cardiology')
    || ' ('
    || COALESCE((SELECT specialty_domain FROM card_pc), 'Primary Care')
    || ') also has high monitoring reliance at '
    || COALESCE((SELECT ROUND(monitoring_pct, 1) FROM card_pc), 0)
    || '%, while '
    || COALESCE((SELECT specialty_name FROM neuro_pc), 'Neurology')
    || ' ('
    || COALESCE((SELECT specialty_domain FROM neuro_pc), 'Primary Care')
    || ') is at '
    || COALESCE((SELECT ROUND(monitoring_pct, 1) FROM neuro_pc), 0)
    || '%, suggesting a more episodic or assessment-oriented pattern in the monitored-code set. '
    || 'At the condition level, Hypertension carries very large monitoring volume ('
    || COALESCE((SELECT printf('%,.0f', monitoring_services) FROM hypertension), '0')
    || ' services), but its monitoring spend per beneficiary ($'
    || COALESCE((SELECT printf('%,.2f', monitoring_spend_per_beneficiary) FROM hypertension), '0.00')
    || ') remains below COPD ($'
    || COALESCE((SELECT printf('%,.2f', monitoring_spend_per_beneficiary) FROM copd), '0.00')
    || '), reinforcing the higher-intensity economics of respiratory monitoring.' AS contents;

SELECT 'chart' AS component,
    'Condition Monitoring Intensity (Services per Beneficiary)' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels;

SELECT
    disease_state AS label,
    ROUND(monitoring_services_per_beneficiary, 2) AS value
FROM condition_monitoring_proxy_table
ORDER BY monitoring_services_per_beneficiary DESC;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    specialty_name AS "Specialty",
    ROUND(monitoring_volume, 0) AS "Monitoring Volume",
    ROUND(total_volume, 0) AS "Total Volume",
    ROUND(monitoring_pct, 1) AS "Monitoring %",
    ROUND(monitoring_spend / 1e6, 2) AS "Monitoring Spend ($M)",
    ROUND(total_spend / 1e6, 2) AS "Total Spend ($M)"
FROM monitoring_procedure_intensity
ORDER BY monitoring_pct DESC
LIMIT 20;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'High-Cost Part B Drug Drivers & Supply Velocity' AS title,
    '' AS contents;

WITH gm AS (
    SELECT *
    FROM part_b_drug_intensity
    WHERE specialty_domain = 'General Medicine'
),
top_total AS (
    SELECT hcpcs_code, procedure_description, total_drug_spend, drug_spend_per_patient, total_drug_administrations
    FROM gm
    ORDER BY total_drug_spend DESC
    LIMIT 1
),
top_spp AS (
    SELECT hcpcs_code, procedure_description, total_drug_spend, drug_spend_per_patient, total_drug_administrations
    FROM gm
    ORDER BY drug_spend_per_patient DESC
    LIMIT 1
),
top_volume AS (
    SELECT hcpcs_code, procedure_description, total_drug_spend, drug_spend_per_patient, total_drug_administrations
    FROM gm
    ORDER BY total_drug_administrations DESC
    LIMIT 1
),
j7060 AS (
    SELECT hcpcs_code, supply_item, refill_velocity
    FROM dme_supply_refill_metrics
    WHERE hcpcs_code = 'J7060'
    LIMIT 1
),
top_refill AS (
    SELECT hcpcs_code, supply_item, refill_velocity
    FROM dme_supply_refill_metrics
    ORDER BY refill_velocity DESC
    LIMIT 1
)
SELECT 'text' AS component,
    'Financial intensity in General Medicine is increasingly shaped by drug mix and replenishment cadence, not service volume alone. '
    || 'The largest total-spend driver is '
    || COALESCE((SELECT procedure_description FROM top_total), 'the top-ranked drug')
    || ' ('
    || COALESCE((SELECT hcpcs_code FROM top_total), 'N/A')
    || ') at about $'
    || COALESCE((SELECT printf('%,.2f', total_drug_spend / 1000000000.0) FROM top_total), '0.00')
    || 'B total spend. '
    || 'The highest per-patient financial intensity comes from '
    || COALESCE((SELECT procedure_description FROM top_spp), 'the leading rare-condition agent')
    || ' ('
    || COALESCE((SELECT hcpcs_code FROM top_spp), 'N/A')
    || ') at approximately $'
    || COALESCE((SELECT printf('%,.2f', drug_spend_per_patient) FROM top_spp), '0.00')
    || ' per patient. '
    || 'By administration volume, '
    || COALESCE((SELECT procedure_description FROM top_volume), 'the highest-volume drug')
    || ' ('
    || COALESCE((SELECT hcpcs_code FROM top_volume), 'N/A')
    || ') leads with '
    || COALESCE((SELECT printf('%,.0f', total_drug_administrations) FROM top_volume), '0')
    || ' administrations, illustrating high scale with a different intensity profile. '
    || CASE
        WHEN (SELECT COUNT(*) FROM j7060) > 0
            THEN 'For DME/supply engagement, J7060 ('
                 || COALESCE((SELECT supply_item FROM j7060), 'Dextrose/Water')
                 || ') shows refill velocity '
                 || COALESCE((SELECT printf('%,.2f', refill_velocity) FROM j7060), '0.00')
                 || ', supporting sustained replenishment-cycle planning.'
        ELSE 'For DME/supply engagement, the current top refill item is '
                 || COALESCE((SELECT hcpcs_code FROM top_refill), 'N/A')
                 || ' ('
                 || COALESCE((SELECT supply_item FROM top_refill), 'Top item')
                 || ') with refill velocity '
                 || COALESCE((SELECT printf('%,.2f', refill_velocity) FROM top_refill), '0.00')
                 || '.'
      END AS contents;

WITH ds AS (
    SELECT
        procedure_description,
        hcpcs_code,
        SUM(total_drug_spend) AS total_spend,
        ROW_NUMBER() OVER (ORDER BY SUM(total_drug_spend) DESC) AS rn
    FROM part_b_drug_intensity
    WHERE specialty_domain = 'General Medicine'
    GROUP BY hcpcs_code, procedure_description
),
grand AS (
    SELECT SUM(total_drug_spend) AS grand_total
    FROM part_b_drug_intensity
    WHERE specialty_domain = 'General Medicine'
),
top2 AS (SELECT * FROM ds WHERE rn <= 2)
SELECT 'text' AS component,
    'Pareto Insight: Drug Spend Concentration' AS title,
    'This Pareto analysis highlights the concentration of Medicare spend within specific high-intensity drugs. '
    || COALESCE((SELECT procedure_description FROM top2 WHERE rn = 1), 'the leading drug')
    || ' (' || COALESCE((SELECT hcpcs_code FROM top2 WHERE rn = 1), 'N/A') || ')'
    || ' and '
    || COALESCE((SELECT procedure_description FROM top2 WHERE rn = 2), 'the second drug')
    || ' (' || COALESCE((SELECT hcpcs_code FROM top2 WHERE rn = 2), 'N/A') || ')'
    || ' represent the highest-spend drugs in General Medicine, together accounting for '
    || printf('%.1f',
           (
               COALESCE((SELECT total_spend FROM top2 WHERE rn = 1), 0) +
               COALESCE((SELECT total_spend FROM top2 WHERE rn = 2), 0)
           ) * 100.0 / NULLIF((SELECT grand_total FROM grand), 0)
       )
    || '% of total Part B drug spend in this segment. '
    || COALESCE((SELECT procedure_description FROM top2 WHERE rn = 1), 'The leading agent')
    || ' alone accounts for over $'
    || COALESCE((SELECT printf('%,.2f', total_spend / 1e9) FROM top2 WHERE rn = 1), '0.00')
    || ' billion, underscoring the outsized financial impact of a narrow set of agents. '
    || 'Understanding these drivers is critical for identifying high-margin therapeutic areas and informing formulary strategy.'
    AS contents;

WITH drug_rank AS (
    SELECT
        hcpcs_code,
        procedure_description,
        SUM(total_drug_spend) AS total_spend,
        ROW_NUMBER() OVER (ORDER BY SUM(total_drug_spend) DESC) AS rn
    FROM part_b_drug_intensity
    WHERE specialty_domain = 'General Medicine'
    GROUP BY hcpcs_code, procedure_description
),
pareto AS (
    SELECT
        rn,
        hcpcs_code,
        procedure_description,
        total_spend,
        SUM(total_spend) OVER (ORDER BY rn) AS cumulative_spend,
        SUM(total_spend) OVER () AS grand_total
    FROM drug_rank
    WHERE rn <= 15
)
SELECT 'chart' AS component,
    'Pareto Analysis of Part B Drug Spend (General Medicine)' AS title,
    'line' AS type,
    TRUE AS labels,
    'Cumulative Spend Share (%)' AS ytitle,
    'Drug (HCPCS Code)' AS xtitle;

WITH drug_rank AS (
    SELECT
        hcpcs_code,
        procedure_description,
        SUM(total_drug_spend) AS total_spend,
        ROW_NUMBER() OVER (ORDER BY SUM(total_drug_spend) DESC) AS rn
    FROM part_b_drug_intensity
    WHERE specialty_domain = 'General Medicine'
    GROUP BY hcpcs_code, procedure_description
),
pareto AS (
    SELECT
        rn,
        hcpcs_code,
        procedure_description,
        total_spend,
        SUM(total_spend) OVER (ORDER BY rn) AS cumulative_spend,
        SUM(total_spend) OVER () AS grand_total
    FROM drug_rank
    WHERE rn <= 15
)
SELECT
    'Cumulative Share %' AS series,
    hcpcs_code AS x,
    procedure_description AS label,
    ROUND((cumulative_spend * 100.0) / NULLIF(grand_total, 0), 2) AS value
FROM pareto
ORDER BY rn;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

WITH drug_rank AS (
    SELECT
        hcpcs_code,
        procedure_description,
        SUM(total_drug_spend) AS total_spend,
        ROW_NUMBER() OVER (ORDER BY SUM(total_drug_spend) DESC) AS rn
    FROM part_b_drug_intensity
    WHERE specialty_domain = 'General Medicine'
    GROUP BY hcpcs_code, procedure_description
),
pareto AS (
    SELECT
        rn,
        hcpcs_code,
        procedure_description,
        total_spend,
        SUM(total_spend) OVER (ORDER BY rn) AS cumulative_spend,
        SUM(total_spend) OVER () AS grand_total
    FROM drug_rank
    WHERE rn <= 15
)
SELECT
    rn AS "Rank",
    hcpcs_code AS "HCPCS",
    procedure_description AS "Drug Name",
    ROUND(total_spend / 1e6, 2) AS "Drug Spend ($M)",
    ROUND((cumulative_spend * 100.0) / NULLIF(grand_total, 0), 2) AS "Cumulative Share (%)"
FROM pareto
ORDER BY rn;

SELECT 'chart' AS component,
    'Supply Refill Velocity Leaders' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Units per Patient' AS xtitle;

SELECT
    hcpcs_code || ' - ' || supply_item AS label,
    ROUND(refill_velocity, 2) AS value
FROM dme_supply_refill_metrics
ORDER BY refill_velocity DESC
LIMIT 10;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Clinical Dominance & Procedure Concentration' AS title,
    '' AS contents;

WITH card_diag AS (
    SELECT hcpcs_code, procedure_description, ROUND(total_services, 0) AS services
    FROM specialty_market_concentration
    WHERE specialty_name = 'Cardiology'
      AND hcpcs_code IN ('93000', '93010', '93015')
      AND dominance_rank = 1
    ORDER BY total_services DESC
    LIMIT 1
),
im_visit AS (
    SELECT hcpcs_code, procedure_description, ROUND(total_services, 0) AS services
    FROM specialty_market_concentration
    WHERE specialty_name = 'Internal Medicine / PCP'
      AND hcpcs_code = '99214'
      AND dominance_rank = 1
    LIMIT 1
),
other_high AS (
    SELECT hcpcs_code, procedure_description, ROUND(total_services, 0) AS services
    FROM specialty_market_concentration
    WHERE specialty_name = 'Other Specialty'
      AND dominance_rank = 1
    ORDER BY total_services DESC
    LIMIT 1
),
pul_proc AS (
    SELECT specialty_name, hcpcs_code, procedure_description, ROUND(pct_of_national_volume, 1) AS dominance_pct
    FROM specialty_market_concentration
    WHERE specialty_name = 'Pulmonology'
      AND dominance_rank = 1
    ORDER BY total_services DESC
    LIMIT 1
)
SELECT 'text' AS component,
    'Market dominance in specific clinical domains typically reflects specialty focus and referral patterns — many procedures show near-absolute (100%) concentration. '
    || 'Cardiology maintains dominant shares on critical diagnostics; for example, '
    || COALESCE((SELECT procedure_description FROM card_diag), 'EKG procedures')
    || ' ('
    || COALESCE((SELECT hcpcs_code FROM card_diag), '93010')
    || ') alone account for '
    || COALESCE((SELECT printf('%,.0f', services) FROM card_diag), '0')
    || ' services. '
    || 'Internal Medicine / PCP similarly dominates high-volume evaluation & management visits; '
    || COALESCE((SELECT procedure_description FROM im_visit), 'established office visit, moderate')
    || ' ('
    || COALESCE((SELECT hcpcs_code FROM im_visit), '99214')
    || ') reaches '
    || COALESCE((SELECT printf('%,.0f', services) FROM im_visit), '0')
    || ' services nationwide. '
    || 'Other Specialty maintains massive volume across therapeutics; the leading procedure is '
    || COALESCE((SELECT procedure_description FROM other_high), 'a high-volume therapeutic')
    || ' ('
    || COALESCE((SELECT hcpcs_code FROM other_high), 'N/A')
    || ') at '
    || COALESCE((SELECT printf('%,.0f', services) FROM other_high), '0')
    || ' administrations. '
    || 'Even specialties with smaller procedural footprints achieve dominance; Pulmonology''s '
    || COALESCE((SELECT procedure_description FROM pul_proc), 'procedure')
    || ' demonstrates control at '
    || COALESCE((SELECT ROUND(dominance_pct, 1) FROM pul_proc), 0)
    || '% national share.' AS contents;

SELECT 'chart' AS component,
    'Top Procedure Concentration by Specialty' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels;

SELECT
    specialty_name || ' | ' || hcpcs_code AS label,
    ROUND(pct_of_national_volume, 1) AS value
FROM specialty_market_concentration
WHERE dominance_rank = 1
ORDER BY pct_of_national_volume DESC
LIMIT 15;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    specialty_name AS "Specialty",
    hcpcs_code AS "HCPCS",
    procedure_description AS "Procedure",
    ROUND(total_services, 0) AS "Services",
    ROUND(total_benes, 0) AS "Beneficiaries",
    ROUND(pct_of_national_volume, 1) AS "Market Share (%)",
    dominance_rank AS "Dominance Rank"
FROM specialty_market_concentration
ORDER BY pct_of_national_volume DESC
LIMIT 25;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Geographic Concentration & Strategic Interaction Models' AS title,
    '' AS contents;

WITH state_rank AS (
    SELECT
        state_abbr,
        ROUND(SUM(state_total_spend), 0) AS total_spend,
        ROUND(SUM(state_patient_volume), 0) AS patient_volume,
        ROUND(SUM(state_total_spend) / NULLIF(SUM(state_patient_volume), 0), 2) AS spend_per_patient,
        ROW_NUMBER() OVER (ORDER BY SUM(state_total_spend) DESC) AS spend_rank
    FROM geographic_market_opportunity
    GROUP BY state_abbr
),
top_1 AS (
    SELECT state_abbr, total_spend, patient_volume, spend_per_patient
    FROM state_rank
    WHERE spend_rank = 1
),
top_2 AS (
    SELECT state_abbr, total_spend, patient_volume, spend_per_patient
    FROM state_rank
    WHERE spend_rank = 2
),
top_3 AS (
    SELECT state_abbr, total_spend, patient_volume, spend_per_patient
    FROM state_rank
    WHERE spend_rank = 3
),
comparison_state AS (
    SELECT state_abbr, total_spend, patient_volume, spend_per_patient
    FROM state_rank
    WHERE spend_rank = 4
)
SELECT 'text' AS component,
    'Geographic Opportunity Concentration: The market opportunity concentrates in high-population states, though spend per patient varies by region. '
    || COALESCE((SELECT state_abbr FROM top_1), 'State 06')
    || ' (California) leads at approximately $'
    || COALESCE((SELECT printf('%,.2f', total_spend / 1000000000.0) FROM top_1), '0.00')
    || 'B in total spend, driven by '
    || COALESCE((SELECT printf('%,.0f', patient_volume) FROM top_1), '0')
    || ' patients. '
    || COALESCE((SELECT state_abbr FROM top_2), 'State 12')
    || ' (Florida) and '
    || COALESCE((SELECT state_abbr FROM top_3), 'State 48')
    || ' (Texas) follow with approximately $'
    || COALESCE((SELECT printf('%,.2f', total_spend / 1000000000.0) FROM top_2), '0.00')
    || 'B and $'
    || COALESCE((SELECT printf('%,.2f', total_spend / 1000000000.0) FROM top_3), '0.00')
    || 'B respectively. '
    || 'Regional cost variance is notable: '
    || COALESCE((SELECT state_abbr FROM comparison_state), 'State 36')
    || ' (New York) shows spend per patient of $'
    || COALESCE((SELECT printf('%,.2f', spend_per_patient) FROM comparison_state), '0.00')
    || ' compared to California''s $'
    || COALESCE((SELECT printf('%,.2f', spend_per_patient) FROM top_1), '0.00')
    || ', reflecting regional differences in care intensity and cost.' AS contents;

SELECT 'table' AS component, TRUE AS hover, TRUE AS striped_rows;

WITH state_rank AS (
    SELECT
        state_abbr,
        ROUND(SUM(state_total_spend), 0) AS total_spend,
        ROUND(SUM(state_patient_volume), 0) AS patient_volume,
        ROUND(SUM(state_total_spend) / NULLIF(SUM(state_patient_volume), 0), 2) AS spend_per_patient,
        ROW_NUMBER() OVER (ORDER BY SUM(state_total_spend) DESC) AS spend_rank
    FROM geographic_market_opportunity
    GROUP BY state_abbr
)
SELECT
    state_abbr AS "State",
    '$' || printf('%,.0f', total_spend) AS "Total Spend",
    printf('%,.0f', patient_volume) AS "Patient Volume",
    '$' || printf('%,.2f', spend_per_patient) AS "Spend Per Patient"
FROM state_rank
WHERE spend_rank <= 3
ORDER BY spend_rank;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Interaction Intensity & Clinical Model Fit' AS title,
    'Understanding interaction frequency and model fit is critical for business model selection.' AS contents;

WITH diagnostic_low AS (
    SELECT disease_state, business_model_fit, interaction_ratio
    FROM mdsd_interaction_model_fit
    ORDER BY interaction_ratio ASC
    LIMIT 1
),
saas_high AS (
    SELECT disease_state, business_model_fit, interaction_ratio
    FROM mdsd_interaction_model_fit
    ORDER BY interaction_ratio DESC
    LIMIT 1
),
top_refill AS (
    SELECT hcpcs_code, supply_item, refill_velocity
    FROM dme_supply_refill_metrics
    ORDER BY refill_velocity DESC
    LIMIT 1
),
j7042_refill AS (
    SELECT hcpcs_code, supply_item, refill_velocity
    FROM dme_supply_refill_metrics
    WHERE hcpcs_code = 'J7042'
    LIMIT 1
)
SELECT 'text' AS component,
    'Diagnostic models (low interaction frequency) contrast sharply with continuous care models (high interaction). '
    || COALESCE((SELECT disease_state FROM diagnostic_low), 'Hypertension')
    || ' exemplifies a '
    || COALESCE((SELECT business_model_fit FROM diagnostic_low), 'Diagnostic Model')
    || ' with interaction ratio '
    || COALESCE((SELECT ROUND(interaction_ratio, 2) FROM diagnostic_low), 0)
    || ', promoting periodic assessment and monitoring. '
    || 'Conversely, '
    || COALESCE((SELECT disease_state FROM saas_high), 'COPD')
    || ' aligns to '
    || COALESCE((SELECT business_model_fit FROM saas_high), 'a SaaS/continuous monitoring')
    || ' model with interaction ratio '
    || COALESCE((SELECT ROUND(interaction_ratio, 2) FROM saas_high), 0)
    || '. '
    || 'Supply velocity reinforces engagement intensity: The highest refill velocity in the data is '
    || COALESCE((SELECT printf('%,.1f', refill_velocity) FROM top_refill), '0.0')
    || ' ('
    || COALESCE((SELECT hcpcs_code FROM top_refill), 'N/A')
    || '), signaling extremely high-frequency supply consumption. '
    || CASE
        WHEN (SELECT COUNT(*) FROM j7042_refill) > 0
            THEN 'For comparison, J7042 (Normal Saline) shows refill velocity '
                 || COALESCE((SELECT printf('%,.2f', refill_velocity) FROM j7042_refill), '0.00')
                 || ', typical of regular maintenance protocols.'
        ELSE ''
      END AS contents;

SELECT 'divider' AS component;

SELECT 'chart' AS component,
    'Geographic Spend Concentration' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Total Spend ($B)' AS xtitle;

SELECT
    state_abbr AS label,
    ROUND(SUM(state_total_spend) / 1e9, 2) AS value
FROM geographic_market_opportunity
GROUP BY state_abbr
ORDER BY SUM(state_total_spend) DESC
LIMIT 15;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    state_abbr AS "State",
    ROUND(SUM(state_patient_volume), 0) AS "Patients",
    ROUND(SUM(state_total_spend) / 1e9, 2) AS "Total Spend ($B)",
    ROUND(SUM(gpci_adjusted_spend) / 1e9, 2) AS "GPCI-Adj Spend ($B)",
    ROUND(SUM(state_total_spend) / NULLIF(SUM(state_patient_volume), 0), 2) AS "$ / Patient"
FROM geographic_market_opportunity
GROUP BY state_abbr
ORDER BY SUM(state_total_spend) DESC
LIMIT 20;

SELECT 'chart' AS component,
    'Strategic Interaction Models' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Interaction Ratio' AS ytitle;

SELECT
    disease_state AS label,
    ROUND(interaction_ratio, 2) AS value
FROM mdsd_interaction_model_fit
ORDER BY interaction_ratio DESC;

SELECT 'divider' AS component;

WITH volume_condition AS (
    SELECT disease_state, patient_volume
    FROM mdsd_global_opportunity_matrix
    ORDER BY patient_volume DESC
    LIMIT 1
),
intensity_condition AS (
    SELECT disease_state, interaction_density
    FROM mdsd_global_opportunity_matrix
    ORDER BY interaction_density DESC
    LIMIT 1
),
high_intensity_spec AS (
    SELECT specialty_name, economic_intensity_index
    FROM mdsd_economic_intensity_proof
    ORDER BY economic_intensity_index DESC
    LIMIT 1
)
SELECT 'text' AS component,
    'Market Structure Summary: The Bifurcated Opportunity' AS title,
    'The consolidated evidence reveals a fundamentally bifurcated market, requiring distinct entry strategies. '
    || 'Volume-driven segments (e.g., '
    || COALESCE((SELECT disease_state FROM volume_condition), 'Hypertension')
    || ' with '
    || COALESCE((SELECT printf('%,.0f', patient_volume) FROM volume_condition), '0')
    || ' patients) thrive in large geographic markets like California and Florida, where scale drives economics. '
    || 'Intensity-driven segments (e.g., '
    || COALESCE((SELECT disease_state FROM intensity_condition), 'COPD')
    || ' with interaction density '
    || COALESCE((SELECT ROUND(interaction_density, 1) FROM intensity_condition), 0)
    || ') prioritize specialized supply chains, rare therapeutics, and continuous monitoring. '
    || 'Clinical dominance remains concentrated: specialty gatekeepers (e.g., '
    || COALESCE((SELECT specialty_name FROM high_intensity_spec), 'leading high-intensity specialty')
    || ' with intensity index '
    || COALESCE((SELECT ROUND(economic_intensity_index, 2) FROM high_intensity_spec), 0)
    || ') control their core procedures absolutely, creating defensible competitive positions.' AS contents;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Clinical Gatekeepers & Market Dominance' AS title,
    '' AS contents;

WITH disease_reach AS (
    SELECT
        disease_state,
        SUM(specialized_patient_reach) AS total_reach,
        AVG(market_share_percentage) AS avg_share
    FROM mdsd_specialty_gatekeepers
    GROUP BY disease_state
),
top_disease AS (
    SELECT disease_state, total_reach, avg_share
    FROM disease_reach
    ORDER BY total_reach DESC
    LIMIT 1
),
second_disease AS (
    SELECT disease_state, total_reach, avg_share
    FROM disease_reach
    ORDER BY total_reach DESC
    LIMIT 1 OFFSET 1
),
top_gatekeeper AS (
    SELECT disease_state, specialty_name, procedure_description, market_share_percentage, specialized_patient_reach
    FROM mdsd_specialty_gatekeepers
    ORDER BY market_share_percentage DESC, specialized_patient_reach DESC
    LIMIT 1
),
top_state AS (
    SELECT
        state_abbr,
        SUM(state_total_spend) AS state_spend
    FROM geographic_market_opportunity
    WHERE specialty_name IN (SELECT DISTINCT specialty_name FROM mdsd_specialty_gatekeepers)
    GROUP BY state_abbr
    ORDER BY SUM(state_total_spend) DESC
    LIMIT 1
)
SELECT 'text' AS component,
    'Gatekeeper dynamics show concentrated clinical control and uneven opportunity distribution across diseases and regions. '
    || COALESCE((SELECT disease_state FROM top_disease), 'Hypertension')
    || ' leads gatekeeper opportunity with '
    || COALESCE((SELECT printf('%,.0f', total_reach) FROM top_disease), '0')
    || ' specialized patients at an average dominance of '
    || COALESCE((SELECT ROUND(avg_share, 1) FROM top_disease), 0)
    || '%, followed by '
    || COALESCE((SELECT disease_state FROM second_disease), 'Type 2 Diabetes')
    || ' with '
    || COALESCE((SELECT printf('%,.0f', total_reach) FROM second_disease), '0')
    || ' specialized patients. '
    || 'At the procedure level, '
    || COALESCE((SELECT specialty_name FROM top_gatekeeper), 'the leading specialty')
    || ' anchors '
    || COALESCE((SELECT disease_state FROM top_gatekeeper), 'the top condition')
    || ' through '
    || COALESCE((SELECT procedure_description FROM top_gatekeeper), 'its lead procedure')
    || ' with '
    || COALESCE((SELECT ROUND(market_share_percentage, 1) FROM top_gatekeeper), 0)
    || '% share and '
    || COALESCE((SELECT printf('%,.0f', specialized_patient_reach) FROM top_gatekeeper), '0')
    || ' specialized patients. '
    || 'Geographically, '
    || COALESCE((SELECT state_abbr FROM top_state), '06')
    || ' is the largest spend concentration for gatekeeper-led specialties at about $'
    || COALESCE((SELECT printf('%,.2f', state_spend / 1000000000.0) FROM top_state), '0.00')
    || 'B.' AS contents;

SELECT 'chart' AS component,
    'Market Opportunity Analysis (Gatekeeper Reach by Disease)' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Specialized Patient Reach' AS xtitle;

SELECT
    disease_state AS label,
    ROUND(SUM(specialized_patient_reach), 0) AS value
FROM mdsd_specialty_gatekeepers
GROUP BY disease_state
ORDER BY SUM(specialized_patient_reach) DESC;

SELECT 'chart' AS component,
    'Gatekeeper Market Dominance' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Market Share (%)' AS ytitle;

SELECT
    specialty_name || ' - ' || disease_state AS label,
    ROUND(market_share_percentage, 1) AS value
FROM mdsd_specialty_gatekeepers
ORDER BY market_share_percentage DESC, specialized_patient_reach DESC
LIMIT 12;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    disease_state AS "Disease State",
    specialty_name AS "Gatekeeper",
    ROUND(market_share_percentage, 1) AS "Share (%)",
    ROUND(specialized_patient_reach, 0) AS "Patient Reach",
    procedure_description AS "Lead Procedure"
FROM mdsd_specialty_gatekeepers
ORDER BY market_share_percentage DESC, specialized_patient_reach DESC;
```

---

## Data Dictionary

```sql mmi/data-dictionary.sql { route: { caption: "Data Dictionary" } }
-- @route.description "Schema reference for all tables, views, and the scoring methodology"

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
    '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
    'Data Dictionary & Pipeline Reference' AS title,
    'Schema reference for the Medigy Disease State Database (MDSD).' AS description,
    'gray' AS color;

-- 2. DATA SOURCES (External)
SELECT 'text' AS component, '### 1. External Data Sources' AS contents_md;
SELECT 'list' AS component;
SELECT 
    title,
    description,
    link,
    'external-link' AS icon,
    'blue' AS color
FROM data_provenance 
WHERE object_type = 'external_source';

-- 3. MASTER TABLES (Dimensions & Reference)
SELECT 'text' AS component, '### 2. Master & Reference Tables' AS contents_md;
SELECT 'table' AS component, TRUE AS hover, TRUE AS striped_rows;
SELECT 
    name AS "Table Name"   
FROM sqlite_schema s
WHERE (name LIKE 'dim_%' OR name LIKE 'uniform_resource_ref_%')  
ORDER BY name;


-- 5. DERIVED TABLES & ANALYTICAL VIEWS
-- 2. Header and Summary Statistics
SELECT 'title' AS component, 'Schema Data Dictionary' AS contents;

SELECT 'big_number' AS component, 1 AS columns;

SELECT 
    'Total Objects' AS title, 
    COUNT(*) AS value, 
    'database' AS icon, 
    'blue' AS color 
FROM data_tables_derived;


-- 3. The Data Table
SELECT 'table' AS component, 
       TRUE AS sort, 
       TRUE AS search,
       TRUE AS markdown,
       'Object Name' AS object_name,
       'Type' AS object_type,
       'Category' AS category;

SELECT 
    object_name,   
    object_type,
    -- You can color-code categories if you want to get fancy
    category
FROM data_tables_derived
ORDER BY category, object_name;

-- 6. PERFORMANCE INDEXES
SELECT 'text' AS component, '### 5. Query Performance Indexes' AS contents_md;

SELECT 'table' AS component, 
    TRUE AS hover, 
    TRUE AS striped_rows;

SELECT 
    index_name AS "Index Name",
    table_name AS "Target Table",
    description AS "Description"
FROM data_dictionary_indexes;
```

## Specialty Listing

```sql mmi/medical-specialities.sql { route: { caption: "Medical Specialties" } }
-- @route.description "Detailed breakdown of medical specialties indexed in the current pipeline."
SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
    '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'table' AS component, 'Indexed Specialties' AS title, true AS search;
SELECT distinct specialty_name AS Name, specialty_domain AS Description FROM dim_specialty;
```

## Procedures

```sql mmi/procedures.sql { route: { caption: "Procedure Inventory" } }
-- @route.description "Full inventory of HCPCS/CPT codes included in the Part B dataset."

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
    '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'table' AS component, 'Available Codes' AS title, true AS search, 20 AS limit;
SELECT hcpcs_code AS Code, procedure_description AS Label FROM dim_procedure;
```

## Geographic Scope

```sql mmi/geography.sql { route: { caption: "Geographic Scope" } }
-- @route.description "States and territories currently processed in the data pipeline."

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
    '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

-- A simple list of states using cards
SELECT 'card' AS component, 6 AS columns;
SELECT distinct state_abbr AS title, locality_name AS description, 'map-pin' AS icon FROM dim_geography;
```

## Disease Clusters

```sql mmi/disease-clusters.sql { route: { caption: "ICD-10 Disease Mapping" } }
-- @route.description "Clusters mapped for opportunity scoring, excluding General/Other categories."

SELECT 'shell' AS component,
       'Medigy Market Intelligence' AS title,
       NULL AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
    CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END AS link,
    '/footer-links.js' AS javascript,
     '© 2026 Medigy Market Intelligence' AS footer,
    '{"link":"' || CASE WHEN instr(sqlpage.path(), 'mmi/') > 0 THEN '../' ELSE './' END || '","title":"Home"}' AS menu_item,
       '{"link":"/mmi/executive-dashboard.sql","title":"Executive Dashboard"}' AS menu_item,
       '{"link":"/mmi/opportunity-scoring.sql","title":"Opportunity Scores"}' AS menu_item,
       '{"link":"/mmi/sleep-apnea-evidence.sql","title":"Evidence"}' AS menu_item,
       '{"link":"/mmi/cms-sleep-apnea-market-analysis.sql","title":"Sleep Apnea Market"}' AS menu_item,
       '{"link":"/mmi/disease-mapping.sql","title":"Disease Mapping"}' AS menu_item,
       '{"link":"/mmi/procedure-drilldown.sql","title":"Procedure Drilldown"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'table' AS component, 'Mapped Clusters' AS title, true AS sort, true AS search;
SELECT distinct disease_state AS Cluster FROM dim_diagnosis WHERE disease_state != 'Other Chronic / Clinical';
```
