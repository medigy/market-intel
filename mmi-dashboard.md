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
surveilr shell sql/medigy-copd.sql
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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

-- HERO
SELECT 'hero' AS component,
    'Medigy Market Intelligence' AS title,
    'CMS Part B & DMEPOS analytics for identifying high-opportunity disease-specialty clusters.' AS description,
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

-- 3. PRIMARY NAVIGATION MODULES
SELECT 'divider' AS component, 'Analytics Core' AS label;
SELECT 'card' AS component, 3 AS columns;

SELECT
    'Executive Dashboard' AS title,
    'Top-line KPIs: total spend, patient reach, and service intensity across all specialties.' AS description,
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
    'Disease Mapping' AS title,
    'ICD-10 cluster coverage and interaction density by disease.' AS description,
    '/mmi/disease-mapping.sql' AS link,
    'virus' AS icon,
    'indigo' AS color;

-- 4. DISEASE STATE SNAPSHOTS
SELECT 'divider' AS component, 'Market Snapshots' AS label;

-- 4a. Sleep Apnea Summary
SELECT 'text' AS component, 'Sleep Apnea Market Snapshot' AS contents_md;
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
    'DME Allowed (CPAP)' AS title,
    '$' || printf('%,.1f', SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt) / 1000000.0) || 'M' AS description,
    '/mmi/cms-sleep-apnea-market-analysis.sql' AS link,
    'device-desktop' AS icon,
    'blue' AS color
FROM uniform_resource_dme_data
WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471');

SELECT
    'Avg. DME Markup' AS title,
    printf('%.1fx', 
        SUM(Tot_Suplr_Srvcs * Avg_Suplr_Sbmtd_Chrg) / 
        NULLIF(SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt), 0)
    ) AS description,
    '/mmi/cms-sleep-apnea-market-analysis.sql' AS link,
    'trending-up' AS icon,
    'indigo' AS color
FROM uniform_resource_dme_data
WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471');

SELECT
    'Patient Funnel' AS title,
    printf('%,.0f', SUM(Tot_Benes)) || ' tested' AS description,
    '/mmi/cms-sleep-apnea-market-analysis.sql' AS link,
    'users' AS icon,
    'cyan' AS color
FROM uniform_resource_diagnostics_data
WHERE Rndrng_Prvdr_Geo_Lvl = 'National';


-- 4b. COPD Summary
SELECT 'text' AS component, 'COPD Respiratory Snapshot' AS contents_md;
SELECT 'card' AS component, 4 AS columns;

SELECT 
    'PFT Allowed' AS title, 
    '$' || printf('%,.1f', total_allowed / 1000000.0) || 'M' AS description,
    '/mmi/copd-hub.sql' AS link,
    'activity-heartbeat' AS icon,
    'teal' AS color
FROM copd_pft_national_kpis;

SELECT 
    'Oxygen DME Allowed' AS title, 
    '$' || printf('%,.1f', total_allowed / 1000000.0) || 'M' AS description,
    '/mmi/copd-hub.sql' AS link,
    'droplet' AS icon,
    'blue' AS color
FROM oxygen_national_market_financial_summary;

SELECT 
    'Untested Gap' AS title, 
    value || unit AS description,
    '/mmi/copd-hub.sql' AS link,
    'alert-triangle' AS icon,
    'orange' AS color
FROM copd_executive_summary_kpis WHERE finding_no = '3';

SELECT 
    'Patient LTV (36mo)' AS title, 
    '$' || printf('%,.0f', ltv_36_months_with_o2) AS description,
    '/mmi/copd-hub.sql' AS link,
    'chart-line' AS icon,
    'indigo' AS color
FROM copd_patient_36mo_ltv_model;


-- 5. REFERENCE & DICTIONARY
SELECT 'divider' AS component, 'Infrastructure' AS label;
SELECT 'card' AS component, 1 AS columns;

SELECT
    'Data Dictionary & Provenance' AS title,
    'Technical schema reference for dimension/fact tables and Singer-protocol data sources.' AS description,
    '/mmi/data-dictionary.sql' AS link,
    'database' AS icon,
    'gray' AS color;

```

---

## Executive Dashboard

```sql mmi/executive-dashboard.sql { route: { caption: "Executive Dashboard" } }
-- @route.description "Top-line Medigy market KPIs: spend, volume, and intensity by specialty"

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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
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
      '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
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
-- @route.description "Evidence dashboard mapped to  Report references and Medigy analytical tables"

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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
    'Evidence & Market Prioritization' AS title,
    'cyan' AS color;

SELECT 'text' AS component,
    'Global Opportunity Matrix & Scoring' AS title,
    '' AS contents;

SELECT 'text' AS component;
SELECT 
    'The market is currently categorized mainly into '
    || COALESCE(top_tier_name, 'a single tier')
    || ' opportunities ('
    || COALESCE(top_tier_count, 0)
    || ' disease states), with meaningful variation in the underlying drivers. '
    || 'Hypertension holds the highest composite opportunity score at '
    || COALESCE(htn_score, 0)
    || ', driven by scale: '
    || printf('%,.0f', COALESCE(htn_volume, 0))
    || ' patients and about $'
    || printf('%,.1f', COALESCE(htn_spend, 0) / 1000.0)
    || 'B total spend. '
    || 'Conversely, COPD scores '
    || COALESCE(copd_score, 0)
    || ' with a much smaller patient base ('
    || printf('%,.0f', COALESCE(copd_volume, 0))
    || ') but the highest interaction density ('
    || COALESCE(ROUND(copd_density, 1), 0)
    || '), aligning to '
    || COALESCE(copd_model_fit, 'its model-fit')
    || '. '
    || 'Heart Failure remains a balanced profile at score '
    || COALESCE(hf_score, 0)
    || ', with '
    || printf('%,.0f', COALESCE(hf_volume, 0))
    || ' patients and interaction density '
    || COALESCE(ROUND(hf_density, 2), 0)
    || '.' AS contents
FROM summary_market_overview
WHERE id = 1;

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
    composite_score AS "Composite Score",
    patient_volume AS "Patients",
    interaction_density AS "Interaction Density",
    spend_millions AS "Spend ($M)",
    market_concentration AS "Market Concentration (%)"
FROM summary_disease_opportunity_list;


SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Specialty Economic Intensity & Efficiency' AS title,
    '' AS contents;


SELECT 'text' AS component, 
       narrative_text AS contents 
FROM summary_specialty_narrative 
WHERE id = 1;

SELECT 'text' AS component, 
       highlight_text AS contents 
FROM summary_intensity_highlights 
WHERE id = 1;

SELECT 'chart' AS component,
    'Economic Intensity Index by Specialty' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Economic Intensity Index' AS ytitle;

SELECT
    specialty_name AS label,
    intensity_value AS value
FROM summary_chart_economic_intensity;

SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    specialty_name AS "Specialty",
    specialty_domain AS "Domain",
    patient_reach AS "Patients",
    avg_allowed AS "$/Patient",
    interaction_freq AS "Interactions/Patient",
    intensity_index AS "Intensity Index"
FROM summary_table_economic_intensity;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Interaction Models & Clinical Gatekeepers' AS title,
    '' AS contents;

SELECT 'text' AS component, 
       narrative_text AS contents 
FROM summary_gatekeeper_narrative 
WHERE id = 1;

SELECT 'chart' AS component,
    'Interaction Model Fit by Condition' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels;

SELECT
    disease_state AS series,
    business_model_fit AS label,
    rounded_ratio AS value
FROM summary_chart_interaction_models;



SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    disease_state AS "Disease State",
    specialty_name AS "Gatekeeper Specialty",
    procedure_desc AS "Procedure",
    market_share AS "Market Share (%)",
    patient_reach AS "Specialized Reach",
    dominance_rank AS "Dominance Rank"
FROM summary_table_gatekeepers;


SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Facility vs. Office Service Distribution' AS title,
    '' AS contents;


SELECT 'text' AS component, 
       narrative_text AS contents 
FROM summary_site_mix_narrative 
WHERE id = 1;

SELECT 'text' AS component,
    'Comparative Specialty Benchmarks' AS title,
    'Internal Medicine / PCP is shown alongside office-centric comparators to contextualize the relative facility footprint.' AS contents;

SELECT 'table' AS component, 
       TRUE AS sort, 
       TRUE AS hover, 
       TRUE AS striped_rows;


SELECT
    specialty_name AS "Specialty Name",
    facility_spend_text AS "Facility Spend",
    office_spend_text AS "Office Spend",
    office_pct_text AS "Office % of Services"
FROM summary_benchmark_specialties
ORDER BY display_order;

-- Table Component
SELECT 'table' AS component, TRUE AS sort, TRUE AS hover, TRUE AS striped_rows;

SELECT
    specialty_name AS "Specialty Name",
    total_services_rounded AS "Total Services"
FROM summary_service_benchmarks
ORDER BY display_order;

-- Text Component
SELECT 'text' AS component,
    takeaway_text AS contents
FROM summary_im_takeaway
WHERE id = 1;

SELECT 'chart' AS component,
    'Internal Medicine Service Distribution' AS title,
    'pie' AS type,
    TRUE AS labels;

-- Direct pull from the 2-row summary table
SELECT label, value_rounded AS value
FROM summary_chart_im_distribution;

SELECT 'table' AS component, 
       TRUE AS sort, 
       TRUE AS hover, 
       TRUE AS striped_rows;

-- Rapid fetch of pre-calculated columns
SELECT
    specialty AS "Specialty",
    domain AS "Domain",
    fac_serv AS "Facility Services",
    off_serv AS "Office Services",
    tot_serv AS "Total Services",
    off_pct AS "Office %",
    fac_spend_b AS "Facility Spend ($B)",
    off_spend_b AS "Office Spend ($B)",
    fac_cost_per_serv AS "Facility $/Service",
    off_cost_per_serv AS "Office $/Service"
FROM summary_table_im_site_mix;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Condition Monitoring & Intensity Proof' AS title,
    '' AS contents;


SELECT 'text' AS component, 
       narrative_text AS contents 
FROM summary_monitoring_narrative 
WHERE id = 1;

SELECT 'chart' AS component,
    'Condition Monitoring Intensity (Services per Beneficiary)' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels;


SELECT
    disease_state AS label,
    intensity_value AS value
FROM summary_chart_monitoring_intensity;

SELECT 'table' AS component, 
       TRUE AS sort, 
       TRUE AS hover, 
       TRUE AS striped_rows;


SELECT
    specialty_name AS "Specialty",
    mon_vol AS "Monitoring Volume",
    tot_vol AS "Total Volume",
    mon_pct AS "Monitoring %",
    mon_spend_m AS "Monitoring Spend ($M)",
    tot_spend_m AS "Total Spend ($M)"
FROM summary_table_monitoring_intensity;


SELECT 'divider' AS component;

SELECT 'text' AS component,
    'High-Cost Part B Drug Drivers & Supply Velocity' AS title,
    '' AS contents;

-- Immediate load: pulls a single static string
SELECT 'text' AS component, 
       narrative_text AS contents 
FROM summary_drug_supply_narrative 
WHERE id = 1;



SELECT 'text' AS component, 
       'Pareto Insight: Drug Spend Concentration' AS title,
       insight_text AS contents 
FROM summary_pareto_drug_insight 
WHERE id = 1;

SELECT 'chart' AS component,
    'Pareto Analysis of Part B Drug Spend (General Medicine)' AS title,
    'line' AS type,          -- Force the line type
    '#007bff' AS color,      -- Explicitly set the Medigy Blue color
    TRUE AS labels,
    0 AS ymin,               -- Start at 0 for Pareto accuracy
    100 AS ymax,             -- End at 100%
    'Cumulative Spend Share (%)' AS ytitle,
    'Drug (HCPCS Code)' AS xtitle;

-- This SELECT must return rows in the exact order of the line
SELECT
    'Cumulative Share' AS series, -- This MUST be the same for all 15 rows
    hcpcs_code AS x,
    procedure_desc AS label,
    cumulative_share_pct AS value
FROM summary_chart_drug_pareto
ORDER BY rn ASC; -- Critical: The line needs points in order to connect them



SELECT
    'Cumulative Share %' AS series,
    hcpcs_code AS x,
    procedure_desc AS label,
    cumulative_share_pct AS value
FROM summary_drug_pareto_series
ORDER BY rn;

SELECT 'table' AS component, 
       TRUE AS sort, 
       TRUE AS hover, 
       TRUE AS striped_rows;


SELECT
    rn AS "Rank",
    hcpcs_code AS "HCPCS",
    drug_name AS "Drug Name",
    spend_m AS "Drug Spend ($M)",
    cumulative_share_pct AS "Cumulative Share (%)"
FROM summary_table_drug_pareto
ORDER BY rn;

SELECT 'chart' AS component,
    'Supply Refill Velocity Leaders' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Units per Patient' AS xtitle;


SELECT
    display_label AS label,
    velocity_value AS value
FROM summary_chart_refill_velocity;

SELECT 'divider' AS component;



SELECT 'text' AS component, 
       'Clinical Dominance & Procedure Concentration' AS title,
       narrative_text AS contents 
FROM summary_clinical_dominance_narrative 
WHERE id = 1;

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



SELECT 'table' AS component, 
       TRUE AS sort, 
       TRUE AS hover, 
       TRUE AS striped_rows;


SELECT
    specialty_name AS "Specialty",
    hcpcs_code AS "HCPCS",
    procedure_description AS "Procedure",
    CAST(total_services AS INT) AS "Services",
    CAST(total_benes AS INT) AS "Beneficiaries",
    ROUND(pct_of_national_volume, 1) AS "Market Share (%)",
    dominance_rank AS "Dominance Rank"
FROM specialty_market_concentration
WHERE pct_of_national_volume IS NOT NULL
ORDER BY pct_of_national_volume DESC
LIMIT 25;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Geographic Concentration & Strategic Interaction Models' AS title,
    '' AS contents;

-- Instantaneous load: pulls one pre-calculated string
SELECT 'text' AS component, 
       narrative_text AS contents 
FROM summary_geo_opportunity_narrative 
WHERE id = 1;

SELECT 'table' AS component, TRUE AS hover, TRUE AS striped_rows;

SELECT
    state_abbr AS "State",
    formatted_spend AS "Total Spend",
    formatted_volume AS "Patient Volume",
    formatted_spp AS "Spend Per Patient"
FROM summary_table_top_states
ORDER BY spend_rank;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Interaction Intensity & Clinical Model Fit' AS title,
    'Understanding interaction frequency and model fit is critical for business model selection.' AS contents;

SELECT 'text' AS component, 
       narrative_text AS contents 
FROM summary_business_model_narrative 
WHERE id = 1;

SELECT 'divider' AS component;

SELECT 'chart' AS component,
    'Geographic Spend Concentration' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Total Spend ($B)' AS xtitle;

SELECT
    state_label AS label,
    spend_billions AS value
FROM summary_chart_geo_spend
ORDER BY rank_order;

SELECT 'table' AS component, 
       TRUE AS sort, 
       TRUE AS hover, 
       TRUE AS striped_rows;


SELECT
    state_abbr AS "State",
    patient_vol AS "Patients",
    total_spend_b AS "Total Spend ($B)",
    gpci_spend_b AS "GPCI-Adj Spend ($B)",
    spend_per_patient AS "$ / Patient"
FROM summary_table_geo_spend_top20
ORDER BY rank_order;

SELECT 'chart' AS component,
    'Strategic Interaction Models' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Interaction Ratio' AS ytitle;

SELECT
    disease_label AS label,
    interaction_value AS value
FROM summary_chart_strategic_models
ORDER BY rank_order;

SELECT 'divider' AS component;

SELECT 'text' AS component, 
       'Market Structure Summary: The Bifurcated Opportunity' AS title,
       narrative_text AS contents 
FROM summary_market_structure_narrative 
WHERE id = 1;

SELECT 'divider' AS component;

SELECT 'text' AS component,
    'Clinical Gatekeepers & Market Dominance' AS title,
    '' AS contents;


SELECT 'text' AS component, 
       'Gatekeeper dynamics show concentrated clinical control and uneven opportunity distribution' AS title,
       narrative_text AS contents 
FROM summary_gatekeeper_narrative 
WHERE id = 1;

SELECT 'chart' AS component,
    'Market Opportunity Analysis (Gatekeeper Reach by Disease)' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Specialized Patient Reach' AS xtitle;


SELECT
    disease_label AS label,
    total_reach AS value
FROM summary_chart_gatekeeper_reach
ORDER BY rank_order;


SELECT 'chart' AS component,
    'Gatekeeper Market Dominance' AS title,
    'bar' AS type,
    TRUE AS horizontal,
    TRUE AS labels,
    'Market Share (%)' AS ytitle;

SELECT
    display_label AS label,
    share_value AS value
FROM summary_chart_gatekeeper_dominance
ORDER BY rank_order;

SELECT 'table' AS component, 
       TRUE AS sort, 
       TRUE AS hover, 
       TRUE AS striped_rows;

-- This query now performs an "Index Scan," which is lightning fast
SELECT
    disease_state AS "Disease State",
    specialty_name AS "Gatekeeper",
    ROUND(market_share_percentage, 1) AS "Share (%)",
    CAST(specialized_patient_reach AS INT) AS "Patient Reach",
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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'hero' AS component,
    'Data Dictionary & Pipeline Reference' AS title,
    'Schema reference for the Medigy Disease State Database (MDSD).' AS description,
    'gray' AS color;

-- 2. DATA SOURCES (External)
SELECT 'text' AS component, '1. External Data Sources' AS contents_md;
SELECT 'list' AS component;
SELECT 
    title,
    description,
    link,
    'external-link' AS icon,
    'blue' AS color
FROM data_provenance 
WHERE object_type = 'external_source';


SELECT 'title' AS component, 'Schema Data Dictionary' AS contents;

SELECT 'big_number' AS component, 1 AS columns;

SELECT 
    'Total Objects' AS title, 
    COUNT(*) AS value, 
    'database' AS icon, 
    'blue' AS color 
FROM data_tables_derived;

-- 3. MASTER TABLES (Dimensions & Reference)
SELECT 'text' AS component, 'Master & Reference Tables' AS contents_md;
SELECT 'table' AS component, TRUE AS hover, TRUE AS striped_rows;
SELECT 
    name AS "Table Name"   
FROM sqlite_schema s
WHERE (name LIKE 'dim_%' OR name LIKE 'uniform_resource_ref_%')  
ORDER BY name;


-- --- CONFIGURATION FOR TABLE 1 ---
SET max_per_page_obj = 10;
SET count_obj = (SELECT COUNT(*) FROM data_tables_derived);
SET pages_obj = (CAST($count_obj AS INT) / $max_per_page_obj) + (CASE WHEN ($count_obj % $max_per_page_obj) = 0 THEN 0 ELSE 1 END);
SET current_page_obj = COALESCE(CAST($page_obj AS INT), 1);

-- --- RENDER TABLE 1 ---
SELECT 'table' AS component, 
       TRUE AS sort, TRUE AS search, TRUE AS markdown,
       'Derived Objects Inventory' AS title;

SELECT 
    object_name,   
    object_type,
    category
FROM data_tables_derived
ORDER BY category, object_name
LIMIT $max_per_page_obj
OFFSET ($current_page_obj - 1) * $max_per_page_obj;

-- --- RENDER PAGINATION 1 ---
SELECT 'pagination' AS component,
    ($current_page_obj = 1) AS previous_disabled,
    ($current_page_obj = $pages_obj) AS next_disabled,
    sqlpage.link(sqlpage.path(), json_object('page_obj', $current_page_obj - 1, 'page_idx', $page_idx)) AS previous_link,
    sqlpage.link(sqlpage.path(), json_object('page_obj', $current_page_obj + 1, 'page_idx', $page_idx)) AS next_link;

WITH RECURSIVE page_numbers AS (
    SELECT 1 AS n UNION ALL SELECT n + 1 FROM page_numbers WHERE n < $pages_obj
)
SELECT n AS contents, 
       sqlpage.link(sqlpage.path(), json_object('page_obj', n, 'page_idx', $page_idx)) AS link,
       (n = $current_page_obj) AS active FROM page_numbers;

-- 6. PERFORMANCE INDEXES
SELECT 'text' AS component, 'Query Performance Indexes' AS contents_md;

-- --- CONFIGURATION FOR TABLE 2 ---
SET max_per_page_idx = 10;
SET count_idx = (SELECT COUNT(*) FROM data_dictionary_indexes);
SET pages_idx = (CAST($count_idx AS INT) / $max_per_page_idx) + (CASE WHEN ($count_idx % $max_per_page_idx) = 0 THEN 0 ELSE 1 END);
SET current_page_idx = COALESCE(CAST($page_idx AS INT), 1);


-- --- RENDER TABLE 2 ---
SELECT 'table' AS component, TRUE AS hover, TRUE AS striped_rows;

SELECT 
    index_name AS "Index Name",
    table_name AS "Target Table",
    description AS "Description"
FROM data_dictionary_indexes
LIMIT $max_per_page_idx
OFFSET ($current_page_idx - 1) * $max_per_page_idx;

-- --- RENDER PAGINATION 2 ---
SELECT 'pagination' AS component,
    ($current_page_idx = 1) AS previous_disabled,
    ($current_page_idx = $pages_idx) AS next_disabled,
    sqlpage.link(sqlpage.path(), json_object('page_idx', $current_page_idx - 1, 'page_obj', $page_obj)) AS previous_link,
    sqlpage.link(sqlpage.path(), json_object('page_idx', $current_page_idx + 1, 'page_obj', $page_obj)) AS next_link;

WITH RECURSIVE idx_page_numbers AS (
    SELECT 1 AS n UNION ALL SELECT n + 1 FROM idx_page_numbers WHERE n < $pages_idx
)
SELECT n AS contents, 
       sqlpage.link(sqlpage.path(), json_object('page_idx', n, 'page_obj', $page_obj)) AS link,
       (n = $current_page_idx) AS active FROM idx_page_numbers;
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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;

SELECT 'table' AS component, 'Mapped Clusters' AS title, true AS sort, true AS search;
SELECT distinct disease_state AS Cluster FROM dim_diagnosis WHERE disease_state != 'Other Chronic / Clinical';
```


## COPD Executive Intelligence Hub

```sql mmi/copd-hub.sql { route: { caption: "COPD Intelligence Hub" } }
-- @route.description "Consolidated COPD evidence views and charts "

-- 1. Shell and Navigation
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
       '{"link":"/mmi/copd-hub.sql","title":"COPD Intelligence Hub"}' AS menu_item,
       '{"link":"/mmi/data-dictionary.sql","title":"Data Dictionary"}' AS menu_item;

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back' AS title, '../' AS link, 'chevron-left' AS icon, 'outline-secondary' AS outline;
-- 2. STRATEGIC ANCHOR
SELECT 'hero' AS component,
    'COPD Executive Intelligence Hub' AS title,
    'A unified multi-layer model of Medicare spend across Diagnostics, Clinical Visits, and DME.' AS description,
    'teal' AS color;

-- 3. NATIONAL STRATEGIC STACK (The "Operational Truth™")
SELECT 'big_number' AS component, 'National Market Totals' AS title;
SELECT 'Total Medicare Allowed' AS title, total_medicare_allowed AS value, '$' AS prefix, 'success' AS color, 'PFT + E&M + Oxygen' AS description FROM copd_national_market_grand_total;
SELECT 'All-Payer TAM (High)' AS title, all_payer_high AS value, '$' AS prefix, 'blue' AS color, 'Estimated 5x Medicare' AS description FROM copd_national_market_grand_total;
SELECT 'System-Wide Markup' AS title, blended_markup_x AS value, 'x' AS suffix, 'warning' AS color, 'Billing friction ratio' AS description FROM copd_national_market_grand_total;
SELECT 'Unique Patients' AS title, approx_unique_beneficiaries AS value, 'indigo' AS color, 'Est. unique beneficiaries' AS description FROM pft_national_beneficiary_summary;

-- 4. THE PATIENT JOURNEY & REVENUE FUNNEL
SELECT 'divider' AS component, 'Patient Journey & Economics' AS label;

SELECT 'steps' AS component, 'The COPD Clinical Pathway' AS title, 6 AS width;
SELECT 'Screening' AS title, 'Voice-based screen' AS description, 'phone' AS icon, 'completed' AS status;
SELECT 'Diagnostics' AS title, 'PFT Confirmation' AS description, 'activity' AS icon, 'active' AS status;
SELECT 'Management' AS title, 'E&M Follow-up' AS description, 'building-hospital' AS icon;
SELECT 'DME Support' AS title, 'Oxygen Rental' AS description, 'droplet' AS icon;

SELECT 'card' AS component, 3 AS columns, 6 AS width;
SELECT 'Visit-to-Test Ratio' AS title, visit_to_test_ratio || 'x' AS description, 'Est. COPD visits per PFT' AS footer, 'activity' AS icon, 'orange' AS color FROM copd_diagnosis_funnel_gap;
SELECT 'Untested Gap' AS title, pct_never_tested || '%' AS description, printf('%,.0f', never_tested_count) || ' patients' AS footer, 'users-minus' AS icon, 'red' AS color FROM copd_diagnosis_funnel_gap;
SELECT '36-Mo Patient LTV' AS title, '$' || printf('%,.0f', ltv_36_months_with_o2) AS description, 'Including O2 DME' AS footer, 'trending-up' AS icon, 'teal' AS color FROM copd_patient_36mo_ltv_model;

-- 5. DIAGNOSTIC & COMPETITIVE ANALYSIS
SELECT 'divider' AS component, 'Diagnostic Domain (PFT)' AS label;

SELECT 'chart' AS component, 'PFT Volume by Setting' AS title, 'donut' AS type, 4 AS width;
SELECT setting AS label, total_services AS value FROM pft_national_setting_distribution;

SELECT 'chart' AS component, 'Pricing: Lab vs. Office vs. Voice' AS title, 'bar' AS type, 8 AS width;
SELECT diagnostic_method AS label, avg_medicare_allowed AS value, 
       CASE WHEN diagnostic_method LIKE '%Voice%' THEN 'teal' ELSE 'blue' END AS color 
FROM pft_competitive_pricing_benchmark;

SELECT 'table' AS component, 'Procedure-Level Analytics' AS title, TRUE AS sort, 12 AS width;
SELECT hcpcs_cd, description, total_services AS "Volume", total_allowed AS "Total Allowed ($)", 
       avg_allowed_per_test AS "Avg Allowed ($)", markup_x || 'x' AS "Markup" 
FROM pft_national_procedure_analytics;

-- 6. CLINICAL MANAGEMENT (E&M) & DME (OXYGEN)
SELECT 'divider' AS component, 'Management & DME Infrastructure' AS label;

SELECT 'chart' AS component, 'Spend Distribution by Layer' AS title, 'donut' AS type, 5 AS width;
SELECT market_name AS label, medicare_allowed AS value FROM copd_total_market_economic_stack;

SELECT 'chart' AS component, 'Medicare Revenue Projection (36-Mo)' AS title, 'bar' AS type, 8 AS width;
SELECT period AS label, medicare_pays AS value FROM oxygen_e1392_amortization_model;

-- 7. GEOGRAPHIC INTELLIGENCE & TIERING
SELECT 'divider' AS component, 'Geographic Burden & Priority Markets' AS label;

SELECT 'chart' AS component, 'Top 10 High-Burden States (Total Allowed $)' AS title, 'bar' AS type, 8 AS width;
SELECT state AS label, (pft_allowed + em_allowed_all_cond + o2_allowed) AS value 
FROM copd_state_composite_market_tiering ORDER BY composite_rank_sum ASC LIMIT 10;

SELECT 'list' AS component, 'Tier 1 Priority States' AS title, 4 AS width;
-- SELECT state AS title, composite_tier AS description, 'map-pin' AS icon, 'teal' AS color 
-- FROM copd_state_composite_market_tiering WHERE composite_tier = 'TIER 1' LIMIT 5;
SELECT 
    state AS title,
    '$' || printf('%,.0f', total_allowed) AS description,
    'map-pin' AS icon,
    'teal' AS color
FROM pft_state_top_market_summary
ORDER BY total_allowed DESC
LIMIT 5;

-- 8. INTEGRATED ECONOMIC STACK (The Final Ledger)
SELECT 'divider' AS component, 'Economic Layer Breakdown' AS label;

SELECT 'chart' AS component, 'Cohort Revenue Streams (10k Patients)' AS title, 'donut' AS type, 4 AS width;
SELECT 'PFT' AS label, total_pft_revenue AS value FROM copd_cohort_36mo_economic_projection
UNION ALL SELECT 'E&M', total_annual_em_revenue FROM copd_cohort_36mo_economic_projection
UNION ALL SELECT 'Oxygen', total_o2_36mo_revenue FROM copd_cohort_36mo_economic_projection;

SELECT 'table' AS component, 'The Economic Stack Detail' AS title, 8 AS width;
SELECT market_name AS "Layer", medicare_allowed AS "Allowed ($)", markup_x || 'x' AS "Markup", 
       billing_friction AS "Friction ($)", all_payer_high AS "TAM High ($)" 
FROM copd_total_market_economic_stack;

-- 9. EXECUTIVE FINDINGS REGISTRY
SELECT 'divider' AS component, 'Executive Key Findings Registry' AS label;
SELECT 'timeline' AS component, 'Operational Truth™ Findings' AS title;
SELECT metric AS title, formatted AS description, 'circle-check' AS icon FROM pft_national_key_findings_summary;
```
