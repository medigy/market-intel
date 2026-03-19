---
sqlpage-conf:
  database_url: "sqlite://resource-surveillance.sqlite.db?mode=rwc"
  web_root: "./dev-src.auto"
  allow_exec: true
  port: 9227
---

# CMS Workflow Task
```bash prepare-db-deploy-server --descr "Ingest Medicare raw files, build normalized analytics tables, and package SQLPage UI."
#!/bin/bash
set -euo pipefail

# Start from a clean database to avoid previously ingested malformed resources.
rm -f resource-surveillance.sqlite.db

surveilr ingest files -r medicare-ds
surveilr orchestrate transform-csv
surveilr shell sql/medicare_business_views.sql 
spry sp spc --package --conf sqlpage/sqlpage.json -m cms_workflow.md | sqlite3 resource-surveillance.sqlite.db

echo "Medicare patient analytics database and SQLPage UI are ready."
```

## SQLPage Layout

```sql PARTIAL global-layout.sql --inject *.sql
SELECT 'shell' AS component,
       'Medicare Business Evidence Hub' AS title,
       'stethoscope' AS icon,
       'fluid' AS layout,
       true AS fixed_top_menu,
       '/' AS link,
       '{"link":"/","title":"Home"}' AS menu_item,
       '{"link":"/procedure-volume-by-specialty.sql","title":"Procedure Volume"}' AS menu_item,
       '{"link":"/opportunity-scoring-view.sql","title":"Opportunity Scores"}' AS menu_item;
```

## Home

```sql index.sql { route: { caption: "Home" } }
-- @route.description "Business-question views over Medicare provider and HCPCS data."

SELECT 'hero' AS component,
       'Medicare Business Questions' AS title,
       'Actionable evidence views built from CMS and reference tables' AS description,
       'azure' AS color;

SELECT 'card' AS component, 'Business Views' AS title, 2 AS columns;

SELECT 'Procedure Volume by Specialty' AS title,
       '/procedure-volume-by-specialty.sql' AS link,
       'CPT/HCPCS volume aggregated at specialty level.' AS description,
       'chart-bar' AS icon,
       'teal' AS color;

SELECT 'Top Procedures per Specialty' AS title,
       '/top-procedures-per-specialty.sql' AS link,
       'Ranked HCPCS procedures within each specialty.' AS description,
       'sort-descending' AS icon,
       'indigo' AS color;

SELECT 'Condition to ICD Mapping' AS title,
       '/condition-to-icd-mapping.sql' AS link,
       'Manual prioritized disease-state to ICD mapping seed.' AS description,
       'affiliate' AS icon,
       'cyan' AS color;

SELECT 'Proxy Condition Activity' AS title,
       '/proxy-condition-activity.sql' AS link,
       'Approximate ICD-to-CPT specialty linkage activity.' AS description,
       'git-merge' AS icon,
       'blue' AS color;

SELECT 'Economic Intensity Index' AS title,
       '/economic-intensity-index.sql' AS link,
       'Derived index from services, beneficiaries, and allowed amounts.' AS description,
       'trending-up' AS icon,
       'orange' AS color;

SELECT 'Opportunity Scoring View' AS title,
       '/opportunity-scoring-view.sql' AS link,
       'Composite score: prevalence, procedure density, concentration.' AS description,
       'target-arrow' AS icon,
       'red' AS color;
```

## Procedure Volume by Specialty

```sql procedure-volume-by-specialty.sql { route: { caption: "Procedure Volume by Specialty" } }
-- @route.description "CPT/HCPCS volume aggregated by specialty."

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back to Home' AS title, '/' AS link, 'chevron-left' AS icon;

SELECT 'big_number' AS component, 3 AS columns;
SELECT 'Specialties' AS title, COUNT(*) AS value, 'users-group' AS icon, 'teal' AS color
FROM procedure_volume_by_specialty;
SELECT 'Total Services' AS title, ROUND(SUM(total_services), 0) AS value, 'activity' AS icon, 'blue' AS color
FROM procedure_volume_by_specialty;
SELECT 'Total Allowed Amount' AS title, ROUND(SUM(total_allowed_amount), 0) AS value, 'currency-dollar' AS icon, 'orange' AS color
FROM procedure_volume_by_specialty;

SELECT 'table' AS component, 'Specialty Volume' AS title, TRUE AS sort, TRUE AS search;
SELECT specialty,
       provider_count,
       ROUND(total_hcpcs_codes, 0) AS total_hcpcs_codes,
       ROUND(total_services, 0) AS total_services,
       ROUND(total_beneficiaries, 0) AS total_beneficiaries,
       ROUND(total_allowed_amount, 2) AS total_allowed_amount,
       ROUND(avg_allowed_per_service, 2) AS avg_allowed_per_service,
       ROUND(avg_services_per_provider, 2) AS avg_services_per_provider
FROM procedure_volume_by_specialty
ORDER BY total_services DESC;
```

## Top Procedures per Specialty

```sql top-procedures-per-specialty.sql { route: { caption: "Top Procedures per Specialty" } }
-- @route.description "Ranking of CPT/HCPCS procedures within each specialty."

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back to Home' AS title, '/' AS link, 'chevron-left' AS icon;

SELECT 'table' AS component, 'Ranked Procedures' AS title, TRUE AS sort, TRUE AS search;
SELECT specialty,
       procedure_rank,
       hcpcs_code,
       hcpcs_desc,
       ROUND(estimated_services, 0) AS estimated_services,
       ROUND(estimated_beneficiaries, 0) AS estimated_beneficiaries,
       ROUND(estimated_allowed_amount, 2) AS estimated_allowed_amount
FROM top_procedures_per_specialty
ORDER BY specialty, procedure_rank;
```

## Condition to ICD Mapping

```sql condition-to-icd-mapping.sql { route: { caption: "Condition to ICD Mapping" } }
-- @route.description "Prioritized disease-state mapping to ICD code sets (manual seed)."

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back to Home' AS title, '/' AS link, 'chevron-left' AS icon;

SELECT 'table' AS component, 'Condition to ICD Mapping' AS title, TRUE AS sort, TRUE AS search;
SELECT condition_name,
       priority_tier,
       prevalence_weight,
       proxy_specialty_pattern,
       icd_prefix,
       icd10_code,
       is_billable,
       description_short,
       description_long
FROM condition_to_icd_mapping
ORDER BY condition_name, icd10_code;
```

## Proxy Condition Activity

```sql proxy-condition-activity.sql { route: { caption: "Proxy Condition Activity" } }
-- @route.description "Approximate linkage between ICD groups and CPT-heavy specialties."

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back to Home' AS title, '/' AS link, 'chevron-left' AS icon;

SELECT 'table' AS component, 'Proxy Condition Activity' AS title, TRUE AS sort, TRUE AS search;
SELECT condition_name,
       priority_tier,
       ROUND(prevalence_weight, 4) AS prevalence_weight,
       mapped_icd_prefix_count,
       mapped_icd_code_count,
       linked_cpt_heavy_specialties,
       ROUND(proxy_total_services, 0) AS proxy_total_services,
       ROUND(proxy_total_beneficiaries, 0) AS proxy_total_beneficiaries,
       ROUND(proxy_total_allowed_amount, 2) AS proxy_total_allowed_amount,
       ROUND(services_per_mapped_icd, 2) AS services_per_mapped_icd
FROM proxy_condition_activity
ORDER BY proxy_total_services DESC;
```

## Economic Intensity Index

```sql economic-intensity-index.sql { route: { caption: "Economic Intensity Index" } }
-- @route.description "Derived metric combining services, beneficiaries, and allowed amount."

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back to Home' AS title, '/' AS link, 'chevron-left' AS icon;

SELECT 'table' AS component, 'Economic Intensity by Specialty' AS title, TRUE AS sort, TRUE AS search;
SELECT specialty,
       ROUND(total_services, 0) AS total_services,
       ROUND(total_beneficiaries, 0) AS total_beneficiaries,
       ROUND(total_allowed_amount, 2) AS total_allowed_amount,
       ROUND(services_score, 4) AS services_score,
       ROUND(beneficiaries_score, 4) AS beneficiaries_score,
       ROUND(allowed_amount_score, 4) AS allowed_amount_score,
       ROUND(economic_intensity_index, 6) AS economic_intensity_index
FROM economic_intensity_index
ORDER BY economic_intensity_index DESC;
```

## Opportunity Scoring

```sql opportunity-scoring-view.sql { route: { caption: "Opportunity Scoring View" } }
-- @route.description "Composite score from prevalence, procedure density, and specialty concentration."

SELECT 'button' AS component, 'start' AS justify;
SELECT 'Back to Home' AS title, '/' AS link, 'chevron-left' AS icon;

SELECT 'table' AS component, 'Condition Opportunity Scores' AS title, TRUE AS sort, TRUE AS search;
SELECT opportunity_rank,
       condition_name,
       ROUND(opportunity_score, 6) AS opportunity_score,
       ROUND(prevalence_score, 4) AS prevalence_score,
       ROUND(procedure_density_score, 4) AS procedure_density_score,
       ROUND(specialty_concentration_score, 4) AS specialty_concentration_score,
       ROUND(total_services, 0) AS total_services,
       ROUND(total_beneficiaries, 0) AS total_beneficiaries,
       linked_specialties
FROM opportunity_scoring_view
ORDER BY opportunity_rank;
```

## Spry Axiom Configuration

```code DEFAULTS
sql * --interpolate --injectable
```

## CMS Workflow Task

```bash prepare-db-deploy-server --descr "Ingest Medicare raw files, build normalized analytics tables, and package SQLPage UI."
#!/bin/bash
set -euo pipefail

rm -f resource-surveillance.sqlite.db

surveilr ingest files -r medicare-ds
echo $(surveilr orchestrate transform-csv)
sqlite3 resource-surveillance.sqlite.db < sql/medicare_business_views.sql
spry sp spc --package --conf sqlpage/sqlpage.json -m cms_workflow.md | sqlite3 resource-surveillance.sqlite.db

echo "Medicare patient analytics database and SQLPage UI are ready."
```
