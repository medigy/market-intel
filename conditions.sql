SELECT
    'shell' AS component,
    COALESCE(NULLIF($condition_group, ''), 'Condition Opportunity Dashboard') AS title,
    'heartbeat' AS icon,
    'fluid' AS layout,
    TRUE AS sidebar,
    'index.sql' AS link,
    '{"title":"Overview","link":"index.sql","icon":"home"}' AS menu_item,
    '{"title":"Specialties","link":"specialties.sql","icon":"activity"}' AS menu_item,
    '{"title":"Procedures","link":"procedures.sql","icon":"list-details"}' AS menu_item,
    '{"title":"Conditions","link":"conditions.sql","icon":"heartbeat"}' AS menu_item,
    '{"title":"Classification QA","link":"classification.sql","icon":"binary-tree-2"}' AS menu_item,
    'Opportunity scoring uses proxy prevalence until CCW/CDC prevalence inputs are loaded.' AS footer;

SELECT
    'form' AS component,
    'GET' AS method,
    TRUE AS auto_submit,
    'Filters' AS title;
SELECT
    'hidden' AS type,
    'view' AS name,
    COALESCE(NULLIF($view, ''), 'executive') AS value;
SELECT
    'condition_group' AS name,
    'select' AS type,
    'Condition group' AS label,
    'All condition groups' AS empty_option,
    TRUE AS searchable,
    json_group_array(
        json_object(
            'label', condition_group,
            'value', condition_group,
            'selected', condition_group = $condition_group
        )
    ) AS options
FROM (
    SELECT condition_group
    FROM opportunity_scoring_view
    ORDER BY opportunity_score DESC
);

SELECT
    'tab' AS component,
    TRUE AS center;
SELECT
    'Executive' AS title,
    COALESCE(NULLIF($view, ''), 'executive') = 'executive' AS active,
    'chart-bar' AS icon,
    'KPI and chart summary' AS description,
    'conditions.sql?view=executive&condition_group=' || replace(replace(replace(COALESCE($condition_group, ''), ' ', '%20'), '/', '%2F'), '&', '%26') AS link;
SELECT
    'Analyst' AS title,
    COALESCE(NULLIF($view, ''), 'executive') = 'analyst' AS active,
    'table' AS icon,
    'Detailed scoring and ICD mapping tables' AS description,
    'conditions.sql?view=analyst&condition_group=' || replace(replace(replace(COALESCE($condition_group, ''), ' ', '%20'), '/', '%2F'), '&', '%26') AS link;

SELECT
    'card' AS component,
    4 AS columns,
    'Condition summary' AS title,
    'Opportunity scoring KPIs for the selected condition scope.' AS description
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
SELECT
    'Scope' AS title,
    '**' || COALESCE(NULLIF($condition_group, ''), 'All condition groups') || '**' AS description_md,
    'heartbeat' AS icon,
    'red' AS color
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
WITH filtered AS (
    SELECT *
    FROM opportunity_scoring_view
    WHERE COALESCE($condition_group, '') = '' OR condition_group = $condition_group
)
SELECT
    'Condition groups' AS title,
    printf('**%d** scored groups', COUNT(*)) AS description_md,
    'list-details' AS icon,
    'blue' AS color
FROM filtered
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
WITH filtered AS (
    SELECT *
    FROM opportunity_scoring_view
    WHERE COALESCE($condition_group, '') = '' OR condition_group = $condition_group
)
SELECT
    'Billable ICD codes' AS title,
    printf('**%d** billable mappings', SUM(billable_icd_codes)) AS description_md,
    'binary-tree-2' AS icon,
    'indigo' AS color
FROM filtered
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
WITH filtered AS (
    SELECT *
    FROM opportunity_scoring_view
    WHERE COALESCE($condition_group, '') = '' OR condition_group = $condition_group
)
SELECT
    'Peak opportunity score' AS title,
    printf('**%.2f** max score', MAX(opportunity_score)) AS description_md,
    'target-arrow' AS icon,
    'orange' AS color
FROM filtered
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';

SELECT
    'chart' AS component,
    'bar' AS type,
    'Condition opportunity scores' AS title,
    TRUE AS horizontal,
    TRUE AS labels,
    TRUE AS toolbar,
    'Opportunity score' AS ytitle
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
SELECT
    condition_group AS label,
    ROUND(opportunity_score, 4) AS value
FROM opportunity_scoring_view
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive'
  AND (COALESCE($condition_group, '') = '' OR condition_group = $condition_group)
ORDER BY opportunity_score DESC
LIMIT CASE WHEN COALESCE($condition_group, '') = '' THEN 12 ELSE 1 END;

SELECT
    'chart' AS component,
    'bar' AS type,
    'Mapped ICD code breadth' AS title,
    TRUE AS horizontal,
    TRUE AS labels,
    TRUE AS toolbar,
    'Mapped ICD codes' AS ytitle
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
SELECT
    condition_group AS label,
    mapped_icd_codes AS value
FROM opportunity_scoring_view
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive'
  AND (COALESCE($condition_group, '') = '' OR condition_group = $condition_group)
ORDER BY mapped_icd_codes DESC
LIMIT CASE WHEN COALESCE($condition_group, '') = '' THEN 12 ELSE 1 END;

SELECT
    'table' AS component,
    'Condition' AS markdown,
    TRUE AS sort,
    TRUE AS search,
    TRUE AS striped_rows,
    TRUE AS hover,
    'Scored condition groups with drilldowns back into their ICD mappings.' AS description
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'analyst';
SELECT
    '[' || condition_group || '](conditions.sql?view=executive&condition_group=' || replace(replace(replace(condition_group, ' ', '%20'), '/', '%2F'), '&', '%26') || ')' AS Condition,
    primary_specialty_proxy AS "Primary specialty proxy",
    mapped_icd_codes AS "Mapped ICD codes",
    billable_icd_codes AS "Billable ICD codes",
    ROUND(beneficiary_share_proxy, 6) AS "Beneficiary share proxy",
    ROUND(prevalence_score, 6) AS "Prevalence score",
    ROUND(procedure_density_score, 6) AS "Procedure density score",
    ROUND(specialty_concentration_score, 6) AS "Specialty concentration score",
    ROUND(opportunity_score, 4) AS "Opportunity score"
FROM opportunity_scoring_view
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'analyst'
  AND (COALESCE($condition_group, '') = '' OR condition_group = $condition_group)
ORDER BY opportunity_score DESC;

SELECT
    'table' AS component,
    TRUE AS sort,
    TRUE AS search,
    TRUE AS striped_rows,
    TRUE AS hover,
    'ICD mappings for the selected condition group, or all mappings if no condition is selected.' AS description
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'analyst';
SELECT
    condition_group AS "Condition group",
    primary_specialty_proxy AS "Primary specialty proxy",
    icd10_code AS "ICD-10 code",
    is_billable AS Billable,
    description_short AS "Short description",
    icd_prefix_rule AS "Seed prefix"
FROM condition_to_icd_mapping
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'analyst'
  AND (COALESCE($condition_group, '') = '' OR condition_group = $condition_group)
ORDER BY condition_group, icd10_code
LIMIT CASE WHEN COALESCE($condition_group, '') = '' THEN 150 ELSE 500 END;
