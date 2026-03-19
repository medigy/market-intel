SELECT
    'shell' AS component,
    COALESCE(NULLIF($specialty, ''), 'Procedure Drilldown') AS title,
    'list-details' AS icon,
    'fluid' AS layout,
    TRUE AS sidebar,
    'index.sql' AS link,
    '{"title":"Overview","link":"index.sql","icon":"home"}' AS menu_item,
    '{"title":"Specialties","link":"specialties.sql","icon":"activity"}' AS menu_item,
    '{"title":"Procedures","link":"procedures.sql","icon":"list-details"}' AS menu_item,
    '{"title":"Conditions","link":"conditions.sql","icon":"heartbeat"}' AS menu_item,
    '{"title":"Classification QA","link":"classification.sql","icon":"binary-tree-2"}' AS menu_item,
    'Use the specialty filter to focus the page; overview mode shows the top 3 procedures per specialty.' AS footer;

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
    'specialty' AS name,
    'select' AS type,
    'Specialty proxy' AS label,
    'All specialty proxies' AS empty_option,
    TRUE AS searchable,
    json_group_array(
        json_object(
            'label', specialty_proxy,
            'value', specialty_proxy,
            'selected', specialty_proxy = $specialty
        )
    ) AS options
FROM (
    SELECT specialty_proxy
    FROM procedure_volume_by_specialty
    ORDER BY total_services DESC
);

SELECT
    'tab' AS component,
    TRUE AS center;
SELECT
    'Executive' AS title,
    COALESCE(NULLIF($view, ''), 'executive') = 'executive' AS active,
    'chart-bar' AS icon,
    'KPI and chart summary' AS description,
    'procedures.sql?view=executive&specialty=' || replace(replace(replace(COALESCE($specialty, ''), ' ', '%20'), '/', '%2F'), '&', '%26') AS link;
SELECT
    'Analyst' AS title,
    COALESCE(NULLIF($view, ''), 'executive') = 'analyst' AS active,
    'table' AS icon,
    'Detailed ranked procedure list' AS description,
    'procedures.sql?view=analyst&specialty=' || replace(replace(replace(COALESCE($specialty, ''), ' ', '%20'), '/', '%2F'), '&', '%26') AS link;

SELECT
    'card' AS component,
    4 AS columns,
    'Procedure summary' AS title,
    'Overview mode shows top 3 procedures per specialty. A selected specialty shows all ranked procedures for that proxy.' AS description
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
SELECT
    'Scope' AS title,
    '**' || COALESCE(NULLIF($specialty, ''), 'All specialties') || '**' AS description_md,
    'activity' AS icon,
    'blue' AS color,
    CASE
        WHEN COALESCE($specialty, '') = '' THEN 'Showing top 3 procedures per specialty.'
        ELSE 'Showing all ranked procedures for the selected specialty.'
    END AS footer
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
WITH filtered AS (
    SELECT *
    FROM top_procedures_per_specialty
    WHERE (
        COALESCE($specialty, '') = ''
        AND procedure_rank <= 3
    ) OR specialty_proxy = $specialty
)
SELECT
    'Procedure rows' AS title,
    printf('**%d** records', COUNT(*)) AS description_md,
    'list-details' AS icon,
    'indigo' AS color
FROM filtered
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
WITH filtered AS (
    SELECT *
    FROM top_procedures_per_specialty
    WHERE (
        COALESCE($specialty, '') = ''
        AND procedure_rank <= 3
    ) OR specialty_proxy = $specialty
)
SELECT
    'Total services' AS title,
    CASE
        WHEN SUM(total_services) >= 1000000 THEN printf('**%.1fM** services', SUM(total_services) / 1000000.0)
        WHEN SUM(total_services) >= 1000 THEN printf('**%.1fK** services', SUM(total_services) / 1000.0)
        ELSE printf('**%.0f** services', SUM(total_services))
    END AS description_md,
    'stethoscope' AS icon,
    'green' AS color
FROM filtered
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
WITH filtered AS (
    SELECT *
    FROM top_procedures_per_specialty
    WHERE (
        COALESCE($specialty, '') = ''
        AND procedure_rank <= 3
    ) OR specialty_proxy = $specialty
)
SELECT
    'Estimated allowed amount' AS title,
    CASE
        WHEN SUM(estimated_total_allowed_amount) >= 1000000000 THEN printf('**$%.1fB** estimated', SUM(estimated_total_allowed_amount) / 1000000000.0)
        WHEN SUM(estimated_total_allowed_amount) >= 1000000 THEN printf('**$%.1fM** estimated', SUM(estimated_total_allowed_amount) / 1000000.0)
        ELSE printf('**$%.0f** estimated', SUM(estimated_total_allowed_amount))
    END AS description_md,
    'currency-dollar' AS icon,
    'orange' AS color
FROM filtered
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';

SELECT
    'chart' AS component,
    'bar' AS type,
    COALESCE('Top procedures for ' || NULLIF($specialty, ''), 'Top procedures across specialty leaders') AS title,
    TRUE AS horizontal,
    TRUE AS labels,
    TRUE AS toolbar,
    'Total services' AS ytitle
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
WITH filtered AS (
    SELECT *
    FROM top_procedures_per_specialty
    WHERE (
        COALESCE($specialty, '') = ''
        AND procedure_rank <= 3
    ) OR specialty_proxy = $specialty
)
SELECT
    hcpcs_code || ' — ' || substr(procedure_description, 1, 42) AS label,
    ROUND(total_services, 2) AS value
FROM filtered
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive'
ORDER BY total_services DESC
LIMIT 15;

SELECT
    'chart' AS component,
    'bar' AS type,
    'Estimated allowed amount by procedure' AS title,
    TRUE AS horizontal,
    TRUE AS labels,
    TRUE AS toolbar,
    'Estimated allowed amount' AS ytitle
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
WITH filtered AS (
    SELECT *
    FROM top_procedures_per_specialty
    WHERE (
        COALESCE($specialty, '') = ''
        AND procedure_rank <= 3
    ) OR specialty_proxy = $specialty
)
SELECT
    hcpcs_code || ' — ' || substr(procedure_description, 1, 42) AS label,
    ROUND(estimated_total_allowed_amount, 2) AS value
FROM filtered
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive'
ORDER BY estimated_total_allowed_amount DESC
LIMIT 15;

SELECT
    'table' AS component,
    'Procedure' AS markdown,
    TRUE AS sort,
    TRUE AS search,
    TRUE AS striped_rows,
    TRUE AS hover,
    'Procedure ranking from top_procedures_per_specialty.' AS description
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'analyst';
WITH filtered AS (
    SELECT *
    FROM top_procedures_per_specialty
    WHERE (
        COALESCE($specialty, '') = ''
        AND procedure_rank <= 3
    ) OR specialty_proxy = $specialty
)
SELECT
    specialty_proxy AS Specialty,
    procedure_rank AS Rank,
    '[' || hcpcs_code || '](classification.sql)' AS Procedure,
    procedure_description AS Description,
    classification_rule AS "Classification rule",
    ROUND(total_services, 2) AS "Total services",
    ROUND(total_beneficiaries, 2) AS Beneficiaries,
    ROUND(estimated_total_allowed_amount, 2) AS "Estimated allowed amount"
FROM filtered
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'analyst'
ORDER BY total_services DESC, specialty_proxy, procedure_rank;