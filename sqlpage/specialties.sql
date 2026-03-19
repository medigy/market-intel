SELECT
    'shell' AS component,
    COALESCE(NULLIF($specialty, ''), 'Specialty Proxy Performance') AS title,
    'activity' AS icon,
    'fluid' AS layout,
    TRUE AS sidebar,
    'index.sql' AS link,
    '{"title":"Overview","link":"index.sql","icon":"home"}' AS menu_item,
    '{"title":"Specialties","link":"specialties.sql","icon":"activity"}' AS menu_item,
    '{"title":"Procedures","link":"procedures.sql","icon":"list-details"}' AS menu_item,
    '{"title":"Conditions","link":"conditions.sql","icon":"heartbeat"}' AS menu_item,
    '{"title":"Classification QA","link":"classification.sql","icon":"binary-tree-2"}' AS menu_item,
    'Specialty proxies are heuristic buckets derived from HCPCS/CPT code patterns and descriptions.' AS footer;

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
    'Summary KPIs and charts' AS description,
    'specialties.sql?view=executive&specialty=' || replace(replace(replace(COALESCE($specialty, ''), ' ', '%20'), '/', '%2F'), '&', '%26') AS link;
SELECT
    'Analyst' AS title,
    COALESCE(NULLIF($view, ''), 'executive') = 'analyst' AS active,
    'table' AS icon,
    'Detailed specialty metrics and drilldowns' AS description,
    'specialties.sql?view=analyst&specialty=' || replace(replace(replace(COALESCE($specialty, ''), ' ', '%20'), '/', '%2F'), '&', '%26') AS link;

SELECT
    'card' AS component,
    4 AS columns,
    'Executive summary' AS title,
    'National utilization and financial scope for the selected specialty view.' AS description
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
SELECT
    'Scope' AS title,
    '**' || COALESCE(NULLIF($specialty, ''), 'All specialties') || '**' AS description_md,
    'activity' AS icon,
    'blue' AS color
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
WITH filtered AS (
    SELECT *
    FROM procedure_volume_by_specialty
    WHERE COALESCE($specialty, '') = '' OR specialty_proxy = $specialty
)
SELECT
    'Beneficiaries' AS title,
    CASE
        WHEN SUM(total_beneficiaries) >= 1000000 THEN printf('**%.1fM** beneficiaries', SUM(total_beneficiaries) / 1000000.0)
        WHEN SUM(total_beneficiaries) >= 1000 THEN printf('**%.1fK** beneficiaries', SUM(total_beneficiaries) / 1000.0)
        ELSE printf('**%.0f** beneficiaries', SUM(total_beneficiaries))
    END AS description_md,
    'users' AS icon,
    'indigo' AS color
FROM filtered
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
WITH filtered AS (
    SELECT *
    FROM procedure_volume_by_specialty
    WHERE COALESCE($specialty, '') = '' OR specialty_proxy = $specialty
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
    FROM procedure_volume_by_specialty
    WHERE COALESCE($specialty, '') = '' OR specialty_proxy = $specialty
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
    'Total services by specialty proxy' AS title,
    TRUE AS horizontal,
    TRUE AS labels,
    TRUE AS toolbar,
    'Services' AS ytitle
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
SELECT
    specialty_proxy AS label,
    ROUND(total_services, 2) AS value
FROM procedure_volume_by_specialty
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive'
  AND (COALESCE($specialty, '') = '' OR specialty_proxy = $specialty)
ORDER BY total_services DESC
LIMIT CASE WHEN COALESCE($specialty, '') = '' THEN 12 ELSE 1 END;

SELECT
    'chart' AS component,
    'bar' AS type,
    'Economic intensity index by specialty proxy' AS title,
    TRUE AS horizontal,
    TRUE AS labels,
    TRUE AS toolbar,
    'Economic intensity index' AS ytitle
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive';
SELECT
    specialty_proxy AS label,
    ROUND(economic_intensity_index, 4) AS value
FROM economic_intensity_index
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'executive'
  AND (COALESCE($specialty, '') = '' OR specialty_proxy = $specialty)
ORDER BY economic_intensity_index DESC
LIMIT CASE WHEN COALESCE($specialty, '') = '' THEN 12 ELSE 1 END;

SELECT
    'table' AS component,
    'Specialty' AS markdown,
    'Top procedures' AS markdown,
    TRUE AS sort,
    TRUE AS search,
    TRUE AS striped_rows,
    TRUE AS hover,
    'specialty-table' AS id,
    'Detailed specialty metrics joined with the economic intensity view.' AS description
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'analyst';
SELECT
    '[' || p.specialty_proxy || '](procedures.sql?view=executive&specialty=' || replace(replace(replace(p.specialty_proxy, ' ', '%20'), '/', '%2F'), '&', '%26') || ')' AS Specialty,
    p.distinct_procedure_count AS "Distinct procedures",
    p.total_rendering_providers AS "Rendering providers",
    p.total_beneficiaries AS Beneficiaries,
    ROUND(p.total_services, 2) AS "Total services",
    ROUND(p.services_per_beneficiary, 4) AS "Services / beneficiary",
    ROUND(e.economic_intensity_index, 4) AS "Economic intensity index",
    '[' || 'Top procedures' || '](procedures.sql?view=executive&specialty=' || replace(replace(replace(p.specialty_proxy, ' ', '%20'), '/', '%2F'), '&', '%26') || ')' AS "Top procedures"
FROM procedure_volume_by_specialty p
LEFT JOIN economic_intensity_index e
    ON e.specialty_proxy = p.specialty_proxy
WHERE COALESCE(NULLIF($view, ''), 'executive') = 'analyst'
  AND (COALESCE($specialty, '') = '' OR p.specialty_proxy = $specialty)
ORDER BY p.total_services DESC;