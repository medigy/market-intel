SELECT
    'shell' AS component,
    'Medicare DSD Evidence Dashboard' AS title,
    'stethoscope' AS icon,
    'fluid' AS layout,
    TRUE AS sidebar,
    'index.sql' AS link,
    '{"title":"Overview","link":"index.sql","icon":"home"}' AS menu_item,
    '{"title":"Specialties","link":"specialties.sql","icon":"activity"}' AS menu_item,
    '{"title":"Procedures","link":"procedures.sql","icon":"list-details"}' AS menu_item,
    '{"title":"Conditions","link":"conditions.sql","icon":"heartbeat"}' AS menu_item,
    '{"title":"Classification QA","link":"classification.sql","icon":"binary-tree-2"}' AS menu_item,
    'Dashboard generated from heuristic specialty, condition, economic intensity, and opportunity views.' AS footer;

SELECT
    'card' AS component,
    4 AS columns,
    'Dashboard sections' AS title,
    'Use these pages to explore the derived Medicare views.' AS description;
SELECT
    'Specialty performance' AS title,
    'Services, beneficiaries, and economic intensity by specialty proxy.' AS description,
    'activity' AS icon,
    'blue' AS color,
    'specialties.sql?view=executive' AS link;
SELECT
    'Procedure drilldown' AS title,
    'Top HCPCS/CPT procedures by specialty, with classification rule context.' AS description,
    'list-details' AS icon,
    'indigo' AS color,
    'procedures.sql?view=executive' AS link;
SELECT
    'Condition opportunities' AS title,
    'Opportunity scoring using ICD breadth and beneficiary share proxies.' AS description,
    'heartbeat' AS icon,
    'red' AS color,
    'conditions.sql?view=executive' AS link;
SELECT
    'Classification QA' AS title,
    'Inspect mapping coverage and review remaining unclassified codes.' AS description,
    'binary-tree-2' AS icon,
    'orange' AS color,
    'classification.sql' AS link;

SELECT
    'card' AS component,
    4 AS columns,
    'Key indicators' AS title,
    'Executive snapshot from the national-level derived views.' AS description;
SELECT
    'National services' AS title,
    printf('**%.1fM** total services', SUM(total_services) / 1000000.0) AS description_md,
    'activity' AS icon,
    'blue' AS color
FROM procedure_volume_by_specialty;
SELECT
    'Estimated allowed amount' AS title,
    printf('**$%.1fB** total allowed', SUM(estimated_total_allowed_amount) / 1000000000.0) AS description_md,
    'currency-dollar' AS icon,
    'green' AS color
FROM procedure_volume_by_specialty;
SELECT
    'Top specialty proxy' AS title,
    '**' || specialty_proxy || '**' AS description_md,
    'stethoscope' AS icon,
    'indigo' AS color,
    printf('%.1fM services', total_services / 1000000.0) AS footer
FROM procedure_volume_by_specialty
ORDER BY total_services DESC
LIMIT 1;
SELECT
    'Highest opportunity' AS title,
    '**' || condition_group || '**' AS description_md,
    'heartbeat' AS icon,
    'red' AS color,
    printf('Opportunity score %.2f', opportunity_score) AS footer
FROM opportunity_scoring_view
ORDER BY opportunity_score DESC
LIMIT 1;

SELECT
    'chart' AS component,
    'bar' AS type,
    'Top specialty proxies by total services' AS title,
    TRUE AS horizontal,
    TRUE AS labels,
    TRUE AS toolbar,
    'Total services' AS ytitle;
SELECT
    specialty_proxy AS label,
    ROUND(total_services, 2) AS value
FROM procedure_volume_by_specialty
ORDER BY total_services DESC
LIMIT 10;

SELECT
    'chart' AS component,
    'bar' AS type,
    'Top opportunity scores by condition group' AS title,
    TRUE AS horizontal,
    TRUE AS labels,
    TRUE AS toolbar,
    'Opportunity score' AS ytitle;
SELECT
    condition_group AS label,
    ROUND(opportunity_score, 2) AS value
FROM opportunity_scoring_view
ORDER BY opportunity_score DESC
LIMIT 8;

SELECT
    'table' AS component,
    'Specialty' AS markdown,
    'Procedures' AS markdown,
    TRUE AS sort,
    TRUE AS search,
    TRUE AS striped_rows,
    'Highest-volume specialty proxies with direct drilldowns.' AS description;
SELECT
    '[' || specialty_proxy || '](specialties.sql?view=executive#specialty-table)' AS Specialty,
    total_services AS "Total services",
    total_beneficiaries AS Beneficiaries,
    ROUND(services_per_beneficiary, 4) AS "Services / beneficiary",
    estimated_total_allowed_amount AS "Estimated allowed amount",
    '[' || 'View procedures' || '](procedures.sql?view=executive&specialty=' || replace(replace(specialty_proxy, ' ', '%20'), '/', '%2F') || ')' AS Procedures
FROM procedure_volume_by_specialty
ORDER BY total_services DESC
LIMIT 12;