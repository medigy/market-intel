SELECT
    'shell' AS component,
    'Classification QA Dashboard' AS title,
    'binary-tree-2' AS icon,
    'fluid' AS layout,
    TRUE AS sidebar,
    'index.sql' AS link,
    '{"title":"Overview","link":"index.sql","icon":"home"}' AS menu_item,
    '{"title":"Specialties","link":"specialties.sql","icon":"activity"}' AS menu_item,
    '{"title":"Procedures","link":"procedures.sql","icon":"list-details"}' AS menu_item,
    '{"title":"Conditions","link":"conditions.sql","icon":"heartbeat"}' AS menu_item,
    '{"title":"Classification QA","link":"classification.sql","icon":"binary-tree-2"}' AS menu_item,
    'Use this page to review heuristic specialty assignment coverage and residual unclassified codes.' AS footer;

SELECT
    'chart' AS component,
    'bar' AS type,
    'Codes by specialty proxy' AS title,
    TRUE AS horizontal,
    TRUE AS labels,
    TRUE AS toolbar,
    'Distinct codes' AS ytitle;
SELECT
    specialty_proxy AS label,
    COUNT(*) AS value
FROM procedure_specialty_proxy_map
GROUP BY specialty_proxy
ORDER BY COUNT(*) DESC;

SELECT
    'table' AS component,
    TRUE AS sort,
    TRUE AS search,
    TRUE AS striped_rows,
    'Classification rule coverage across all mapped HCPCS/CPT codes.' AS description;
SELECT
    specialty_proxy AS "Specialty proxy",
    classification_rule AS "Classification rule",
    COUNT(*) AS "Distinct codes"
FROM procedure_specialty_proxy_map
GROUP BY specialty_proxy, classification_rule
ORDER BY COUNT(*) DESC, specialty_proxy;

SELECT
    'table' AS component,
    TRUE AS sort,
    TRUE AS search,
    TRUE AS striped_rows,
    TRUE AS hover,
    'Top remaining procedures in Other / Unclassified for follow-up refinement.' AS description,
    'Procedure' AS markdown;
SELECT
    '[' || hcpcs_code || '](procedures.sql)' AS Procedure,
    procedure_description AS Description,
    total_services AS "Total services",
    total_beneficiaries AS Beneficiaries,
    estimated_total_allowed_amount AS "Estimated allowed amount"
FROM top_procedures_per_specialty
WHERE specialty_proxy = 'Other / Unclassified'
ORDER BY procedure_rank
LIMIT 50;