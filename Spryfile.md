---
sqlpage-conf:
  database_url: sqlite:./resource-surveillance.sqlite.db
  web_root: "./dev-src.auto"
  allow_exec: false
  port: 8080
---

# Medicare DSD Evidence SQLPage Application

This Spry playbook embeds the current Medicare dashboard SQLPage pages so they
can be generated from a single markdown source.

```bash prepare-db --descr "Delete and recreate the SQLite database used by SQLPage"
#!/usr/bin/env -S bash
rm -f medicare.sqlite.db                 
surveilr ingest files -r medicare-ds/ 
surveilr orchestrate transform-csv
surveilr shell --engine rusqlite business_question_views.sql -d resource-surveillance.sqlite.db
```

## Spry Axiom configuration

`code DEFAULTS` is a special directive use by Spry's Axiom library to supply
default flags to specific code blocks like `sql`, `text`, etc. allowing them to
be interpolatable (`${...}`) and injectable (using `PARTIAL`s) by default
instead of having to pass `--interpolate` and `--injectable` into each code
cell. 💡 `code DEFAULTS` is necessary in Spry SQLPage playbooks to tell Axiom
how to treat `sql` code fenced blocks.

```code DEFAULTS
sql * --interpolate --injectable
```

## SQLPage Dev / Watch mode

While you're developing, Spry's `dev-src.auto` generator should be used:

```bash prepare-sqlpage-dev --descr "Generate the dev-src.auto directory to work in SQLPage dev mode"
spry sp spc --fs dev-src.auto --destroy-first --conf sqlpage/sqlpage.json
```

```bash clean --descr "Clean up the project directory's generated artifacts"
rm -rf dev-src.auto
```

In development mode, here’s the `--watch` convenience you can use so that
whenever you update `Spryfile.md`, it regenerates the SQLPage `dev-src.auto`,
which is then picked up automatically by the SQLPage server:

```bash
spry sp spc --fs dev-src.auto --destroy-first --conf sqlpage/sqlpage.json --watch --with-sqlpage
```

- `--watch` turns on watching all `--md` files passed in (defaults to
  `Spryfile.md`)
- `--with-sqlpage` starts and stops SQLPage after each build

Restarting SQLPage after each re-generation of dev-src.auto is **not**
necessary, so you can also use `--watch` without `--with-sqlpage` in one
terminal window while keeping the SQLPage server running in another terminal
window.

If you're running SQLPage in another terminal window, use:

```bash
spry sp spc --fs dev-src.auto --destroy-first --conf sqlpage/sqlpage.json --watch
```

## SQLPage single database deployment mode

After development is complete, the `dev-src.auto` can be removed and
single-database deployment can be used:

```bash deploy -C --descr "Generate sqlpage_files table upsert SQL and push them to SQLite"
rm -rf dev-src.auto
spry sp spc --package --conf sqlpage/sqlpage.json | sqlite3 resource-surveillance.sqlite.db
```

```sql PARTIAL global-layout.sql --inject **/*
-- BEGIN: PARTIAL global-layout.sql
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


SET resource_json = sqlpage.read_file_as_text('spry.d/auto/resource/${path}.auto.json');
SET page_title  = json_extract($resource_json, '$.route.caption');
SET page_path = json_extract($resource_json, '$.route.path');


-- END: PARTIAL global-layout.sql
```

## Overview page

```sql index.sql { route: { caption: "Overview", description: "Executive overview of specialty, procedure, condition, and classification views." } }


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
```

## Specialties page

```sql specialties.sql { route: { caption: "Specialties", description: "Specialty proxy utilization and economic intensity dashboard." } }

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
```

## Procedures page

```sql procedures.sql { route: { caption: "Procedures", description: "Procedure drilldown by specialty proxy with classification context." } }

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
```

## Conditions page

```sql conditions.sql { route: { caption: "Conditions", description: "Opportunity scoring dashboard and ICD mapping drilldown." } }

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
```

## Classification page

```sql classification.sql { route: { caption: "Classification QA", description: "Heuristic classification coverage and residual unclassified procedures." } }

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
```
