-- Medicare business-question views
-- Data source tables are expected from `surveilr orchestrate transform-csv` as:
--   uniform_resource_cms_bygeography
--   uniform_resource_cms_provider
--   uniform_resource_ref_anes_conversion_factor
--   uniform_resource_ref_geo_adjustment
--   uniform_resource_ref_hcpcs_level_two_procedures
--   uniform_resource_ref_icd10_diagnosis
--   uniform_resource_ref_condition_icd_mapping
--   uniform_resource_ref_medicare_localities
--   uniform_resource_ref_opps_price_cap
--   uniform_resource_ref_procedure_code
--   uniform_resource_ref_rvu_qpp

DROP VIEW IF EXISTS procedure_volume_by_specialty;
CREATE VIEW procedure_volume_by_specialty AS
SELECT
  p."Rndrng_Prvdr_Type" AS specialty,
  COUNT(DISTINCT p."Rndrng_NPI") AS provider_count,
  SUM(CAST(REPLACE(COALESCE(p."Tot_HCPCS_Cds", '0'), ',', '') AS REAL)) AS total_hcpcs_codes,
  SUM(CAST(REPLACE(COALESCE(p."Tot_Srvcs", '0'), ',', '') AS REAL)) AS total_services,
  SUM(CAST(REPLACE(COALESCE(p."Tot_Benes", '0'), ',', '') AS REAL)) AS total_beneficiaries,
  SUM(CAST(REPLACE(COALESCE(p."Tot_Mdcr_Alowd_Amt", '0'), ',', '') AS REAL)) AS total_allowed_amount,
  CASE
    WHEN SUM(CAST(REPLACE(COALESCE(p."Tot_Srvcs", '0'), ',', '') AS REAL)) = 0 THEN 0
    ELSE
      SUM(CAST(REPLACE(COALESCE(p."Tot_Mdcr_Alowd_Amt", '0'), ',', '') AS REAL))
      / SUM(CAST(REPLACE(COALESCE(p."Tot_Srvcs", '0'), ',', '') AS REAL))
  END AS avg_allowed_per_service,
  CASE
    WHEN COUNT(DISTINCT p."Rndrng_NPI") = 0 THEN 0
    ELSE
      SUM(CAST(REPLACE(COALESCE(p."Tot_Srvcs", '0'), ',', '') AS REAL))
      / COUNT(DISTINCT p."Rndrng_NPI")
  END AS avg_services_per_provider
FROM uniform_resource_cms_provider p
WHERE COALESCE(TRIM(p."Rndrng_Prvdr_Type"), '') <> ''
GROUP BY p."Rndrng_Prvdr_Type";

DROP VIEW IF EXISTS top_procedures_per_specialty;
CREATE VIEW top_procedures_per_specialty AS
WITH specialty_share AS (
  SELECT
    specialty,
    total_services,
    CASE
      WHEN SUM(total_services) OVER () = 0 THEN 0
      ELSE total_services * 1.0 / SUM(total_services) OVER ()
    END AS specialty_service_share
  FROM procedure_volume_by_specialty
),
national_procedure_volume AS (
  SELECT
    g."HCPCS_Cd" AS hcpcs_code,
    MAX(g."HCPCS_Desc") AS hcpcs_desc,
    SUM(CAST(REPLACE(COALESCE(g."Tot_Srvcs", '0'), ',', '') AS REAL)) AS national_total_services,
    SUM(CAST(REPLACE(COALESCE(g."Tot_Benes", '0'), ',', '') AS REAL)) AS national_total_beneficiaries,
    AVG(CAST(REPLACE(COALESCE(g."Avg_Mdcr_Alowd_Amt", '0'), ',', '') AS REAL)) AS avg_allowed_amount
  FROM uniform_resource_cms_bygeography g
  WHERE COALESCE(g."Rndrng_Prvdr_Geo_Lvl", '') = 'National'
  GROUP BY g."HCPCS_Cd"
),
allocated AS (
  SELECT
    s.specialty,
    n.hcpcs_code,
    n.hcpcs_desc,
    n.national_total_services * s.specialty_service_share AS estimated_services,
    n.national_total_beneficiaries * s.specialty_service_share AS estimated_beneficiaries,
    (n.national_total_services * s.specialty_service_share) * COALESCE(n.avg_allowed_amount, 0) AS estimated_allowed_amount
  FROM specialty_share s
  CROSS JOIN national_procedure_volume n
),
ranked AS (
  SELECT
    a.*,
    ROW_NUMBER() OVER (
      PARTITION BY a.specialty
      ORDER BY a.estimated_services DESC, a.hcpcs_code
    ) AS procedure_rank
  FROM allocated a
)
SELECT
  specialty,
  procedure_rank,
  hcpcs_code,
  hcpcs_desc,
  estimated_services,
  estimated_beneficiaries,
  estimated_allowed_amount
FROM ranked
WHERE procedure_rank <= 25;

DROP VIEW IF EXISTS condition_to_icd_mapping;
CREATE VIEW condition_to_icd_mapping AS
SELECT
  m.condition_name,
  CAST(COALESCE(m.priority_tier, 0) AS INTEGER) AS priority_tier,
  CAST(COALESCE(m.prevalence_weight, 0) AS REAL) AS prevalence_weight,
  m.proxy_specialty_pattern,
  m.icd_prefix,
  d.icd10_code,
  d.is_billable,
  d.description_short,
  d.description_long
FROM uniform_resource_ref_condition_icd_mapping m
LEFT JOIN uniform_resource_ref_icd10_diagnosis d
  ON UPPER(COALESCE(d.icd10_code, '')) LIKE UPPER(m.icd_prefix) || '%';

DROP VIEW IF EXISTS proxy_condition_activity;
CREATE VIEW proxy_condition_activity AS
WITH specialty_activity AS (
  SELECT
    specialty,
    total_services,
    total_beneficiaries,
    total_allowed_amount,
    NTILE(4) OVER (ORDER BY total_services DESC) AS service_quartile
  FROM procedure_volume_by_specialty
),
cpt_heavy_specialty AS (
  SELECT *
  FROM specialty_activity
  WHERE service_quartile = 1
),
condition_specialty AS (
  SELECT DISTINCT
    c.condition_name,
    c.priority_tier,
    c.prevalence_weight,
    c.icd_prefix,
    c.icd10_code,
    h.specialty,
    h.total_services,
    h.total_beneficiaries,
    h.total_allowed_amount
  FROM condition_to_icd_mapping c
  JOIN cpt_heavy_specialty h
    ON LOWER(h.specialty) LIKE '%' || LOWER(c.proxy_specialty_pattern) || '%'
)
SELECT
  condition_name,
  priority_tier,
  AVG(prevalence_weight) AS prevalence_weight,
  COUNT(DISTINCT icd_prefix) AS mapped_icd_prefix_count,
  COUNT(DISTINCT icd10_code) AS mapped_icd_code_count,
  COUNT(DISTINCT specialty) AS linked_cpt_heavy_specialties,
  SUM(total_services) AS proxy_total_services,
  SUM(total_beneficiaries) AS proxy_total_beneficiaries,
  SUM(total_allowed_amount) AS proxy_total_allowed_amount,
  CASE
    WHEN COUNT(DISTINCT icd10_code) = 0 THEN 0
    ELSE SUM(total_services) * 1.0 / COUNT(DISTINCT icd10_code)
  END AS services_per_mapped_icd
FROM condition_specialty
GROUP BY condition_name, priority_tier;

DROP VIEW IF EXISTS economic_intensity_index;
CREATE VIEW economic_intensity_index AS
WITH base AS (
  SELECT
    specialty,
    total_services,
    total_beneficiaries,
    total_allowed_amount
  FROM procedure_volume_by_specialty
),
scored AS (
  SELECT
    b.*,
    CASE
      WHEN MAX(total_services) OVER () = MIN(total_services) OVER () THEN 0
      ELSE (total_services - MIN(total_services) OVER ())
           / NULLIF(MAX(total_services) OVER () - MIN(total_services) OVER (), 0)
    END AS services_score,
    CASE
      WHEN MAX(total_beneficiaries) OVER () = MIN(total_beneficiaries) OVER () THEN 0
      ELSE (total_beneficiaries - MIN(total_beneficiaries) OVER ())
           / NULLIF(MAX(total_beneficiaries) OVER () - MIN(total_beneficiaries) OVER (), 0)
    END AS beneficiaries_score,
    CASE
      WHEN MAX(total_allowed_amount) OVER () = MIN(total_allowed_amount) OVER () THEN 0
      ELSE (total_allowed_amount - MIN(total_allowed_amount) OVER ())
           / NULLIF(MAX(total_allowed_amount) OVER () - MIN(total_allowed_amount) OVER (), 0)
    END AS allowed_amount_score
  FROM base b
)
SELECT
  specialty,
  total_services,
  total_beneficiaries,
  total_allowed_amount,
  services_score,
  beneficiaries_score,
  allowed_amount_score,
  ROUND((0.40 * services_score) + (0.20 * beneficiaries_score) + (0.40 * allowed_amount_score), 6) AS economic_intensity_index
FROM scored;

DROP VIEW IF EXISTS opportunity_scoring_view;
CREATE VIEW opportunity_scoring_view AS
WITH condition_specialty AS (
  SELECT DISTINCT
    c.condition_name,
    c.prevalence_weight,
    p.specialty,
    p.total_services,
    p.total_beneficiaries
  FROM condition_to_icd_mapping c
  JOIN procedure_volume_by_specialty p
    ON LOWER(p.specialty) LIKE '%' || LOWER(c.proxy_specialty_pattern) || '%'
),
condition_rollup AS (
  SELECT
    condition_name,
    AVG(prevalence_weight) AS prevalence_score_raw,
    SUM(total_services) AS total_services,
    SUM(total_beneficiaries) AS total_beneficiaries,
    COUNT(DISTINCT specialty) AS linked_specialties
  FROM condition_specialty
  GROUP BY condition_name
),
condition_concentration AS (
  SELECT
    condition_name,
    MAX(total_services) * 1.0 / NULLIF(SUM(total_services), 0) AS specialty_concentration_raw
  FROM condition_specialty
  GROUP BY condition_name
),
combined AS (
  SELECT
    r.condition_name,
    r.prevalence_score_raw,
    CASE
      WHEN r.total_beneficiaries = 0 THEN 0
      ELSE r.total_services * 1.0 / r.total_beneficiaries
    END AS procedure_density_raw,
    c.specialty_concentration_raw,
    r.total_services,
    r.total_beneficiaries,
    r.linked_specialties
  FROM condition_rollup r
  JOIN condition_concentration c
    ON c.condition_name = r.condition_name
),
normalized AS (
  SELECT
    x.*,
    CASE
      WHEN MAX(prevalence_score_raw) OVER () = MIN(prevalence_score_raw) OVER () THEN 0
      ELSE (prevalence_score_raw - MIN(prevalence_score_raw) OVER ())
           / NULLIF(MAX(prevalence_score_raw) OVER () - MIN(prevalence_score_raw) OVER (), 0)
    END AS prevalence_score,
    CASE
      WHEN MAX(procedure_density_raw) OVER () = MIN(procedure_density_raw) OVER () THEN 0
      ELSE (procedure_density_raw - MIN(procedure_density_raw) OVER ())
           / NULLIF(MAX(procedure_density_raw) OVER () - MIN(procedure_density_raw) OVER (), 0)
    END AS procedure_density_score,
    CASE
      WHEN MAX(specialty_concentration_raw) OVER () = MIN(specialty_concentration_raw) OVER () THEN 0
      ELSE (specialty_concentration_raw - MIN(specialty_concentration_raw) OVER ())
           / NULLIF(MAX(specialty_concentration_raw) OVER () - MIN(specialty_concentration_raw) OVER (), 0)
    END AS specialty_concentration_score
  FROM combined x
),
scored AS (
  SELECT
    n.*,
    ROUND(
      (0.45 * prevalence_score)
      + (0.35 * procedure_density_score)
      + (0.20 * specialty_concentration_score),
      6
    ) AS opportunity_score
  FROM normalized n
)
SELECT
  ROW_NUMBER() OVER (ORDER BY opportunity_score DESC, total_services DESC, condition_name) AS opportunity_rank,
  condition_name,
  opportunity_score,
  prevalence_score,
  procedure_density_score,
  specialty_concentration_score,
  total_services,
  total_beneficiaries,
  linked_specialties
FROM scored;