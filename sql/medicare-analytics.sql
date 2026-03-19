-- =============================================================================
-- MEDICARE MARKET INTELLIGENCE — CONSOLIDATED ELT PIPELINE
-- Client: Manos Health & CCIQ | Prepared: March 2026
-- Pattern: Raw Ingestion → Normalization → Analytical Views → Opportunity Scoring
-- Database: SQLite (surveilr RSSD)
--
-- MERGE NOTES:
--   Base: medicare_analytics_refined.sql (full star schema, normalized scoring)
--   From original mdcare-extraction.sql (3 additions):
--     [+] specialty_dominance_ratio   — market concentration signal per HCPCS code
--     [+] chronic_interaction_density — standalone view for per-code repeat-visit signal
--     [+] refill_velocity             — cleaner naming in monitoring supply view
-- =============================================================================


-- =============================================================================
-- SECTION 0: PERFORMANCE INDEXES
-- Run once after raw ingestion. All joins on million-row tables depend on these.
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_raw_geo_hcpcs     ON uniform_resource_cms_bygeography (HCPCS_Cd);
CREATE INDEX IF NOT EXISTS idx_raw_geo_state      ON uniform_resource_cms_bygeography (Rndrng_Prvdr_Geo_Cd);
CREATE INDEX IF NOT EXISTS idx_raw_geo_lvl        ON uniform_resource_cms_bygeography (Rndrng_Prvdr_Geo_Lvl);
CREATE INDEX IF NOT EXISTS idx_raw_geo_place      ON uniform_resource_cms_bygeography (Place_Of_Srvc);

CREATE INDEX IF NOT EXISTS idx_raw_prov_state     ON uniform_resource_cms_provider (Rndrng_Prvdr_State_Abrvtn);
CREATE INDEX IF NOT EXISTS idx_raw_prov_type      ON uniform_resource_cms_provider (Rndrng_Prvdr_Type);
CREATE INDEX IF NOT EXISTS idx_raw_prov_npi       ON uniform_resource_cms_provider (Rndrng_NPI);
CREATE INDEX IF NOT EXISTS idx_raw_prov_entity    ON uniform_resource_cms_provider (Rndrng_Prvdr_Ent_Cd);

CREATE INDEX IF NOT EXISTS idx_ref_icd10_code     ON uniform_resource_ref_icd10_diagnosis (icd10_code);
CREATE INDEX IF NOT EXISTS idx_ref_hcpcs_l2_code  ON uniform_resource_ref_hcpcs_level_two_procedures (hcpcs_code);
CREATE INDEX IF NOT EXISTS idx_ref_proc_hcpcs     ON uniform_resource_ref_procedure_code (HCPCS);
CREATE INDEX IF NOT EXISTS idx_ref_rvu_hcpcs      ON uniform_resource_ref_rvu_qpp (HCPCS);
CREATE INDEX IF NOT EXISTS idx_ref_geo_state      ON uniform_resource_ref_geo_adjustment (State);
CREATE INDEX IF NOT EXISTS idx_ref_opps_hcpcs     ON uniform_resource_ref_opps_price_cap (HCPCS);
CREATE INDEX IF NOT EXISTS idx_pos_map_code        ON uniform_resource_cms_bygeo_place_of_service_mapping (pos_code);


-- =============================================================================
-- SECTION 1: NORMALIZATION LAYER — DIMENSION TABLES
-- One clean lookup per domain. All fact tables join through these.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_procedure
-- Merges CPT Level I + HCPCS Level II into one lookup.
-- Adds: procedure_category (clinical range), is_monitoring_flag (repeat-visit),
--       RVU values for economic weight comparisons.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_procedure;
CREATE TABLE dim_procedure AS
SELECT
    p.HCPCS                                                         AS hcpcs_code,
    TRIM(p.DESCRIPTION)                                             AS procedure_description,
    p."STATUS CODE"                                                 AS status_code,
    CAST(NULLIF(TRIM(p."WORK RVU"),        '') AS REAL)             AS work_rvu,
    CAST(NULLIF(TRIM(p."NON-FAC PE RVU"),  '') AS REAL)             AS non_fac_pe_rvu,
    CAST(NULLIF(TRIM(p."FACILITY PE RVU"), '') AS REAL)             AS facility_pe_rvu,
    CAST(NULLIF(TRIM(p."MEDICARE PAYMENT"),'') AS REAL)             AS medicare_fee_schedule_payment,
    CASE
        WHEN p.HCPCS BETWEEN '99202' AND '99499' THEN 'Evaluation & Management'
        WHEN p.HCPCS BETWEEN '70000' AND '79999' THEN 'Radiology / Imaging'
        WHEN p.HCPCS BETWEEN '80000' AND '89999' THEN 'Pathology & Laboratory'
        WHEN p.HCPCS BETWEEN '90000' AND '99199' THEN 'Medicine & Monitoring'
        WHEN p.HCPCS BETWEEN '00100' AND '01999' THEN 'Anesthesia'
        WHEN p.HCPCS BETWEEN '10000' AND '69999' THEN 'Surgery'
        WHEN p.HCPCS GLOB '[A-Z]*'               THEN 'HCPCS Level II (DME / Drug / Other)'
        ELSE 'Unclassified'
    END                                                             AS procedure_category,
    -- 1 = monitoring/repeat-visit procedure (key signal for interaction density)
    CASE
        WHEN p.HCPCS IN ('82947','82950','82962','83036','83037')           THEN 1  -- glucose / A1c
        WHEN p.HCPCS IN ('90935','90937','90945','90947','90999',
                         'G0491','G0492')                                   THEN 1  -- dialysis
        WHEN p.HCPCS IN ('93000','93005','93010','93224','93225',
                         '93226','93227')                                   THEN 1  -- cardiac monitoring
        WHEN p.HCPCS IN ('94010','94060','94070','94150','94250',
                         '94640','94660')                                   THEN 1  -- pulmonary / COPD
        WHEN p.HCPCS BETWEEN '99202' AND '99215'                           THEN 1  -- E&M office visits
        ELSE 0
    END                                                             AS is_monitoring_flag
FROM uniform_resource_ref_procedure_code p
WHERE p.HCPCS IS NOT NULL AND TRIM(p.HCPCS) != '';

CREATE INDEX idx_dim_proc_code     ON dim_procedure(hcpcs_code);
CREATE INDEX idx_dim_proc_category ON dim_procedure(procedure_category);
CREATE INDEX idx_dim_proc_monitor  ON dim_procedure(is_monitoring_flag);


-- -----------------------------------------------------------------------------
-- dim_diagnosis
-- All 10 CCW chronic conditions + cancers + neuro + musculoskeletal.
-- Two levels: granular disease_state + rollup body_system for executive views.
-- Note: CASE order matters — more specific codes must precede their parent prefix.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_diagnosis;
CREATE TABLE dim_diagnosis AS
SELECT
    icd10_code,
    description_long                                AS diagnosis_description,
    CASE
        WHEN icd10_code LIKE 'N186'                 THEN 'End-Stage Renal Disease (ESRD)'
        WHEN icd10_code LIKE 'N185'                 THEN 'End-Stage Renal Disease (ESRD)'
        WHEN icd10_code LIKE 'N18%'                 THEN 'Chronic Kidney Disease'
        WHEN icd10_code LIKE 'E10%'                 THEN 'Type 1 Diabetes'
        WHEN icd10_code LIKE 'E11%'                 THEN 'Type 2 Diabetes'
        WHEN icd10_code LIKE 'E13%'                 THEN 'Diabetes - Other Specified'
        WHEN icd10_code LIKE 'I50%'                 THEN 'Heart Failure / CHF'
        WHEN icd10_code LIKE 'I48%'                 THEN 'Atrial Fibrillation'
        WHEN icd10_code LIKE 'I25%'                 THEN 'Ischemic Heart Disease / CAD'
        WHEN icd10_code LIKE 'I10%'                 THEN 'Hypertension'
        WHEN icd10_code LIKE 'J44%'                 THEN 'COPD'
        WHEN icd10_code LIKE 'J45%'                 THEN 'Asthma'
        WHEN icd10_code LIKE 'G30%'                 THEN 'Alzheimers Disease'
        WHEN icd10_code LIKE 'F01%'                 THEN 'Vascular Dementia'
        WHEN icd10_code LIKE 'F03%'                 THEN 'Dementia - Unspecified'
        WHEN icd10_code LIKE 'F32%'                 THEN 'Major Depressive Disorder'
        WHEN icd10_code LIKE 'F33%'                 THEN 'Recurrent Depressive Disorder'
        WHEN icd10_code LIKE 'G20%'                 THEN 'Parkinsons Disease'
        WHEN icd10_code LIKE 'G45%'                 THEN 'TIA (Stroke Precursor)'
        WHEN icd10_code BETWEEN 'I60' AND 'I699'    THEN 'Stroke / Cerebrovascular Disease'
        WHEN icd10_code LIKE 'M17%'                 THEN 'Osteoarthritis - Knee'
        WHEN icd10_code LIKE 'M16%'                 THEN 'Osteoarthritis - Hip'
        WHEN icd10_code LIKE 'M80%'                 THEN 'Osteoporosis with Fracture'
        WHEN icd10_code LIKE 'M81%'                 THEN 'Osteoporosis without Fracture'
        WHEN icd10_code LIKE 'C34%'                 THEN 'Lung Cancer'
        WHEN icd10_code LIKE 'C50%'                 THEN 'Breast Cancer'
        WHEN icd10_code LIKE 'C61%'                 THEN 'Prostate Cancer'
        WHEN icd10_code LIKE 'C18%'                 THEN 'Colon Cancer'
        WHEN icd10_code BETWEEN 'C00' AND 'C979'    THEN 'Cancer - Malignant Neoplasm'
        ELSE 'General / Other'
    END                                             AS disease_state,
    CASE
        WHEN icd10_code LIKE 'E1%'                              THEN 'Endocrine & Metabolic'
        WHEN icd10_code LIKE 'I%'                               THEN 'Cardiovascular'
        WHEN icd10_code LIKE 'N1%'                              THEN 'Renal & Urological'
        WHEN icd10_code LIKE 'J%'                               THEN 'Respiratory'
        WHEN icd10_code LIKE 'G%' OR icd10_code LIKE 'F%'      THEN 'Neurological & Mental Health'
        WHEN icd10_code LIKE 'M%'                               THEN 'Musculoskeletal'
        WHEN icd10_code LIKE 'C%'                               THEN 'Oncology'
        ELSE 'Other'
    END                                             AS body_system
FROM uniform_resource_ref_icd10_diagnosis
WHERE icd10_code IS NOT NULL;

CREATE INDEX idx_dim_diag_code   ON dim_diagnosis(icd10_code);
CREATE INDEX idx_dim_diag_state  ON dim_diagnosis(disease_state);
CREATE INDEX idx_dim_diag_system ON dim_diagnosis(body_system);


-- -----------------------------------------------------------------------------
-- dim_specialty
-- Normalizes CMS raw specialty strings to canonical names + domain grouping.
-- The specialty_domain column is the bridge to disease body_system in scoring.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_specialty;
CREATE TABLE dim_specialty AS
SELECT DISTINCT
    Rndrng_Prvdr_Type                                           AS raw_specialty_name,
    CASE
        WHEN Rndrng_Prvdr_Type LIKE '%Internal Medicine%'       THEN 'Internal Medicine'
        WHEN Rndrng_Prvdr_Type LIKE '%Family Practice%'
          OR Rndrng_Prvdr_Type LIKE '%Family Medicine%'         THEN 'Family Medicine'
        WHEN Rndrng_Prvdr_Type LIKE '%Cardiology%'              THEN 'Cardiology'
        WHEN Rndrng_Prvdr_Type LIKE '%Nephrology%'              THEN 'Nephrology'
        WHEN Rndrng_Prvdr_Type LIKE '%Endocrinology%'           THEN 'Endocrinology'
        WHEN Rndrng_Prvdr_Type LIKE '%Pulmonology%'
          OR Rndrng_Prvdr_Type LIKE '%Pulmonary%'               THEN 'Pulmonology'
        WHEN Rndrng_Prvdr_Type LIKE '%Oncology%'
          OR Rndrng_Prvdr_Type LIKE '%Hematology%'              THEN 'Oncology / Hematology'
        WHEN Rndrng_Prvdr_Type LIKE '%Neurology%'               THEN 'Neurology'
        WHEN Rndrng_Prvdr_Type LIKE '%Orthopedic%'              THEN 'Orthopedic Surgery'
        WHEN Rndrng_Prvdr_Type LIKE '%Psychiatry%'
          OR Rndrng_Prvdr_Type LIKE '%Psychology%'              THEN 'Psychiatry / Psychology'
        WHEN Rndrng_Prvdr_Type LIKE '%Ophthalmology%'           THEN 'Ophthalmology'
        WHEN Rndrng_Prvdr_Type LIKE '%Urology%'                 THEN 'Urology'
        WHEN Rndrng_Prvdr_Type LIKE '%Gastroenterology%'        THEN 'Gastroenterology'
        WHEN Rndrng_Prvdr_Type LIKE '%Nurse Practitioner%'      THEN 'Nurse Practitioner'
        WHEN Rndrng_Prvdr_Type LIKE '%Physician Assistant%'     THEN 'Physician Assistant'
        WHEN Rndrng_Prvdr_Type LIKE '%Physical Therapy%'        THEN 'Physical Therapy'
        ELSE Rndrng_Prvdr_Type
    END                                                         AS specialty_name,
    CASE
        WHEN Rndrng_Prvdr_Type LIKE '%Cardiology%'              THEN 'Cardiovascular'
        WHEN Rndrng_Prvdr_Type LIKE '%Nephrology%'              THEN 'Renal & Urological'
        WHEN Rndrng_Prvdr_Type LIKE '%Endocrinology%'           THEN 'Endocrine & Metabolic'
        WHEN Rndrng_Prvdr_Type LIKE '%Pulmonology%'
          OR Rndrng_Prvdr_Type LIKE '%Pulmonary%'               THEN 'Respiratory'
        WHEN Rndrng_Prvdr_Type LIKE '%Oncology%'
          OR Rndrng_Prvdr_Type LIKE '%Hematology%'              THEN 'Oncology'
        WHEN Rndrng_Prvdr_Type LIKE '%Neurology%'
          OR Rndrng_Prvdr_Type LIKE '%Psychiatry%'              THEN 'Neurological & Mental Health'
        WHEN Rndrng_Prvdr_Type LIKE '%Orthopedic%'              THEN 'Musculoskeletal'
        WHEN Rndrng_Prvdr_Type LIKE '%Internal Medicine%'
          OR Rndrng_Prvdr_Type LIKE '%Family%'                  THEN 'Primary Care'
        ELSE 'General / Other'
    END                                                         AS specialty_domain
FROM uniform_resource_cms_provider
WHERE Rndrng_Prvdr_Type IS NOT NULL;

CREATE INDEX idx_dim_spec_raw    ON dim_specialty(raw_specialty_name);
CREATE INDEX idx_dim_spec_name   ON dim_specialty(specialty_name);
CREATE INDEX idx_dim_spec_domain ON dim_specialty(specialty_domain);


-- -----------------------------------------------------------------------------
-- dim_geography
-- State-level GPCI factors for cost-adjusted market sizing.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_geography;
CREATE TABLE dim_geography AS
SELECT
    g.State                                                             AS state_abbr,
    g."Medicare Administrative Contractor (MAC)"                       AS mac_name,
    g."Locality Name"                                                   AS locality_name,
    CAST(NULLIF(TRIM(g."2026 PW GPCI (with 1.0 Floor)"), '') AS REAL)  AS pw_gpci,
    CASE
        WHEN CAST(NULLIF(TRIM(g."2026 PW GPCI (with 1.0 Floor)"), '') AS REAL) >= 1.1
            THEN 'High Cost Market'
        WHEN CAST(NULLIF(TRIM(g."2026 PW GPCI (with 1.0 Floor)"), '') AS REAL) >= 1.0
            THEN 'Average Cost Market'
        ELSE 'Low Cost Market'
    END                                                                 AS cost_tier
FROM uniform_resource_ref_geo_adjustment g
WHERE g.State IS NOT NULL;

CREATE INDEX idx_dim_geo_state ON dim_geography(state_abbr);




-- =============================================================================
-- SECTION 2: CORE FACT TABLE — fact_utilization
--
-- SOURCE: bygeography (HCPCS + spend) joined to a specialty lookup derived
-- from cms_provider via fips_state_map (FIPS code → state abbreviation).
--
-- Join path:
--   bygeography (FIPS state code + HCPCS + spend)
--   → fips_state_map (FIPS → state abbr)
--   → specialty_by_state (state abbr + specialty, deduplicated from provider)
--
-- specialty_by_state is DISTINCT state × specialty so there is exactly one
-- specialty match per state in the join — no Cartesian explosion.
-- The result is one row per HCPCS × state × specialty × place_of_service.
-- =============================================================================

DROP TABLE IF EXISTS fact_utilization;
CREATE TABLE fact_utilization AS
WITH base_geo AS (
    -- 1. Get the raw volume from the table that definitely has HCPCS_Cd
    SELECT 
        HCPCS_Cd AS hcpcs_code,
        TRIM(UPPER(Rndrng_Prvdr_Geo_Cd)) AS state_abbr,
        Place_Of_Srvc AS place_of_service_code,
        SUM(CAST(NULLIF(Tot_Benes, '') AS INTEGER)) AS total_beneficiaries,
        SUM(CAST(NULLIF(Tot_Srvcs, '') AS REAL)) AS total_services,
        SUM(CAST(NULLIF(Tot_Rndrng_Prvdrs, '') AS INTEGER)) AS total_rendering_providers,
        SUM(CAST(NULLIF(Tot_Srvcs, '') AS REAL) * CAST(NULLIF(Avg_Mdcr_Pymt_Amt, '') AS REAL)) AS total_medicare_payment,
        SUM(
        CAST(NULLIF(Tot_Srvcs, '') AS REAL) * CAST(NULLIF(Avg_Mdcr_Alowd_Amt, '') AS REAL)
        ) AS total_allowed_amt    
    FROM uniform_resource_cms_bygeography
    -- Use UPPER to prevent case-sensitivity issues with 'State' vs 'STATE'
    WHERE UPPER(Rndrng_Prvdr_Geo_Lvl) = 'STATE'
    GROUP BY 1, 2, 3
)
SELECT
    -- 2. Attribute a Specialty based on the HCPCS code range
    CASE 
        WHEN h.hcpcs_code BETWEEN '93000' AND '93999' THEN 'Cardiology'
        WHEN h.hcpcs_code BETWEEN '90935' AND '90999' THEN 'Nephrology'
        WHEN h.hcpcs_code BETWEEN '99201' AND '99499' THEN 'Internal Medicine / Primary Care'
        WHEN h.hcpcs_code BETWEEN '70010' AND '79999' THEN 'Radiology'
        WHEN h.hcpcs_code BETWEEN '10000' AND '69999' THEN 'Surgical Specialties'
        WHEN h.hcpcs_code LIKE 'J%' THEN 'Infusion / Oncology'
        ELSE 'Other Specialties'
    END AS specialty_name,
    
    CASE 
        WHEN h.hcpcs_code BETWEEN '99201' AND '99499' THEN 'Primary Care'
        WHEN h.hcpcs_code BETWEEN '10000' AND '69999' THEN 'Surgery'
        ELSE 'Specialty Care'
    END AS specialty_domain,
    
    h.*
FROM base_geo h;

-- DROP TABLE IF EXISTS fact_utilization;
-- CREATE TABLE fact_utilization AS
-- SELECT
--     sbs.specialty_name,
--     sbs.specialty_domain,
--     g.HCPCS_Cd AS hcpcs_code,
--     fsm.state_abbr,
--     g.Place_Of_Srvc AS place_of_service_code,    
--     COALESCE(pos.pos_category, g.Place_Of_Srvc) AS place_of_service,       
--     SUM(CAST(NULLIF(g.Tot_Benes, '') AS INTEGER)) AS total_beneficiaries,
--     SUM(CAST(NULLIF(g.Tot_Srvcs, '') AS INTEGER)) AS total_services,
--      SUM(CAST(NULLIF(g.Tot_Rndrng_Prvdrs, '') AS INTEGER)) AS total_rendering_providers,
--      SUM(
--         CAST(NULLIF(g.Tot_Srvcs, '') AS REAL) * CAST(NULLIF(g.Avg_Mdcr_Alowd_Amt, '') AS REAL)
--     ) AS total_allowed_amt,
--     SUM(
--         CAST(NULLIF(g.Tot_Srvcs, '') AS REAL) * CAST(NULLIF(g.Avg_Mdcr_Pymt_Amt, '') AS REAL)
--     ) AS total_medicare_payment
-- FROM stage_geo_clean g
-- JOIN fips_state_map fsm ON g.fips_code = fsm.fips_code
-- JOIN specialty_by_state sbs ON fsm.state_abbr = sbs.state_abbr
-- LEFT JOIN uniform_resource_cms_bygeo_place_of_service_mapping pos ON g.Place_Of_Srvc = pos.pos_code
-- GROUP BY 1, 2, 3, 4, 5, 6;

-- Now these indexes will work perfectly:
CREATE INDEX IF NOT EXISTS idx_fact_util_spec     ON fact_utilization(specialty_name);
CREATE INDEX IF NOT EXISTS idx_fact_util_hcpcs    ON fact_utilization(hcpcs_code); 
CREATE INDEX IF NOT EXISTS idx_fact_util_state    ON fact_utilization(state_abbr);
CREATE INDEX IF NOT EXISTS idx_fact_util_domain   ON fact_utilization(specialty_domain);
--CREATE INDEX IF NOT EXISTS idx_fact_util_place_cd ON fact_utilization(place_of_service_code);
CREATE INDEX IF NOT EXISTS idx_fact_util_geo_spec ON fact_utilization(state_abbr, specialty_name);

-- =============================================================================
-- SECTION 2B: SPECIALTY MARKET DYNAMICS TABLE  [FROM ORIGINAL — PRESERVED]
-- Adds specialty_dominance_ratio: what % of a given HCPCS code does each
-- specialty own nationally? Answers "does Nephrology own dialysis codes?"
-- =============================================================================
DROP TABLE IF EXISTS specialty_market_dynamics;

CREATE TABLE specialty_market_dynamics AS
WITH specialty_hcpcs_totals AS (
    -- Pulling directly from your new fact table
    SELECT 
        specialty_name,
        hcpcs_code,
        SUM(total_beneficiaries) AS specialty_hcpcs_benes -- Using benes as the volume metric
    FROM fact_utilization
    GROUP BY 1, 2
),
global_hcpcs_totals AS (
    -- This table exists and contains the national totals
    SELECT 
        HCPCS_Cd AS hcpcs_code,
        SUM(CAST(NULLIF(Tot_Benes, '') AS INTEGER)) AS global_benes
    FROM uniform_resource_cms_bygeography
    WHERE UPPER(Rndrng_Prvdr_Geo_Lvl) = 'NATIONAL' 
    GROUP BY 1
)
SELECT 
    s.specialty_name,
    s.hcpcs_code,
    s.specialty_hcpcs_benes,
    g.global_benes,
    -- Dominance: What % of the national patient base for this code belongs to this specialty?
    ROUND(CAST(s.specialty_hcpcs_benes AS REAL) / NULLIF(g.global_benes, 0), 4) AS specialty_dominance_ratio
FROM specialty_hcpcs_totals s
JOIN global_hcpcs_totals g ON s.hcpcs_code = g.hcpcs_code;

-- Corrected Index (using the column name we defined in the SELECT)
CREATE INDEX IF NOT EXISTS idx_smd_spec  ON specialty_market_dynamics(specialty_name);
CREATE INDEX IF NOT EXISTS idx_smd_hcpcs ON specialty_market_dynamics(hcpcs_code);


-- =============================================================================
-- SECTION 3: ANALYTICAL VIEWS — Business Question Layer
-- =============================================================================

-- -----------------------------------------------------------------------------
-- VIEW 1: specialty_activity_summary
-- Executive KPI — total volume, patient reach, and spend per specialty.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS specialty_activity_summary;
CREATE VIEW specialty_activity_summary AS
SELECT
    specialty_name,
    specialty_domain,
    SUM(total_services)                                                 AS service_volume,
    SUM(total_beneficiaries)                                            AS patient_reach,
    SUM(total_rendering_providers)                                      AS provider_count,
    SUM(total_allowed_amt)                                              AS total_allowed_spend,
    SUM(total_medicare_payment)                                         AS total_medicare_spend,
    
    -- Added CAST to REAL to ensure decimal precision
    ROUND(CAST(SUM(total_medicare_payment) AS REAL) / NULLIF(SUM(total_beneficiaries),0), 2)
                                                                        AS spend_per_patient,
    ROUND(CAST(SUM(total_services) AS REAL) / NULLIF(SUM(total_beneficiaries),0), 2)
                                                                        AS avg_srvcs_per_patient
FROM fact_utilization
GROUP BY specialty_name, specialty_domain;
-- ORDER BY moved to the query level for better compatibility


-- -----------------------------------------------------------------------------
-- VIEW 2: specialty_economic_intensity
-- Combined spend-per-patient × visits-per-patient intensity index.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS specialty_economic_intensity;
CREATE VIEW specialty_economic_intensity AS
SELECT
    specialty_name,
    specialty_domain,
    total_medicare_spend,
    patient_reach,
    spend_per_patient,
    avg_srvcs_per_patient,
    -- Your original custom index
    ROUND(spend_per_patient * avg_srvcs_per_patient, 2)                AS economic_intensity_index,
    -- Standard Intensity: How much do we pay every time this specialty performs a service?
    ROUND(total_medicare_spend / NULLIF(service_volume, 0), 2)         AS avg_cost_per_service,
    
    RANK() OVER (
        PARTITION BY specialty_domain
        ORDER BY (spend_per_patient * avg_srvcs_per_patient) DESC
    )                                                                  AS rank_within_domain
FROM specialty_activity_summary
-- Filter out 'noise' from specialties with very low patient volume
WHERE patient_reach > 50;

-- -----------------------------------------------------------------------------
-- VIEW 3: specialty_top_procedures
-- Top 10 procedures per specialty by volume, with spend rank alongside.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS specialty_top_procedures;

CREATE VIEW specialty_top_procedures AS
WITH procedure_summary AS (
    SELECT
        f.specialty_name,
        f.hcpcs_code,
        dp.procedure_description,
        dp.procedure_category,
        dp.is_monitoring_flag,
        -- Use MAX to avoid row splitting if dim_procedure has duplicates
        MAX(dp.work_rvu)                                                AS work_rvu, 
        SUM(f.total_services)                                           AS total_services,
        SUM(f.total_beneficiaries)                                      AS total_beneficiaries,
        SUM(f.total_medicare_payment)                                   AS total_spend,
        ROUND(
            SUM(f.total_services) * 1.0 / NULLIF(SUM(f.total_beneficiaries), 0)
        , 2)                                                            AS srvcs_per_patient
    FROM fact_utilization f
    LEFT JOIN dim_procedure dp ON f.hcpcs_code = dp.hcpcs_code
    GROUP BY 1, 2, 3, 4, 5
)
SELECT * FROM (
    SELECT 
        *,
        -- Standard rank for volume
        RANK() OVER (PARTITION BY specialty_name ORDER BY total_services DESC) AS service_rank,
        -- Standard rank for financial impact
        RANK() OVER (PARTITION BY specialty_name ORDER BY total_spend DESC)    AS spend_rank
    FROM procedure_summary
)
WHERE service_rank <= 10;


-- -----------------------------------------------------------------------------
-- VIEW 4: specialty_market_concentration  [FROM ORIGINAL — PRESERVED + CLEANED]
-- Dominance-based ranking: which specialties own the highest share of key codes?
-- Use to validate that "Nephrology owns dialysis" or "Cardiology owns echo".
-- -----------------------------------------------------------------------------
-- VIEW 4: specialty_market_concentration
DROP VIEW IF EXISTS specialty_market_concentration;
CREATE VIEW specialty_market_concentration AS
SELECT
    smd.specialty_name,
    smd.hcpcs_code,
    COALESCE(dp.procedure_description, 'Unknown Procedure') AS procedure_description,
    total_services,
    total_benes,
    ROUND(specialty_dominance_ratio * 100, 1)                           AS pct_of_national_volume,
    ROUND(CAST(total_services AS REAL) * specialty_dominance_ratio, 0)  AS weighted_dominance_score,
    RANK() OVER (
        PARTITION BY specialty_name
        ORDER BY specialty_dominance_ratio DESC
    )                                                                   AS dominance_rank
FROM specialty_market_dynamics
LEFT JOIN dim_procedure dp 
    ON smd.hcpcs_code = dp.hcpcs_code
WHERE total_benes > 500; -- Order By removed for View compatibility


-- VIEW 5: chronic_interaction_density
DROP VIEW IF EXISTS chronic_interaction_density;
CREATE VIEW chronic_interaction_density AS
WITH base_metrics AS (
    SELECT
        s.specialty_name,
        s.hcpcs_code,
        dp.procedure_description, -- Now this column exists!
        dp.procedure_category,
        dp.is_monitoring_flag,
        s.specialty_hcpcs_benes AS total_benes,
        -- Note: If you have a services column in 's', use it here. 
        -- Otherwise, we assume 1:1 for this specific calculation example.
        ROUND(CAST(s.specialty_hcpcs_benes AS REAL) / NULLIF(s.specialty_hcpcs_benes, 0), 2) AS srv_per_pat
    FROM specialty_market_dynamics s
    LEFT JOIN dim_procedure dp ON s.hcpcs_code = dp.hcpcs_code
    WHERE s.specialty_hcpcs_benes > 500
)
SELECT 
    *,
    CASE
        WHEN srv_per_pat >= 12 THEN 'High (12+ sessions/yr)'
        WHEN srv_per_pat >= 4  THEN 'Moderate (4-11 sessions/yr)'
        ELSE 'Low (< 4 sessions/yr)'
    END AS interaction_tier
FROM base_metrics;


-- VIEW 6: monitoring_procedure_intensity
DROP VIEW IF EXISTS monitoring_procedure_intensity;
CREATE VIEW monitoring_procedure_intensity AS
SELECT
    f.specialty_name,
    f.specialty_domain,
    -- Using SUM(CASE...) is the most portable way to aggregate flags in SQLite
    SUM(CASE WHEN dp.is_monitoring_flag = 1 THEN f.total_services ELSE 0 END) AS monitoring_volume,
    SUM(f.total_services) AS total_volume,
    -- Force floating point by multiplying by 100.0 first
    ROUND(
        (SUM(CASE WHEN dp.is_monitoring_flag = 1 THEN f.total_services ELSE 0 END) * 100.0) 
        / NULLIF(SUM(f.total_services), 0)
    , 1) AS monitoring_pct,
    SUM(CASE WHEN dp.is_monitoring_flag = 1 THEN f.total_medicare_payment ELSE 0 END) AS monitoring_spend,
    SUM(f.total_medicare_payment) AS total_spend
FROM fact_utilization f
LEFT JOIN dim_procedure dp ON f.hcpcs_code = dp.hcpcs_code
GROUP BY f.specialty_name, f.specialty_domain
HAVING total_volume > 1000;

-- -----------------------------------------------------------------------------
-- VIEW 7: dme_supply_refill_metrics  [MERGED: supply_category from refined +
--          refill_velocity naming from original]
-- DME/supply repeat-dispensing as a proxy for ongoing disease management.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS dme_supply_refill_metrics;
CREATE VIEW dme_supply_refill_metrics AS
SELECT
    h.short_description AS supply_item,
    h.hcpcs_code,
    SUM(CAST(NULLIF(g.Tot_Srvcs,'') AS INTEGER)) AS total_units,
    SUM(CAST(NULLIF(g.Tot_Benes,'') AS INTEGER)) AS total_patients,
    ROUND(
        SUM(CAST(NULLIF(g.Tot_Srvcs,'') AS REAL)) / 
        NULLIF(SUM(CAST(NULLIF(g.Tot_Benes,'') AS INTEGER)), 0)
    , 2) AS refill_velocity,
    CASE
        WHEN h.hcpcs_code LIKE 'E%' THEN 'Durable Medical Equipment'
        WHEN h.hcpcs_code LIKE 'A%' THEN 'Medical / Surgical Supplies'
        WHEN h.hcpcs_code LIKE 'B%' THEN 'Enteral & Parenteral Therapy'
        WHEN h.hcpcs_code LIKE 'K%' THEN 'DME - Temporary Codes'
        WHEN h.hcpcs_code LIKE 'L%' THEN 'Orthotics & Prosthetics'
        WHEN h.hcpcs_code LIKE 'J%' THEN 'Part B Drugs (Injections)'
        WHEN h.hcpcs_code LIKE 'G%' THEN 'CMS-Defined Services'
        ELSE 'Other HCPCS Level II'
    END AS supply_category
FROM uniform_resource_ref_hcpcs_level_two_procedures h
JOIN uniform_resource_cms_bygeography g ON h.hcpcs_code = g.HCPCS_Cd
GROUP BY 1, 2
HAVING total_units > 1000;


-- -----------------------------------------------------------------------------
-- VIEW 8: surgical_economic_metrics
-- High-intensity surgical clusters scored by anesthesia conversion weight.
-- -----------------------------------------------------------------------------

DROP VIEW IF EXISTS surgical_economic_metrics;
CREATE VIEW surgical_economic_metrics AS
-- First, handle the reference data to avoid duplication
WITH clean_anes_factors AS (
    SELECT DISTINCT
        Contractor,
        -- Using backticks or double quotes depending on your DB (SQLite/Postgres)
        -- Ensure this column name matches exactly what is in your schema
        CAST(NULLIF("Non-Qualifying APM National Anes CF (with 2.5% statutory increase) of 20.599835", '') AS REAL) AS anes_cf
    FROM uniform_resource_ref_anes_conversion_factor
)
SELECT
    g.HCPCS_Desc AS procedure_name,
    g.HCPCS_Cd AS hcpcs_code,
    SUM(CAST(NULLIF(g.Tot_Srvcs,'') AS INTEGER)) AS volume,
    SUM(CAST(NULLIF(g.Tot_Benes,'') AS INTEGER)) AS patients,
    AVG(a.anes_cf) AS avg_anesthesia_cf,
    ROUND(
        SUM(CAST(NULLIF(g.Tot_Srvcs,'') AS INTEGER)) * COALESCE(AVG(a.anes_cf), 0)
    , 2) AS estimated_total_anesthesia_cost
FROM uniform_resource_cms_bygeography g
JOIN clean_anes_factors a 
    ON TRIM(UPPER(g.Rndrng_Prvdr_Geo_Cd)) = TRIM(UPPER(a.Contractor))
-- HCPCS Anesthesia range: 00100 through 01999
WHERE g.HCPCS_Cd BETWEEN '00100' AND '01999'
GROUP BY 1, 2;

-- -----------------------------------------------------------------------------
-- VIEW 9: part_b_drug_intensity
-- Drug spend per specialty — key for oncology + nephrology market sizing.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS part_b_drug_intensity;
CREATE VIEW part_b_drug_intensity AS
SELECT
    f.specialty_name,
    f.specialty_domain,
    f.hcpcs_code,
    dp.procedure_description, -- Pulled from the dimension table
    SUM(f.total_services)                                               AS total_drug_administrations,
    SUM(f.total_beneficiaries)                                          AS patients_receiving_drug,
    SUM(f.total_medicare_payment)                                       AS total_drug_spend,
    ROUND(
        CAST(SUM(f.total_medicare_payment) AS REAL) / NULLIF(SUM(f.total_beneficiaries), 0)
    , 2)                                                                AS drug_spend_per_patient
FROM fact_utilization f
-- JOIN to get the description and the drug flag
JOIN dim_procedure dp ON f.hcpcs_code = dp.hcpcs_code
-- Filter for drugs (J-codes are the standard for Part B Injectables)
WHERE f.hcpcs_code LIKE 'J%' 
   OR f.hcpcs_code LIKE 'Q%' -- Some drugs use Q-codes
GROUP BY f.specialty_name, f.specialty_domain, f.hcpcs_code, dp.procedure_description
ORDER BY total_drug_spend DESC;


-- -----------------------------------------------------------------------------
-- VIEW 10: geographic_market_opportunity
-- State-level volume + GPCI-adjusted spend per specialty for geo targeting.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS geographic_market_opportunity;
CREATE VIEW geographic_market_opportunity AS
SELECT
    f.state_abbr,
    dg.cost_tier,
    dg.mac_name,
    f.specialty_name,
    f.specialty_domain,
    SUM(f.total_beneficiaries)                                         AS state_patient_volume,
    SUM(f.total_medicare_payment)                                      AS state_total_spend,
    ROUND(
        SUM(f.total_medicare_payment) / NULLIF(SUM(f.total_beneficiaries),0)
    , 2)                                                               AS state_spend_per_patient,
    ROUND(
        SUM(f.total_medicare_payment) * COALESCE(dg.pw_gpci, 1.0)
    , 2)                                                               AS gpci_adjusted_spend
FROM fact_utilization f
LEFT JOIN dim_geography dg ON f.state_abbr = dg.state_abbr
GROUP BY f.state_abbr, dg.cost_tier, dg.mac_name, f.specialty_name, f.specialty_domain
ORDER BY state_total_spend DESC;


-- -----------------------------------------------------------------------------
-- VIEW 11: facility_vs_office_split
-- Care setting mix per specialty — office dominance = higher patient ownership.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS facility_vs_office_split;
CREATE VIEW facility_vs_office_split AS
SELECT
    specialty_name,
    specialty_domain,
    SUM(CASE WHEN UPPER(place_of_service_code) = 'F' THEN total_services ELSE 0 END) AS facility_services,
    SUM(CASE WHEN UPPER(place_of_service_code) = 'O' THEN total_services ELSE 0 END) AS office_services,
    SUM(total_services) AS total_services,
    ROUND(
        100.0 * SUM(CASE WHEN UPPER(place_of_service_code) = 'O' THEN total_services ELSE 0 END)
        / NULLIF(SUM(total_services), 0)
    , 1) AS office_pct,
    SUM(CASE WHEN UPPER(place_of_service_code) = 'F' THEN total_medicare_payment ELSE 0 END) AS facility_spend,
    SUM(CASE WHEN UPPER(place_of_service_code) = 'O' THEN total_medicare_payment ELSE 0 END) AS office_spend
FROM fact_utilization
GROUP BY 1, 2;


-- -----------------------------------------------------------------------------
-- VIEW 12: disease_state_icd_coverage
-- Code count per disease cluster — use to validate we haven't missed ICD ranges.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS disease_state_icd_coverage;
CREATE VIEW disease_state_icd_coverage AS
SELECT
    disease_state,
    body_system,
    COUNT(*) AS icd_code_count,
    -- Added a limit check note; SQLite GROUP_CONCAT is 1024 chars by default
    GROUP_CONCAT(icd10_code, ', ') AS sample_icd_codes
FROM dim_diagnosis
WHERE disease_state != 'General / Other'
GROUP BY 1, 2;


-- =============================================================================
-- SECTION 4: OPPORTUNITY SCORING ENGINE  (PRIMARY DELIVERABLE)
-- Formula: 35% Volume + 35% Intensity + 30% Economics
-- Includes: market_concentration_bonus from dominance_ratio [FROM ORIGINAL]
-- All dimensions normalized via NTILE(100) before combining.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- VIEW 13: opportunity_scoring_view
-- Final ranked table of disease-state × specialty clusters.
-- Tiers: >= 75 = Tier 1 High, >= 50 = Tier 2 Moderate, < 50 = Tier 3 Low
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS opportunity_scoring_view;
CREATE VIEW opportunity_scoring_view AS
WITH mapping_logic AS (
    -- This CTE forces a perfect match between Disease System and Specialty Domain
    SELECT 'Endocrine & Metabolic' AS diag_sys, 'Endocrine & Metabolic' AS spec_dom UNION ALL
    SELECT 'Cardiovascular', 'Cardiovascular' UNION ALL
    SELECT 'Renal & Urological', 'Renal & Urological' UNION ALL
    SELECT 'Respiratory', 'Respiratory' UNION ALL
    SELECT 'Oncology', 'Oncology' UNION ALL
    SELECT 'Neurological & Mental Health', 'Neurological & Mental Health' UNION ALL
    SELECT 'Musculoskeletal', 'Musculoskeletal'
),
disease_specialty_bridge AS (
    SELECT
        d.disease_state,
        d.body_system,
        f.specialty_name,
        f.specialty_domain,
        SUM(f.total_beneficiaries) AS patient_volume,
        SUM(f.total_services) AS service_volume,
        SUM(f.total_medicare_payment) AS total_spend,
        ROUND(SUM(f.total_services) * 1.0 / NULLIF(SUM(f.total_beneficiaries),0), 2) AS avg_srvcs_per_patient,
        AVG(COALESCE(smd.specialty_dominance_ratio, 0)) AS avg_dominance_ratio
    FROM (SELECT DISTINCT disease_state, body_system FROM dim_diagnosis WHERE disease_state != 'General / Other') d
    JOIN mapping_logic m ON d.body_system = m.diag_sys
    JOIN fact_utilization f ON (
        f.specialty_domain = m.spec_dom 
        OR f.specialty_domain = 'Primary Care'
    )
    LEFT JOIN specialty_market_dynamics smd 
      ON f.specialty_name = smd.specialty_name 
      AND f.hcpcs_code = smd.hcpcs_code
    GROUP BY 1, 2, 3, 4
    -- Lowered the threshold to 1 for debugging; change back to 100 later
    HAVING patient_volume >= 1
),
percentile_ranks AS (
    SELECT
        *,
        NTILE(100) OVER (ORDER BY patient_volume) AS vol_percentile,
        NTILE(100) OVER (ORDER BY avg_srvcs_per_patient) AS intensity_percentile,
        NTILE(100) OVER (ORDER BY total_spend) AS econ_percentile,
        ROUND(avg_dominance_ratio * 10.0, 1) AS dominance_bonus
    FROM disease_specialty_bridge
),
final_calculation AS (
    SELECT
        *,
        ROUND(
            (0.35 * vol_percentile) +
            (0.35 * intensity_percentile) +
            (0.30 * econ_percentile) +
            dominance_bonus
        , 1) AS composite_opportunity_score
    FROM percentile_ranks
)
SELECT
    disease_state,
    body_system,
    specialty_name,
    patient_volume,
    service_volume,
    avg_srvcs_per_patient,
    ROUND(total_spend / 1000000.0, 2) AS total_spend_millions,
    vol_percentile AS volume_score,
    intensity_percentile AS intensity_score,
    econ_percentile AS economics_score,
    ROUND(avg_dominance_ratio * 100, 1) AS market_concentration_pct,
    composite_opportunity_score,
    CASE
        WHEN composite_opportunity_score >= 75 THEN 'Tier 1 — High'
        WHEN composite_opportunity_score >= 50 THEN 'Tier 2 — Moderate'
        ELSE 'Tier 3 — Low'
    END AS opportunity_tier
FROM final_calculation;


-- =============================================================================
-- SECTION 5: STARTER ANALYST QUERIES  (Uncomment and run directly)
-- =============================================================================

-- A. Top 20 specialties by total Medicare spend
-- SELECT specialty_name, specialty_domain,
--        ROUND(total_medicare_spend/1e6,2) AS spend_millions,
--        patient_reach, spend_per_patient, avg_srvcs_per_patient
-- FROM specialty_activity_summary LIMIT 20;

-- B. Top 20 CPT codes nationally by volume
-- SELECT hcpcs_code, procedure_description, procedure_category,
--        SUM(total_services) AS national_volume,
--        ROUND(SUM(total_medicare_payment)/1e6,2) AS total_spend_millions
-- FROM fact_utilization
-- LEFT JOIN dim_procedure USING (hcpcs_code)
-- GROUP BY hcpcs_code, procedure_description, procedure_category
-- ORDER BY national_volume DESC LIMIT 20;

-- C. Specialties with highest repeat-visit intensity
-- SELECT specialty_name, avg_srvcs_per_patient, patient_reach,
--        ROUND(total_medicare_spend/1e6,2) AS spend_millions
-- FROM specialty_activity_summary
-- WHERE patient_reach > 10000
-- ORDER BY avg_srvcs_per_patient DESC LIMIT 20;

-- D. Procedures where ONE specialty owns > 70% of national volume (market lock)
-- SELECT specialty_name, procedure_description, pct_of_national_volume, total_services
-- FROM specialty_market_concentration
-- WHERE pct_of_national_volume >= 70
-- ORDER BY total_services DESC LIMIT 20;

-- E. Most "sticky" codes (dialysis, glucose monitoring, cardiac rhythm — the real repeat-interaction story)
-- SELECT specialty_name, procedure_description, services_per_patient, interaction_tier,
--        total_benes
-- FROM chronic_interaction_density
-- WHERE interaction_tier = 'High (12+ sessions/yr)'
-- ORDER BY services_per_patient DESC LIMIT 20;

-- F. Final Tier 1 opportunities for Manos / CCIQ
-- SELECT disease_state, specialty_name, composite_opportunity_score, opportunity_tier,
--        patient_volume, avg_srvcs_per_patient, total_spend_millions,
--        market_concentration_pct
-- FROM opportunity_scoring_view
-- WHERE opportunity_tier = 'Tier 1 — High Opportunity'
-- ORDER BY composite_opportunity_score DESC;

-- G. State geo-targeting for Nephrology (dialysis markets)
-- SELECT state_abbr, cost_tier, state_patient_volume, gpci_adjusted_spend
-- FROM geographic_market_opportunity
-- WHERE specialty_name = 'Nephrology'
-- ORDER BY gpci_adjusted_spend DESC LIMIT 15;

-- H. Part B drug spend by specialty (oncology / infusion intensity)
-- SELECT specialty_name,
--        SUM(total_drug_spend) AS total_drug_spend,
--        ROUND(SUM(total_drug_spend)/NULLIF(SUM(patients_receiving_drug),0),2) AS avg_drug_spend_per_patient
-- FROM part_b_drug_intensity
-- GROUP BY specialty_name
-- ORDER BY total_drug_spend DESC LIMIT 15;

-- I. Office-dominant specialties — ambulatory ownership signal
-- SELECT specialty_name, office_pct,
--        ROUND(office_spend/1e6,2) AS office_spend_millions,
--        ROUND(facility_spend/1e6,2) AS facility_spend_millions
-- FROM facility_vs_office_split
-- WHERE total_services > 50000
-- ORDER BY office_pct DESC LIMIT 20;
