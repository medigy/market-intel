-- =============================================================================
-- Medigy MARKET INTELLIGENCE — CONSOLIDATED ELT PIPELINE
-- Database: SQLite (surveilr RSSD)
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
-- Adds: procedure_category (clinical range), procedure_signal (business model),
--       and is_monitoring_flag (repeat-visit signal).
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_procedure;
CREATE TABLE dim_procedure AS
SELECT
    p.HCPCS AS hcpcs_code,
    TRIM(p.DESCRIPTION) AS procedure_description,
    
    -- BROAD CATEGORY: Essential for the master_evidence_hub join 
    CASE
        WHEN p.HCPCS BETWEEN '99202' AND '99499' THEN 'Evaluation & Management'
        WHEN p.HCPCS BETWEEN '70000' AND '79999' THEN 'Radiology / Imaging'
        WHEN p.HCPCS BETWEEN '80000' AND '89999' THEN 'Pathology & Laboratory'
        WHEN p.HCPCS BETWEEN '90000' AND '99199' THEN 'Medicine & Monitoring'
        WHEN p.HCPCS BETWEEN '00100' AND '01999' THEN 'Anesthesia'
        WHEN p.HCPCS BETWEEN '10000' AND '69999' THEN 'Surgery'
        WHEN p.HCPCS GLOB '[A-Z]*'               THEN 'HCPCS Level II (DME / Drug / Other)'
        ELSE 'Unclassified'
    END AS procedure_category,
    
    -- SPECIFIC SIGNAL: Used for Commercial Prioritization [cite: 159, 180]
    CASE
        -- Diagnostic Triggers (Model B) [cite: 176, 299]
        WHEN p.HCPCS IN ('95810','95811')                          THEN 'Sleep Lab (High Intensity)'
        WHEN p.HCPCS IN ('95812','95819')                          THEN 'Neuro Assessment'
        WHEN p.HCPCS IN ('99483')                                  THEN 'Cognitive Assessment'
        
        -- Monitoring Interactions (Model C / SaaS) [cite: 179, 325]
        WHEN p.HCPCS IN ('G0238')                                  THEN 'Respiratory Rehab (High Freq)'
        WHEN p.HCPCS IN ('99490')                                  THEN 'Chronic Care Management'
        WHEN p.HCPCS IN ('G2086','G2087')                          THEN 'SUD/Opioid Treatment'
        
        -- Low Margin Baselines [cite: 157, 353]
        WHEN p.HCPCS IN ('99214')                                  THEN 'Standard E/M Visit'
        WHEN p.HCPCS IN ('83036')                                  THEN 'Lab Screening (A1C)'
        ELSE 'Standard Care'
    END AS procedure_signal,

    -- INTERACTIVE DENSITY FLAG: Used to calculate interaction ratios [cite: 123, 222]
    CASE 
        WHEN p.HCPCS IN ('G0238', '99490', 'G2086', 'G2087')       THEN 1 -- SaaS/CCM
        WHEN p.HCPCS BETWEEN '99211' AND '99215'                   THEN 1 -- Routine Office
        WHEN p.HCPCS IN ('95810','95811')                          THEN 1 -- Diagnostic Trigger
        ELSE 0 
    END AS is_monitoring_flag
FROM uniform_resource_ref_procedure_code p;

-- -----------------------------------------------------------------------------
-- PERFORMANCE INDEX
-- -----------------------------------------------------------------------------

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
    description_long AS diagnosis_description,
    CASE
        -- Tier 1 & 2: Respiratory, Sleep & Neuro [cite: 270, 291, 311, 327]
        WHEN icd10_code LIKE 'G47.3%' OR icd10_code LIKE 'P28.3%' THEN 'Sleep Apnea'
        WHEN icd10_code LIKE 'J44%'                               THEN 'COPD'
        WHEN icd10_code LIKE 'G20%'                               THEN 'Parkinsons Disease'
        WHEN icd10_code LIKE 'I50%'                               THEN 'Heart Failure'
        WHEN icd10_code LIKE 'F11%'                               THEN 'Opioid Use Disorder'

        -- Tier 3: Metabolic & Chronic (Comparison Baselines) [cite: 353]
        WHEN icd10_code LIKE 'I10%'                               THEN 'Hypertension'
        WHEN icd10_code LIKE 'E11%'                               THEN 'Type 2 Diabetes'
        WHEN icd10_code LIKE 'E03%'                               THEN 'Thyroid (Hypothyroidism)'
        WHEN icd10_code LIKE 'J45%'                               THEN 'Asthma'

        -- Tier 4: Mental Health [cite: 357]
        WHEN icd10_code LIKE 'F32%' OR icd10_code LIKE 'F33%'     THEN 'Major Depression'
        WHEN icd10_code LIKE 'F41.1%'                             THEN 'Anxiety (GAD)'
        WHEN icd10_code LIKE 'F43.1%'                             THEN 'PTSD'
        WHEN icd10_code LIKE 'F31%'                               THEN 'Bipolar Disorder'

        -- Tier 4: Specialty & Niche [cite: 357]
        WHEN icd10_code LIKE 'G30%'                               THEN 'Alzheimers'
        WHEN icd10_code LIKE 'G35%'                               THEN 'Multiple Sclerosis'
        WHEN icd10_code LIKE 'C%'                                 THEN 'Oncology'
        WHEN icd10_code LIKE 'R54%' OR icd10_code LIKE 'R49%'      THEN 'Frailty / Vocal Disorders'
        
        -- Catch-all for Dataset Completeness
        ELSE 'Other Chronic / Clinical'
    END AS disease_state,

    -- System Rollup for Specialty Matching [cite: 192, 211]
    CASE
        WHEN icd10_code LIKE 'F%'                                 THEN 'Neurological & Mental Health'
        WHEN icd10_code LIKE 'G%'                                 THEN 'Neurological & Mental Health'
        WHEN icd10_code LIKE 'I%'                                 THEN 'Cardiovascular'
        WHEN icd10_code LIKE 'J%' OR icd10_code LIKE 'G47%'       THEN 'Respiratory & Sleep'
        WHEN icd10_code LIKE 'E%'                                 THEN 'Endocrine & Metabolic'
        WHEN icd10_code LIKE 'C%'                                 THEN 'Oncology'
        ELSE 'General Medicine'
    END AS body_system
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
    -- Canonical Specialty Name [cite: 101, 109]
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
        WHEN Rndrng_Prvdr_Type LIKE '%Dermatology%'             THEN 'Dermatology'
        WHEN Rndrng_Prvdr_Type LIKE '%Nurse Practitioner%'      THEN 'Nurse Practitioner'
        WHEN Rndrng_Prvdr_Type LIKE '%Physician Assistant%'     THEN 'Physician Assistant'
        WHEN Rndrng_Prvdr_Type LIKE '%Physical Therapy%'        THEN 'Physical Therapy'
        ELSE Rndrng_Prvdr_Type
    END                                                         AS specialty_name,
    
    -- DOMAIN GROUPING (Matched to dim_diagnosis logic) [cite: 193, 206]
    CASE
        WHEN Rndrng_Prvdr_Type LIKE '%Cardiology%'              THEN 'Cardiovascular'
        WHEN Rndrng_Prvdr_Type LIKE '%Nephrology%'              THEN 'Renal & Urological'
        WHEN Rndrng_Prvdr_Type LIKE '%Endocrinology%'           THEN 'Endocrine & Metabolic'
        WHEN Rndrng_Prvdr_Type LIKE '%Pulmon%'                  THEN 'Respiratory & Sleep'
        WHEN Rndrng_Prvdr_Type LIKE '%Oncology%'
          OR Rndrng_Prvdr_Type LIKE '%Hematology%'              THEN 'Oncology'
        WHEN Rndrng_Prvdr_Type LIKE '%Neurology%'
          OR Rndrng_Prvdr_Type LIKE '%Psych%'                   THEN 'Neurological & Mental Health'
        WHEN Rndrng_Prvdr_Type LIKE '%Orthopedic%'              THEN 'Musculoskeletal'
        WHEN Rndrng_Prvdr_Type LIKE '%Gastro%'                  THEN 'Digestive'
        WHEN Rndrng_Prvdr_Type LIKE '%Dermat%'                  THEN 'Dermatology'
        WHEN Rndrng_Prvdr_Type LIKE '%Ophthalm%'                THEN 'Ophthalmology & Otology'
        WHEN Rndrng_Prvdr_Type LIKE '%Internal Medicine%'
          OR Rndrng_Prvdr_Type LIKE '%Family%'                  THEN 'Primary Care'
        ELSE 'Other Clinical'
    END                                                         AS specialty_domain
FROM uniform_resource_cms_provider
WHERE Rndrng_Prvdr_Type IS NOT NULL;

-- -----------------------------------------------------------------------------
-- PERFORMANCE INDEXES
-- -----------------------------------------------------------------------------

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
    SELECT 
        HCPCS_Cd AS hcpcs_code,
        TRIM(UPPER(Rndrng_Prvdr_Geo_Cd)) AS state_abbr,
        Place_Of_Srvc AS place_of_service_code,
        SUM(CAST(NULLIF(Tot_Benes, '') AS INTEGER)) AS total_beneficiaries,
        SUM(CAST(NULLIF(Tot_Srvcs, '') AS REAL)) AS total_services,
        SUM(CAST(NULLIF(Tot_Rndrng_Prvdrs, '') AS INTEGER)) AS total_rendering_providers,
        SUM(CAST(NULLIF(Tot_Srvcs, '') AS REAL) * CAST(NULLIF(Avg_Mdcr_Alowd_Amt, '') AS REAL)) AS total_allowed_amt,        
        SUM(CAST(NULLIF(Tot_Srvcs, '') AS REAL) * CAST(NULLIF(Avg_Mdcr_Pymt_Amt, '') AS REAL)) AS total_medicare_payment 
    FROM uniform_resource_cms_bygeography
    WHERE UPPER(Rndrng_Prvdr_Geo_Lvl) = 'STATE'
    GROUP BY 1, 2, 3
)
SELECT
    CASE 
        -- 1. SLEEP & RESPIRATORY (Critical for Sleep Apnea/COPD pilots)
        WHEN h.hcpcs_code BETWEEN '95800' AND '95811' THEN 'Sleep Medicine'
        WHEN h.hcpcs_code BETWEEN '94000' AND '94799' THEN 'Pulmonology'
        WHEN h.hcpcs_code IN ('G0237', 'G0238', 'G0239') THEN 'Pulmonology' -- COPD Rehab
        
        -- 2. NEURO & MENTAL HEALTH (Parkinson's / Alzheimer's)
        WHEN h.hcpcs_code BETWEEN '95812' AND '95999' THEN 'Neurology'
        WHEN h.hcpcs_code = '99483' THEN 'Neurology' -- Alzheimer's Cognitive Assess
        WHEN h.hcpcs_code BETWEEN '90791' AND '90899' THEN 'Psychiatry'
        
        -- 3. CARDIOVASCULAR (Heart Failure / Hypertension)
        WHEN h.hcpcs_code BETWEEN '93000' AND '93999' THEN 'Cardiology'
        WHEN h.hcpcs_code IN ('99490', '99439', 'G2058') THEN 'Cardiology' -- CCM for Heart Failure
        
        -- 4. PRIMARY CARE BASELINE
        WHEN h.hcpcs_code BETWEEN '99201' AND '99499' THEN 'Internal Medicine / PCP'
        
        ELSE 'Other Specialty'
    END AS specialty_name,    
    
    CASE 
        WHEN h.hcpcs_code BETWEEN '93000' AND '93999' THEN 'Cardiovascular'
        WHEN h.hcpcs_code BETWEEN '94000' AND '94799' THEN 'Respiratory & Sleep'
        WHEN h.hcpcs_code BETWEEN '95800' AND '95811' THEN 'Respiratory & Sleep'
        WHEN h.hcpcs_code IN ('G0237', 'G0238', 'G0239') THEN 'Respiratory & Sleep'
        WHEN h.hcpcs_code BETWEEN '95812' AND '95999' THEN 'Neurological & Mental Health'
        WHEN h.hcpcs_code BETWEEN '99201' AND '99499' THEN 'Primary Care'
        ELSE 'General Medicine'
    END AS specialty_domain,
    
    h.*
FROM base_geo h;

CREATE INDEX IF NOT EXISTS idx_fact_util_spec     ON fact_utilization(specialty_name);
CREATE INDEX IF NOT EXISTS idx_fact_util_hcpcs    ON fact_utilization(hcpcs_code); 
CREATE INDEX IF NOT EXISTS idx_fact_util_state    ON fact_utilization(state_abbr);
CREATE INDEX IF NOT EXISTS idx_fact_util_domain   ON fact_utilization(specialty_domain);
CREATE INDEX IF NOT EXISTS idx_fact_util_geo_spec ON fact_utilization(state_abbr, specialty_name);


DROP TABLE IF EXISTS master_evidence_hub;
-- Use this surgical JOIN to prevent data bleeding
DROP TABLE IF EXISTS master_evidence_hub;
CREATE TABLE master_evidence_hub AS
SELECT 
    d.disease_state,
    d.body_system,
    p.procedure_category,
    p.procedure_signal,
    f.specialty_name, 
    SUM(f.total_services) AS srvc_vol,
    SUM(f.total_beneficiaries) AS bene_vol,
    ROUND(CAST(SUM(f.total_services) AS REAL) / NULLIF(SUM(f.total_beneficiaries), 0), 2) AS interaction_density,
    SUM(f.total_allowed_amt) AS total_spend
FROM fact_utilization f
JOIN dim_procedure p ON f.hcpcs_code = p.hcpcs_code
JOIN dim_diagnosis d ON (
    -- ONLY join these specific anchors to these specific diseases
    (d.disease_state = 'Sleep Apnea' AND p.procedure_signal = 'Sleep Lab (High Intensity)') OR
    (d.disease_state = 'COPD' AND p.procedure_signal = 'Respiratory Rehab (High Freq)') OR
    (d.disease_state = 'Parkinsons Disease' AND p.procedure_signal = 'Neuro Assessment') OR
    (d.disease_state = 'Heart Failure' AND p.procedure_signal = 'Chronic Care Management') OR
    -- Use this for the "Low Intensity" baseline proof
    (d.disease_state = 'Hypertension' AND p.procedure_signal = 'Standard E/M Visit')
)
GROUP BY 1, 2, 3, 4, 5;


-- 3. CRITICAL: Add indexes so your report queries are fast
-- Indexing the Fact Table
CREATE INDEX idx_fact_hcpcs ON fact_utilization(hcpcs_code);
CREATE INDEX idx_fact_domain ON fact_utilization(specialty_domain);

-- Indexing the Dimension Tables
CREATE INDEX idx_dim_proc_hcpcs ON dim_procedure(hcpcs_code);

-- =============================================================================
-- SECTION 2B: SPECIALTY MARKET DYNAMICS TABLE  [FROM ORIGINAL — PRESERVED]
-- Adds specialty_dominance_ratio: what % of a given HCPCS code does each
-- specialty own nationally? Answers "does Nephrology own dialysis codes?"
-- Answers: "What % of Sleep Apnea diagnostics does Sleep Medicine own?"
-- =============================================================================

DROP TABLE IF EXISTS specialty_market_dynamics;
CREATE TABLE specialty_market_dynamics AS
WITH specialty_hcpcs_totals AS (
    SELECT 
        specialty_name,
        hcpcs_code,
        SUM(total_beneficiaries) AS spec_benes,
        SUM(total_services) AS spec_services 
    FROM fact_utilization
    GROUP BY 1, 2
),
global_hcpcs_totals AS (
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
    s.spec_benes,
    s.spec_services,
    g.global_benes,
    MIN(1.0, ROUND(CAST(s.spec_benes AS REAL) / NULLIF(g.global_benes, 0), 4)) AS specialty_dominance_ratio
FROM specialty_hcpcs_totals s
JOIN global_hcpcs_totals g ON s.hcpcs_code = g.hcpcs_code;


CREATE INDEX IF NOT EXISTS idx_smd_spec  ON specialty_market_dynamics(specialty_name);
CREATE INDEX IF NOT EXISTS idx_smd_hcpcs ON specialty_market_dynamics(hcpcs_code);


-- -----------------------------------------------------------------------------
-- condition_monitoring_proxy
-- Condition-level repeat-interaction proxy using ONLY monitoring-flagged HCPCS.
-- Adds disease-specific relevance filters so conditions do not collapse to a
-- single uniform value across the body-system bridge.
-- -----------------------------------------------------------------------------

DROP VIEW IF EXISTS condition_monitoring_proxy;
CREATE VIEW condition_monitoring_proxy AS
WITH disease_states AS (
    SELECT DISTINCT disease_state, body_system
    FROM dim_diagnosis
    WHERE disease_state != 'Other Chronic / Clinical'
),
monitoring_base AS (
    SELECT
        f.specialty_name,
        f.specialty_domain,
        f.hcpcs_code,
        SUM(f.total_services)       AS total_services,
        SUM(f.total_beneficiaries)  AS total_beneficiaries,
        SUM(f.total_medicare_payment) AS total_spend
    FROM fact_utilization f
    JOIN dim_procedure p ON f.hcpcs_code = p.hcpcs_code
    WHERE p.is_monitoring_flag = 1
    GROUP BY 1, 2, 3
),
matched_data AS (
    SELECT
        d.disease_state,
        d.body_system,
        mb.hcpcs_code,
        mb.total_services,
        mb.total_beneficiaries,
        mb.total_spend
    FROM disease_states d
    JOIN monitoring_base mb ON 1 = 1
    WHERE (
        (d.disease_state = 'Sleep Apnea' AND mb.hcpcs_code IN ('95810','95811')) OR
        (d.disease_state = 'COPD' AND mb.hcpcs_code = 'G0238') OR
        (d.body_system = 'Cardiovascular' AND mb.hcpcs_code LIKE '93%') OR
        (d.body_system = 'Neurological & Mental Health' AND mb.hcpcs_code BETWEEN '95812' AND '95999') OR
        (d.disease_state = 'Hypertension' AND mb.hcpcs_code = '99214')
    )
)
SELECT
    disease_state,
    body_system,
    COUNT(DISTINCT hcpcs_code) AS monitoring_hcpcs_count,
    SUM(total_services) AS monitoring_services,
    SUM(total_beneficiaries) AS monitoring_beneficiaries,
    ROUND(SUM(total_services) * 1.0 / NULLIF(SUM(total_beneficiaries), 0), 3) AS monitoring_services_per_beneficiary,
    SUM(total_spend) AS monitoring_total_spend,
    ROUND(SUM(total_spend) * 1.0 / NULLIF(SUM(total_beneficiaries), 0), 2) AS monitoring_spend_per_beneficiary,
    SUM(CASE WHEN (total_services*1.0/total_beneficiaries) >= 4 THEN 1 ELSE 0 END) AS high_frequency_hcpcs_count,
    RANK() OVER (ORDER BY SUM(total_services) * 1.0 / NULLIF(SUM(total_beneficiaries), 0) DESC) AS interaction_rank
FROM matched_data
GROUP BY 1, 2;

-- Now the CREATE TABLE will work perfectly
DROP TABLE IF EXISTS condition_monitoring_proxy_table;
CREATE TABLE condition_monitoring_proxy_table AS 
SELECT * FROM condition_monitoring_proxy;



CREATE INDEX IF NOT EXISTS idx_cmp_table_state ON condition_monitoring_proxy_table(disease_state);
CREATE INDEX IF NOT EXISTS idx_cmp_table_rank  ON condition_monitoring_proxy_table(interaction_rank);


-- =============================================================================
-- SECTION 3:  Business Question Layer
-- =============================================================================
-- -----------------------------------------------------------------------------
-- specialty_activity_summary
-- Executive KPI — total volume, patient reach, and spend per specialty.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS specialty_activity_summary;
CREATE TABLE specialty_activity_summary AS
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
--  specialty_economic_intensity
-- Combined spend-per-patient × visits-per-patient intensity index.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS specialty_economic_intensity;
CREATE TABLE specialty_economic_intensity AS
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
-- specialty_top_procedures
-- Top 10 procedures per specialty by volume, with spend rank alongside.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS specialty_top_procedures;

CREATE TABLE specialty_top_procedures AS
WITH procedure_summary AS (
    SELECT
        f.specialty_name,
        f.hcpcs_code,
        dp.procedure_description,
        dp.procedure_category,
        dp.is_monitoring_flag,        
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
        -- Standard rank for volume (Proves Market Reach)
        RANK() OVER (PARTITION BY specialty_name ORDER BY total_services DESC) AS service_rank,
        -- Standard rank for financial impact (Proves Economic Intensity)
        RANK() OVER (PARTITION BY specialty_name ORDER BY total_spend DESC)     AS spend_rank
    FROM procedure_summary
)
WHERE service_rank <= 10;

-- -----------------------------------------------------------------------------
--  specialty_market_concentration  [FROM ORIGINAL — PRESERVED + CLEANED]
-- Dominance-based ranking: which specialties own the highest share of key codes?
-- Use to validate that "Nephrology owns dialysis" or "Cardiology owns echo".
-- -----------------------------------------------------------------------------
-- specialty_market_concentration
DROP TABLE IF EXISTS specialty_market_concentration;
CREATE TABLE specialty_market_concentration AS
SELECT
    s.specialty_name, -- This IS in your table, we just need to use the right alias
    s.hcpcs_code,
    COALESCE(dp.procedure_description, 'Unknown Procedure') AS procedure_description,
    s.spec_services AS total_services,
    s.spec_benes AS total_benes,
    ROUND(s.specialty_dominance_ratio * 100, 1) AS pct_of_national_volume,
    -- This score proves the "Moat" for the B2B strategy
    ROUND(CAST(s.spec_services AS REAL) * s.specialty_dominance_ratio, 0) AS weighted_dominance_score,
    RANK() OVER (
        PARTITION BY s.specialty_name
        ORDER BY s.specialty_dominance_ratio DESC
    ) AS dominance_rank
FROM specialty_market_dynamics s
LEFT JOIN dim_procedure dp 
    ON s.hcpcs_code = dp.hcpcs_code
WHERE s.spec_benes > 10; --


-- chronic_interaction_density
DROP TABLE IF EXISTS chronic_interaction_density;
CREATE TABLE chronic_interaction_density AS
SELECT
    s.specialty_name,
    s.hcpcs_code,
    dp.procedure_description,
    s.spec_benes AS total_benes,
    ROUND(CAST(s.spec_services AS REAL) / NULLIF(s.spec_benes, 0), 2) AS interaction_density,
    CASE
        WHEN (CAST(s.spec_services AS REAL) / NULLIF(s.spec_benes, 0)) >= 12 THEN 'Tier 1 (SaaS Model)'
        WHEN (CAST(s.spec_services AS REAL) / NULLIF(s.spec_benes, 0)) < 4  THEN 'Tier 1 (Diagnostic Model)'
        ELSE 'Maintenance Case'
    END AS business_model_fit
FROM specialty_market_dynamics s
LEFT JOIN dim_procedure dp ON s.hcpcs_code = dp.hcpcs_code
WHERE s.spec_benes > 10;


--  monitoring_procedure_intensity
DROP TABLE IF EXISTS monitoring_procedure_intensity;
CREATE TABLE monitoring_procedure_intensity AS
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
--  dme_supply_refill_metrics  [MERGED: supply_category from refined +
--          refill_velocity naming from original]
-- DME/supply repeat-dispensing as a proxy for ongoing disease management.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dme_supply_refill_metrics;
CREATE TABLE dme_supply_refill_metrics AS
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
-- surgical_economic_metrics
-- High-intensity surgical clusters scored by anesthesia conversion weight.
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS surgical_economic_metrics;
CREATE TABLE surgical_economic_metrics AS
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
--  part_b_drug_intensity
-- Drug spend per specialty — key for oncology + nephrology market sizing.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS part_b_drug_intensity;
CREATE TABLE part_b_drug_intensity AS
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
--  geographic_market_opportunity
-- State-level volume + GPCI-adjusted spend per specialty for geo targeting.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS geographic_market_opportunity;
CREATE TABLE geographic_market_opportunity AS
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
--  facility_vs_office_split
-- Care setting mix per specialty — office dominance = higher patient ownership.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS facility_vs_office_split;
CREATE TABLE facility_vs_office_split AS
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
--  disease_state_icd_coverage
-- Code count per disease cluster — use to validate we haven't missed ICD ranges.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS disease_state_icd_coverage;
CREATE TABLE disease_state_icd_coverage AS
SELECT
    disease_state,
    body_system,
    COUNT(*) AS icd_code_count,
    -- Added a limit check note; SQLite GROUP_CONCAT is 1024 chars by default
    GROUP_CONCAT(icd10_code, ', ') AS sample_icd_codes
FROM dim_diagnosis
WHERE disease_state != 'General / Other'
GROUP BY 1, 2;


-- -----------------------------------------------------------------------------
-- disease_procedure_bridge
-- Makes the Disease → Procedure → Specialty relationship explicit and reusable.
-- Links dim_diagnosis (disease_state, body_system) to fact_utilization
-- (hcpcs_code, specialty_name, specialty_domain) via body_system ↔ specialty_domain.
-- Consumed by opportunity_scoring_view instead of duplicating join logic there.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS disease_procedure_bridge;
CREATE TABLE disease_procedure_bridge AS
WITH mapping_logic AS (
    SELECT 'Endocrine & Metabolic'       AS diag_sys, 'Endocrine & Metabolic'       AS spec_dom UNION ALL
    SELECT 'Cardiovascular',                           'Cardiovascular'                          UNION ALL
    SELECT 'Renal & Urological',                       'Renal & Urological'                      UNION ALL
    SELECT 'Respiratory',                              'Respiratory'                             UNION ALL
    SELECT 'Oncology',                                 'Oncology'                                UNION ALL
    SELECT 'Neurological & Mental Health',             'Neurological & Mental Health'            UNION ALL
    SELECT 'Musculoskeletal',                          'Musculoskeletal'
)
SELECT
    d.disease_state,
    d.body_system,
    f.hcpcs_code,
    p.procedure_description,
    p.procedure_category,
    f.specialty_name,
    f.specialty_domain,
    SUM(f.total_beneficiaries)  AS patient_volume,
    SUM(f.total_services)       AS service_volume,
    SUM(f.total_medicare_payment) AS total_medicare_payment,
    ROUND(SUM(f.total_services) * 1.0 / NULLIF(SUM(f.total_beneficiaries), 0), 2) AS avg_srvcs_per_patient
FROM (
    SELECT DISTINCT disease_state, body_system
    FROM dim_diagnosis
    WHERE disease_state != 'General / Other'
) d
JOIN mapping_logic m ON d.body_system = m.diag_sys
JOIN fact_utilization f ON (
    f.specialty_domain = m.spec_dom
    OR f.specialty_domain = 'Primary Care'
)
LEFT JOIN dim_procedure p ON f.hcpcs_code = p.hcpcs_code
GROUP BY 1, 2, 3, 4, 5, 6, 7
HAVING patient_volume >= 1;



-- 1. FINAL CONDITION DEEP DIVE (The "Evidence" Table)
-- This table captures unique clinical and economic markers for ALL 17+ states.
DROP TABLE IF EXISTS mdsd_condition_deep_dive;
CREATE TABLE mdsd_condition_deep_dive AS
SELECT 
    d.disease_state,
    d.body_system,
    f.specialty_name,
    SUM(f.total_beneficiaries) AS total_patients,
    SUM(f.total_services) AS total_services,
    ROUND(SUM(f.total_allowed_amt) / NULLIF(SUM(f.total_beneficiaries), 0), 2) AS avg_revenue_per_patient,
    ROUND(SUM(f.total_services) * 1.0 / NULLIF(SUM(f.total_beneficiaries), 0), 2) AS interaction_density,
    ROUND(AVG(COALESCE(smd.specialty_dominance_ratio, 0)) * 100, 1) AS specialty_share_pct
FROM fact_utilization f
JOIN dim_procedure p ON f.hcpcs_code = p.hcpcs_code
JOIN dim_diagnosis d ON (
    -- TIER 1 & 2 (Pilots & High Value)
    (d.disease_state = 'Sleep Apnea' AND p.procedure_signal = 'Sleep Lab (High Intensity)') OR
    (d.disease_state = 'COPD' AND p.procedure_signal = 'Respiratory Rehab (High Freq)') OR
    (d.disease_state = 'Parkinsons Disease' AND p.procedure_signal = 'Neuro Assessment') OR
    (d.disease_state = 'Heart Failure' AND p.procedure_signal = 'Chronic Care Management') OR
    
    -- TIER 3 & 4 (Baselines & Research - DO NOT LEAVE THESE BEHIND)
    (d.disease_state = 'Hypertension' AND p.procedure_signal = 'Standard E/M Visit') OR
    (d.disease_state = 'Type 2 Diabetes' AND p.procedure_signal = 'Lab Screening (A1C)') OR
    (d.disease_state = 'Alzheimers' AND p.procedure_signal = 'Cognitive Assessment') OR
    (d.disease_state = 'Major Depression' AND p.procedure_signal = 'Standard E/M Visit') OR
    (d.disease_state = 'Oncology' AND p.procedure_category = 'Oncology Administration')
)
LEFT JOIN specialty_market_dynamics smd ON f.specialty_name = smd.specialty_name AND f.hcpcs_code = smd.hcpcs_code
GROUP BY 1, 2, 3;


-- -----------------------------------------------------------------------------
--  opportunity_scoring_view
-- Final ranked table of disease-state × specialty clusters.
-- Tiers: >= 75 = Tier 1 High, >= 50 = Tier 2 Moderate, < 50 = Tier 3 Low
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS opportunity_scoring_view;

CREATE TABLE opportunity_scoring_view AS
WITH mapping_logic AS (
    -- Surgical Mapping: Connects specific disease states to their clinical 'anchors'
    -- This prevents the '142M patient volume' bleeding error.
    SELECT 'Sleep Apnea' AS disease, 'Sleep Lab (High Intensity)' AS signal UNION ALL
    SELECT 'COPD',                  'Respiratory Rehab (High Freq)'         UNION ALL
    SELECT 'Parkinsons Disease',     'Neuro Assessment'                      UNION ALL
    SELECT 'Heart Failure',          'Chronic Care Management'               UNION ALL
    SELECT 'Opioid Use Disorder',    'SUD/Opioid Treatment'                  UNION ALL
    SELECT 'Hypertension',           'Standard E/M Visit'                    UNION ALL
    SELECT 'Type 2 Diabetes',        'Lab Screening (A1C)'
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
    FROM (SELECT DISTINCT disease_state, body_system FROM dim_diagnosis) d
    JOIN mapping_logic m ON d.disease_state = m.disease -- Surgical Join Here
    JOIN dim_procedure p ON m.signal = p.procedure_signal
    JOIN fact_utilization f ON f.hcpcs_code = p.hcpcs_code
    LEFT JOIN specialty_market_dynamics smd 
      ON f.specialty_name = smd.specialty_name 
      AND f.hcpcs_code = smd.hcpcs_code
    GROUP BY 1, 2, 3, 4
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
        -- Score: 35% Volume, 35% Intensity, 30% Economics + Dominance Bonus
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

-- 1. Global Opportunity Matrix Table
--  This table captures the high-level scoring for all conditions mapped in your diagnostic dimension.
DROP TABLE IF EXISTS mdsd_global_opportunity_matrix;
CREATE TABLE mdsd_global_opportunity_matrix AS
SELECT 
    disease_state,
    opportunity_tier,
    composite_opportunity_score,
    patient_volume,
    avg_srvcs_per_patient AS interaction_density,
    total_spend_millions,
    market_concentration_pct
FROM opportunity_scoring_view
WHERE disease_state != 'Other Chronic / Clinical' -- Include everything else
ORDER BY composite_opportunity_score DESC;

DROP TABLE IF EXISTS mdsd_economic_intensity_proof;
CREATE TABLE mdsd_economic_intensity_proof AS
SELECT 
    specialty_name,
    specialty_domain,
    patient_reach,
    spend_per_patient AS avg_allowed_per_patient,
    avg_srvcs_per_patient AS interaction_frequency,
    economic_intensity_index
FROM specialty_economic_intensity
-- WHERE clause removed to include all comparative baselines
ORDER BY economic_intensity_index DESC;

DROP TABLE IF EXISTS mdsd_interaction_model_fit;
CREATE TABLE mdsd_interaction_model_fit AS
SELECT 
    disease_state,
    monitoring_services_per_beneficiary AS interaction_ratio,
    interaction_rank,
    CASE 
        WHEN monitoring_services_per_beneficiary >= 12 THEN 'Tier 1 (SaaS Model)'
        WHEN monitoring_services_per_beneficiary < 4 THEN 'Tier 1 (Diagnostic Model)'
        ELSE 'Review Case'
    END AS business_model_fit
FROM condition_monitoring_proxy_table
ORDER BY monitoring_services_per_beneficiary DESC;


-- -----------------------------------------------------------------------------
-- mdsd_specialty_gatekeepers
-- Identifies the specific specialties 'owning' the high-value CPT volume.
-- Used to prove B2B sales strategy in the final report.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS mdsd_specialty_gatekeepers;
CREATE TABLE mdsd_specialty_gatekeepers AS
SELECT DISTINCT
    d.disease_state,
    smd.specialty_name,
    dp.procedure_description,
    ROUND(smd.specialty_dominance_ratio * 100, 1) AS market_share_percentage,
    smd.spec_benes AS specialized_patient_reach,
    dominance_rank
FROM (
    SELECT 
        *,
        RANK() OVER (PARTITION BY hcpcs_code ORDER BY specialty_dominance_ratio DESC) as dominance_rank
    FROM specialty_market_dynamics
) smd
JOIN dim_procedure dp ON smd.hcpcs_code = dp.hcpcs_code
JOIN dim_diagnosis d ON (
    -- Universal Mapping: Ensures all 17+ states are included distinctly
    (d.disease_state = 'Sleep Apnea' AND dp.procedure_signal = 'Sleep Lab (High Intensity)') OR
    (d.disease_state = 'COPD' AND dp.procedure_signal = 'Respiratory Rehab (High Freq)') OR
    (d.disease_state = 'Parkinsons Disease' AND dp.procedure_signal = 'Neuro Assessment') OR
    (d.disease_state = 'Heart Failure' AND dp.procedure_signal = 'Chronic Care Management') OR
    (d.disease_state = 'Hypertension' AND dp.procedure_signal = 'Standard E/M Visit') OR
    (d.disease_state = 'Type 2 Diabetes' AND dp.procedure_signal = 'Lab Screening (A1C)') OR
    (d.disease_state = 'Alzheimers' AND dp.procedure_signal = 'Cognitive Assessment') OR
    (d.disease_state = 'Opioid Use Disorder' AND dp.procedure_signal = 'SUD/Opioid Treatment')
)
WHERE smd.dominance_rank = 1; -- Only take the primary gatekeeper for each code

--optimization for sleep-apnea-evidence.sql

-- 1. Ensure indexes exist on the large source tables
CREATE INDEX IF NOT EXISTS idx_global_matrix_lookup 
ON mdsd_global_opportunity_matrix(disease_state, opportunity_tier);

CREATE INDEX IF NOT EXISTS idx_model_fit_lookup 
ON mdsd_interaction_model_fit(disease_state);

-- 2. Clean start for the summary table
DROP TABLE IF EXISTS summary_market_overview;


CREATE INDEX IF NOT EXISTS idx_mdsd_global_scores 
ON mdsd_global_opportunity_matrix (composite_opportunity_score DESC, disease_state);

CREATE TABLE summary_market_overview (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    top_tier_name TEXT,
    top_tier_count INTEGER,
    htn_score REAL,
    htn_volume INTEGER,
    htn_spend REAL,
    copd_score REAL,
    copd_volume INTEGER,
    copd_density REAL,
    copd_model_fit TEXT,
    hf_score REAL,
    hf_volume INTEGER,
    hf_density REAL
);

-- 3. Populate with a single, idempotent INSERT
INSERT INTO summary_market_overview (
    id, top_tier_name, top_tier_count, 
    htn_score, htn_volume, htn_spend, 
    copd_score, copd_volume, copd_density, copd_model_fit, 
    hf_score, hf_volume, hf_density
)
SELECT 
    1,
    (SELECT opportunity_tier FROM mdsd_global_opportunity_matrix GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1),
    (SELECT COUNT(*) FROM mdsd_global_opportunity_matrix WHERE opportunity_tier = (
        SELECT opportunity_tier FROM mdsd_global_opportunity_matrix GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1
    )),
    (SELECT composite_opportunity_score FROM mdsd_global_opportunity_matrix WHERE disease_state = 'Hypertension' LIMIT 1),
    (SELECT patient_volume FROM mdsd_global_opportunity_matrix WHERE disease_state = 'Hypertension' LIMIT 1),
    (SELECT total_spend_millions FROM mdsd_global_opportunity_matrix WHERE disease_state = 'Hypertension' LIMIT 1),
    (SELECT composite_opportunity_score FROM mdsd_global_opportunity_matrix WHERE disease_state = 'COPD' LIMIT 1),
    (SELECT patient_volume FROM mdsd_global_opportunity_matrix WHERE disease_state = 'COPD' LIMIT 1),
    (SELECT interaction_density FROM mdsd_global_opportunity_matrix WHERE disease_state = 'COPD' LIMIT 1),
    (SELECT business_model_fit FROM mdsd_interaction_model_fit WHERE disease_state = 'COPD' LIMIT 1),
    (SELECT composite_opportunity_score FROM mdsd_global_opportunity_matrix WHERE disease_state = 'Heart Failure' LIMIT 1),
    (SELECT patient_volume FROM mdsd_global_opportunity_matrix WHERE disease_state = 'Heart Failure' LIMIT 1),
    (SELECT interaction_density FROM mdsd_global_opportunity_matrix WHERE disease_state = 'Heart Failure' LIMIT 1);



-- 1. Drop and recreate the formatted summary table
DROP TABLE IF EXISTS summary_disease_opportunity_list;

CREATE TABLE summary_disease_opportunity_list (
    disease_state TEXT,
    opportunity_tier TEXT,
    composite_score REAL,
    patient_volume INTEGER,
    interaction_density REAL,
    spend_millions REAL,
    market_concentration REAL
);

-- 2. Populate it with pre-sorted, pre-rounded data
INSERT INTO summary_disease_opportunity_list
SELECT
    disease_state,
    opportunity_tier,
    ROUND(composite_opportunity_score, 2),
    patient_volume,
    ROUND(interaction_density, 2),
    ROUND(total_spend_millions, 2),
    ROUND(market_concentration_pct, 1)
FROM mdsd_global_opportunity_matrix
ORDER BY composite_opportunity_score DESC;

-- 3. Add an index to the summary table just in case you sort by other columns later
CREATE INDEX idx_summary_disease_name ON summary_disease_opportunity_list(disease_state);

-- 1. Ensure indexes on source tables for the one-time build
CREATE INDEX IF NOT EXISTS idx_econ_proof_intensity ON mdsd_economic_intensity_proof(economic_intensity_index DESC);
CREATE INDEX IF NOT EXISTS idx_spec_activity_name ON specialty_activity_summary(specialty_name);
CREATE INDEX IF NOT EXISTS idx_spec_econ_name ON specialty_economic_intensity(specialty_name);

-- 2. Drop and recreate the narrative summary table
DROP TABLE IF EXISTS summary_specialty_narrative;

CREATE TABLE summary_specialty_narrative (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    narrative_text TEXT
);

-- 3. Populate with pre-formatted narrative
INSERT INTO summary_specialty_narrative (id, narrative_text)
SELECT 1,
    'Economic intensity provides a metric for the financial weight of clinical interventions relative to patient reach. '
    || 'The highest current intensity is ' || ti.specialty_name || ' (' || ti.specialty_domain || ') '
    || 'with an Economic Intensity Index of ' || ROUND(ti.economic_intensity_index, 2)
    || ', supported by ' || ROUND(ti.interaction_frequency, 2) || ' services per patient and $'
    || printf('%,.2f', ti.avg_allowed_per_patient) || ' spend per patient. '
    || 'Internal Medicine / PCP carries major scale with ' || printf('%,.0f', im.patient_reach)
    || ' patients and about $' || printf('%,.1f', im.total_medicare_spend / 1000000000.0) || 'B total Medicare spend. '
    || 'Pulmonology reflects the leanest economic model in its profile, with average cost per service of $'
    || printf('%,.2f', p.avg_cost_per_service) || ' and economic intensity '
    || ROUND(p.economic_intensity_index, 2) || '.'
FROM 
    (SELECT * FROM mdsd_economic_intensity_proof ORDER BY economic_intensity_index DESC LIMIT 1) ti,
    (SELECT * FROM specialty_activity_summary WHERE specialty_name = 'Internal Medicine / PCP' LIMIT 1) im,
    (SELECT * FROM specialty_economic_intensity WHERE specialty_name = 'Pulmonology' ORDER BY avg_cost_per_service ASC LIMIT 1) p;

    -- 1. Ensure indexes exist for the sorting columns
CREATE INDEX IF NOT EXISTS idx_econ_proof_sort ON mdsd_economic_intensity_proof(economic_intensity_index DESC);
CREATE INDEX IF NOT EXISTS idx_spec_activity_spend ON specialty_activity_summary(total_medicare_spend DESC);

-- 2. Drop and recreate the intensity summary table
DROP TABLE IF EXISTS summary_intensity_highlights;

CREATE TABLE summary_intensity_highlights (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    highlight_text TEXT
);

-- 3. Populate with pre-calculated narrative
INSERT INTO summary_intensity_highlights (id, highlight_text)
SELECT 1,
    ti.specialty_name || ' exhibits the highest economic intensity at '
    || ROUND(ti.economic_intensity_index, 2)
    || ', indicating a model driven by frequent ('
    || ROUND(ti.interaction_frequency, 2) || ' services per patient) and high-value ($'
    || printf('%,.2f', ti.avg_allowed_per_patient) || ' per patient) interactions. '
    || ts.specialty_name || ' manages the largest absolute Medicare spend at about $'
    || printf('%,.1f', ts.total_medicare_spend / 1000000000.0) || 'B.'
FROM 
    (SELECT specialty_name, economic_intensity_index, interaction_frequency, avg_allowed_per_patient 
     FROM mdsd_economic_intensity_proof 
     ORDER BY economic_intensity_index DESC LIMIT 1) ti,
    (SELECT specialty_name, total_medicare_spend 
     FROM specialty_activity_summary 
     ORDER BY total_medicare_spend DESC LIMIT 1) ts;


     -- 1. Ensure index exists for fast sorting during the build
CREATE INDEX IF NOT EXISTS idx_econ_intensity_value 
ON mdsd_economic_intensity_proof(economic_intensity_index DESC);

-- 2. Drop and recreate the chart summary table
DROP TABLE IF EXISTS summary_chart_economic_intensity;

CREATE TABLE summary_chart_economic_intensity (
    specialty_name TEXT,
    intensity_value REAL
);

-- 3. Populate with pre-sorted, pre-rounded top 12 specialties
INSERT INTO summary_chart_economic_intensity (specialty_name, intensity_value)
SELECT 
    specialty_name, 
    ROUND(economic_intensity_index, 2)
FROM mdsd_economic_intensity_proof
ORDER BY economic_intensity_index DESC
LIMIT 12;


-- 1. Ensure index exists for the sort column
CREATE INDEX IF NOT EXISTS idx_econ_intensity_master 
ON mdsd_economic_intensity_proof(economic_intensity_index DESC);

-- 2. Drop and recreate the table summary
DROP TABLE IF EXISTS summary_table_economic_intensity;

CREATE TABLE summary_table_economic_intensity (
    specialty_name TEXT,
    specialty_domain TEXT,
    patient_reach INTEGER,
    avg_allowed REAL,
    interaction_freq REAL,
    intensity_index REAL
);

-- 3. Populate with pre-sorted, pre-rounded data (Top 20)
INSERT INTO summary_table_economic_intensity
SELECT
    specialty_name,
    specialty_domain,
    patient_reach,
    ROUND(avg_allowed_per_patient, 2),
    ROUND(interaction_frequency, 2),
    ROUND(economic_intensity_index, 2)
FROM mdsd_economic_intensity_proof
ORDER BY economic_intensity_index DESC
LIMIT 20;


-- 1. Indexes to speed up the pre-calculation
CREATE INDEX IF NOT EXISTS idx_model_fit_ratio ON mdsd_interaction_model_fit(interaction_ratio);
CREATE INDEX IF NOT EXISTS idx_gatekeepers_lookup ON mdsd_specialty_gatekeepers(disease_state, market_share_percentage DESC);

-- 2. Drop and recreate the narrative summary table
DROP TABLE IF EXISTS summary_gatekeeper_narrative;

CREATE TABLE summary_gatekeeper_narrative (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    narrative_text TEXT
);

-- 3. Populate with the pre-baked string
INSERT INTO summary_gatekeeper_narrative (id, narrative_text)
SELECT 1,
    'Interaction model fit indicates distinct business patterns based on provider-patient engagement frequency. '
    || hm.disease_state || ' aligns to ' || hm.business_model_fit 
    || ' with interaction ratio ' || ROUND(hm.interaction_ratio, 2)
    || ', signaling suitability for continuous or high-frequency digital monitoring. '
    || lm.disease_state || ' aligns to ' || lm.business_model_fit 
    || ' with interaction ratio ' || ROUND(lm.interaction_ratio, 2)
    || ', favoring periodic assessment. Gatekeeper dominance remains concentrated: '
    || hfg.specialty_name || ' controls ' || ROUND(hfg.market_share_percentage, 1)
    || '% of Heart Failure gatekeeper activity, while '
    || cg.specialty_name || ' controls ' || ROUND(cg.market_share_percentage, 1)
    || '% of COPD gatekeeper activity.'
FROM 
    (SELECT disease_state, interaction_ratio, business_model_fit FROM mdsd_interaction_model_fit ORDER BY interaction_ratio DESC LIMIT 1) hm,
    (SELECT disease_state, interaction_ratio, business_model_fit FROM mdsd_interaction_model_fit ORDER BY interaction_ratio ASC LIMIT 1) lm,
    (SELECT specialty_name, market_share_percentage FROM mdsd_specialty_gatekeepers WHERE disease_state = 'Heart Failure' ORDER BY market_share_percentage DESC LIMIT 1) hfg,
    (SELECT specialty_name, market_share_percentage FROM mdsd_specialty_gatekeepers WHERE disease_state = 'COPD' ORDER BY market_share_percentage DESC LIMIT 1) cg;


-- 1. Ensure index exists for fast sorting during the build
CREATE INDEX IF NOT EXISTS idx_interaction_ratio_sort 
ON mdsd_interaction_model_fit(interaction_ratio DESC);

-- 2. Drop and recreate the chart summary table
DROP TABLE IF EXISTS summary_chart_interaction_models;

CREATE TABLE summary_chart_interaction_models (
    disease_state TEXT,
    business_model_fit TEXT,
    rounded_ratio REAL
);

-- 3. Populate with pre-sorted and pre-rounded data
INSERT INTO summary_chart_interaction_models (disease_state, business_model_fit, rounded_ratio)
SELECT 
    disease_state, 
    business_model_fit, 
    ROUND(interaction_ratio, 2)
FROM mdsd_interaction_model_fit
ORDER BY interaction_ratio DESC;


-- 1. Create a composite index to make the initial build and future refreshes instant
CREATE INDEX IF NOT EXISTS idx_gatekeeper_perf 
ON mdsd_specialty_gatekeepers(market_share_percentage DESC, specialized_patient_reach DESC);

-- 2. Drop and recreate the table summary
DROP TABLE IF EXISTS summary_table_gatekeepers;

CREATE TABLE summary_table_gatekeepers (
    disease_state TEXT,
    specialty_name TEXT,
    procedure_desc TEXT,
    market_share REAL,
    patient_reach INTEGER,
    dominance_rank INTEGER
);

-- 3. Populate with pre-sorted and pre-rounded data
INSERT INTO summary_table_gatekeepers
SELECT
    disease_state,
    specialty_name,
    procedure_description,
    ROUND(market_share_percentage, 1),
    specialized_patient_reach,
    dominance_rank
FROM mdsd_specialty_gatekeepers
ORDER BY market_share_percentage DESC, specialized_patient_reach DESC;

-- 1. Create index to help with the initial build (if not already there)
CREATE INDEX IF NOT EXISTS idx_facility_split_name ON facility_vs_office_split(specialty_name);

-- 2. Drop and recreate the site mix summary table
DROP TABLE IF EXISTS summary_site_mix_narrative;

CREATE TABLE summary_site_mix_narrative (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    narrative_text TEXT
);

-- 3. Populate with pre-calculated narrative
INSERT INTO summary_site_mix_narrative (id, narrative_text)
WITH raw_agg AS (
    SELECT
        SUM(facility_services) AS f_serv,
        SUM(office_services) AS o_serv,
        SUM(total_services) AS t_serv,
        SUM(facility_spend) AS f_spend,
        SUM(office_spend) AS o_spend
    FROM facility_vs_office_split
    WHERE specialty_name LIKE '%Internal Medicine%'
       OR specialty_name LIKE '%PCP%'
),
calc AS (
    SELECT
        *,
        CASE WHEN t_serv > 0 THEN (f_serv * 100.0) / t_serv ELSE 0 END AS f_share,
        CASE WHEN t_serv > 0 THEN (o_serv * 100.0) / t_serv ELSE 0 END AS o_share,
        CASE WHEN f_serv > 0 THEN f_spend / f_serv ELSE 0 END AS f_cps,
        CASE WHEN o_serv > 0 THEN o_spend / o_serv ELSE 0 END AS o_cps
    FROM raw_agg
)
SELECT 
    1,
    'Comparing Internal Medicine / PCP economics across care settings shows that '
    || CASE
        WHEN f_cps > o_cps THEN 'facility-based services carry the higher cost-per-service burden'
        WHEN f_cps < o_cps THEN 'office-based services carry the higher cost-per-service burden'
        ELSE 'both settings are currently at similar cost-per-service levels'
       END
    || '. Facility services represent ' || ROUND(f_share, 1) || '% of total volume and account for about $'
    || printf('%,.2f', f_spend / 1000000000.0) || 'B in spend, while office services represent '
    || ROUND(o_share, 1) || '% of volume with about $'
    || printf('%,.2f', o_spend / 1000000000.0) || 'B in spend. Cost-per-service is approximately $'
    || printf('%,.2f', f_cps) || ' in facility settings versus $'
    || printf('%,.2f', o_cps) || ' in office settings.'
FROM calc;

-- 1. Ensure index on the lookup columns
CREATE INDEX IF NOT EXISTS idx_facility_split_lookup 
ON facility_vs_office_split(specialty_name, specialty_domain);

-- 2. Drop and recreate the benchmark summary table
DROP TABLE IF EXISTS summary_benchmark_specialties;

CREATE TABLE summary_benchmark_specialties (
    display_order INTEGER PRIMARY KEY,
    specialty_name TEXT,
    facility_spend_text TEXT,
    office_spend_text TEXT,
    office_pct_text TEXT
);

-- 3. Populate with pre-calculated, pre-formatted data
INSERT INTO summary_benchmark_specialties
SELECT
    CASE
        WHEN specialty_name = 'Internal Medicine / PCP' THEN 1
        WHEN specialty_name = 'Cardiology' AND specialty_domain = 'Primary Care' THEN 2
        WHEN specialty_name = 'Neurology' AND specialty_domain = 'Primary Care' THEN 3
        WHEN specialty_name = 'Pulmonology' THEN 4
    END AS display_order,
    CASE
        WHEN specialty_name = 'Internal Medicine / PCP' THEN 'Internal Medicine / PCP'
        WHEN specialty_name = 'Cardiology' AND specialty_domain = 'Primary Care' THEN 'Cardiology (Primary Care)'
        WHEN specialty_name = 'Neurology' AND specialty_domain = 'Primary Care' THEN 'Neurology (Primary Care)'
        WHEN specialty_name = 'Pulmonology' THEN 'Pulmonology'
    END AS specialty_name,
    '$' || printf('%,.0f', ROUND(facility_spend, 0)),
    '$' || printf('%,.0f', ROUND(office_spend, 0)),
    ROUND(office_pct, 1) || '%'
FROM facility_vs_office_split
WHERE specialty_name = 'Internal Medicine / PCP'
   OR (specialty_name = 'Cardiology' AND specialty_domain = 'Primary Care')
   OR (specialty_name = 'Neurology' AND specialty_domain = 'Primary Care')
   OR specialty_name = 'Pulmonology'
ORDER BY display_order;


-- 1. Ensure lookup indexes
CREATE INDEX IF NOT EXISTS idx_fac_split_bench ON facility_vs_office_split(specialty_name, specialty_domain);

-- 2. Clean start for benchmark and takeaway summaries
DROP TABLE IF EXISTS summary_service_benchmarks;
DROP TABLE IF EXISTS summary_im_takeaway;

CREATE TABLE summary_service_benchmarks (
    display_order INTEGER PRIMARY KEY,
    specialty_name TEXT,
    total_services_rounded INTEGER
);

CREATE TABLE summary_im_takeaway (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    takeaway_text TEXT
);

-- 3. Populate Benchmark Table
INSERT INTO summary_service_benchmarks
SELECT
    CASE
        WHEN specialty_name = 'Internal Medicine / PCP' THEN 1
        WHEN specialty_name = 'Cardiology' AND specialty_domain = 'Primary Care' THEN 2
        WHEN specialty_name = 'Neurology' AND specialty_domain = 'Primary Care' THEN 3
        WHEN specialty_name = 'Pulmonology' THEN 4
    END AS display_order,
    CASE
        WHEN specialty_name = 'Internal Medicine / PCP' THEN 'Internal Medicine / PCP'
        WHEN specialty_name = 'Cardiology' AND specialty_domain = 'Primary Care' THEN 'Cardiology (Primary Care)'
        WHEN specialty_name = 'Neurology' AND specialty_domain = 'Primary Care' THEN 'Neurology (Primary Care)'
        WHEN specialty_name = 'Pulmonology' THEN 'Pulmonology'
    END AS specialty_name,
    ROUND(total_services, 0)
FROM facility_vs_office_split
WHERE specialty_name = 'Internal Medicine / PCP'
   OR (specialty_name = 'Cardiology' AND specialty_domain = 'Primary Care')
   OR (specialty_name = 'Neurology' AND specialty_domain = 'Primary Care')
   OR specialty_name = 'Pulmonology'
ORDER BY display_order;

-- 4. Populate Takeaway Text
INSERT INTO summary_im_takeaway (id, takeaway_text)
SELECT 1,
    'Key takeaway: Internal Medicine / PCP remains a major anchor for facility-based coordination, with about '
    || printf('%,.0f', facility_services)
    || ' services delivered in facility settings. Across both settings, this specialty accounts for approximately $'
    || printf('%,.1f', (facility_spend + office_spend) / 1000000000.0)
    || 'B in Medicare spend and '
    || printf('%,.0f', total_services)
    || ' total services.'
FROM facility_vs_office_split
WHERE specialty_name = 'Internal Medicine / PCP'
LIMIT 1;


-- 1. Create index for the build process
CREATE INDEX IF NOT EXISTS idx_fac_split_spec_name ON facility_vs_office_split(specialty_name);

-- 2. Drop and recreate the pie chart summary
DROP TABLE IF EXISTS summary_chart_im_distribution;

CREATE TABLE summary_chart_im_distribution (
    label TEXT,
    value_rounded INTEGER
);

-- 3. Populate with a SINGLE pass over the data (no UNION needed)
INSERT INTO summary_chart_im_distribution (label, value_rounded)
WITH aggregated AS (
    SELECT 
        SUM(office_services) AS total_office,
        SUM(facility_services) AS total_facility
    FROM facility_vs_office_split
    WHERE specialty_name LIKE '%Internal Medicine%'
       OR specialty_name LIKE '%PCP%'
)
SELECT 'Office Services', ROUND(total_office, 0) FROM aggregated
UNION ALL
SELECT 'Facility Services', ROUND(total_facility, 0) FROM aggregated;

-- 1. Ensure index exists for the search columns
CREATE INDEX IF NOT EXISTS idx_fac_split_naming ON facility_vs_office_split(specialty_name);

-- 2. Drop and recreate the site-mix detailed summary table
DROP TABLE IF EXISTS summary_table_im_site_mix;

CREATE TABLE summary_table_im_site_mix (
    specialty TEXT,
    domain TEXT,
    fac_serv INTEGER,
    off_serv INTEGER,
    tot_serv INTEGER,
    off_pct REAL,
    fac_spend_b REAL,
    off_spend_b REAL,
    fac_cost_per_serv REAL,
    off_cost_per_serv REAL
);

-- 3. Populate with pre-calculated metrics
INSERT INTO summary_table_im_site_mix
SELECT
    specialty_name,
    specialty_domain,
    ROUND(facility_services, 0),
    ROUND(office_services, 0),
    ROUND(total_services, 0),
    ROUND(office_pct, 1),
    ROUND(facility_spend / 1000000000.0, 2),
    ROUND(office_spend / 1000000000.0, 2),
    ROUND(facility_spend / NULLIF(facility_services, 0), 2),
    ROUND(office_spend / NULLIF(office_services, 0), 2)
FROM facility_vs_office_split
WHERE specialty_name LIKE '%Internal Medicine%'
   OR specialty_name LIKE '%PCP%';

-- 1. Create indexes for the source tables to speed up the build
CREATE INDEX IF NOT EXISTS idx_monitoring_spec ON monitoring_procedure_intensity(specialty_name, specialty_domain);
CREATE INDEX IF NOT EXISTS idx_condition_proxy_disease ON condition_monitoring_proxy_table(disease_state);

-- 2. Drop and recreate the narrative summary table
DROP TABLE IF EXISTS summary_monitoring_narrative;

CREATE TABLE summary_monitoring_narrative (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    narrative_text TEXT
);

-- 3. Populate with pre-calculated, formatted narrative
INSERT INTO summary_monitoring_narrative (id, narrative_text)
SELECT 1,
    'Monitoring intensity identifies which diseases and specialties require the most longitudinal oversight. '
    || sm.specialty_name || ' shows a high monitoring share at ' || ROUND(sm.monitoring_pct, 1)
    || '%, indicating that a substantial portion of service volume is tied to ongoing tracking. '
    || cp.specialty_name || ' (' || cp.specialty_domain || ') also has high monitoring reliance at '
    || ROUND(cp.monitoring_pct, 1) || '%, while '
    || np.specialty_name || ' (' || np.specialty_domain || ') is at '
    || ROUND(np.monitoring_pct, 1) || '%, suggesting a more episodic or assessment-oriented pattern. '
    || 'At the condition level, Hypertension carries very large monitoring volume ('
    || printf('%,.0f', h.monitoring_services) || ' services), but its monitoring spend per beneficiary ($'
    || printf('%,.2f', h.monitoring_spend_per_beneficiary) || ') remains below COPD ($'
    || printf('%,.2f', c.monitoring_spend_per_beneficiary) || '), reinforcing the higher-intensity economics of respiratory monitoring.'
FROM 
    (SELECT specialty_name, monitoring_pct FROM monitoring_procedure_intensity WHERE specialty_name = 'Sleep Medicine' ORDER BY total_volume DESC LIMIT 1) sm,
    (SELECT specialty_name, specialty_domain, monitoring_pct FROM monitoring_procedure_intensity WHERE specialty_name = 'Cardiology' AND specialty_domain = 'Primary Care' LIMIT 1) cp,
    (SELECT specialty_name, specialty_domain, monitoring_pct FROM monitoring_procedure_intensity WHERE specialty_name = 'Neurology' AND specialty_domain = 'Primary Care' LIMIT 1) np,
    (SELECT monitoring_services, monitoring_spend_per_beneficiary FROM condition_monitoring_proxy_table WHERE disease_state = 'Hypertension' LIMIT 1) h,
    (SELECT monitoring_spend_per_beneficiary FROM condition_monitoring_proxy_table WHERE disease_state = 'COPD' LIMIT 1) c;


-- 1. Ensure index exists for fast sorting during the build
CREATE INDEX IF NOT EXISTS idx_monitoring_intensity_sort 
ON condition_monitoring_proxy_table(monitoring_services_per_beneficiary DESC);

-- 2. Drop and recreate the chart summary table
DROP TABLE IF EXISTS summary_chart_monitoring_intensity;

CREATE TABLE summary_chart_monitoring_intensity (
    disease_state TEXT,
    intensity_value REAL
);

-- 3. Populate with pre-sorted, pre-rounded data
INSERT INTO summary_chart_monitoring_intensity (disease_state, intensity_value)
SELECT 
    disease_state, 
    ROUND(monitoring_services_per_beneficiary, 2)
FROM condition_monitoring_proxy_table
ORDER BY monitoring_services_per_beneficiary DESC;

-- 1. Index the percentage column to make the build and future refreshes instant
CREATE INDEX IF NOT EXISTS idx_monitoring_pct_sort 
ON monitoring_procedure_intensity(monitoring_pct DESC);

-- 2. Drop and recreate the monitoring summary table
DROP TABLE IF EXISTS summary_table_monitoring_intensity;

CREATE TABLE summary_table_monitoring_intensity (
    specialty_name TEXT,
    mon_vol INTEGER,
    tot_vol INTEGER,
    mon_pct REAL,
    mon_spend_m REAL,
    tot_spend_m REAL
);

-- 3. Populate with pre-sorted, pre-calculated Top 20 rows
INSERT INTO summary_table_monitoring_intensity
SELECT
    specialty_name,
    ROUND(monitoring_volume, 0),
    ROUND(total_volume, 0),
    ROUND(monitoring_pct, 1),
    ROUND(monitoring_spend / 1000000.0, 2),
    ROUND(total_spend / 1000000.0, 2)
FROM monitoring_procedure_intensity
ORDER BY monitoring_pct DESC
LIMIT 20;

-- 1. Create indexes to speed up the pre-calculation build
CREATE INDEX IF NOT EXISTS idx_drug_intensity_gm ON part_b_drug_intensity(specialty_domain, total_drug_spend DESC);
CREATE INDEX IF NOT EXISTS idx_dme_refill_velocity ON dme_supply_refill_metrics(refill_velocity DESC);

-- 2. Drop and recreate the drug summary table
DROP TABLE IF EXISTS summary_drug_supply_narrative;

CREATE TABLE summary_drug_supply_narrative (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    narrative_text TEXT
);

-- 3. Populate with pre-calculated, formatted narrative
INSERT INTO summary_drug_supply_narrative (id, narrative_text)
SELECT 1,
    'Financial intensity in General Medicine is increasingly shaped by drug mix and replenishment cadence, not service volume alone. '
    || 'The largest total-spend driver is ' || tt.procedure_description || ' (' || tt.hcpcs_code || ') at about $'
    || printf('%,.2f', tt.total_drug_spend / 1000000000.0) || 'B total spend. '
    || 'The highest per-patient financial intensity comes from ' || ts.procedure_description || ' (' || ts.hcpcs_code || ') at approximately $'
    || printf('%,.2f', ts.drug_spend_per_patient) || ' per patient. '
    || 'By administration volume, ' || tv.procedure_description || ' (' || tv.hcpcs_code || ') leads with '
    || printf('%,.0f', tv.total_drug_administrations) || ' administrations. '
    || CASE 
        WHEN j.hcpcs_code IS NOT NULL THEN 'For DME/supply engagement, J7060 (' || j.supply_item || ') shows refill velocity ' || printf('%,.2f', j.refill_velocity) || ', supporting sustained replenishment-cycle planning.'
        ELSE 'For DME/supply engagement, the current top refill item is ' || tr.hcpcs_code || ' (' || tr.supply_item || ') with refill velocity ' || printf('%,.2f', tr.refill_velocity) || '.'
       END
FROM 
    (SELECT hcpcs_code, procedure_description, total_drug_spend FROM part_b_drug_intensity WHERE specialty_domain = 'General Medicine' ORDER BY total_drug_spend DESC LIMIT 1) tt,
    (SELECT procedure_description, hcpcs_code, drug_spend_per_patient FROM part_b_drug_intensity WHERE specialty_domain = 'General Medicine' ORDER BY drug_spend_per_patient DESC LIMIT 1) ts,
    (SELECT procedure_description, hcpcs_code, total_drug_administrations FROM part_b_drug_intensity WHERE specialty_domain = 'General Medicine' ORDER BY total_drug_administrations DESC LIMIT 1) tv,
    (SELECT hcpcs_code, supply_item, refill_velocity FROM dme_supply_refill_metrics WHERE hcpcs_code = 'J7060' LIMIT 1) j,
    (SELECT hcpcs_code, supply_item, refill_velocity FROM dme_supply_refill_metrics ORDER BY refill_velocity DESC LIMIT 1) tr;


-- 1. Ensure index for fast aggregation
CREATE INDEX IF NOT EXISTS idx_drug_spend_genmed ON part_b_drug_intensity(specialty_domain, total_drug_spend DESC);

-- 2. Drop and recreate the Pareto summary table
-- 1. Reset the summary table
DROP TABLE IF EXISTS summary_pareto_drug_insight;

CREATE TABLE summary_pareto_drug_insight (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    insight_text TEXT
);

-- 2. Populate with the fix for the column reference error
INSERT INTO summary_pareto_drug_insight (id, insight_text)
WITH ds AS (
    SELECT
        procedure_description,
        hcpcs_code,
        SUM(total_drug_spend) AS total_spend, -- Renamed here
        ROW_NUMBER() OVER (ORDER BY SUM(total_drug_spend) DESC) AS rn
    FROM part_b_drug_intensity
    WHERE specialty_domain = 'General Medicine'
    GROUP BY hcpcs_code, procedure_description
),
grand AS (
    -- FIXED: Sum 'total_spend' (the name defined in 'ds'), not the original column
    SELECT SUM(total_spend) AS grand_total FROM ds
),
top_stats AS (
    SELECT 
        MAX(CASE WHEN rn = 1 THEN procedure_description END) as p1_desc,
        MAX(CASE WHEN rn = 1 THEN hcpcs_code END) as p1_code,
        MAX(CASE WHEN rn = 1 THEN total_spend END) as p1_spend,
        MAX(CASE WHEN rn = 2 THEN procedure_description END) as p2_desc,
        MAX(CASE WHEN rn = 2 THEN hcpcs_code END) as p2_code,
        MAX(CASE WHEN rn = 2 THEN total_spend END) as p2_spend
    FROM ds WHERE rn <= 2
)
SELECT 
    1,
    'This Pareto analysis highlights the concentration of Medicare spend within specific high-intensity drugs. '
    || COALESCE(p1_desc, 'the leading drug') || ' (' || COALESCE(p1_code, 'N/A') || ') and ' 
    || COALESCE(p2_desc, 'the second drug') || ' (' || COALESCE(p2_code, 'N/A') || ') '
    || 'represent the highest-spend drugs in General Medicine, together accounting for '
    || printf('%.1f', (COALESCE(p1_spend, 0) + COALESCE(p2_spend, 0)) * 100.0 / NULLIF((SELECT grand_total FROM grand), 0))
    || '% of total Part B drug spend in this segment. '
    || COALESCE(p1_desc, 'The leading agent') || ' alone accounts for over $'
    || printf('%,.2f', COALESCE(p1_spend, 0) / 1000000000.0)
    || ' billion, underscoring the outsized financial impact of a narrow set of agents.'
FROM top_stats;

-- 1. Ensure index for fast aggregation
CREATE INDEX IF NOT EXISTS idx_drug_spend_pareto ON part_b_drug_intensity(specialty_domain, total_drug_spend DESC);

-- 2. Drop and recreate the Pareto chart summary table
DROP TABLE IF EXISTS summary_chart_drug_pareto;

CREATE TABLE summary_chart_drug_pareto (
    rn INTEGER PRIMARY KEY,
    hcpcs_code TEXT,
    procedure_desc TEXT,
    cumulative_share_pct REAL
);

-- 3. Populate with pre-calculated cumulative values
INSERT INTO summary_chart_drug_pareto (rn, hcpcs_code, procedure_desc, cumulative_share_pct)
WITH drug_rank AS (
    SELECT
        hcpcs_code,
        procedure_description,
        SUM(total_drug_spend) AS total_spend
    FROM part_b_drug_intensity
    WHERE specialty_domain = 'General Medicine'
    GROUP BY hcpcs_code, procedure_description
),
ranked_list AS (
    SELECT 
        hcpcs_code,
        procedure_description,
        total_spend,
        ROW_NUMBER() OVER (ORDER BY total_spend DESC) AS rn,
        SUM(total_spend) OVER () AS grand_total
    FROM drug_rank
),
pareto_calc AS (
    SELECT
        rn,
        hcpcs_code,
        procedure_description,
        SUM(total_spend) OVER (ORDER BY rn) AS cumulative_spend,
        grand_total
    FROM ranked_list
    WHERE rn <= 15
)
SELECT 
    rn, 
    hcpcs_code, 
    procedure_description, 
    ROUND((cumulative_spend * 100.0) / NULLIF(grand_total, 0), 2)
FROM pareto_calc;


-- 1. Ensure index for fast aggregation and sorting
CREATE INDEX IF NOT EXISTS idx_drug_spend_genmed_agg 
ON part_b_drug_intensity(specialty_domain, total_drug_spend DESC);

-- 2. Drop and recreate the Pareto series summary table
DROP TABLE IF EXISTS summary_drug_pareto_series;

CREATE TABLE summary_drug_pareto_series (
    rn INTEGER PRIMARY KEY,
    hcpcs_code TEXT,
    procedure_desc TEXT,
    cumulative_share_pct REAL
);

-- 3. Populate with pre-calculated values
INSERT INTO summary_drug_pareto_series (rn, hcpcs_code, procedure_desc, cumulative_share_pct)
WITH drug_rank AS (
    SELECT
        hcpcs_code,
        procedure_description,
        SUM(total_drug_spend) AS total_spend
    FROM part_b_drug_intensity
    WHERE specialty_domain = 'General Medicine'
    GROUP BY hcpcs_code, procedure_description
),
ranked_data AS (
    SELECT 
        rn, hcpcs_code, procedure_description, total_spend,
        SUM(total_spend) OVER (ORDER BY rn) AS cumulative_spend,
        SUM(total_spend) OVER () AS grand_total
    FROM (
        SELECT *, ROW_NUMBER() OVER (ORDER BY total_spend DESC) AS rn 
        FROM drug_rank
    )
    WHERE rn <= 15
)
SELECT 
    rn, 
    hcpcs_code, 
    procedure_description, 
    ROUND((cumulative_spend * 100.0) / NULLIF(grand_total, 0), 2)
FROM ranked_data;


-- 1. Index to speed up the initial build
CREATE INDEX IF NOT EXISTS idx_part_b_spend_genmed 
ON part_b_drug_intensity(specialty_domain, total_drug_spend DESC);

-- 2. Drop and recreate the Pareto table summary
DROP TABLE IF EXISTS summary_table_drug_pareto;

CREATE TABLE summary_table_drug_pareto (
    rn INTEGER PRIMARY KEY,
    hcpcs_code TEXT,
    drug_name TEXT,
    spend_m REAL,
    cumulative_share_pct REAL
);

-- 3. Populate with pre-calculated values
INSERT INTO summary_table_drug_pareto
WITH drug_rank AS (
    SELECT
        hcpcs_code,
        procedure_description,
        SUM(total_drug_spend) AS total_spend
    FROM part_b_drug_intensity
    WHERE specialty_domain = 'General Medicine'
    GROUP BY hcpcs_code, procedure_description
),
pareto_calc AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY total_spend DESC) AS rn,
        hcpcs_code,
        procedure_description,
        total_spend,
        SUM(total_spend) OVER (ORDER BY total_spend DESC) AS cumulative_spend,
        SUM(total_spend) OVER () AS grand_total
    FROM drug_rank
)
SELECT
    rn,
    hcpcs_code,
    procedure_description,
    ROUND(total_spend / 1000000.0, 2),
    ROUND((cumulative_spend * 100.0) / NULLIF(grand_total, 0), 2)
FROM pareto_calc
WHERE rn <= 15;

-- 1. Index for fast sorting during the build
CREATE INDEX IF NOT EXISTS idx_dme_refill_sort 
ON dme_supply_refill_metrics(refill_velocity DESC);

-- 2. Drop and recreate the chart summary table
DROP TABLE IF EXISTS summary_chart_refill_velocity;

CREATE TABLE summary_chart_refill_velocity (
    display_label TEXT,
    velocity_value REAL
);

-- 3. Populate with pre-sorted, pre-formatted top 10
INSERT INTO summary_chart_refill_velocity (display_label, velocity_value)
SELECT 
    hcpcs_code || ' - ' || supply_item, 
    ROUND(refill_velocity, 2)
FROM dme_supply_refill_metrics
ORDER BY refill_velocity DESC
LIMIT 10;

-- 1. Create indexes to make the lookup near-instant
CREATE INDEX IF NOT EXISTS idx_market_conc_spec_hcpcs 
ON specialty_market_concentration(specialty_name, hcpcs_code, dominance_rank);
-- This index makes your "Top 25" and "Top 15" queries near-instant
CREATE INDEX IF NOT EXISTS idx_market_conc_dominance_perf 
ON specialty_market_concentration (dominance_rank, pct_of_national_volume DESC);

-- 2. Drop and recreate the narrative summary table
DROP TABLE IF EXISTS summary_clinical_dominance_narrative;

CREATE TABLE summary_clinical_dominance_narrative (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    narrative_text TEXT
);

-- 3. Populate with pre-calculated, formatted narrative
INSERT INTO summary_clinical_dominance_narrative (id, narrative_text)
SELECT 1,
    'Market dominance in specific clinical domains typically reflects specialty focus and referral patterns — many procedures show near-absolute (100%) concentration. '
    || 'Cardiology maintains dominant shares on critical diagnostics; for example, ' || cd.procedure_description || ' (' || cd.hcpcs_code || ') alone account for ' || printf('%,.0f', cd.services) || ' services. '
    || 'Internal Medicine / PCP similarly dominates high-volume evaluation & management visits; ' || im.procedure_description || ' (' || im.hcpcs_code || ') reaches ' || printf('%,.0f', im.services) || ' services nationwide. '
    || 'Other Specialty maintains massive volume across therapeutics; the leading procedure is ' || os.procedure_description || ' (' || os.hcpcs_code || ') at ' || printf('%,.0f', os.services) || ' administrations. '
    || 'Even specialties with smaller procedural footprints achieve dominance; Pulmonology''s ' || pp.procedure_description || ' demonstrates control at ' || ROUND(pp.dominance_pct, 1) || '% national share.'
FROM 
    (SELECT hcpcs_code, procedure_description, total_services AS services FROM specialty_market_concentration WHERE specialty_name = 'Cardiology' AND hcpcs_code IN ('93000', '93010', '93015') AND dominance_rank = 1 ORDER BY total_services DESC LIMIT 1) cd,
    (SELECT hcpcs_code, procedure_description, total_services AS services FROM specialty_market_concentration WHERE specialty_name = 'Internal Medicine / PCP' AND hcpcs_code = '99214' AND dominance_rank = 1 LIMIT 1) im,
    (SELECT hcpcs_code, procedure_description, total_services AS services FROM specialty_market_concentration WHERE specialty_name = 'Other Specialty' AND dominance_rank = 1 ORDER BY total_services DESC LIMIT 1) os,
    (SELECT procedure_description, pct_of_national_volume AS dominance_pct FROM specialty_market_concentration WHERE specialty_name = 'Pulmonology' AND dominance_rank = 1 ORDER BY total_services DESC LIMIT 1) pp;


-- This index makes the Top 25 sort near-instant
CREATE INDEX IF NOT EXISTS idx_market_conc_table_perf 
ON specialty_market_concentration (pct_of_national_volume DESC);

-- 1. Ensure index for fast state-level aggregation
CREATE INDEX IF NOT EXISTS idx_geo_market_state ON geographic_market_opportunity(state_abbr);

-- 2. Drop and recreate the geographic narrative summary table
DROP TABLE IF EXISTS summary_geo_opportunity_narrative;

CREATE TABLE summary_geo_opportunity_narrative (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    narrative_text TEXT
);

-- 3. Populate with pre-calculated, formatted narrative
INSERT INTO summary_geo_opportunity_narrative (id, narrative_text)
WITH state_stats AS (
    SELECT
        state_abbr,
        SUM(state_total_spend) AS total_spend,
        SUM(state_patient_volume) AS patient_volume,
        SUM(state_total_spend) / NULLIF(SUM(state_patient_volume), 0) AS spend_per_patient,
        ROW_NUMBER() OVER (ORDER BY SUM(state_total_spend) DESC) AS spend_rank
    FROM geographic_market_opportunity
    GROUP BY state_abbr
),
ranks AS (
    SELECT 
        MAX(CASE WHEN spend_rank = 1 THEN state_abbr END) as s1_abbr,
        MAX(CASE WHEN spend_rank = 1 THEN total_spend END) as s1_spend,
        MAX(CASE WHEN spend_rank = 1 THEN patient_volume END) as s1_vol,
        MAX(CASE WHEN spend_rank = 1 THEN spend_per_patient END) as s1_spp,
        MAX(CASE WHEN spend_rank = 2 THEN state_abbr END) as s2_abbr,
        MAX(CASE WHEN spend_rank = 2 THEN total_spend END) as s2_spend,
        MAX(CASE WHEN spend_rank = 3 THEN state_abbr END) as s3_abbr,
        MAX(CASE WHEN spend_rank = 3 THEN total_spend END) as s3_spend,
        MAX(CASE WHEN spend_rank = 4 THEN state_abbr END) as s4_abbr,
        MAX(CASE WHEN spend_rank = 4 THEN spend_per_patient END) as s4_spp
    FROM state_stats WHERE spend_rank <= 4
)
SELECT 1,
    'Geographic Opportunity Concentration: The market opportunity concentrates in high-population states. '
    || s1_abbr || ' (California) leads at approximately $' || printf('%,.2f', s1_spend / 1e9) || 'B in total spend, driven by '
    || printf('%,.0f', s1_vol) || ' patients. '
    || s2_abbr || ' (Florida) and ' || s3_abbr || ' (Texas) follow with approximately $'
    || printf('%,.2f', s2_spend / 1e9) || 'B and $' || printf('%,.2f', s3_spend / 1e9) || 'B respectively. '
    || 'Regional cost variance is notable: ' || s4_abbr || ' (New York) shows spend per patient of $'
    || printf('%,.2f', s4_spp) || ' compared to ' || s1_abbr || '''s $' || printf('%,.2f', s1_spp)
    || ', reflecting regional differences in care intensity and cost.'
FROM ranks;



-- 1. Ensure index for state-level aggregation speed
CREATE INDEX IF NOT EXISTS idx_geo_market_aggr ON geographic_market_opportunity(state_abbr);

-- 2. Drop and recreate the Top 3 summary table
DROP TABLE IF EXISTS summary_table_top_states;

CREATE TABLE summary_table_top_states (
    spend_rank INTEGER PRIMARY KEY,
    state_abbr TEXT,
    formatted_spend TEXT,
    formatted_volume TEXT,
    formatted_spp TEXT
);

-- 3. Populate with pre-calculated, pre-formatted data
INSERT INTO summary_table_top_states
WITH state_rank AS (
    SELECT
        state_abbr,
        SUM(state_total_spend) AS raw_spend,
        SUM(state_patient_volume) AS raw_volume,
        SUM(state_total_spend) / NULLIF(SUM(state_patient_volume), 0) AS raw_spp,
        ROW_NUMBER() OVER (ORDER BY SUM(state_total_spend) DESC) AS rnk
    FROM geographic_market_opportunity
    GROUP BY state_abbr
)
SELECT
    rnk,
    state_abbr,
    '$' || printf('%,.0f', ROUND(raw_spend, 0)),
    printf('%,.0f', ROUND(raw_volume, 0)),
    '$' || printf('%,.2f', ROUND(raw_spp, 2))
FROM state_rank
WHERE rnk <= 3;

-- 1. Ensure indexes for the ranking columns
CREATE INDEX IF NOT EXISTS idx_mdsd_interaction_ratio ON mdsd_interaction_model_fit(interaction_ratio);
CREATE INDEX IF NOT EXISTS idx_dme_refill_velocity_val ON dme_supply_refill_metrics(refill_velocity);

-- 2. Drop and recreate the narrative summary table
DROP TABLE IF EXISTS summary_business_model_narrative;

CREATE TABLE summary_business_model_narrative (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    narrative_text TEXT
);

-- 3. Populate with pre-calculated, formatted narrative
INSERT INTO summary_business_model_narrative (id, narrative_text)
SELECT 1,
    'Diagnostic models (low interaction frequency) contrast sharply with continuous care models (high interaction). '
    || dl.disease_state || ' exemplifies a ' || dl.business_model_fit 
    || ' with interaction ratio ' || ROUND(dl.interaction_ratio, 2)
    || ', promoting periodic assessment and monitoring. Conversely, '
    || sh.disease_state || ' aligns to ' || sh.business_model_fit 
    || ' model with interaction ratio ' || ROUND(sh.interaction_ratio, 2) || '. '
    || 'Supply velocity reinforces engagement intensity: The highest refill velocity in the data is '
    || printf('%,.1f', tr.refill_velocity) || ' (' || tr.hcpcs_code 
    || '), signaling extremely high-frequency supply consumption. '
    || CASE 
        WHEN j.hcpcs_code IS NOT NULL THEN 'For comparison, J7042 (Normal Saline) shows refill velocity ' || printf('%,.2f', j.refill_velocity) || ', typical of regular maintenance protocols.'
        ELSE ''
       END
FROM 
    (SELECT disease_state, business_model_fit, interaction_ratio FROM mdsd_interaction_model_fit ORDER BY interaction_ratio ASC LIMIT 1) dl,
    (SELECT disease_state, business_model_fit, interaction_ratio FROM mdsd_interaction_model_fit ORDER BY interaction_ratio DESC LIMIT 1) sh,
    (SELECT hcpcs_code, refill_velocity FROM dme_supply_refill_metrics ORDER BY refill_velocity DESC LIMIT 1) tr,
    (SELECT hcpcs_code, refill_velocity FROM dme_supply_refill_metrics WHERE hcpcs_code = 'J7042' LIMIT 1) j;


-- 1. Ensure index for fast state-level aggregation
CREATE INDEX IF NOT EXISTS idx_geo_spend_aggr ON geographic_market_opportunity(state_abbr);

-- 2. Drop and recreate the geographic spend summary table
DROP TABLE IF EXISTS summary_chart_geo_spend;

CREATE TABLE summary_chart_geo_spend (
    state_label TEXT,
    spend_billions REAL,
    rank_order INTEGER PRIMARY KEY
);

-- 3. Populate with pre-calculated Top 15
INSERT INTO summary_chart_geo_spend (state_label, spend_billions, rank_order)
SELECT 
    state_abbr, 
    ROUND(SUM(state_total_spend) / 1000000000.0, 2),
    ROW_NUMBER() OVER (ORDER BY SUM(state_total_spend) DESC)
FROM geographic_market_opportunity
GROUP BY state_abbr
ORDER BY SUM(state_total_spend) DESC
LIMIT 15;


-- 1. Ensure index for state-level aggregation speed
CREATE INDEX IF NOT EXISTS idx_geo_market_state_aggr ON geographic_market_opportunity(state_abbr);

-- 2. Drop and recreate the Top 20 Geographic summary table
DROP TABLE IF EXISTS summary_table_geo_spend_top20;

CREATE TABLE summary_table_geo_spend_top20 (
    rank_order INTEGER PRIMARY KEY,
    state_abbr TEXT,
    patient_vol INTEGER,
    total_spend_b REAL,
    gpci_spend_b REAL,
    spend_per_patient REAL
);

-- 3. Populate with pre-calculated, pre-sorted data
INSERT INTO summary_table_geo_spend_top20 (rank_order, state_abbr, patient_vol, total_spend_b, gpci_spend_b, spend_per_patient)
SELECT 
    ROW_NUMBER() OVER (ORDER BY SUM(state_total_spend) DESC),
    state_abbr,
    ROUND(SUM(state_patient_volume), 0),
    ROUND(SUM(state_total_spend) / 1000000000.0, 2),
    ROUND(SUM(gpci_adjusted_spend) / 1000000000.0, 2),
    ROUND(SUM(state_total_spend) / NULLIF(SUM(state_patient_volume), 0), 2)
FROM geographic_market_opportunity
GROUP BY state_abbr
ORDER BY SUM(state_total_spend) DESC
LIMIT 20;


-- 1. Ensure index for fast sorting during the build
CREATE INDEX IF NOT EXISTS idx_interaction_fit_sort 
ON mdsd_interaction_model_fit(interaction_ratio DESC);

-- 2. Drop and recreate the strategic chart summary
DROP TABLE IF EXISTS summary_chart_strategic_models;

CREATE TABLE summary_chart_strategic_models (
    rank_order INTEGER PRIMARY KEY,
    disease_label TEXT,
    interaction_value REAL
);

-- 3. Populate with pre-sorted, pre-rounded data
INSERT INTO summary_chart_strategic_models (rank_order, disease_label, interaction_value)
SELECT 
    ROW_NUMBER() OVER (ORDER BY interaction_ratio DESC),
    disease_state, 
    ROUND(interaction_ratio, 2)
FROM mdsd_interaction_model_fit
ORDER BY interaction_ratio DESC;


-- 1. Ensure indexes for the ranking columns
CREATE INDEX IF NOT EXISTS idx_mdsd_global_vol ON mdsd_global_opportunity_matrix(patient_volume);
CREATE INDEX IF NOT EXISTS idx_mdsd_global_density ON mdsd_global_opportunity_matrix(interaction_density);
CREATE INDEX IF NOT EXISTS idx_mdsd_econ_intensity ON mdsd_economic_intensity_proof(economic_intensity_index);

-- 2. Drop and recreate the market summary narrative table
DROP TABLE IF EXISTS summary_market_structure_narrative;

CREATE TABLE summary_market_structure_narrative (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    narrative_text TEXT
);

-- 3. Populate with pre-calculated, formatted narrative
INSERT INTO summary_market_structure_narrative (id, narrative_text)
SELECT 1,
    'The consolidated evidence reveals a fundamentally bifurcated market, requiring distinct entry strategies. '
    || 'Volume-driven segments (e.g., ' || vc.disease_state || ' with ' || printf('%,.0f', vc.patient_volume) 
    || ' patients) thrive in large geographic markets like California and Florida, where scale drives economics. '
    || 'Intensity-driven segments (e.g., ' || ic.disease_state || ' with interaction density ' || ROUND(ic.interaction_density, 1) 
    || ') prioritize specialized supply chains, rare therapeutics, and continuous monitoring. '
    || 'Clinical dominance remains concentrated: specialty gatekeepers (e.g., ' || hs.specialty_name || ' with intensity index ' 
    || ROUND(hs.economic_intensity_index, 2) || ') control their core procedures absolutely, creating defensible competitive positions.'
FROM 
    (SELECT disease_state, patient_volume FROM mdsd_global_opportunity_matrix ORDER BY patient_volume DESC LIMIT 1) vc,
    (SELECT disease_state, interaction_density FROM mdsd_global_opportunity_matrix ORDER BY interaction_density DESC LIMIT 1) ic,
    (SELECT specialty_name, economic_intensity_index FROM mdsd_economic_intensity_proof ORDER BY economic_intensity_index DESC LIMIT 1) hs;


-- 1. Create indexes to speed up the cross-table aggregation
CREATE INDEX IF NOT EXISTS idx_gatekeepers_disease ON mdsd_specialty_gatekeepers(disease_state, market_share_percentage);
CREATE INDEX IF NOT EXISTS idx_geo_specialty_spend ON geographic_market_opportunity(specialty_name, state_total_spend);

-- 2. Drop and recreate the gatekeeper narrative table
DROP TABLE IF EXISTS summary_gatekeeper_narrative;

CREATE TABLE summary_gatekeeper_narrative (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    narrative_text TEXT
);

-- 3. Populate with pre-calculated, formatted narrative
INSERT INTO summary_gatekeeper_narrative (id, narrative_text)
WITH disease_reach AS (
    SELECT
        disease_state,
        SUM(specialized_patient_reach) AS total_reach,
        AVG(market_share_percentage) AS avg_share,
        ROW_NUMBER() OVER (ORDER BY SUM(specialized_patient_reach) DESC) as rn
    FROM mdsd_specialty_gatekeepers
    GROUP BY disease_state
),
top_gatekeeper AS (
    SELECT specialty_name, disease_state, procedure_description, market_share_percentage, specialized_patient_reach
    FROM mdsd_specialty_gatekeepers
    ORDER BY market_share_percentage DESC, specialized_patient_reach DESC
    LIMIT 1
),
top_state AS (
    SELECT state_abbr, SUM(state_total_spend) AS state_spend
    FROM geographic_market_opportunity
    WHERE specialty_name IN (SELECT DISTINCT specialty_name FROM mdsd_specialty_gatekeepers)
    GROUP BY state_abbr
    ORDER BY SUM(state_total_spend) DESC
    LIMIT 1
)
SELECT 1,
    'Gatekeeper dynamics show concentrated clinical control and uneven opportunity distribution. '
    || d1.disease_state || ' leads with ' || printf('%,.0f', d1.total_reach) 
    || ' specialized patients at an average dominance of ' || ROUND(d1.avg_share, 1) 
    || '%, followed by ' || d2.disease_state || ' with ' || printf('%,.0f', d2.total_reach) || '. '
    || 'At the procedure level, ' || tg.specialty_name || ' anchors ' || tg.disease_state 
    || ' through ' || tg.procedure_description || ' with ' || ROUND(tg.market_share_percentage, 1) 
    || '% share and ' || printf('%,.0f', tg.specialized_patient_reach) || ' patients. '
    || 'Geographically, ' || ts.state_abbr || ' is the largest spend concentration for gatekeeper-led specialties at about $'
    || printf('%,.2f', ts.state_spend / 1e9) || 'B.'
FROM 
    (SELECT * FROM disease_reach WHERE rn = 1) d1,
    (SELECT * FROM disease_reach WHERE rn = 2) d2,
    top_gatekeeper tg,
    top_state ts;


    -- 1. Ensure index for fast disease-level aggregation
CREATE INDEX IF NOT EXISTS idx_gatekeepers_reach_aggr 
ON mdsd_specialty_gatekeepers(disease_state, specialized_patient_reach);

-- 2. Drop and recreate the gatekeeper reach summary table
DROP TABLE IF EXISTS summary_chart_gatekeeper_reach;

CREATE TABLE summary_chart_gatekeeper_reach (
    rank_order INTEGER PRIMARY KEY,
    disease_label TEXT,
    total_reach INTEGER
);

-- 3. Populate with pre-calculated Top Diseases
INSERT INTO summary_chart_gatekeeper_reach (rank_order, disease_label, total_reach)
SELECT 
    ROW_NUMBER() OVER (ORDER BY SUM(specialized_patient_reach) DESC),
    disease_state, 
    ROUND(SUM(specialized_patient_reach), 0)
FROM mdsd_specialty_gatekeepers
GROUP BY disease_state
ORDER BY SUM(specialized_patient_reach) DESC;


-- 1. Create a composite index to make the multi-column sort instant
CREATE INDEX IF NOT EXISTS idx_gatekeeper_dominance_sort 
ON mdsd_specialty_gatekeepers(market_share_percentage DESC, specialized_patient_reach DESC);

-- 2. Drop and recreate the dominance chart summary table
DROP TABLE IF EXISTS summary_chart_gatekeeper_dominance;

CREATE TABLE summary_chart_gatekeeper_dominance (
    rank_order INTEGER PRIMARY KEY,
    display_label TEXT,
    share_value REAL
);

-- 3. Populate with pre-sorted, pre-formatted Top 12
INSERT INTO summary_chart_gatekeeper_dominance (rank_order, display_label, share_value)
SELECT 
    ROW_NUMBER() OVER (ORDER BY market_share_percentage DESC, specialized_patient_reach DESC),
    specialty_name || ' - ' || disease_state, 
    ROUND(market_share_percentage, 1)
FROM mdsd_specialty_gatekeepers
ORDER BY market_share_percentage DESC, specialized_patient_reach DESC
LIMIT 12;

-- This index targets the multi-column sort used in your table
CREATE INDEX IF NOT EXISTS idx_gatekeeper_table_sort 
ON mdsd_specialty_gatekeepers (market_share_percentage DESC, specialized_patient_reach DESC);