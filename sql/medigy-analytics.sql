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
-- VIEW 6B: condition_monitoring_proxy
-- Condition-level repeat-interaction proxy using ONLY monitoring-flagged HCPCS.
-- Adds disease-specific relevance filters so conditions do not collapse to a
-- single uniform value across the body-system bridge.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS condition_monitoring_proxy;

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
-- VIEW 4: specialty_market_concentration  [FROM ORIGINAL — PRESERVED + CLEANED]
-- Dominance-based ranking: which specialties own the highest share of key codes?
-- Use to validate that "Nephrology owns dialysis" or "Cardiology owns echo".
-- -----------------------------------------------------------------------------
-- VIEW 4: specialty_market_concentration
DROP VIEW IF EXISTS specialty_market_concentration;
CREATE VIEW specialty_market_concentration AS
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


-- VIEW 5: chronic_interaction_density
DROP VIEW IF EXISTS chronic_interaction_density;
CREATE VIEW chronic_interaction_density AS
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


-- -----------------------------------------------------------------------------
-- VIEW 13a: disease_procedure_bridge
-- Makes the Disease → Procedure → Specialty relationship explicit and reusable.
-- Links dim_diagnosis (disease_state, body_system) to fact_utilization
-- (hcpcs_code, specialty_name, specialty_domain) via body_system ↔ specialty_domain.
-- Consumed by opportunity_scoring_view instead of duplicating join logic there.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS disease_procedure_bridge;
CREATE VIEW disease_procedure_bridge AS
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
-- VIEW 13: opportunity_scoring_view
-- Final ranked table of disease-state × specialty clusters.
-- Tiers: >= 75 = Tier 1 High, >= 50 = Tier 2 Moderate, < 50 = Tier 3 Low
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS opportunity_scoring_view;

CREATE VIEW opportunity_scoring_view AS
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

