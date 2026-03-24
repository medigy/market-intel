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
    CAST(NULLIF(TRIM(p."WORK RVU"),          '') AS REAL)           AS work_rvu,
    CAST(NULLIF(TRIM(p."NON-FAC PE RVU"),  '') AS REAL)             AS non_fac_pe_rvu,
    CAST(NULLIF(TRIM(p."FACILITY PE RVU"), '') AS REAL)             AS facility_pe_rvu,
    CAST(NULLIF(TRIM(p."MEDICARE PAYMENT"),'') AS REAL)             AS medicare_fee_schedule_payment,
    -- BROAD CATEGORY (Keeps everything organized)
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
    
    -- SPECIFIC SIGNAL (This is what Cline uses for the report)
    CASE
        -- Sleep Apnea (Monday Pilot)
        WHEN p.HCPCS IN ('95800','95801','95806','95810','95811','G0398')   THEN 'Sleep Study'
        -- COPD (Monday Pilot)
        WHEN p.HCPCS IN ('94010','94060','94660','G0237','G0238','G0239')   THEN 'Pulmonary Function/Rehab'
        -- Hypertension (Monday Pilot)
        WHEN p.HCPCS IN ('93784','93788','93790')                           THEN 'BP Monitoring'
        -- General Monitoring (Future Proofing)
        WHEN p.HCPCS BETWEEN '99211' AND '99215'                           THEN 'Office Visit (Repeat)'
        ELSE 'Standard Care'
    END                                                             AS procedure_signal,

    -- Keep your original binary flag for backward compatibility
    CASE 
        WHEN p.HCPCS BETWEEN '99202' AND '99215' THEN 1
        WHEN p.HCPCS IN (
            '82947','82950','82962','83036','83037',
            '90935','90937','90945','90947','90999',
            '93000','93005','93010','93224','93225','93226','93227',
            '93784','93788','93790',
            '94010','94060','94660',
            '95800','95801','95806','95810','95811',
            'G0237','G0238','G0239','G0398','G0491','G0492'
        ) THEN 1
        ELSE 0 
    END AS is_monitoring_flag
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

-- DROP TABLE IF EXISTS dim_diagnosis;
-- CREATE TABLE dim_diagnosis AS
-- SELECT
--     icd10_code,
--     description_long                                AS diagnosis_description,
--     -- SECTION 1: SPECIFIC DISEASE STATE IDENTIFICATION (Shahid's Targets)
--     CASE
--         -- Priority 1: Respiratory & Sleep (Voxia Core)
--         WHEN icd10_code LIKE 'G473%'                 THEN 'Sleep Apnea'
--         WHEN icd10_code LIKE 'P283%'                 THEN 'Sleep Apnea'
--         WHEN icd10_code LIKE 'J44%'                  THEN 'COPD'
--         WHEN icd10_code LIKE 'J45%'                  THEN 'Asthma'

--         -- Priority 2: Cardiovascular
--         WHEN icd10_code LIKE 'I50%'                  THEN 'Heart Failure / CHF'
--         WHEN icd10_code LIKE 'I48%'                  THEN 'Atrial Fibrillation'
--         WHEN icd10_code LIKE 'I25%'                  THEN 'Ischemic Heart Disease / CAD'
--         WHEN icd10_code LIKE 'I10%'                  THEN 'Hypertension'
--         WHEN icd10_code BETWEEN 'I60' AND 'I69'      THEN 'Stroke / Cerebrovascular Disease'

--         -- Priority 3: Mental Health & Neurological
--         WHEN icd10_code LIKE 'F32%' OR icd10_code LIKE 'F33%' THEN 'Major Depression'
--         WHEN icd10_code LIKE 'G30%' OR icd10_code LIKE 'F01%' OR icd10_code LIKE 'F03%' THEN 'Alzheimers / Dementia'
--         WHEN icd10_code LIKE 'G20%'                  THEN 'Parkinsons Disease'
--         WHEN icd10_code LIKE 'G45%'                  THEN 'TIA (Stroke Precursor)'

--         -- Priority 4: Renal & Metabolic
--         WHEN icd10_code LIKE 'N186' OR icd10_code LIKE 'N185' THEN 'End-Stage Renal Disease (ESRD)'
--         WHEN icd10_code LIKE 'N18%'                  THEN 'Chronic Kidney Disease'
--         WHEN icd10_code LIKE 'E10%'                  THEN 'Type 1 Diabetes'
--         WHEN icd10_code LIKE 'E11%'                  THEN 'Type 2 Diabetes'
        
--         -- Priority 5: Musculoskeletal & Oncology
--         WHEN icd10_code LIKE 'M16%' OR icd10_code LIKE 'M17%' THEN 'Osteoarthritis (Hip/Knee)'
--         WHEN icd10_code LIKE 'M80%' OR icd10_code LIKE 'M81%' THEN 'Osteoporosis'
--         WHEN icd10_code LIKE 'C34%'                  THEN 'Lung Cancer'
--         WHEN icd10_code LIKE 'C50%'                  THEN 'Breast Cancer'
--         WHEN icd10_code LIKE 'C61%'                  THEN 'Prostate Cancer'
--         WHEN icd10_code LIKE 'C18%'                  THEN 'Colon Cancer'
--         WHEN icd10_code BETWEEN 'C00' AND 'C97'      THEN 'Cancer (Other Malignant)'

--         -- SECTION 2: FALLBACK TO ICD-10 CHAPTERS (Prevents "General / Other")
--         WHEN icd10_code GLOB '[AB]*'                 THEN 'Infectious / Parasitic Diseases'
--         WHEN icd10_code GLOB 'D[0-4]*'               THEN 'Neoplasms (Benign/Unspecified)'
--         WHEN icd10_code GLOB 'D[5-8]*'               THEN 'Blood / Immune Disorders'
--         WHEN icd10_code GLOB 'E*'                    THEN 'Endocrine / Metabolic (Other)'
--         WHEN icd10_code GLOB 'F*'                    THEN 'Mental / Behavioral (Other)'
--         WHEN icd10_code GLOB 'G*'                    THEN 'Nervous System (Other)'
--         WHEN icd10_code GLOB 'H[0-5]*'               THEN 'Eye / Adnexa'
--         WHEN icd10_code GLOB 'H[6-9]*'               THEN 'Ear / Mastoid'
--         WHEN icd10_code GLOB 'I*'                    THEN 'Circulatory (Other)'
--         WHEN icd10_code GLOB 'J*'                    THEN 'Respiratory (Other)'
--         WHEN icd10_code GLOB 'K*'                    THEN 'Digestive System'
--         WHEN icd10_code GLOB 'L*'                    THEN 'Skin / Subcutaneous'
--         WHEN icd10_code GLOB 'M*'                    THEN 'Musculoskeletal (Other)'
--         WHEN icd10_code GLOB 'N*'                    THEN 'Genitourinary (Other)'
--         WHEN icd10_code GLOB 'O*'                    THEN 'Pregnancy / Childbirth'
--         WHEN icd10_code GLOB 'P*'                    THEN 'Perinatal Conditions'
--         WHEN icd10_code GLOB 'Q*'                    THEN 'Congenital Malformations'
--         WHEN icd10_code GLOB 'R*'                    THEN 'Symptoms / Clinical Findings (NEC)'
--         WHEN icd10_code GLOB '[ST]*'                 THEN 'Injury / Poisoning / Trauma'
--         WHEN icd10_code GLOB '[VWXY]*'               THEN 'External Causes of Morbidity'
--         WHEN icd10_code GLOB 'Z*'                    THEN 'Health Status / Screenings'
--         ELSE 'Unclassified / Special Codes'
--     END                                             AS disease_state,
--     -- SECTION 3: BODY SYSTEM GROUPING
--     CASE
--         WHEN icd10_code LIKE 'E%'                    THEN 'Endocrine & Metabolic'
--         WHEN icd10_code LIKE 'I%'                    THEN 'Cardiovascular'
--         WHEN icd10_code LIKE 'N%'                    THEN 'Renal & Urological'
--         WHEN icd10_code LIKE 'J%' OR icd10_code LIKE 'G47%' OR icd10_code LIKE 'P283%' THEN 'Respiratory & Sleep'
--         WHEN icd10_code LIKE 'G%' OR icd10_code LIKE 'F%' THEN 'Neurological & Mental Health'
--         WHEN icd10_code LIKE 'M%'                    THEN 'Musculoskeletal'
--         WHEN icd10_code LIKE 'C%' OR icd10_code LIKE 'D[0-4]%' THEN 'Oncology'
--         WHEN icd10_code LIKE 'K%'                    THEN 'Digestive'
--         WHEN icd10_code LIKE 'L%'                    THEN 'Dermatology'
--         WHEN icd10_code LIKE 'H%'                    THEN 'Ophthalmology & Otology'
--         WHEN icd10_code LIKE 'S%' OR icd10_code LIKE 'T%' THEN 'Injury & Trauma'
--         ELSE 'Other Clinical'
--     END                                             AS body_system
-- FROM uniform_resource_ref_icd10_diagnosis
-- WHERE icd10_code IS NOT NULL;

DROP TABLE IF EXISTS dim_diagnosis;
CREATE TABLE dim_diagnosis AS
SELECT
    icd10_code,
    description_long                                AS diagnosis_description,
    -- SECTION 1: SPECIFIC PILOT CONDITIONS (High Priority)
    CASE
        -- Sleep Apnea
        WHEN icd10_code LIKE 'G47.3%' OR icd10_code LIKE 'P28.3%' THEN 'Sleep Apnea'
        
        --  COPD
        WHEN icd10_code LIKE 'J44%'                               THEN 'COPD'
        
        --  Hypertension
        WHEN icd10_code LIKE 'I10%'                               THEN 'Hypertension'
        
        -- Future Conditions (Easily add more here)
        WHEN icd10_code LIKE 'G20%'                               THEN 'Parkinsons Disease'
        WHEN icd10_code LIKE 'I50%'                               THEN 'Heart Failure'
        WHEN icd10_code LIKE 'E11%'                               THEN 'Type 2 Diabetes'
        WHEN icd10_code LIKE 'N18%'                               THEN 'Chronic Kidney Disease'
        
        -- SECTION 2: AUTOMATED CATCH-ALL (Ensures 0% data loss)
        WHEN icd10_code GLOB '[AB]*'                               THEN 'Infectious Diseases'
        WHEN icd10_code GLOB 'C*'                                 THEN 'Oncology / Cancer'
        WHEN icd10_code GLOB 'E*'                                 THEN 'Endocrine / Metabolic'
        WHEN icd10_code GLOB 'F*'                                 THEN 'Mental / Behavioral'
        WHEN icd10_code GLOB 'G*'                                 THEN 'Neurological'
        WHEN icd10_code GLOB 'I*'                                 THEN 'Cardiovascular'
        WHEN icd10_code GLOB 'J*'                                 THEN 'Respiratory'
        WHEN icd10_code GLOB 'M*'                                 THEN 'Musculoskeletal'
        WHEN icd10_code GLOB 'N*'                                 THEN 'Genitourinary'
        ELSE 'Other Chronic / Clinical'
    END                                             AS disease_state,

    -- SECTION 3: SYSTEM ROLLUP (The Bridge to Specialties)
    CASE
        WHEN icd10_code LIKE 'G47%' OR icd10_code LIKE 'J%'       THEN 'Respiratory & Sleep'
        WHEN icd10_code LIKE 'I%'                                 THEN 'Cardiovascular'
        WHEN icd10_code LIKE 'G%' OR icd10_code LIKE 'F%'         THEN 'Neurological & Mental Health'
        WHEN icd10_code LIKE 'E%'                                 THEN 'Endocrine & Metabolic'
        WHEN icd10_code LIKE 'M%'                                 THEN 'Musculoskeletal'
        WHEN icd10_code LIKE 'N%'                                 THEN 'Renal & Urological'
        WHEN icd10_code LIKE 'C%'                                 THEN 'Oncology'
        ELSE 'General Medicine'
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

-- CREATE TABLE dim_specialty AS
-- SELECT DISTINCT
--     Rndrng_Prvdr_Type                                           AS raw_specialty_name,
--     CASE
--         WHEN Rndrng_Prvdr_Type LIKE '%Internal Medicine%'       THEN 'Internal Medicine'
--         WHEN Rndrng_Prvdr_Type LIKE '%Family Practice%'
--           OR Rndrng_Prvdr_Type LIKE '%Family Medicine%'         THEN 'Family Medicine'
--         WHEN Rndrng_Prvdr_Type LIKE '%Cardiology%'              THEN 'Cardiology'
--         WHEN Rndrng_Prvdr_Type LIKE '%Nephrology%'              THEN 'Nephrology'
--         WHEN Rndrng_Prvdr_Type LIKE '%Endocrinology%'           THEN 'Endocrinology'
--         WHEN Rndrng_Prvdr_Type LIKE '%Pulmonology%'
--           OR Rndrng_Prvdr_Type LIKE '%Pulmonary%'               THEN 'Pulmonology'
--         WHEN Rndrng_Prvdr_Type LIKE '%Oncology%'
--           OR Rndrng_Prvdr_Type LIKE '%Hematology%'              THEN 'Oncology / Hematology'
--         WHEN Rndrng_Prvdr_Type LIKE '%Neurology%'               THEN 'Neurology'
--         WHEN Rndrng_Prvdr_Type LIKE '%Orthopedic%'              THEN 'Orthopedic Surgery'
--         WHEN Rndrng_Prvdr_Type LIKE '%Psychiatry%'
--           OR Rndrng_Prvdr_Type LIKE '%Psychology%'              THEN 'Psychiatry / Psychology'
--         WHEN Rndrng_Prvdr_Type LIKE '%Ophthalmology%'           THEN 'Ophthalmology'
--         WHEN Rndrng_Prvdr_Type LIKE '%Urology%'                 THEN 'Urology'
--         WHEN Rndrng_Prvdr_Type LIKE '%Gastroenterology%'        THEN 'Gastroenterology'
--         WHEN Rndrng_Prvdr_Type LIKE '%Nurse Practitioner%'      THEN 'Nurse Practitioner'
--         WHEN Rndrng_Prvdr_Type LIKE '%Physician Assistant%'     THEN 'Physician Assistant'
--         WHEN Rndrng_Prvdr_Type LIKE '%Physical Therapy%'        THEN 'Physical Therapy'
--         ELSE Rndrng_Prvdr_Type
--     END                                                         AS specialty_name,
--     CASE
--         WHEN Rndrng_Prvdr_Type LIKE '%Cardiology%'              THEN 'Cardiovascular'
--         WHEN Rndrng_Prvdr_Type LIKE '%Nephrology%'              THEN 'Renal & Urological'
--         WHEN Rndrng_Prvdr_Type LIKE '%Endocrinology%'           THEN 'Endocrine & Metabolic'
--         WHEN Rndrng_Prvdr_Type LIKE '%Pulmonology%'
--           OR Rndrng_Prvdr_Type LIKE '%Pulmonary%'               THEN 'Respiratory'
--         WHEN Rndrng_Prvdr_Type LIKE '%Oncology%'
--           OR Rndrng_Prvdr_Type LIKE '%Hematology%'              THEN 'Oncology'
--         WHEN Rndrng_Prvdr_Type LIKE '%Neurology%'
--           OR Rndrng_Prvdr_Type LIKE '%Psychiatry%'              THEN 'Neurological & Mental Health'
--         WHEN Rndrng_Prvdr_Type LIKE '%Orthopedic%'              THEN 'Musculoskeletal'
--         WHEN Rndrng_Prvdr_Type LIKE '%Internal Medicine%'
--           OR Rndrng_Prvdr_Type LIKE '%Family%'                  THEN 'Primary Care'
--         ELSE 'General / Other'
--     END                                                         AS specialty_domain
-- FROM uniform_resource_cms_provider
-- WHERE Rndrng_Prvdr_Type IS NOT NULL;


DROP TABLE IF EXISTS dim_specialty;
CREATE TABLE dim_specialty AS
SELECT DISTINCT
    Rndrng_Prvdr_Type                                           AS raw_specialty_name,
    -- Canonical Specialty Name
    CASE
        WHEN Rndrng_Prvdr_Type LIKE '%Internal Medicine%'       THEN 'Internal Medicine'
        WHEN Rndrng_Prvdr_Type LIKE '%Family Practice%'
          OR Rndrng_Prvdr_Type LIKE '%Family Medicine%'         THEN 'Family Medicine'
        WHEN Rndrng_Prvdr_Type LIKE '%Cardiology%'               THEN 'Cardiology'
        WHEN Rndrng_Prvdr_Type LIKE '%Nephrology%'               THEN 'Nephrology'
        WHEN Rndrng_Prvdr_Type LIKE '%Endocrinology%'            THEN 'Endocrinology'
        WHEN Rndrng_Prvdr_Type LIKE '%Pulmonology%'
          OR Rndrng_Prvdr_Type LIKE '%Pulmonary%'               THEN 'Pulmonology'
        WHEN Rndrng_Prvdr_Type LIKE '%Oncology%'
          OR Rndrng_Prvdr_Type LIKE '%Hematology%'               THEN 'Oncology / Hematology'
        WHEN Rndrng_Prvdr_Type LIKE '%Neurology%'               THEN 'Neurology'
        WHEN Rndrng_Prvdr_Type LIKE '%Orthopedic%'               THEN 'Orthopedic Surgery'
        WHEN Rndrng_Prvdr_Type LIKE '%Psychiatry%'
          OR Rndrng_Prvdr_Type LIKE '%Psychology%'               THEN 'Psychiatry / Psychology'
        WHEN Rndrng_Prvdr_Type LIKE '%Ophthalmology%'           THEN 'Ophthalmology'
        WHEN Rndrng_Prvdr_Type LIKE '%Urology%'                 THEN 'Urology'
        WHEN Rndrng_Prvdr_Type LIKE '%Gastroenterology%'        THEN 'Gastroenterology'
        WHEN Rndrng_Prvdr_Type LIKE '%Dermatology%'             THEN 'Dermatology'
        WHEN Rndrng_Prvdr_Type LIKE '%Nurse Practitioner%'       THEN 'Nurse Practitioner'
        WHEN Rndrng_Prvdr_Type LIKE '%Physician Assistant%'     THEN 'Physician Assistant'
        WHEN Rndrng_Prvdr_Type LIKE '%Physical Therapy%'         THEN 'Physical Therapy'
        ELSE Rndrng_Prvdr_Type
    END                                                         AS specialty_name,
    
    -- DOMAIN GROUPING (Must match dim_diagnosis.body_system exactly)
    CASE
        WHEN Rndrng_Prvdr_Type LIKE '%Cardiology%'               THEN 'Cardiovascular'
        WHEN Rndrng_Prvdr_Type LIKE '%Nephrology%'               THEN 'Renal & Urological'
        WHEN Rndrng_Prvdr_Type LIKE '%Endocrinology%'            THEN 'Endocrine & Metabolic'
        -- CRITICAL: Match for Sleep Apnea Pilot
        WHEN Rndrng_Prvdr_Type LIKE '%Pulmon%'                  THEN 'Respiratory & Sleep'
        WHEN Rndrng_Prvdr_Type LIKE '%Oncology%'
          OR Rndrng_Prvdr_Type LIKE '%Hematology%'               THEN 'Oncology'
        WHEN Rndrng_Prvdr_Type LIKE '%Neurology%'
          OR Rndrng_Prvdr_Type LIKE '%Psych%'                    THEN 'Neurological & Mental Health'
        WHEN Rndrng_Prvdr_Type LIKE '%Orthopedic%'               THEN 'Musculoskeletal'
        WHEN Rndrng_Prvdr_Type LIKE '%Gastro%'                  THEN 'Digestive'
        WHEN Rndrng_Prvdr_Type LIKE '%Dermat%'                  THEN 'Dermatology'
        WHEN Rndrng_Prvdr_Type LIKE '%Ophthalm%'                THEN 'Ophthalmology & Otology'
        WHEN Rndrng_Prvdr_Type LIKE '%Internal Medicine%'
          OR Rndrng_Prvdr_Type LIKE '%Family%'                  THEN 'Primary Care'
        ELSE 'Other Clinical'
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
    SELECT 
        HCPCS_Cd AS hcpcs_code,
        TRIM(UPPER(Rndrng_Prvdr_Geo_Cd)) AS state_abbr,
        Place_Of_Srvc AS place_of_service_code,
        SUM(CAST(NULLIF(Tot_Benes, '') AS INTEGER)) AS total_beneficiaries,
        SUM(CAST(NULLIF(Tot_Srvcs, '') AS REAL)) AS total_services,
        SUM(CAST(NULLIF(Tot_Rndrng_Prvdrs, '') AS INTEGER)) AS total_rendering_providers,
        SUM(CAST(NULLIF(Tot_Srvcs, '') AS REAL) * CAST(NULLIF(Avg_Mdcr_Alowd_Amt, '') AS REAL)) AS total_allowed_amt,        
        SUM(
            CAST(NULLIF(Tot_Srvcs, '') AS REAL) * CAST(NULLIF(Avg_Mdcr_Pymt_Amt, '') AS REAL)
        ) AS total_medicare_payment 
    FROM uniform_resource_cms_bygeography
    WHERE UPPER(Rndrng_Prvdr_Geo_Lvl) = 'STATE'
    GROUP BY 1, 2, 3
)
SELECT
    -- 1. Specialty Attribution (Matches your dim_specialty logic)
    CASE 
        WHEN h.hcpcs_code BETWEEN '93000' AND '93999' THEN 'Cardiology'
        WHEN h.hcpcs_code BETWEEN '94000' AND '94799' THEN 'Pulmonology' -- NEW: Specifically for Sleep/COPD
        WHEN h.hcpcs_code BETWEEN '95800' AND '95811' THEN 'Sleep Medicine' -- NEW: Specifically for Sleep Apnea
        WHEN h.hcpcs_code BETWEEN '90935' AND '90999' THEN 'Nephrology'
        WHEN h.hcpcs_code BETWEEN '99201' AND '99499' THEN 'Internal Medicine'
        ELSE 'Other Specialty'
    END AS specialty_name,
    
    -- 2. THE BRIDGE COLUMN: This MUST match dim_diagnosis.body_system exactly
    CASE 
        WHEN h.hcpcs_code BETWEEN '93000' AND '93999' THEN 'Cardiovascular'
        WHEN h.hcpcs_code BETWEEN '94000' AND '94799' THEN 'Respiratory & Sleep'
        WHEN h.hcpcs_code BETWEEN '95800' AND '95811' THEN 'Respiratory & Sleep'
        WHEN h.hcpcs_code BETWEEN '90935' AND '90999' THEN 'Renal & Urological'
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

CREATE TABLE master_evidence_hub AS
WITH disease_states AS (
    SELECT DISTINCT disease_state, body_system
    FROM dim_diagnosis
    WHERE disease_state != 'General / Other'
),
mapping_logic AS (
    SELECT 'Endocrine & Metabolic'       AS diag_sys, 'Endocrine & Metabolic'       AS spec_dom UNION ALL
    SELECT 'Cardiovascular',                           'Cardiovascular'                          UNION ALL
    SELECT 'Renal & Urological',                       'Renal & Urological'                      UNION ALL
    SELECT 'Respiratory & Sleep',                      'Respiratory & Sleep'                     UNION ALL
    SELECT 'Oncology',                                 'Oncology'                                UNION ALL
    SELECT 'Neurological & Mental Health',             'Neurological & Mental Health'            UNION ALL
    SELECT 'Musculoskeletal',                          'Musculoskeletal'                         UNION ALL
    SELECT 'General Medicine',                         'General Medicine'
),
procedure_lookup AS (
    SELECT
        hcpcs_code,
        MAX(procedure_category) AS procedure_category,
        MAX(procedure_signal) AS procedure_signal,
        MAX(is_monitoring_flag) AS is_monitoring_flag
    FROM dim_procedure
    GROUP BY hcpcs_code
)
SELECT 
    d.disease_state,
    d.body_system,
    p.procedure_category,
    p.procedure_signal,
    f.specialty_name,
    f.specialty_domain,
    SUM(f.total_services) AS srvc_vol,
    SUM(f.total_beneficiaries) AS bene_vol,
    ROUND(CAST(SUM(f.total_services) AS FLOAT) / NULLIF(SUM(f.total_beneficiaries), 0), 2) AS interaction_density,
    SUM(f.total_allowed_amt) AS total_allowed_amt,
    SUM(f.total_allowed_amt) AS total_spend,
    SUM(f.total_medicare_payment) AS total_medicare_payment,
    MAX(p.is_monitoring_flag) AS is_monitoring_flag
FROM fact_utilization f
JOIN procedure_lookup p ON f.hcpcs_code = p.hcpcs_code
JOIN mapping_logic m ON (
    f.specialty_domain = m.spec_dom
    OR f.specialty_domain = 'Primary Care'
)
JOIN disease_states d ON d.body_system = m.diag_sys
GROUP BY 1, 2, 3, 4, 5, 6;


-- 3. CRITICAL: Add indexes so your report queries are fast
-- Indexing the Fact Table
CREATE INDEX idx_fact_hcpcs ON fact_utilization(hcpcs_code);
CREATE INDEX idx_fact_domain ON fact_utilization(specialty_domain);

-- Indexing the Dimension Tables
CREATE INDEX idx_dim_proc_hcpcs ON dim_procedure(hcpcs_code);



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
        SUM(total_services) AS total_services,
        SUM(total_beneficiaries) AS total_benes
    FROM fact_utilization
    GROUP BY 1, 2
),
global_hcpcs_totals AS (
    -- This table exists and contains the national totals
    SELECT 
        HCPCS_Cd AS hcpcs_code,
        SUM(CAST(NULLIF(Tot_Srvcs, '') AS REAL)) AS global_services,
        SUM(CAST(NULLIF(Tot_Benes, '') AS INTEGER)) AS global_benes
    FROM uniform_resource_cms_bygeography
    WHERE UPPER(Rndrng_Prvdr_Geo_Lvl) = 'NATIONAL' 
    GROUP BY 1
)
SELECT 
    s.specialty_name,
    s.hcpcs_code,
    s.total_services,
    s.total_benes,
    g.global_services,
    g.global_benes,
    -- Dominance should use services, not beneficiaries, because beneficiary counts
    -- are not additive across place-of-service rollups in CMS geography data.
    ROUND(CAST(s.total_services AS REAL) / NULLIF(g.global_services, 0), 4) AS specialty_dominance_ratio
FROM specialty_hcpcs_totals s
JOIN global_hcpcs_totals g ON s.hcpcs_code = g.hcpcs_code;

-- Corrected Index (using the column name we defined in the SELECT)
CREATE INDEX IF NOT EXISTS idx_smd_spec  ON specialty_market_dynamics(specialty_name);
CREATE INDEX IF NOT EXISTS idx_smd_hcpcs ON specialty_market_dynamics(hcpcs_code);

-------------------------

-- -----------------------------------------------------------------------------
-- VIEW 6B: condition_monitoring_proxy
-- Condition-level repeat-interaction proxy using ONLY monitoring-flagged HCPCS.
-- Adds disease-specific relevance filters so conditions do not collapse to a
-- single uniform value across the body-system bridge.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS condition_monitoring_proxy;
CREATE VIEW condition_monitoring_proxy AS
WITH disease_states AS (
    SELECT DISTINCT disease_state, body_system
    FROM dim_diagnosis
    WHERE disease_state != 'General / Other'
),
procedure_flags AS (
    SELECT
        hcpcs_code,
        MAX(is_monitoring_flag) AS is_monitoring_flag
    FROM dim_procedure
    GROUP BY hcpcs_code
),
procedure_lookup AS (
    SELECT
        hcpcs_code,
        MAX(procedure_description) AS procedure_description
    FROM dim_procedure
    GROUP BY hcpcs_code
),
monitoring_base AS (
    SELECT
        f.specialty_name,
        f.specialty_domain,
        f.hcpcs_code,
        SUM(f.total_services)       AS total_services,
        SUM(f.total_beneficiaries)  AS total_beneficiaries,
        SUM(f.total_allowed_amt) AS total_spend
    FROM fact_utilization f
    JOIN procedure_flags p ON f.hcpcs_code = p.hcpcs_code
    WHERE p.is_monitoring_flag = 1
    GROUP BY 1, 2, 3
),
condition_monitoring_matches AS (
    SELECT
        d.disease_state,
        d.body_system,
        mb.specialty_name,
        mb.specialty_domain,
        mb.hcpcs_code,
        COALESCE(dp.procedure_description, 'Unknown Procedure') AS procedure_description,
        mb.total_services,
        mb.total_beneficiaries,
        mb.total_spend,
        ROUND(mb.total_services * 1.0 / NULLIF(mb.total_beneficiaries, 0), 3) AS srvcs_per_beneficiary
    FROM disease_states d
    JOIN monitoring_base mb ON 1 = 1
    LEFT JOIN procedure_lookup dp ON mb.hcpcs_code = dp.hcpcs_code
    WHERE (
        -- Diabetes-focused monitoring
        (
            d.disease_state = 'Type 2 Diabetes'
            AND mb.specialty_name = 'Internal Medicine'
            AND mb.hcpcs_code IN ('82947','82950','82962','83036','83037','99211','99212','99213','99214','99215')
        )
        OR
        -- Renal-focused monitoring
        (
            d.disease_state = 'Chronic Kidney Disease'
            AND mb.specialty_name IN ('Nephrology', 'Internal Medicine')
            AND mb.hcpcs_code IN ('90935','90937','90945','90947','90999','G0491','G0492','99211','99212','99213','99214','99215')
        )
        OR
        -- Cardiovascular-focused monitoring
        (
            d.body_system = 'Cardiovascular'
            AND mb.specialty_name IN ('Cardiology', 'Internal Medicine')
            AND mb.hcpcs_code IN ('93000','93005','93010','93224','93225','93226','93227','93784','93788','93790')
        )
        OR
        -- Respiratory-focused monitoring
        (
            d.body_system = 'Respiratory & Sleep'
            AND mb.specialty_name IN ('Pulmonology', 'Sleep Medicine', 'Internal Medicine')
            AND mb.hcpcs_code IN ('94010','94060','94660','95800','95801','95806','95810','95811','G0398')
        )
        OR
        -- For oncology and other chronic conditions, use office-visit monitoring baseline.
        (
            d.body_system = 'Oncology'
            AND mb.specialty_name IN ('Internal Medicine', 'Other Specialty')
            AND mb.hcpcs_code BETWEEN '99211' AND '99215'
        )
        OR
        -- For neuro / musculoskeletal / general medicine, keep repeat-visit office baseline.
        (
            d.body_system IN ('Neurological & Mental Health', 'Musculoskeletal', 'General Medicine')
            AND mb.specialty_name IN ('Internal Medicine', 'Other Specialty')
            AND mb.hcpcs_code BETWEEN '99211' AND '99215'
        )
    )
),
condition_monitoring_agg AS (
    SELECT
        disease_state,
        body_system,
        COUNT(DISTINCT hcpcs_code)                                                 AS monitoring_hcpcs_count,
        SUM(total_services)                                                        AS monitoring_services,
        SUM(total_beneficiaries)                                                   AS monitoring_beneficiaries,
        ROUND(SUM(total_services) * 1.0 / NULLIF(SUM(total_beneficiaries), 0), 3) AS monitoring_services_per_beneficiary,
        SUM(total_spend)                                                           AS monitoring_total_spend,
        ROUND(SUM(total_spend) * 1.0 / NULLIF(SUM(total_beneficiaries), 0), 2)    AS monitoring_spend_per_beneficiary,
        SUM(CASE WHEN srvcs_per_beneficiary >= 4 THEN 1 ELSE 0 END)               AS high_frequency_hcpcs_count
    FROM condition_monitoring_matches
    GROUP BY disease_state, body_system
)
SELECT
    d.disease_state,
    d.body_system,
    COALESCE(a.monitoring_hcpcs_count, 0)                                      AS monitoring_hcpcs_count,
    COALESCE(a.monitoring_services, 0)                                         AS monitoring_services,
    COALESCE(a.monitoring_beneficiaries, 0)                                    AS monitoring_beneficiaries,
    COALESCE(a.monitoring_services_per_beneficiary, 0)                         AS monitoring_services_per_beneficiary,
    COALESCE(a.monitoring_total_spend, 0)                                      AS monitoring_total_spend,
    COALESCE(a.monitoring_spend_per_beneficiary, 0)                            AS monitoring_spend_per_beneficiary,
    COALESCE(a.high_frequency_hcpcs_count, 0)                                  AS high_frequency_hcpcs_count,
    CASE
        WHEN COALESCE(a.monitoring_beneficiaries, 0) = 0 THEN NULL
        ELSE RANK() OVER (
        ORDER BY
            COALESCE(a.monitoring_services_per_beneficiary, 0) DESC,
            COALESCE(a.monitoring_total_spend, 0) DESC
        )
    END                                                                       AS interaction_rank
FROM disease_states d
LEFT JOIN condition_monitoring_agg a
  ON d.disease_state = a.disease_state
 AND d.body_system = a.body_system;

DROP TABLE IF EXISTS condition_monitoring_proxy_table;
CREATE TABLE condition_monitoring_proxy_table AS
SELECT
        disease_state,
        body_system,
        monitoring_hcpcs_count,
        monitoring_services,
        monitoring_beneficiaries,
        monitoring_services_per_beneficiary,
        monitoring_total_spend,
        monitoring_spend_per_beneficiary,
        high_frequency_hcpcs_count,
        interaction_rank
FROM condition_monitoring_proxy;

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
    smd.total_services,
    smd.total_benes,
    ROUND(smd.specialty_dominance_ratio * 100, 1)                       AS pct_of_national_volume,
    ROUND(CAST(smd.total_services AS REAL) * smd.specialty_dominance_ratio, 0)
                                                                        AS weighted_dominance_score,
    RANK() OVER (
        PARTITION BY smd.specialty_name
        ORDER BY smd.specialty_dominance_ratio DESC
    )                                                                   AS dominance_rank
FROM specialty_market_dynamics smd
LEFT JOIN (
    SELECT
        hcpcs_code,
        MAX(procedure_description) AS procedure_description
    FROM dim_procedure
    GROUP BY hcpcs_code
) dp ON smd.hcpcs_code = dp.hcpcs_code
WHERE smd.total_benes > 500; -- Order By removed for View compatibility


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
        s.total_services,
        s.total_benes,
        ROUND(CAST(s.total_services AS REAL) / NULLIF(s.total_benes, 0), 2) AS services_per_patient
    FROM specialty_market_dynamics s
    LEFT JOIN (
        SELECT
            hcpcs_code,
            MAX(procedure_description) AS procedure_description,
            MAX(procedure_category) AS procedure_category,
            MAX(is_monitoring_flag) AS is_monitoring_flag
        FROM dim_procedure
        GROUP BY hcpcs_code
    ) dp ON s.hcpcs_code = dp.hcpcs_code
    WHERE s.total_benes > 500
)
SELECT 
    *,
    CASE
        WHEN services_per_patient >= 12 THEN 'High (12+ sessions/yr)'
        WHEN services_per_patient >= 4  THEN 'Moderate (4-11 sessions/yr)'
        ELSE 'Low (< 4 sessions/yr)'
    END AS interaction_tier
FROM base_metrics;

-- -----------------------------------------------------------------------------
-- VIEW 5B: relevant_procedure_frequency
-- Direct answer for: how often do the relevant CPT/HCPCS procedures appear in
-- Medicare utilization? Uses total services + beneficiaries + frequency ratio.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS relevant_procedure_frequency;
CREATE VIEW relevant_procedure_frequency AS
WITH procedure_lookup AS (
    SELECT
        hcpcs_code,
        MAX(procedure_description) AS procedure_description,
        MAX(procedure_category) AS procedure_category,
        MAX(procedure_signal) AS procedure_signal,
        MAX(is_monitoring_flag) AS is_monitoring_flag
    FROM dim_procedure
    GROUP BY hcpcs_code
),
medicare_totals AS (
    SELECT SUM(total_services) AS all_medicare_services
    FROM fact_utilization
)
SELECT
    f.hcpcs_code,
    COALESCE(pl.procedure_description, 'Unknown Procedure')             AS procedure_description,
    COALESCE(pl.procedure_category, 'Unclassified')                     AS procedure_category,
    COALESCE(pl.procedure_signal, 'Standard Care')                      AS procedure_signal,
    SUM(f.total_services)                                               AS total_services,
    SUM(f.total_beneficiaries)                                          AS total_beneficiaries,
    COUNT(DISTINCT f.state_abbr)                                        AS states_present,
    ROUND(SUM(f.total_services) * 1.0 / NULLIF(SUM(f.total_beneficiaries), 0), 2)
                                                                        AS services_per_beneficiary,
    ROUND(SUM(f.total_medicare_payment), 2)                             AS total_medicare_payment,
    ROUND(
        100.0 * SUM(f.total_services)
        / NULLIF((SELECT all_medicare_services FROM medicare_totals), 0)
    , 4)                                                                AS pct_of_all_medicare_services
FROM fact_utilization f
JOIN procedure_lookup pl ON f.hcpcs_code = pl.hcpcs_code
WHERE pl.is_monitoring_flag = 1
   OR pl.procedure_signal != 'Standard Care'
GROUP BY 1, 2, 3, 4;


-- VIEW 6: monitoring_procedure_intensity
DROP VIEW IF EXISTS monitoring_procedure_intensity;
CREATE VIEW monitoring_procedure_intensity AS
WITH procedure_flags AS (
    SELECT
        hcpcs_code,
        MAX(is_monitoring_flag) AS is_monitoring_flag
    FROM dim_procedure
    GROUP BY hcpcs_code
)
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
LEFT JOIN procedure_flags dp ON f.hcpcs_code = dp.hcpcs_code
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
    SELECT 'Respiratory & Sleep',                      'Respiratory & Sleep'                     UNION ALL
    SELECT 'Oncology',                                 'Oncology'                                UNION ALL
    SELECT 'Neurological & Mental Health',             'Neurological & Mental Health'            UNION ALL
    SELECT 'Musculoskeletal',                          'Musculoskeletal'                         UNION ALL
    SELECT 'General Medicine',                         'General Medicine'
),
procedure_lookup AS (
    SELECT
        hcpcs_code,
        MAX(procedure_description) AS procedure_description,
        MAX(procedure_category) AS procedure_category,
        MAX(procedure_signal) AS procedure_signal,
        MAX(is_monitoring_flag) AS is_monitoring_flag
    FROM dim_procedure
    GROUP BY hcpcs_code
)
SELECT
    d.disease_state,
    d.body_system,
    f.hcpcs_code,
    p.procedure_description,
    p.procedure_category,
    p.procedure_signal,
    COALESCE(p.is_monitoring_flag, 0) AS is_monitoring_flag,
    f.specialty_name,
    f.specialty_domain,
    SUM(f.total_beneficiaries)  AS patient_volume,
    SUM(f.total_services)       AS service_volume,
    SUM(f.total_allowed_amt) AS total_allowed_amt,
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
LEFT JOIN procedure_lookup p ON f.hcpcs_code = p.hcpcs_code
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
HAVING patient_volume >= 1;



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
WITH disease_specialty_bridge AS (
    SELECT
        dpb.disease_state,
        dpb.body_system,
        dpb.specialty_name,
        dpb.specialty_domain,
        SUM(dpb.patient_volume) AS patient_volume,
        SUM(dpb.service_volume) AS service_volume,
        SUM(dpb.total_allowed_amt) AS total_allowed_amt,
        SUM(dpb.total_medicare_payment) AS total_medicare_payment,
        ROUND(SUM(dpb.service_volume) * 1.0 / NULLIF(SUM(dpb.patient_volume),0), 2) AS avg_srvcs_per_patient,
        AVG(COALESCE(smd.specialty_dominance_ratio, 0)) AS avg_dominance_ratio
    FROM disease_procedure_bridge dpb
    LEFT JOIN specialty_market_dynamics smd 
      ON dpb.specialty_name = smd.specialty_name 
      AND dpb.hcpcs_code = smd.hcpcs_code
    GROUP BY 1, 2, 3, 4
    HAVING patient_volume >= 1
),
percentile_ranks AS (
    SELECT
        *,
        NTILE(100) OVER (ORDER BY patient_volume) AS vol_percentile,
        NTILE(100) OVER (ORDER BY avg_srvcs_per_patient) AS intensity_percentile,
        NTILE(100) OVER (ORDER BY total_allowed_amt) AS econ_percentile,
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
    ROUND(total_allowed_amt / 1000000.0, 2) AS total_allowed_spend_millions,
    ROUND(total_medicare_payment / 1000000.0, 2) AS total_medicare_payment_millions,
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
--        patient_volume, avg_srvcs_per_patient, total_allowed_spend_millions,
--        market_concentration_pct
-- FROM opportunity_scoring_view
-- WHERE opportunity_tier = 'Tier 1 — High'
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

-- J. Relevant CPT/HCPCS procedures by Medicare frequency
-- SELECT hcpcs_code, procedure_description, procedure_signal,
--        total_services, total_beneficiaries, services_per_beneficiary,
--        pct_of_all_medicare_services
-- FROM relevant_procedure_frequency
-- ORDER BY total_services DESC, services_per_beneficiary DESC LIMIT 25;

-- K. Condition proxy summary: likely specialties, repeat interaction, allowed spend
-- SELECT disease_state, specialty_name, specialty_domain,
--        SUM(patient_volume) AS patient_volume_proxy,
--        SUM(service_volume) AS service_volume,
--        ROUND(SUM(service_volume) * 1.0 / NULLIF(SUM(patient_volume), 0), 2) AS avg_srvcs_per_patient,
--        ROUND(SUM(total_allowed_amt) / 1e6, 2) AS total_allowed_spend_millions
-- FROM disease_procedure_bridge
-- WHERE disease_state = 'COPD'
-- GROUP BY disease_state, specialty_name, specialty_domain
-- ORDER BY total_allowed_spend_millions DESC;

-- L. Condition monitoring detail: high-frequency diagnostic / monitoring procedures
-- SELECT disease_state, hcpcs_code, procedure_description, specialty_name,
--        service_volume, avg_srvcs_per_patient, total_allowed_amt
-- FROM disease_procedure_bridge
-- WHERE disease_state = 'COPD' AND is_monitoring_flag = 1
-- ORDER BY service_volume DESC, avg_srvcs_per_patient DESC LIMIT 25;

-- I. Office-dominant specialties — ambulatory ownership signal
-- SELECT specialty_name, office_pct,
--        ROUND(office_spend/1e6,2) AS office_spend_millions,
--        ROUND(facility_spend/1e6,2) AS facility_spend_millions
-- FROM facility_vs_office_split
-- WHERE total_services > 50000
-- ORDER BY office_pct DESC LIMIT 20;
