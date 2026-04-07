-- =============================================================================
-- MEDIGY MARKET INTELLIGENCE — UNIFIED EXTENSIBLE ELT PIPELINE v2
-- Database: SQLite (surveilr RSSD)
--
-- ARCHITECTURE PRINCIPLE:
--   The ONLY place you touch to add a new disease is dim_condition_registry.
--   All downstream dimensions, facts, and analytics derive automatically.
--
-- LAYER 0  → Performance indexes on raw tables
-- LAYER 1  → dim_condition_registry  (master disease catalog — seed only here)
-- LAYER 2  → dim_* dimension tables  (derived from registry + raw refs)
-- LAYER 3  → fact_utilization_unified (multi-source fact — GEO + DME + HOSPITAL)
-- LAYER 4  → Analytics views / tables (generic, condition-agnostic)
-- =============================================================================


-- =============================================================================
-- SCHEMA REFERENCE — Confirmed column names per source table
-- (Verify anytime with: PRAGMA table_info(<table_name>);)
--
-- uniform_resource_cms_bygeography / uniform_resource_diagnostics_data
--   HCPCS_Cd | Rndrng_Prvdr_Geo_Lvl | Rndrng_Prvdr_Geo_Cd | Place_Of_Srvc
--   Tot_Rndrng_Prvdrs | Tot_Benes | Tot_Srvcs
--   Avg_Mdcr_Alowd_Amt | Avg_Mdcr_Pymt_Amt
--
-- uniform_resource_dme_data / uniform_resource_copd_oxygen
-- / uniform_resource_dme_cpap_e0601_e0470_e0471   (all same Rfrg_ layout)
--   HCPCS_Cd | Rfrg_Prvdr_State_Abrvtn
--   Tot_Suplr_Benes | Tot_Suplr_Srvcs
--   Avg_Suplr_Mdcr_Alowd_Amt | Avg_Suplr_Mdcr_Pymt_Amt
--
-- uniform_resource_cms_outpatienthospitals_byproviderandservice
--   APC_Cd  ← procedure code column (NOT HCPCS_Cd)
--   Rndrng_Prvdr_State_Abrvtn | Rndrng_Prvdr_CCN
--   Tot_Benes | Tot_Srvcs | Avg_Mdcr_Alowd_Amt | Avg_Mdcr_Pymt_Amt
--
-- uniform_resource_cms_inpatienthospitals_byproviderandservice
--   DRG_Cd  ← service code column (NOT HCPCS_Cd)
--   Rndrng_Prvdr_St | Rndrng_Prvdr_CCN
--
-- uniform_resource_ref_geo_adjustment
--   State | "Locality Name" | "Medicare Administrative Contractor (MAC)"
--   "2026 PW GPCI (with 1.0 Floor)"
-- =============================================================================


-- =============================================================================
-- LAYER 0 — RAW TABLE INDEXES
-- Run once after raw ingestion. Never need changing for new diseases.
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
CREATE INDEX IF NOT EXISTS idx_pos_map_code       ON uniform_resource_cms_bygeo_place_of_service_mapping (pos_code);

CREATE INDEX IF NOT EXISTS idx_raw_inpt_ccn       ON uniform_resource_cms_inpatienthospitals_byproviderandservice (Rndrng_Prvdr_CCN);
CREATE INDEX IF NOT EXISTS idx_raw_inpt_state     ON uniform_resource_cms_inpatienthospitals_byproviderandservice (Rndrng_Prvdr_St);
-- Note: inpatient uses DRG_Cd (Diagnosis-Related Group), not HCPCS_Cd
-- Index on DRG_Cd only if your schema has it; comment out if column absent:
-- CREATE INDEX IF NOT EXISTS idx_raw_inpt_service ON uniform_resource_cms_inpatienthospitals_byproviderandservice (DRG_Cd);

CREATE INDEX IF NOT EXISTS idx_raw_outpt_ccn      ON uniform_resource_cms_outpatienthospitals_byproviderandservice (Rndrng_Prvdr_CCN);
CREATE INDEX IF NOT EXISTS idx_raw_outpt_state    ON uniform_resource_cms_outpatienthospitals_byproviderandservice (Rndrng_Prvdr_State_Abrvtn);
-- Outpatient hospital table uses APC_Cd (Ambulatory Payment Classification), not HCPCS_Cd
CREATE INDEX IF NOT EXISTS idx_raw_outpt_apc      ON uniform_resource_cms_outpatienthospitals_byproviderandservice (APC_Cd);

CREATE INDEX IF NOT EXISTS idx_raw_dme_hcpcs      ON uniform_resource_dme_data (HCPCS_Cd);

CREATE INDEX IF NOT EXISTS idx_raw_pos_code       ON uniform_resource_cms_providerandservice_pos ("Place of Service Code");


-- =============================================================================
-- LAYER 1 — MASTER DISEASE REGISTRY
--
-- ► ADD A NEW DISEASE: INSERT one row here. Nothing else changes.
--
-- Column guide:
--   icd10_prefix        → primary ICD-10 prefix match (LIKE prefix || '%')
--   icd10_prefix_2      → optional secondary prefix
--   hcpcs_range_start/end → inclusive CPT range for bygeography matching
--   hcpcs_exact_list    → JSON array of exact HCPCS codes (overrides range)
--   dme_hcpcs_list      → JSON array of HCPCS codes for DMEPOS dataset
--   use_bygeo           → 1 = pull from CMS Part B geographic data
--   use_dmepos          → 1 = pull from DMEPOS supplier data
--   use_hospital        → 1 = pull from inpatient / outpatient hospital data
--   em_share_pct        → estimated fraction of E&M visits attributable
--   dme_cap_months      → max rental months (13=CPAP, 36=O2, 0=N/A)
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim_condition_registry (
    condition_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_name      TEXT NOT NULL UNIQUE,
    body_system         TEXT NOT NULL,
    tier                INTEGER NOT NULL DEFAULT 2,   -- 1=Flagship, 2=Core, 3=Baseline
    icd10_prefix        TEXT,
    icd10_prefix_2      TEXT,
    hcpcs_range_start   TEXT,
    hcpcs_range_end     TEXT,
    hcpcs_exact_list    TEXT,                         -- JSON: ["94010","94060"]
    dme_hcpcs_list      TEXT,                         -- JSON: ["E0601","E0470"]
    use_bygeo           INTEGER DEFAULT 1,
    use_dmepos          INTEGER DEFAULT 0,
    use_hospital        INTEGER DEFAULT 0,
    specialty_domain    TEXT,
    b2b_tier_primary    TEXT,
    em_share_pct        REAL DEFAULT 0.05,
    dme_cap_months      INTEGER DEFAULT 0,
    icon                TEXT DEFAULT 'activity',      -- Tabler icon for UI
    color               TEXT DEFAULT 'azure',         -- SQLPage color for UI
    is_active           INTEGER DEFAULT 1,
    notes               TEXT
);


-- ─────────────────────────────────────────────────────────────────────────────
-- SEED: Current disease portfolio
-- ► To add Heart Failure: INSERT one row below and re-run the pipeline.
-- ─────────────────────────────────────────────────────────────────────────────

INSERT OR IGNORE INTO dim_condition_registry
(condition_name, body_system, tier, icd10_prefix, icd10_prefix_2,
 hcpcs_range_start, hcpcs_range_end, hcpcs_exact_list,
 dme_hcpcs_list, use_bygeo, use_dmepos, use_hospital,
 specialty_domain, b2b_tier_primary, em_share_pct, dme_cap_months,
 icon, color)
VALUES
-- ── Tier 1: Flagship ─────────────────────────────────────────────────────────
('Sleep Apnea',
 'Respiratory & Sleep', 1,
 'G47.3', 'P28.3',
 '95800', '95811',
 '["95800","95801","95805","95806","95807","95808","95810","95811"]',
 '["E0601","E0470","E0471"]',
 1, 1, 1,
 'Respiratory & Sleep', 'Sleep Medicine', 0.06, 13,
 'lungs', 'teal'),

('COPD',
 'Respiratory & Sleep', 1,
 'J44', NULL,
 '94010', '94799',
 '["94010","94060","94726","94729","G0237","G0238","G0239"]',
 '["E0434","E1392"]',
 1, 1, 1,
 'Respiratory & Sleep', 'Pulmonology', 0.08, 36,
 'wind', 'orange'),

-- ── Tier 2: Core ─────────────────────────────────────────────────────────────
('Hypertriglyceridaemia',
 'Endocrine & Metabolic', 2,
 'E78.1', 'E78.5',
 '80061', '84478',
 '["80061","83721","83704","82465","84478","80090"]',
 '["S0265"]',
 1, 0, 0,
 'Endocrine & Metabolic', 'Endocrinology', 0.05, 0,
 'test-pipe', 'indigo'),

('Heart Failure',
 'Cardiovascular', 2,
 'I50', NULL,
 '93000', '93999',
 '["93000","93303","93306","93350","93351","99490","99439"]',
 NULL,
 1, 0, 1,
 'Cardiovascular', 'Cardiology', 0.07, 0,
 'heart', 'red'),

('Type 2 Diabetes',
 'Endocrine & Metabolic', 2,
 'E11', NULL,
 '83036', '83036',
 '["83036","82947","82962","95251","99213","99214"]',
 NULL,
 1, 0, 0,
 'Endocrine & Metabolic', 'Endocrinology', 0.06, 0,
 'droplet', 'grape'),

-- ── Tier 3: Baseline / Benchmark ─────────────────────────────────────────────
('Hypertension',
 'Cardiovascular', 3,
 'I10', NULL,
 '99213', '99215',
 '["99213","99214","99215","93000"]',
 NULL,
 1, 0, 0,
 'Cardiovascular', 'Internal Medicine', 0.05, 0,
 'heartbeat', 'blue'),

('Parkinsons Disease',
 'Neurological & Mental Health', 2,
 'G20', NULL,
 '95812', '95830',
 '["95812","95819","99483","G0296"]',
 NULL,
 1, 0, 0,
 'Neurological & Mental Health', 'Neurology', 0.06, 0,
 'brain', 'violet');


CREATE INDEX IF NOT EXISTS idx_reg_name   ON dim_condition_registry (condition_name);
CREATE INDEX IF NOT EXISTS idx_reg_active ON dim_condition_registry (is_active);
CREATE INDEX IF NOT EXISTS idx_reg_tier   ON dim_condition_registry (tier);


-- =============================================================================
-- LAYER 2 — DIMENSION TABLES  (fully auto-derived; never hand-edit)
-- =============================================================================

-- ── dim_diagnosis ─────────────────────────────────────────────────────────────
-- Dynamic ICD-10 → condition mapping driven by registry prefixes.
-- ── dim_diagnosis ─────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dim_diagnosis;
CREATE TABLE dim_diagnosis AS
SELECT
    d.icd10_code,
    d.description_long                                   AS diagnosis_description,
    COALESCE(r.condition_name, 'Other Chronic / Clinical') AS disease_state,
    COALESCE(r.body_system,   'General Medicine')          AS body_system
FROM uniform_resource_ref_icd10_diagnosis d
LEFT JOIN dim_condition_registry r
    ON  r.is_active = 1
    AND (   d.icd10_code LIKE r.icd10_prefix  || '%'
         OR d.icd10_code LIKE r.icd10_prefix_2 || '%')
WHERE d.icd10_code IS NOT NULL;

-- ── dim_procedure ──────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dim_procedure;
CREATE TABLE dim_procedure AS
SELECT
    p.HCPCS                                              AS hcpcs_code,
    TRIM(p.DESCRIPTION)                                  AS procedure_description,
    CASE
        WHEN p.HCPCS BETWEEN '99202' AND '99499'         THEN 'Evaluation & Management'
        WHEN p.HCPCS BETWEEN '70000' AND '79999'         THEN 'Radiology / Imaging'
        WHEN p.HCPCS BETWEEN '80000' AND '89999'         THEN 'Pathology & Laboratory'
        WHEN p.HCPCS BETWEEN '90000' AND '99199'         THEN 'Medicine & Monitoring'
        WHEN p.HCPCS BETWEEN '00100' AND '01999'         THEN 'Anesthesia'
        WHEN p.HCPCS BETWEEN '10000' AND '69999'         THEN 'Surgery'
        WHEN p.HCPCS GLOB '[A-Z]*'                       THEN 'HCPCS Level II (DME / Drug / Other)'
        ELSE 'Unclassified'
    END                                                  AS procedure_category,
    COALESCE(r.condition_name || ' — Diagnostic', 'Standard Care') AS procedure_signal,
    r.condition_name                                     AS linked_condition
FROM uniform_resource_ref_procedure_code p
LEFT JOIN dim_condition_registry r
    ON  r.is_active = 1
    AND (
            (r.hcpcs_range_start IS NOT NULL AND p.HCPCS >= r.hcpcs_range_start AND p.HCPCS <= r.hcpcs_range_end)
         OR (r.hcpcs_exact_list IS NOT NULL AND INSTR(r.hcpcs_exact_list, '"' || p.HCPCS || '"') > 0)
        );
        
CREATE INDEX IF NOT EXISTS idx_dim_proc_code    ON dim_procedure (hcpcs_code);
CREATE INDEX IF NOT EXISTS idx_dim_proc_cond    ON dim_procedure (linked_condition);
-- CREATE INDEX IF NOT EXISTS idx_dim_proc_monitor ON dim_procedure (is_monitoring_flag);


-- ── dim_specialty ──────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dim_specialty;
CREATE TABLE dim_specialty AS
SELECT DISTINCT
    Rndrng_Prvdr_Type AS raw_specialty_name,
    CASE
        WHEN Rndrng_Prvdr_Type LIKE '%Internal Medicine%'   THEN 'Internal Medicine'
        WHEN Rndrng_Prvdr_Type LIKE '%Family Practice%'
          OR Rndrng_Prvdr_Type LIKE '%Family Medicine%'     THEN 'Family Medicine'
        WHEN Rndrng_Prvdr_Type LIKE '%Cardiology%'          THEN 'Cardiology'
        WHEN Rndrng_Prvdr_Type LIKE '%Nephrology%'          THEN 'Nephrology'
        WHEN Rndrng_Prvdr_Type LIKE '%Endocrinology%'       THEN 'Endocrinology'
        WHEN Rndrng_Prvdr_Type LIKE '%Pulmon%'
          OR Rndrng_Prvdr_Type LIKE '%Pulmonary%'           THEN 'Pulmonology'
        WHEN Rndrng_Prvdr_Type LIKE '%Sleep%'               THEN 'Sleep Medicine'
        WHEN Rndrng_Prvdr_Type LIKE '%Oncology%'
          OR Rndrng_Prvdr_Type LIKE '%Hematology%'          THEN 'Oncology / Hematology'
        WHEN Rndrng_Prvdr_Type LIKE '%Neurology%'           THEN 'Neurology'
        WHEN Rndrng_Prvdr_Type LIKE '%Orthopedic%'          THEN 'Orthopedic Surgery'
        WHEN Rndrng_Prvdr_Type LIKE '%Psychiatry%'
          OR Rndrng_Prvdr_Type LIKE '%Psychology%'          THEN 'Psychiatry / Psychology'
        WHEN Rndrng_Prvdr_Type LIKE '%Gastroenterology%'   THEN 'Gastroenterology'
        WHEN Rndrng_Prvdr_Type LIKE '%Dermatology%'        THEN 'Dermatology'
        WHEN Rndrng_Prvdr_Type LIKE '%Nurse Practitioner%' THEN 'Nurse Practitioner'
        WHEN Rndrng_Prvdr_Type LIKE '%Physician Assistant%'THEN 'Physician Assistant'
        ELSE Rndrng_Prvdr_Type
    END AS specialty_name,
    CASE
        WHEN Rndrng_Prvdr_Type LIKE '%Cardiology%'          THEN 'Cardiovascular'
        WHEN Rndrng_Prvdr_Type LIKE '%Nephrology%'          THEN 'Renal & Urological'
        WHEN Rndrng_Prvdr_Type LIKE '%Endocrinology%'       THEN 'Endocrine & Metabolic'
        WHEN Rndrng_Prvdr_Type LIKE '%Pulmon%'              THEN 'Respiratory & Sleep'
        WHEN Rndrng_Prvdr_Type LIKE '%Sleep%'               THEN 'Respiratory & Sleep'
        WHEN Rndrng_Prvdr_Type LIKE '%Oncology%'
          OR Rndrng_Prvdr_Type LIKE '%Hematology%'          THEN 'Oncology'
        WHEN Rndrng_Prvdr_Type LIKE '%Neurology%'
          OR Rndrng_Prvdr_Type LIKE '%Psych%'               THEN 'Neurological & Mental Health'
        WHEN Rndrng_Prvdr_Type LIKE '%Orthopedic%'          THEN 'Musculoskeletal'
        WHEN Rndrng_Prvdr_Type LIKE '%Gastro%'              THEN 'Digestive'
        WHEN Rndrng_Prvdr_Type LIKE '%Dermat%'              THEN 'Dermatology'
        WHEN Rndrng_Prvdr_Type LIKE '%Internal Medicine%'
          OR Rndrng_Prvdr_Type LIKE '%Family%'              THEN 'Primary Care'
        ELSE 'Other Clinical'
    END AS specialty_domain
FROM uniform_resource_cms_provider
WHERE Rndrng_Prvdr_Type IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dim_spec_name   ON dim_specialty (specialty_name);
CREATE INDEX IF NOT EXISTS idx_dim_spec_domain ON dim_specialty (specialty_domain);


-- ── dim_geography ──────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dim_geography;
CREATE TABLE dim_geography AS
SELECT
    g.State                                                              AS state_abbr,
    g."Medicare Administrative Contractor (MAC)"                        AS mac_name,
    g."Locality Name"                                                    AS locality_name,
    CAST(NULLIF(TRIM(g."2026 PW GPCI (with 1.0 Floor)"), '') AS REAL)   AS pw_gpci,
    CASE
        WHEN CAST(NULLIF(TRIM(g."2026 PW GPCI (with 1.0 Floor)"), '') AS REAL) >= 1.1
            THEN 'High Cost Market'
        WHEN CAST(NULLIF(TRIM(g."2026 PW GPCI (with 1.0 Floor)"), '') AS REAL) >= 1.0
            THEN 'Average Cost Market'
        ELSE 'Low Cost Market'
    END AS cost_tier
FROM uniform_resource_ref_geo_adjustment g
WHERE g.State IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dim_geo_state ON dim_geography (state_abbr);


-- =============================================================================
-- LAYER 3 — UNIFIED FACT TABLE
--
-- Sources pulled are controlled by use_bygeo / use_dmepos / use_hospital flags
-- in dim_condition_registry. Add a new source dataset:
--   1. Add a new CTE below with the pattern of the existing CTEs.
--   2. UNION ALL it into the final SELECT.
--   3. No other code changes needed.
-- =============================================================================
-- DROP TABLE IF EXISTS fact_utilization_unified;
-- CREATE TABLE fact_utilization_unified AS

-- -- 1. CMS Part B Geography (Physician Services)
-- WITH geo AS (
--     SELECT 'CMS_GEO' AS source_type, TRIM(UPPER(g.Rndrng_Prvdr_Geo_Cd)) AS state_abbr, g.HCPCS_Cd AS hcpcs_code,
--            r.condition_name, r.body_system,r.specialty_domain,  r.tier, r.b2b_tier_primary, r.icon, r.color,
--            SUM(CAST(NULLIF(g.Tot_Benes, '') AS INTEGER)) AS total_beneficiaries,
--            SUM(CAST(NULLIF(g.Tot_Srvcs, '') AS REAL)) AS total_services,
--            SUM(CAST(NULLIF(g.Tot_Rndrng_Prvdrs, '') AS INTEGER)) AS total_providers,
--            SUM(CAST(NULLIF(g.Tot_Srvcs,'') AS REAL) * CAST(NULLIF(g.Avg_Mdcr_Alowd_Amt,'') AS REAL)) AS total_allowed_amt,
--            SUM(CAST(NULLIF(g.Tot_Srvcs,'') AS REAL) * CAST(NULLIF(g.Avg_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
--     FROM uniform_resource_cms_bygeography g
--     JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_bygeo = 1
--         AND (g.HCPCS_Cd BETWEEN r.hcpcs_range_start AND r.hcpcs_range_end OR INSTR(r.hcpcs_exact_list, '"' || g.HCPCS_Cd || '"') > 0)
--     WHERE UPPER(g.Rndrng_Prvdr_Geo_Lvl) = 'STATE' GROUP BY 1,2,3,4,5,6,7,8,9
-- ),

-- -- 2. General DMEPOS (Medical Equipment)
-- dme AS (
--     SELECT 'CMS_DME' AS source_type, d.Rfrg_Prvdr_State_Abrvtn AS state_abbr, d.HCPCS_Cd AS hcpcs_code,
--            r.condition_name, r.body_system,r.specialty_domain,  r.tier, r.b2b_tier_primary, r.icon, r.color,
--            SUM(CAST(NULLIF(d.Tot_Suplr_Benes, '') AS INTEGER)) AS total_beneficiaries,
--            SUM(CAST(NULLIF(d.Tot_Suplr_Srvcs, '') AS REAL)) AS total_services, NULL AS total_providers,
--            SUM(CAST(NULLIF(d.Tot_Suplr_Srvcs,'') AS REAL) * CAST(NULLIF(d.Avg_Suplr_Mdcr_Alowd_Amt,'') AS REAL)) AS total_allowed_amt,
--            SUM(CAST(NULLIF(d.Tot_Suplr_Srvcs,'') AS REAL) * CAST(NULLIF(d.Avg_Suplr_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
--     FROM uniform_resource_dme_data d
--     JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_dmepos = 1
--         AND INSTR(r.dme_hcpcs_list, '"' || d.HCPCS_Cd || '"') > 0
--     GROUP BY 1,2,3,4,5,6,7,8,9
-- ),

-- -- 3. COPD Oxygen (Condition-Specific DME)
-- copd_o2 AS (
--     SELECT 'CMS_DME_O2' AS source_type, o.Rfrg_Prvdr_State_Abrvtn AS state_abbr, o.HCPCS_Cd AS hcpcs_code,
--            r.condition_name, r.body_system,r.specialty_domain,  r.tier, r.b2b_tier_primary, r.icon, r.color,
--            SUM(CAST(NULLIF(o.Tot_Suplr_Benes, '') AS INTEGER)) AS total_beneficiaries,
--            SUM(CAST(NULLIF(o.Tot_Suplr_Srvcs, '') AS REAL)) AS total_services, NULL AS total_providers,
--            SUM(CAST(NULLIF(o.Tot_Suplr_Srvcs,'') AS REAL) * CAST(NULLIF(o.Avg_Suplr_Mdcr_Alowd_Amt,'') AS REAL)) AS total_allowed_amt,
--            SUM(CAST(NULLIF(o.Tot_Suplr_Srvcs,'') AS REAL) * CAST(NULLIF(o.Avg_Suplr_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
--     FROM uniform_resource_copd_oxygen o
--     JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_dmepos = 1
--         AND INSTR(r.dme_hcpcs_list, '"' || o.HCPCS_Cd || '"') > 0
--     GROUP BY 1,2,3,4,5,6,7,8,9
-- ),

-- -- 4. CPAP (Condition-Specific DME)
-- cpap AS (
--     SELECT 'CMS_DME_CPAP' AS source_type, c.Rfrg_Prvdr_State_Abrvtn AS state_abbr, c.HCPCS_Cd AS hcpcs_code,
--            r.condition_name, r.body_system,r.specialty_domain,  r.tier, r.b2b_tier_primary, r.icon, r.color,
--            SUM(CAST(NULLIF(c.Tot_Suplr_Benes, '') AS INTEGER)) AS total_beneficiaries,
--            SUM(CAST(NULLIF(c.Tot_Suplr_Srvcs, '') AS REAL)) AS total_services, NULL AS total_providers,
--            SUM(CAST(NULLIF(c.Tot_Suplr_Srvcs,'') AS REAL) * CAST(NULLIF(c.Avg_Suplr_Mdcr_Alowd_Amt,'') AS REAL)) AS total_allowed_amt,
--            SUM(CAST(NULLIF(c.Tot_Suplr_Srvcs,'') AS REAL) * CAST(NULLIF(c.Avg_Suplr_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
--     FROM uniform_resource_dme_cpap_e0601_e0470_e0471 c
--     JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_dmepos = 1
--         AND INSTR(r.dme_hcpcs_list, '"' || c.HCPCS_Cd || '"') > 0
--     GROUP BY 1,2,3,4,5,6,7,8,9
-- ),

-- -- 5. Hospital Outpatient (Corrected Columns)
-- -- 5. Outpatient (Bene_Cnt / CAPC_Srvcs)
-- outpatient AS (
--     SELECT 'CMS_HOSPITAL_OP' AS source_type, h.Rndrng_Prvdr_State_Abrvtn AS state_abbr, h.APC_Cd AS hcpcs_code,
--            r.condition_name, r.body_system,r.specialty_domain,  r.tier, r.b2b_tier_primary, r.icon, r.color,
--            SUM(CAST(NULLIF(h.Bene_Cnt, '') AS INTEGER)) AS total_beneficiaries,
--            SUM(CAST(NULLIF(h.CAPC_Srvcs, '') AS REAL)) AS total_services, NULL AS total_providers,
--            SUM(CAST(NULLIF(h.CAPC_Srvcs, '') AS REAL) * CAST(NULLIF(h.Avg_Mdcr_Alowd_Amt,'') AS REAL)) AS total_allowed_amt,
--            SUM(CAST(NULLIF(h.CAPC_Srvcs, '') AS REAL) * CAST(NULLIF(h.Avg_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
--     FROM uniform_resource_cms_outpatienthospitals_byproviderandservice h
--     JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_hospital = 1
--         AND (h.APC_Cd BETWEEN r.hcpcs_range_start AND r.hcpcs_range_end OR INSTR(r.hcpcs_exact_list, '"' || h.APC_Cd || '"') > 0)
--     GROUP BY 1,2,3,4,5,6,7,8,9
-- ),

-- -- 6. Diagnostics Data
-- diag_data AS (
--     SELECT 'CMS_DIAGNOSTICS' AS source_type, TRIM(UPPER(g.Rndrng_Prvdr_Geo_Cd)) AS state_abbr, g.HCPCS_Cd AS hcpcs_code,
--            r.condition_name, r.body_system,r.specialty_domain,  r.tier, r.b2b_tier_primary, r.icon, r.color,
--            SUM(CAST(NULLIF(g.Tot_Benes, '') AS INTEGER)) AS total_beneficiaries,
--            SUM(CAST(NULLIF(g.Tot_Srvcs, '') AS REAL)) AS total_services,
--            SUM(CAST(NULLIF(g.Tot_Rndrng_Prvdrs, '') AS INTEGER)) AS total_providers,
--            SUM(CAST(NULLIF(g.Tot_Srvcs,'') AS REAL) * CAST(NULLIF(g.Avg_Mdcr_Alowd_Amt,'') AS REAL)) AS total_allowed_amt,
--            SUM(CAST(NULLIF(g.Tot_Srvcs,'') AS REAL) * CAST(NULLIF(g.Avg_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
--     FROM uniform_resource_diagnostics_data g
--     JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_bygeo = 1
--         AND (g.HCPCS_Cd BETWEEN r.hcpcs_range_start AND r.hcpcs_range_end OR INSTR(r.hcpcs_exact_list, '"' || g.HCPCS_Cd || '"') > 0)
--     WHERE UPPER(g.Rndrng_Prvdr_Geo_Lvl) = 'STATE' GROUP BY 1,2,3,4,5,6,7,8,9
-- ),

-- -- 7. Inpatient (Tot_Dschrgs / Avg_Tot_Pymt_Amt)
-- inpatient AS (
--     SELECT 'CMS_HOSPITAL_INPT' AS source_type, i.Rndrng_Prvdr_St AS state_abbr, i.DRG_Cd AS hcpcs_code,
--            r.condition_name, r.body_system,r.specialty_domain,  r.tier, r.b2b_tier_primary, r.icon, r.color,
--            SUM(CAST(NULLIF(i.Tot_Dschrgs, '') AS INTEGER)) AS total_beneficiaries,
--            SUM(CAST(NULLIF(i.Tot_Dschrgs, '') AS REAL)) AS total_services, NULL AS total_providers,
--            SUM(CAST(NULLIF(i.Tot_Dschrgs, '') AS REAL) * CAST(NULLIF(i.Avg_Tot_Pymt_Amt,'') AS REAL)) AS total_allowed_amt,
--            SUM(CAST(NULLIF(i.Tot_Dschrgs, '') AS REAL) * CAST(NULLIF(i.Avg_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
--     FROM uniform_resource_cms_inpatienthospitals_byproviderandservice i
--     JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_hospital = 1
--     AND (
--         (r.condition_name = 'COPD' AND i.DRG_Cd IN ('190','191','192')) OR
--         (r.condition_name = 'Heart Failure' AND i.DRG_Cd IN ('291','292','293')) OR
--         (r.condition_name = 'Sleep Apnea' AND i.DRG_Cd IN ('154','155','156'))
--     )
--     GROUP BY 1,2,3,4,5,6,7,8,9
-- )
-- -- ── COMBINE ALL 7 SOURCES ────────────────────────────────────────────────────
-- SELECT * FROM geo UNION ALL SELECT * FROM dme UNION ALL SELECT * FROM copd_o2 UNION ALL 
-- SELECT * FROM cpap UNION ALL SELECT * FROM outpatient UNION ALL SELECT * FROM diag_data UNION ALL
-- SELECT * FROM inpatient;


-- =============================================================================
-- LAYER 3 — UNIFIED FACT TABLE (CORRECTED GROUP BY)
-- =============================================================================

DROP TABLE IF EXISTS fact_utilization_unified;
CREATE TABLE fact_utilization_unified AS

-- 1. CMS Part B Geography
WITH geo AS (
    SELECT 'CMS_GEO' AS source_type, TRIM(UPPER(g.Rndrng_Prvdr_Geo_Cd)) AS state_abbr, g.HCPCS_Cd AS hcpcs_code,
           TRIM(r.condition_name) AS condition_name, r.body_system, r.specialty_domain, r.tier, r.b2b_tier_primary, r.icon, r.color,
           SUM(CAST(NULLIF(g.Tot_Benes, '') AS INTEGER)) AS total_beneficiaries,
           SUM(CAST(NULLIF(g.Tot_Srvcs, '') AS REAL)) AS total_services,
           SUM(CAST(NULLIF(g.Tot_Rndrng_Prvdrs, '') AS INTEGER)) AS total_providers,
           SUM(CAST(NULLIF(g.Tot_Srvcs,'') AS REAL) * CAST(NULLIF(g.Avg_Mdcr_Alowd_Amt,'') AS REAL)) AS total_allowed_amt,
           SUM(CAST(NULLIF(g.Tot_Srvcs,'') AS REAL) * CAST(NULLIF(g.Avg_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
    FROM uniform_resource_cms_bygeography g
    JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_bygeo = 1
        AND (g.HCPCS_Cd BETWEEN r.hcpcs_range_start AND r.hcpcs_range_end OR INSTR(r.hcpcs_exact_list, '"' || g.HCPCS_Cd || '"') > 0)
    WHERE UPPER(g.Rndrng_Prvdr_Geo_Lvl) = 'STATE' 
    GROUP BY 1,2,3,4,5,6,7,8,9,10
),

-- 2. General DMEPOS
dme AS (
    SELECT 'CMS_DME' AS source_type, d.Rfrg_Prvdr_State_Abrvtn AS state_abbr, d.HCPCS_Cd AS hcpcs_code,
           TRIM(r.condition_name) AS condition_name, r.body_system, r.specialty_domain, r.tier, r.b2b_tier_primary, r.icon, r.color,
           SUM(CAST(NULLIF(d.Tot_Suplr_Benes, '') AS INTEGER)) AS total_beneficiaries,
           SUM(CAST(NULLIF(d.Tot_Suplr_Srvcs, '') AS REAL)) AS total_services, NULL AS total_providers,
           SUM(CAST(NULLIF(d.Tot_Suplr_Srvcs,'') AS REAL) * CAST(NULLIF(d.Avg_Suplr_Mdcr_Alowd_Amt,'') AS REAL)) AS total_allowed_amt,
           SUM(CAST(NULLIF(d.Tot_Suplr_Srvcs,'') AS REAL) * CAST(NULLIF(d.Avg_Suplr_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
    FROM uniform_resource_dme_data d
    JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_dmepos = 1
        AND INSTR(r.dme_hcpcs_list, '"' || d.HCPCS_Cd || '"') > 0
    GROUP BY 1,2,3,4,5,6,7,8,9,10
),

-- 3. COPD Oxygen
copd_o2 AS (
    SELECT 'CMS_DME_O2' AS source_type, o.Rfrg_Prvdr_State_Abrvtn AS state_abbr, o.HCPCS_Cd AS hcpcs_code,
           TRIM(r.condition_name) AS condition_name, r.body_system, r.specialty_domain, r.tier, r.b2b_tier_primary, r.icon, r.color,
           SUM(CAST(NULLIF(o.Tot_Suplr_Benes, '') AS INTEGER)) AS total_beneficiaries,
           SUM(CAST(NULLIF(o.Tot_Suplr_Srvcs, '') AS REAL)) AS total_services, NULL AS total_providers,
           SUM(CAST(NULLIF(o.Tot_Suplr_Srvcs,'') AS REAL) * CAST(NULLIF(o.Avg_Suplr_Mdcr_Alowd_Amt,'') AS REAL)) AS total_allowed_amt,
           SUM(CAST(NULLIF(o.Tot_Suplr_Srvcs,'') AS REAL) * CAST(NULLIF(o.Avg_Suplr_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
    FROM uniform_resource_copd_oxygen o
    JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_dmepos = 1
        AND INSTR(r.dme_hcpcs_list, '"' || o.HCPCS_Cd || '"') > 0
    GROUP BY 1,2,3,4,5,6,7,8,9,10
),

-- 4. CPAP
cpap AS (
    SELECT 'CMS_DME_CPAP' AS source_type, c.Rfrg_Prvdr_State_Abrvtn AS state_abbr, c.HCPCS_Cd AS hcpcs_code,
           TRIM(r.condition_name) AS condition_name, r.body_system, r.specialty_domain, r.tier, r.b2b_tier_primary, r.icon, r.color,
           SUM(CAST(NULLIF(c.Tot_Suplr_Benes, '') AS INTEGER)) AS total_beneficiaries,
           SUM(CAST(NULLIF(c.Tot_Suplr_Srvcs, '') AS REAL)) AS total_services, NULL AS total_providers,
           SUM(CAST(NULLIF(c.Tot_Suplr_Srvcs,'') AS REAL) * CAST(NULLIF(c.Avg_Suplr_Mdcr_Alowd_Amt,'') AS REAL)) AS total_allowed_amt,
           SUM(CAST(NULLIF(c.Tot_Suplr_Srvcs,'') AS REAL) * CAST(NULLIF(c.Avg_Suplr_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
    FROM uniform_resource_dme_cpap_e0601_e0470_e0471 c
    JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_dmepos = 1
        AND INSTR(r.dme_hcpcs_list, '"' || c.HCPCS_Cd || '"') > 0
    GROUP BY 1,2,3,4,5,6,7,8,9,10
),

-- 5. Outpatient (Bene_Cnt / CAPC_Srvcs)
outpatient AS (
    SELECT 'CMS_HOSPITAL_OP' AS source_type, h.Rndrng_Prvdr_State_Abrvtn AS state_abbr, h.APC_Cd AS hcpcs_code,
           TRIM(r.condition_name) AS condition_name, r.body_system, r.specialty_domain, r.tier, r.b2b_tier_primary, r.icon, r.color,
           SUM(CAST(NULLIF(h.Bene_Cnt, '') AS INTEGER)) AS total_beneficiaries,
           SUM(CAST(NULLIF(h.CAPC_Srvcs, '') AS REAL)) AS total_services, NULL AS total_providers,
           SUM(CAST(NULLIF(h.CAPC_Srvcs, '') AS REAL) * CAST(NULLIF(h.Avg_Mdcr_Alowd_Amt,'') AS REAL)) AS total_allowed_amt,
           SUM(CAST(NULLIF(h.CAPC_Srvcs, '') AS REAL) * CAST(NULLIF(h.Avg_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
    FROM uniform_resource_cms_outpatienthospitals_byproviderandservice h
    JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_hospital = 1
        AND (h.APC_Cd BETWEEN r.hcpcs_range_start AND r.hcpcs_range_end OR INSTR(r.hcpcs_exact_list, '"' || h.APC_Cd || '"') > 0)
    GROUP BY 1,2,3,4,5,6,7,8,9,10
),

-- 6. Diagnostics
diag_data AS (
    SELECT 'CMS_DIAGNOSTICS' AS source_type, TRIM(UPPER(g.Rndrng_Prvdr_Geo_Cd)) AS state_abbr, g.HCPCS_Cd AS hcpcs_code,
           TRIM(r.condition_name) AS condition_name, r.body_system, r.specialty_domain, r.tier, r.b2b_tier_primary, r.icon, r.color,
           SUM(CAST(NULLIF(g.Tot_Benes, '') AS INTEGER)) AS total_beneficiaries,
           SUM(CAST(NULLIF(g.Tot_Srvcs, '') AS REAL)) AS total_services,
           SUM(CAST(NULLIF(g.Tot_Rndrng_Prvdrs, '') AS INTEGER)) AS total_providers,
           SUM(CAST(NULLIF(g.Tot_Srvcs,'') AS REAL) * CAST(NULLIF(g.Avg_Mdcr_Alowd_Amt,'') AS REAL)) AS total_allowed_amt,
           SUM(CAST(NULLIF(g.Tot_Srvcs,'') AS REAL) * CAST(NULLIF(g.Avg_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
    FROM uniform_resource_diagnostics_data g
    JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_bygeo = 1
        AND (g.HCPCS_Cd BETWEEN r.hcpcs_range_start AND r.hcpcs_range_end OR INSTR(r.hcpcs_exact_list, '"' || g.HCPCS_Cd || '"') > 0)
    WHERE UPPER(g.Rndrng_Prvdr_Geo_Lvl) = 'STATE' 
    GROUP BY 1,2,3,4,5,6,7,8,9,10
),

-- 7. Inpatient (Tot_Dschrgs / Avg_Tot_Pymt_Amt)
inpatient AS (
    SELECT 'CMS_HOSPITAL_INPT' AS source_type, i.Rndrng_Prvdr_St AS state_abbr, i.DRG_Cd AS hcpcs_code,
           TRIM(r.condition_name) AS condition_name, r.body_system, r.specialty_domain, r.tier, r.b2b_tier_primary, r.icon, r.color,
           SUM(CAST(NULLIF(i.Tot_Dschrgs, '') AS INTEGER)) AS total_beneficiaries,
           SUM(CAST(NULLIF(i.Tot_Dschrgs, '') AS REAL)) AS total_services, NULL AS total_providers,
           SUM(CAST(NULLIF(i.Tot_Dschrgs, '') AS REAL) * CAST(NULLIF(i.Avg_Tot_Pymt_Amt,'') AS REAL)) AS total_allowed_amt,
           SUM(CAST(NULLIF(i.Tot_Dschrgs, '') AS REAL) * CAST(NULLIF(i.Avg_Mdcr_Pymt_Amt,'') AS REAL)) AS total_medicare_payment
    FROM uniform_resource_cms_inpatienthospitals_byproviderandservice i
    JOIN dim_condition_registry r ON r.is_active = 1 AND r.use_hospital = 1
    AND (
        (LOWER(TRIM(r.condition_name)) = 'COPD' AND i.DRG_Cd IN ('190','191','192')) OR
        (LOWER(TRIM(r.condition_name)) = 'Heart Failure' AND i.DRG_Cd IN ('291','292','293')) OR
        (LOWER(TRIM(r.condition_name)) = 'Sleep Apnea' AND i.DRG_Cd IN ('154','155','156'))
    )
    GROUP BY 1,2,3,4,5,6,7,8,9,10
)
-- ── COMBINE ALL 7 SOURCES
SELECT * FROM geo UNION ALL SELECT * FROM dme UNION ALL SELECT * FROM copd_o2 UNION ALL 
SELECT * FROM cpap UNION ALL SELECT * FROM outpatient UNION ALL SELECT * FROM diag_data UNION ALL
SELECT * FROM inpatient;



-- ── Unified Fact Indexes ──────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_u_cond  ON fact_utilization_unified (condition_name);
CREATE INDEX IF NOT EXISTS idx_fact_u_state ON fact_utilization_unified (state_abbr);
CREATE INDEX IF NOT EXISTS idx_fact_u_src   ON fact_utilization_unified (source_type);
CREATE INDEX IF NOT EXISTS idx_fact_u_tier  ON fact_utilization_unified (tier);
CREATE INDEX IF NOT EXISTS idx_fact_u_hcpcs ON fact_utilization_unified (hcpcs_code);


-- =============================================================================
-- LAYER 4 — ANALYTICS VIEWS  (generic / condition-agnostic)
--
-- These views power every SQLPage screen. They are condition-agnostic — they
-- aggregate over ALL diseases. New diseases appear automatically.
-- Never add disease-specific logic here; put it in the registry (Layer 1).
-- =============================================================================


-- =============================================================================
-- LAYER 4 — UPDATED ANALYTICS VIEW (condition_national_summary)
-- =============================================================================

DROP VIEW IF EXISTS condition_national_summary;
CREATE VIEW condition_national_summary AS
SELECT
    f.condition_name,
    f.specialty_domain,
    f.tier,
    f.b2b_tier_primary,
    f.icon,
    f.color,
    os.opportunity_score,
    COUNT(DISTINCT f.source_type)              AS data_sources,
    COUNT(DISTINCT f.state_abbr)               AS states_with_data,
    SUM(f.total_beneficiaries)                 AS total_beneficiaries,
    SUM(f.total_services)                      AS total_services,
    SUM(f.total_allowed_amt)                   AS total_allowed_amt,
    SUM(f.total_medicare_payment)              AS total_medicare_payment,
    -- Derived columns required by the dashboard
    ROUND(SUM(f.total_allowed_amt) / NULLIF(SUM(f.total_beneficiaries), 0), 2) AS allowed_per_patient,
    ROUND(SUM(f.total_services) / NULLIF(SUM(f.total_beneficiaries), 0), 2) AS services_per_patient
FROM fact_utilization_unified f
LEFT JOIN opportunity_score os ON TRIM(LOWER(f.condition_name)) = TRIM(LOWER(os.condition_name))
GROUP BY 1, 2, 3, 4, 5, 6, 7;



-- ── condition_state_breakdown ─────────────────────────────────────────────
-- State-level metrics per condition — drives geographic drilldown pages.
DROP VIEW IF EXISTS condition_state_breakdown;
CREATE VIEW condition_state_breakdown AS
SELECT
    f.condition_name,
    f.state_abbr,
    g.locality_name,
    g.cost_tier,
    g.pw_gpci,
    SUM(f.total_beneficiaries)  AS total_beneficiaries,
    SUM(f.total_services)       AS total_services,
    SUM(f.total_allowed_amt)    AS total_allowed_amt,
    SUM(f.total_medicare_payment) AS total_medicare_payment,
    ROUND(SUM(f.total_allowed_amt) /
          NULLIF(SUM(f.total_beneficiaries), 0), 2) AS allowed_per_patient
FROM fact_utilization_unified f
LEFT JOIN dim_geography g ON TRIM(UPPER(f.state_abbr)) = TRIM(UPPER(g.state_abbr))
GROUP BY 1, 2, 3, 4, 5;


-- ── condition_source_breakdown ─────────────────────────────────────────────
-- Source-layer breakdown per condition — shows GEO vs DME vs Hospital split.
DROP VIEW IF EXISTS condition_source_breakdown;
CREATE VIEW condition_source_breakdown AS
SELECT
    f.condition_name,
    f.source_type,
    SUM(f.total_beneficiaries)   AS total_beneficiaries,
    SUM(f.total_services)        AS total_services,
    SUM(f.total_allowed_amt)     AS total_allowed_amt,
    SUM(f.total_medicare_payment) AS total_medicare_payment
FROM fact_utilization_unified f
GROUP BY 1, 2;


-- ── condition_hcpcs_detail ────────────────────────────────────────────────
-- Procedure-level detail per condition — drives the procedure drilldown page.
DROP VIEW IF EXISTS condition_hcpcs_detail;
CREATE VIEW condition_hcpcs_detail AS
SELECT
    f.condition_name,
    f.hcpcs_code,
    COALESCE(p.procedure_description, 'Inpatient DRG ' || f.hcpcs_code) AS procedure_description,
    COALESCE(p.procedure_category, 'Hospital Inpatient') AS procedure_category,
    f.source_type,
    SUM(f.total_beneficiaries)   AS total_beneficiaries,
    SUM(f.total_services)        AS total_services,
    SUM(f.total_allowed_amt)     AS total_allowed_amt,
    ROUND(SUM(f.total_allowed_amt) /
          NULLIF(SUM(f.total_services), 0), 2) AS avg_allowed_per_service          
FROM fact_utilization_unified f
LEFT JOIN dim_procedure p ON f.hcpcs_code = p.hcpcs_code
GROUP BY 1, 2, 3, 4, 5;


-- ── executive_kpis ────────────────────────────────────────────────────────
-- Top-line numbers for the executive dashboard big_number cards.
DROP VIEW IF EXISTS executive_kpis;
CREATE VIEW executive_kpis AS
SELECT
    COUNT(DISTINCT f.condition_name)    AS total_conditions,
    COUNT(DISTINCT f.state_abbr)        AS total_states,
    COUNT(DISTINCT f.hcpcs_code)        AS total_procedures,
    SUM(f.total_beneficiaries)          AS total_beneficiaries,
    SUM(f.total_allowed_amt)            AS total_allowed_amt,
    SUM(f.total_medicare_payment)       AS total_medicare_payment,
    COUNT(DISTINCT f.source_type)       AS active_data_sources
FROM fact_utilization_unified f;


-- ── opportunity_score ─────────────────────────────────────────────────────
-- Composite opportunity score per condition for the scoring dashboard.
-- Score = normalized(beneficiaries) × 0.4 + normalized(allowed_amt) × 0.4
--         + (4 - tier) × 0.2   (Tier 1 = 3 pts, Tier 2 = 2 pts, Tier 3 = 1 pt)
DROP VIEW IF EXISTS opportunity_score;
CREATE VIEW opportunity_score AS
WITH base AS (
    SELECT
        condition_name,
        specialty_domain,
        tier,
        b2b_tier_primary,
        icon,
        color,
        SUM(total_beneficiaries) AS total_benes,
        SUM(total_allowed_amt)   AS total_allowed
    FROM fact_utilization_unified
    GROUP BY 1, 2, 3, 4, 5, 6
),
maxvals AS (
    SELECT
        MAX(total_benes)   AS max_benes,
        MAX(total_allowed) AS max_allowed
    FROM base
)
SELECT
    b.condition_name,
    b.specialty_domain,
    b.tier,
    b.b2b_tier_primary,
    b.icon,
    b.color,
    b.total_benes,
    b.total_allowed,
    ROUND(
        (CAST(b.total_benes AS REAL)   / NULLIF(m.max_benes,   0)) * 40
      + (CAST(b.total_allowed AS REAL) / NULLIF(m.max_allowed, 0)) * 40
      + (4 - b.tier) * 20
    , 1) AS opportunity_score
FROM base b, maxvals m
ORDER BY opportunity_score DESC;


-- ── top_states_by_condition ───────────────────────────────────────────────
-- Top 10 states per condition by total allowed spend.
DROP VIEW IF EXISTS top_states_by_condition;
CREATE VIEW top_states_by_condition AS
SELECT
    condition_name,
    state_abbr,
    total_beneficiaries,
    total_allowed_amt,
    total_medicare_payment,
    allowed_per_patient,
    ROW_NUMBER() OVER (PARTITION BY condition_name
                       ORDER BY total_allowed_amt DESC) AS state_rank
FROM condition_state_breakdown;


-- =============================================================================
-- END OF PIPELINE
-- =============================================================================
