-- =============================================================================
--  COPD ANALYTICS
-- Script 01: Schema Creation
-- Database: SQLite
-- Covers: COPD PFT Diagnostics | E&M Visits | Oxygen DME
-- =============================================================================

-- -----------------------------------------------------------------------------
-- TABLE 1: COPD PFT DIAGNOSTICS
-- Source: CMS Geographic Variation Public Use File
-- CPT Codes: 94010, 94060, 94726, 94729
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS copd_pft; 
CREATE TABLE IF NOT EXISTS copd_pft (
    -- id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    geo_level           TEXT    NOT NULL,   -- 'National' or 'State'
    geo_code            TEXT,
    geo_desc            TEXT    NOT NULL,   -- State name or 'National'
    hcpcs_cd            TEXT    NOT NULL,   -- 94010 | 94060 | 94726 | 94729
    hcpcs_desc          TEXT    NOT NULL,
    place_of_srvc       TEXT    NOT NULL,   -- 'F' = Facility | 'O' = Office
    tot_rndrng_prvdrs   INTEGER,
    tot_benes           INTEGER,
    tot_srvcs           INTEGER,
    tot_bene_day_srvcs  INTEGER,
    avg_sbmtd_chrg      REAL,
    avg_mdcr_alowd_amt  REAL,
    avg_mdcr_pymt_amt   REAL,
    avg_mdcr_stdzd_amt  REAL
);

CREATE INDEX IF NOT EXISTS idx_pft_geo_level  ON copd_pft (geo_level);
CREATE INDEX IF NOT EXISTS idx_pft_geo_desc   ON copd_pft (geo_desc);
CREATE INDEX IF NOT EXISTS idx_pft_hcpcs      ON copd_pft (hcpcs_cd);
CREATE INDEX IF NOT EXISTS idx_pft_place      ON copd_pft (place_of_srvc);


-- -----------------------------------------------------------------------------
-- TABLE 2: COPD E&M VISITS
-- Source: CMS Geographic Variation Public Use File
-- CPT Codes: 99213, 99214
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS copd_em; 
CREATE TABLE IF NOT EXISTS copd_em (
 
    -- id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    geo_level           TEXT    NOT NULL,
    geo_code            TEXT,
    geo_desc            TEXT    NOT NULL,
    hcpcs_cd            TEXT    NOT NULL,   -- 99213 | 99214
    hcpcs_desc          TEXT    NOT NULL,
    place_of_srvc       TEXT    NOT NULL,
    tot_rndrng_prvdrs   INTEGER,
    tot_benes           INTEGER,
    tot_srvcs           INTEGER,
    tot_bene_day_srvcs  INTEGER,
    avg_sbmtd_chrg      REAL,
    avg_mdcr_alowd_amt  REAL,
    avg_mdcr_pymt_amt   REAL,
    avg_mdcr_stdzd_amt  REAL
);

CREATE INDEX IF NOT EXISTS idx_em_geo_level   ON copd_em (geo_level);
CREATE INDEX IF NOT EXISTS idx_em_geo_desc    ON copd_em (geo_desc);
CREATE INDEX IF NOT EXISTS idx_em_hcpcs       ON copd_em (hcpcs_cd);


-- -----------------------------------------------------------------------------
-- TABLE 3: OXYGEN DME
-- Source: CMS Referring Provider DMEPOS Public Use File
-- HCPCS Codes: E0434 (portable liquid oxygen), E1392 (portable concentrator)
-- Note: tot_suplr_benes is NULL for rows with <11 beneficiaries (CMS suppression)
--       tot_suplr_srvcs = rental MONTHS billed (not individual procedures)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS copd_oxygen; 
CREATE TABLE IF NOT EXISTS copd_oxygen (
    -- id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    rfrg_npi                 TEXT    NOT NULL,
    prvdr_last_name          TEXT,
    prvdr_first_name         TEXT,
    prvdr_credentials        TEXT,
    prvdr_state              TEXT    NOT NULL,   -- 2-letter state abbreviation
    prvdr_state_fips         TEXT,
    prvdr_zip5               TEXT,
    ruca_cat                 TEXT,               -- 'Urban' | 'Rural' | 'Unknown'
    ruca_code                TEXT,
    ruca_desc                TEXT,
    country                  TEXT,
    specialty_cd             TEXT,
    specialty_desc           TEXT,
    rbcs_lvl                 TEXT,
    rbcs_id                  TEXT,
    rbcs_desc                TEXT,
    hcpcs_cd                 TEXT    NOT NULL,   -- E0434 | E1392
    hcpcs_desc               TEXT    NOT NULL,
    suplr_rentl_ind          TEXT,               -- 'Y' = rental
    tot_suplrs               INTEGER,
    tot_suplr_benes          INTEGER,            -- NULL when <11 (suppressed)
    tot_suplr_clms           INTEGER,
    tot_suplr_srvcs          INTEGER,            -- rental months billed
    avg_suplr_sbmtd_chrg     REAL,
    avg_suplr_mdcr_alowd_amt REAL,
    avg_suplr_mdcr_pymt_amt  REAL,
    avg_suplr_mdcr_stdzd_amt REAL
);

CREATE INDEX IF NOT EXISTS idx_o2_npi         ON copd_oxygen (rfrg_npi);
CREATE INDEX IF NOT EXISTS idx_o2_state       ON copd_oxygen (prvdr_state);
CREATE INDEX IF NOT EXISTS idx_o2_hcpcs       ON copd_oxygen (hcpcs_cd);
CREATE INDEX IF NOT EXISTS idx_o2_ruca        ON copd_oxygen (ruca_cat);
CREATE INDEX IF NOT EXISTS idx_o2_specialty   ON copd_oxygen (specialty_desc);


-- =============================================================================
--  COPD ANALYTICS
-- Script 02: Data Load
-- Database: SQLite
-- =============================================================================
-- -----------------------------------------------------------------------------
-- STEP 1: Migrate raw -> clean tables with type casting
-- -----------------------------------------------------------------------------

-- PFT: filter only the 4 COPD CPT codes and cast types
INSERT OR REPLACE INTO copd_pft (
    geo_level, geo_code, geo_desc, hcpcs_cd, hcpcs_desc,
    place_of_srvc, tot_rndrng_prvdrs, tot_benes, tot_srvcs,
    tot_bene_day_srvcs, avg_sbmtd_chrg, avg_mdcr_alowd_amt,
    avg_mdcr_pymt_amt, avg_mdcr_stdzd_amt
)
SELECT
    TRIM(Rndrng_Prvdr_Geo_Lvl),
    TRIM(Rndrng_Prvdr_Geo_Cd),
    TRIM(Rndrng_Prvdr_Geo_Desc),
    TRIM(HCPCS_Cd),
    TRIM(HCPCS_Desc),
    TRIM(Place_Of_Srvc),
    CAST(NULLIF(TRIM(Tot_Rndrng_Prvdrs), '') AS INTEGER),
    CAST(NULLIF(TRIM(Tot_Benes),          '') AS INTEGER),
    CAST(NULLIF(TRIM(Tot_Srvcs),          '') AS INTEGER),
    CAST(NULLIF(TRIM(Tot_Bene_Day_Srvcs), '') AS INTEGER),
    CAST(NULLIF(TRIM(Avg_Sbmtd_Chrg),     '') AS REAL),
    CAST(NULLIF(TRIM(Avg_Mdcr_Alowd_Amt), '') AS REAL),
    CAST(NULLIF(TRIM(Avg_Mdcr_Pymt_Amt),  '') AS REAL),
    CAST(NULLIF(TRIM(Avg_Mdcr_Stdzd_Amt), '') AS REAL)
FROM uniform_resource_cms_bygeography
WHERE TRIM(HCPCS_Cd) IN ('94010','94060','94726','94729');


-- E&M: filter only 99213 and 99214
INSERT OR REPLACE INTO copd_em (
    geo_level, geo_code, geo_desc, hcpcs_cd, hcpcs_desc,
    place_of_srvc, tot_rndrng_prvdrs, tot_benes, tot_srvcs,
    tot_bene_day_srvcs, avg_sbmtd_chrg, avg_mdcr_alowd_amt,
    avg_mdcr_pymt_amt, avg_mdcr_stdzd_amt
)
SELECT
    TRIM(Rndrng_Prvdr_Geo_Lvl),
    TRIM(Rndrng_Prvdr_Geo_Cd),
    TRIM(Rndrng_Prvdr_Geo_Desc),
    TRIM(HCPCS_Cd),
    TRIM(HCPCS_Desc),
    TRIM(Place_Of_Srvc),
    CAST(NULLIF(TRIM(Tot_Rndrng_Prvdrs), '') AS INTEGER),
    CAST(NULLIF(TRIM(Tot_Benes),          '') AS INTEGER),
    CAST(NULLIF(TRIM(Tot_Srvcs),          '') AS INTEGER),
    CAST(NULLIF(TRIM(Tot_Bene_Day_Srvcs), '') AS INTEGER),
    CAST(NULLIF(TRIM(Avg_Sbmtd_Chrg),     '') AS REAL),
    CAST(NULLIF(TRIM(Avg_Mdcr_Alowd_Amt), '') AS REAL),
    CAST(NULLIF(TRIM(Avg_Mdcr_Pymt_Amt),  '') AS REAL),
    CAST(NULLIF(TRIM(Avg_Mdcr_Stdzd_Amt), '') AS REAL)
FROM uniform_resource_diagnostics_data
WHERE TRIM(HCPCS_Cd) IN ('99213','99214');


-- Oxygen DME: filter E0434 and E1392 only
INSERT OR REPLACE INTO copd_oxygen (
    rfrg_npi, prvdr_last_name, prvdr_first_name, prvdr_credentials,
    prvdr_state, prvdr_state_fips, prvdr_zip5,
    ruca_cat, ruca_code, ruca_desc, country,
    specialty_cd, specialty_desc, rbcs_lvl, rbcs_id, rbcs_desc,
    hcpcs_cd, hcpcs_desc, suplr_rentl_ind,
    tot_suplrs, tot_suplr_benes, tot_suplr_clms, tot_suplr_srvcs,
    avg_suplr_sbmtd_chrg, avg_suplr_mdcr_alowd_amt,
    avg_suplr_mdcr_pymt_amt, avg_suplr_mdcr_stdzd_amt
)
SELECT
    TRIM(Rfrg_NPI),
    TRIM(Rfrg_Prvdr_Last_Name_Org),
    TRIM(Rfrg_Prvdr_First_Name),
    TRIM(Rfrg_Prvdr_Crdntls),
    TRIM(Rfrg_Prvdr_State_Abrvtn),
    TRIM(Rfrg_Prvdr_State_FIPS),
    TRIM(Rfrg_Prvdr_Zip5),
    TRIM(Rfrg_Prvdr_RUCA_Cat),
    TRIM(Rfrg_Prvdr_RUCA),
    TRIM(Rfrg_Prvdr_RUCA_Desc),
    TRIM(Rfrg_Prvdr_Cntry),
    TRIM(Rfrg_Prvdr_Spclty_Cd),
    TRIM(Rfrg_Prvdr_Spclty_Desc),
    TRIM(RBCS_Lvl),
    TRIM(RBCS_Id),
    TRIM(RBCS_Desc),
    TRIM(HCPCS_CD),
    TRIM(HCPCS_Desc),
    TRIM(Suplr_Rentl_Ind),
    CAST(NULLIF(TRIM(Tot_Suplrs),              '') AS INTEGER),
    CAST(NULLIF(TRIM(Tot_Suplr_Benes),         '') AS INTEGER),
    CAST(NULLIF(TRIM(Tot_Suplr_Clms),          '') AS INTEGER),
    CAST(NULLIF(TRIM(Tot_Suplr_Srvcs),         '') AS INTEGER),
    CAST(NULLIF(TRIM(Avg_Suplr_Sbmtd_Chrg),    '') AS REAL),
    CAST(NULLIF(TRIM(Avg_Suplr_Mdcr_Alowd_Amt),'') AS REAL),
    CAST(NULLIF(TRIM(Avg_Suplr_Mdcr_Pymt_Amt), '') AS REAL),
    CAST(NULLIF(TRIM(Avg_Suplr_Mdcr_Stdzd_Amt),'') AS REAL)
FROM uniform_resource_copd_oxygen
WHERE TRIM(HCPCS_CD) IN ('E0434','E1392');




-- =============================================================================
--  COPD ANALYTICS
-- Script 03: COPD PFT Diagnostic Queries
-- Source table: copd_pft
-- Covers every KPI, table, and observation in the COPD PFT Evidence Report
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 1: NATIONAL TOP-LINE KPIs
-- ─────────────────────────────────────────────────────────────────────────────

-- KPI 1: Total Medicare Allowed Payments
DROP VIEW IF EXISTS copd_pft_national_kpis;
CREATE VIEW copd_pft_national_kpis AS
SELECT
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)          AS total_allowed,
    ROUND(SUM(avg_sbmtd_chrg     * tot_srvcs), 2)          AS total_submitted,
    ROUND(SUM(avg_mdcr_pymt_amt  * tot_srvcs), 2)          AS total_payment,
    SUM(tot_srvcs)                                          AS total_services,
    SUM(tot_rndrng_prvdrs)                                  AS total_providers_sum,
    ROUND(
        SUM(avg_sbmtd_chrg * tot_srvcs) /
        SUM(avg_mdcr_alowd_amt * tot_srvcs), 2
    )                                                       AS system_markup_x,
    ROUND(
        SUM(avg_sbmtd_chrg * tot_srvcs) -
        SUM(avg_mdcr_alowd_amt * tot_srvcs), 2
    )                                                       AS billing_friction
FROM copd_pft
WHERE geo_level = 'National';


-- KPI 2: Unique beneficiaries (max per code to approximate unique patients)
DROP VIEW IF EXISTS pft_national_beneficiary_summary;
CREATE VIEW pft_national_beneficiary_summary AS
SELECT
    SUM(max_benes_per_code)                                 AS approx_unique_beneficiaries
FROM (
    SELECT
        hcpcs_cd,
        MAX(tot_benes)                                      AS max_benes_per_code
    FROM copd_pft
    WHERE geo_level = 'National'
    GROUP BY hcpcs_cd
);


-- KPI 3: Average allowed per procedure (all codes combined)
DROP VIEW IF EXISTS pft_national_unit_economics;
CREATE VIEW pft_national_unit_economics AS
SELECT
    ROUND(
        SUM(avg_mdcr_alowd_amt * tot_srvcs) /
        SUM(tot_srvcs), 2
    )                                                       AS avg_allowed_per_procedure
FROM copd_pft
WHERE geo_level = 'National';


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 2: PROCEDURE-LEVEL BREAKDOWN (Report Table 2.1)
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS pft_national_procedure_analytics;

CREATE VIEW pft_national_procedure_analytics AS
WITH national_totals AS (
    SELECT 
        p.hcpcs_cd,
        MAX(p.hcpcs_desc) AS description,
        SUM(p.tot_srvcs) AS total_services,
        MAX(p.tot_benes) AS beneficiaries_approx,
        SUM(p.avg_mdcr_alowd_amt * p.tot_srvcs) AS total_allowed_raw,
        SUM(p.avg_sbmtd_chrg * p.tot_srvcs) AS total_submitted_raw
    FROM copd_pft p
    WHERE geo_level = 'National'
    GROUP BY p.hcpcs_cd
)
SELECT
    hcpcs_cd,
    description,
    total_services,
    ROUND(total_services * 100.0 / SUM(total_services) OVER (), 1) AS pct_volume,
    beneficiaries_approx,
    ROUND(total_allowed_raw, 2) AS total_allowed,
    ROUND(total_allowed_raw / total_services, 2) AS avg_allowed_per_test,
    ROUND(total_submitted_raw / total_services, 2) AS avg_submitted_per_test,
    ROUND(total_submitted_raw / total_allowed_raw, 2) AS markup_x,
    ROUND(total_services * 1.0 / beneficiaries_approx, 2) AS interaction_density
FROM national_totals;


-- Table: Simple average allowed per code per setting (facility vs office)
-- Used in Report Section 2.1 footnote
DROP VIEW IF EXISTS copd_pft_setting_comparison;
CREATE VIEW copd_pft_setting_comparison AS
SELECT
    hcpcs_cd,
    place_of_srvc,
    tot_srvcs,
    tot_benes,
    avg_mdcr_alowd_amt                                      AS avg_allowed,
    avg_sbmtd_chrg                                          AS avg_submitted,
    ROUND(avg_sbmtd_chrg / avg_mdcr_alowd_amt, 2)           AS markup_x
FROM copd_pft
WHERE geo_level = 'National'
ORDER BY hcpcs_cd, place_of_srvc;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 2.2: FACILITY vs OFFICE SPLIT
-- ─────────────────────────────────────────────────────────────────────────────

DROP VIEW IF EXISTS pft_national_setting_distribution;

CREATE VIEW pft_national_setting_distribution AS
WITH base_metrics AS (
    SELECT
        place_of_srvc,
        CASE place_of_srvc 
            WHEN 'F' THEN 'Facility' 
            ELSE 'Office / Non-Facility' 
        END AS setting,
        SUM(tot_srvcs) AS services,
        SUM(avg_mdcr_alowd_amt * tot_srvcs) AS allowed_raw,
        SUM(tot_benes) AS benes
    FROM copd_pft
    WHERE geo_level = 'National'
    GROUP BY place_of_srvc
),
totals AS (
    SELECT 
        *,
        SUM(services) OVER () AS national_total_services,
        SUM(allowed_raw) OVER () AS national_total_allowed
    FROM base_metrics
)
SELECT
    place_of_srvc,
    setting,
    services AS total_services,
    ROUND(services * 100.0 / national_total_services, 1) AS pct_services,
    ROUND(allowed_raw, 2) AS total_allowed,
    ROUND(allowed_raw * 100.0 / national_total_allowed, 1) AS pct_allowed,
    benes AS total_benes,
    ROUND(allowed_raw / services, 2) AS avg_allowed_per_test
FROM totals
ORDER BY place_of_srvc;

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 3: PRICING INEFFICIENCY
-- ─────────────────────────────────────────────────────────────────────────────


-- -----------------------------------------------------------------------------
-- Per-code markup table (Report Section 3)
-- VIEW: PFT Pricing Inefficiency Analytics
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS pft_national_pricing_inefficiency;

CREATE VIEW pft_national_pricing_inefficiency AS
WITH revenue_totals AS (
    SELECT
        hcpcs_cd,
        SUM(tot_srvcs) AS total_services,
        SUM(avg_sbmtd_chrg * tot_srvcs) AS total_submitted_raw,
        SUM(avg_mdcr_alowd_amt * tot_srvcs) AS total_allowed_raw
    FROM copd_pft
    WHERE geo_level = 'National'
    GROUP BY hcpcs_cd
)
SELECT
    hcpcs_cd,
    ROUND(total_submitted_raw / total_services, 2) AS avg_submitted,
    ROUND(total_allowed_raw / total_services, 2) AS avg_allowed,
    ROUND(total_submitted_raw / total_allowed_raw, 2) AS markup_x,
    ROUND(total_submitted_raw - total_allowed_raw, 2) AS total_friction
FROM revenue_totals
ORDER BY markup_x DESC;


-- -----------------------------------------------------------------------------
-- VIEW: National PFT Market Financial Summary
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS pft_national_market_financial_summary;

CREATE VIEW pft_national_market_financial_summary AS
WITH totals AS (
    SELECT 
        SUM(avg_sbmtd_chrg * tot_srvcs) AS total_submitted_raw,
        SUM(avg_mdcr_alowd_amt * tot_srvcs) AS total_allowed_raw
    FROM copd_pft
    WHERE geo_level = 'National'
)
SELECT
    ROUND(total_submitted_raw, 2) AS total_submitted,
    ROUND(total_allowed_raw, 2) AS total_allowed,
    ROUND(total_submitted_raw - total_allowed_raw, 2) AS total_friction,
    ROUND(total_submitted_raw / total_allowed_raw, 2) AS blended_markup_x
FROM totals;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 4: INTERACTION DENSITY (Report Table Section 4)
-- ─────────────────────────────────────────────────────────────────────────────
-- -----------------------------------------------------------------------------
-- VIEW: National PFT Clinical Interaction Density
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS pft_national_clinical_interaction_density;

CREATE VIEW pft_national_clinical_interaction_density AS
SELECT
    hcpcs_cd,
    MAX(hcpcs_desc) AS description,
    SUM(tot_srvcs) AS total_services,
    MAX(tot_benes) AS beneficiaries_approx,
    ROUND(SUM(tot_srvcs) * 1.0 / MAX(tot_benes), 2) AS interaction_density,
    CASE hcpcs_cd
        WHEN '94010' THEN 'First-line screening'
        WHEN '94060' THEN 'Diagnostic confirmation'
        WHEN '94726' THEN 'Severity classification'
        WHEN '94729' THEN 'Ongoing monitoring'
        ELSE 'Other PFT'
    END AS clinical_role
FROM copd_pft
WHERE geo_level = 'National'
GROUP BY hcpcs_cd
ORDER BY interaction_density DESC;


-- Overall interaction density
-- -----------------------------------------------------------------------------
-- VIEW: National PFT Overall Interaction Density
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS pft_national_overall_interaction_density;

CREATE VIEW pft_national_overall_interaction_density AS
SELECT
    ROUND(
        SUM(tot_srvcs) * 1.0 /
        (SELECT SUM(max_benes)
         FROM (SELECT MAX(tot_benes) AS max_benes 
               FROM copd_pft
               WHERE geo_level = 'National' 
               GROUP BY hcpcs_cd)), 2
    ) AS overall_density
FROM copd_pft
WHERE geo_level = 'National';


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5.1: TOP 10 STATES BY TOTAL ALLOWED (Report Table 5.1)
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: State-Level PFT Market Summary
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS pft_state_top_market_summary;

CREATE VIEW pft_state_top_market_summary AS
SELECT
    geo_desc                                                AS state,
    SUM(tot_srvcs)                                          AS total_services,
    SUM(tot_benes)                                          AS total_benes,
    SUM(tot_rndrng_prvdrs)                                  AS total_providers,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)           AS total_allowed,
    ROUND(SUM(avg_sbmtd_chrg     * tot_srvcs), 2)           AS total_submitted,
    ROUND(
        SUM(avg_mdcr_alowd_amt * tot_srvcs) /
        SUM(tot_srvcs), 2
    )                                                       AS avg_allowed_per_test
FROM copd_pft
WHERE geo_level = 'State'
GROUP BY geo_desc;


-- Top 10 states share of national total
-- -----------------------------------------------------------------------------
-- VIEW: PFT Market Share of Top 10 States
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS pft_top_10_states_market_share;

CREATE VIEW pft_top_10_states_market_share AS
SELECT
    ROUND(
        SUM(state_allowed) * 100.0 /
        (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) 
         FROM copd_pft 
         WHERE geo_level = 'National'), 1
    ) AS top10_pct_of_national
FROM (
    SELECT
        geo_desc,
        SUM(avg_mdcr_alowd_amt * tot_srvcs) AS state_allowed
    FROM copd_pft
    WHERE geo_level = 'State'
    GROUP BY geo_desc
    ORDER BY state_allowed DESC
    LIMIT 10
);

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5.2: PROVIDER ACCESS GAP — LOWEST PROVIDER STATES
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: State-Level PFT Provider Access Gap
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS pft_state_provider_access_gap;

CREATE VIEW pft_state_provider_access_gap AS
SELECT
    geo_desc                                                AS state,
    SUM(tot_rndrng_prvdrs)                                  AS total_providers,
    SUM(tot_srvcs)                                          AS total_services,
    ROUND(SUM(tot_srvcs) * 1.0 / NULLIF(SUM(tot_rndrng_prvdrs), 0), 1)
                                                            AS services_per_provider,
    CASE
        WHEN SUM(tot_rndrng_prvdrs) < 30  THEN 'CRITICAL'
        WHEN SUM(tot_rndrng_prvdrs) < 100 THEN 'HIGH'
        WHEN SUM(tot_rndrng_prvdrs) < 250 THEN 'MODERATE'
        ELSE 'ADEQUATE'
    END                                                     AS access_gap_level
FROM copd_pft
WHERE geo_level = 'State'
GROUP BY geo_desc
HAVING SUM(tot_rndrng_prvdrs) IS NOT NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5.3: MARKET READINESS — OFFICE TEST ADOPTION BY STATE
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: State-Level PFT Market Readiness (Office vs Facility)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS pft_state_market_readiness;

CREATE VIEW pft_state_market_readiness AS
SELECT
    geo_desc                                                AS state,
    SUM(CASE WHEN place_of_srvc = 'O' THEN tot_srvcs ELSE 0 END) AS office_services,
    SUM(CASE WHEN place_of_srvc = 'F' THEN tot_srvcs ELSE 0 END) AS facility_services,
    SUM(tot_srvcs)                                          AS total_services,
    ROUND(
        SUM(CASE WHEN place_of_srvc = 'O' THEN tot_srvcs ELSE 0 END) * 100.0 /
        NULLIF(SUM(tot_srvcs), 0), 1
    )                                                       AS office_pct,
    CASE
        WHEN SUM(CASE WHEN place_of_srvc = 'O' THEN tot_srvcs ELSE 0 END) * 100.0 /
             NULLIF(SUM(tot_srvcs), 0) >= 80 THEN 'HIGHEST'
        WHEN SUM(CASE WHEN place_of_srvc = 'O' THEN tot_srvcs ELSE 0 END) * 100.0 /
             NULLIF(SUM(tot_srvcs), 0) >= 70 THEN 'HIGH'
        WHEN SUM(CASE WHEN place_of_srvc = 'O' THEN tot_srvcs ELSE 0 END) * 100.0 /
             NULLIF(SUM(tot_srvcs), 0) >= 60 THEN 'MODERATE'
        ELSE 'TRANSITIONING'
    END                                                     AS market_readiness
FROM copd_pft
WHERE geo_level = 'State'
GROUP BY geo_desc
HAVING SUM(tot_srvcs) > 1000;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 6.1: MARKET SIZING — DIAGNOSTIC-AS-PRODUCT MODEL
-- ─────────────────────────────────────────────────────────────────────────────
-- -----------------------------------------------------------------------------
-- VIEW: PFT Revenue Model Assumptions (Market Sizing)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS pft_revenue_model_assumptions;

CREATE VIEW pft_revenue_model_assumptions AS
SELECT
    'Medicare diagnosed/tested annually (DLCO proxy)'       AS assumption,
    MAX(tot_benes)                                          AS value
FROM copd_pft
WHERE geo_level = 'National' AND hcpcs_cd = '94729' AND place_of_srvc = 'O'

UNION ALL SELECT 'Undiagnosed COPD (estimated)',          12000000
UNION ALL SELECT 'Voice screen price ($)',                79
UNION ALL SELECT 'Conservative capture 0.5% (tests)',    60000
UNION ALL SELECT 'Conservative Year 1 revenue ($)',      4740000
UNION ALL SELECT 'Moderate capture 2.0% (tests)',        240000
UNION ALL SELECT 'Moderate Year 1 revenue ($)',          18960000;

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 7: COMPETITIVE BENCHMARK TABLE (Report Section 7)
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: PFT Competitive Pricing Benchmark
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS pft_competitive_pricing_benchmark;

CREATE VIEW pft_competitive_pricing_benchmark AS
SELECT
    'Spirometry in-lab (94010 Facility)'        AS diagnostic_method,
    ROUND(AVG(avg_sbmtd_chrg), 2)              AS avg_submitted_patient_facing,
    ROUND(AVG(avg_mdcr_alowd_amt), 2)          AS avg_medicare_allowed,
    ROUND(AVG(avg_sbmtd_chrg) / 
          NULLIF(AVG(avg_mdcr_alowd_amt), 0), 2) AS markup_x,
    'Requires in-person visit + equipment'      AS friction
FROM copd_pft
WHERE geo_level = 'National' AND hcpcs_cd = '94010' AND place_of_srvc = 'F'

UNION ALL
SELECT
    'Spirometry office (94010 Office)',
    ROUND(AVG(avg_sbmtd_chrg), 2),
    ROUND(AVG(avg_mdcr_alowd_amt), 2),
    ROUND(AVG(avg_sbmtd_chrg) / 
          NULLIF(AVG(avg_mdcr_alowd_amt), 0), 2),
    'In-person, spirometer required'
FROM copd_pft
WHERE geo_level = 'National' AND hcpcs_cd = '94010' AND place_of_srvc = 'O'

UNION ALL
SELECT
    'Post-bronchodilator test (94060 Office)',
    ROUND(AVG(avg_sbmtd_chrg), 2),
    ROUND(AVG(avg_mdcr_alowd_amt), 2),
    ROUND(AVG(avg_sbmtd_chrg) / 
          NULLIF(AVG(avg_mdcr_alowd_amt), 0), 2),
    'In-person, spirometer + medication'
FROM copd_pft
WHERE geo_level = 'National' AND hcpcs_cd = '94060' AND place_of_srvc = 'O'

UNION ALL
SELECT
    'Voice Screen',
    NULL, 99.0, NULL,
    '60 seconds on phone, no equipment'

ORDER BY avg_medicare_allowed ASC;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 8: KEY FINDINGS SUMMARY — All metrics in one result set
-- ─────────────────────────────────────────────────────────────────────────────
-- -----------------------------------------------------------------------------
-- VIEW: National PFT Key Findings Summary (Executive Dashboard)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS pft_national_key_findings_summary;

CREATE VIEW pft_national_key_findings_summary AS
SELECT '1'  AS finding_no,
       'Medicare-only diagnostic market'  AS metric,
       ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 0) AS value_raw,
       '$' || ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) / 1000000, 2) || 'M' AS formatted
FROM copd_pft WHERE geo_level = 'National'

UNION ALL
SELECT '2', 'Total procedures billed',
       SUM(tot_srvcs), CAST(SUM(tot_srvcs) AS TEXT)
FROM copd_pft WHERE geo_level = 'National'

UNION ALL
SELECT '3', 'System markup (submitted / allowed)',
       ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) / SUM(avg_mdcr_alowd_amt * tot_srvcs), 2),
       ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) / SUM(avg_mdcr_alowd_amt * tot_srvcs), 2) || 'x'
FROM copd_pft WHERE geo_level = 'National'

UNION ALL
SELECT '4', 'Billing friction ($)',
       ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) - SUM(avg_mdcr_alowd_amt * tot_srvcs), 0),
       '$' || ROUND((SUM(avg_sbmtd_chrg * tot_srvcs) - SUM(avg_mdcr_alowd_amt * tot_srvcs)) / 1000000, 1) || 'M'
FROM copd_pft WHERE geo_level = 'National'

UNION ALL
SELECT '5', 'Office share of all services (%)',
       ROUND(SUM(CASE WHEN place_of_srvc='O' THEN tot_srvcs ELSE 0 END) * 100.0 / SUM(tot_srvcs), 1),
       ROUND(SUM(CASE WHEN place_of_srvc='O' THEN tot_srvcs ELSE 0 END) * 100.0 / SUM(tot_srvcs), 1) || '%'
FROM copd_pft WHERE geo_level = 'National'

UNION ALL
SELECT '6', 'Office share of all spend (%)',
       ROUND(SUM(CASE WHEN place_of_srvc='O' THEN avg_mdcr_alowd_amt * tot_srvcs ELSE 0 END) * 100.0 /
             SUM(avg_mdcr_alowd_amt * tot_srvcs), 1),
       ROUND(SUM(CASE WHEN place_of_srvc='O' THEN avg_mdcr_alowd_amt * tot_srvcs ELSE 0 END) * 100.0 /
             SUM(avg_mdcr_alowd_amt * tot_srvcs), 1) || '%'
FROM copd_pft WHERE geo_level = 'National';


-- =============================================================================
--  COPD ANALYTICS
-- Script 04: E&M Visits Queries (99213 / 99214)
-- Source table: copd_em
-- Covers every KPI, table, and observation in the E&M Integrated Report
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 1: NATIONAL TOP-LINE KPIs
-- ─────────────────────────────────────────────────────────────────────────────
-- -----------------------------------------------------------------------------
-- VIEW: National E&M All-Condition Market Summary
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS em_national_all_condition_summary;

CREATE VIEW em_national_all_condition_summary AS
WITH raw_totals AS (
    SELECT
        SUM(tot_srvcs) AS srvcs,
        SUM(avg_mdcr_alowd_amt * tot_srvcs) AS allowed_raw,
        SUM(avg_sbmtd_chrg * tot_srvcs) AS submitted_raw,
        SUM(tot_rndrng_prvdrs) AS providers
    FROM copd_em
    WHERE geo_level = 'National'
)
SELECT
    srvcs AS total_all_condition_services,
    ROUND(allowed_raw, 2) AS total_allowed,
    ROUND(submitted_raw, 2) AS total_submitted,
    ROUND(submitted_raw / allowed_raw, 2) AS system_markup_x,
    ROUND(allowed_raw / 1000000000.0, 2) AS total_allowed_billions,
    providers AS total_providers_sum
FROM raw_totals;

-- COPD-estimated share (8% applied to all-condition E&M)
-- -----------------------------------------------------------------------------
-- VIEW: National COPD Estimated E&M Share (8% Baseline)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS em_national_copd_estimated_share;

CREATE VIEW em_national_copd_estimated_share AS
SELECT
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08, 2)       AS copd_est_allowed,
    ROUND(SUM(tot_srvcs) * 0.08, 0)                             AS copd_est_services,
    0.08                                                        AS copd_share_pct,
    'Epidemiological estimate — 8% of Medicare E&M'             AS basis
FROM copd_em
WHERE geo_level = 'National';

-- -----------------------------------------------------------------------------
-- VIEW: E&M Market vs PFT Diagnostic Market Ratio
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS market_em_vs_pft_diagnostic_ratio;

CREATE VIEW market_em_vs_pft_diagnostic_ratio AS
WITH totals AS (
    SELECT 
        (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) FROM copd_em WHERE geo_level = 'National') AS em_total,
        (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) FROM copd_pft WHERE geo_level = 'National') AS pft_total
)
SELECT
    ROUND(em_total / pft_total, 1)        AS em_vs_pft_multiplier,
    ROUND(em_total * 0.08, 2)             AS copd_em_est,
    ROUND(pft_total, 2)                   AS pft_exact
FROM totals;

-- -----------------------------------------------------------------------------
-- VIEW: National E&M HCPCS Breakdown (99213 vs 99214)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS em_national_hcpcs_breakdown;

CREATE VIEW em_national_hcpcs_breakdown AS
WITH hcpcs_totals AS (
    SELECT
        hcpcs_cd,
        MAX(hcpcs_desc) AS description,
        SUM(tot_srvcs) AS services,
        MAX(tot_benes) AS benes,
        SUM(avg_mdcr_alowd_amt * tot_srvcs) AS allowed_raw,
        SUM(avg_sbmtd_chrg * tot_srvcs) AS submitted_raw
    FROM copd_em
    WHERE geo_level = 'National'
    GROUP BY hcpcs_cd
)
SELECT
    hcpcs_cd,
    description,
    services AS total_services,
    ROUND(services * 100.0 / SUM(services) OVER (), 1) AS pct_of_volume,
    benes AS beneficiaries_approx,
    ROUND(allowed_raw, 2) AS total_allowed,
    ROUND(allowed_raw / services, 2) AS avg_allowed_per_visit,
    ROUND(submitted_raw / services, 2) AS avg_submitted_per_visit,
    ROUND(submitted_raw / allowed_raw, 2) AS markup_x,
    ROUND(services * 1.0 / benes, 2) AS interaction_density
FROM hcpcs_totals;

-- -----------------------------------------------------------------------------
-- VIEW: National E&M Setting Split (Office vs Facility)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS em_national_setting_split;

CREATE VIEW em_national_setting_split AS
WITH setting_totals AS (
    SELECT
        place_of_srvc,
        CASE place_of_srvc WHEN 'F' THEN 'Facility' ELSE 'Office / Non-Facility' END AS setting,
        SUM(tot_srvcs) AS services,
        SUM(avg_mdcr_alowd_amt * tot_srvcs) AS allowed_raw
    FROM copd_em
    WHERE geo_level = 'National'
    GROUP BY place_of_srvc
)
SELECT
    place_of_srvc,
    setting,
    services AS total_services,
    ROUND(services * 100.0 / SUM(services) OVER (), 1) AS pct_services,
    ROUND(allowed_raw, 2) AS total_allowed,
    ROUND(allowed_raw * 100.0 / SUM(allowed_raw) OVER (), 1) AS pct_allowed
FROM setting_totals;

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 3: THE DIAGNOSIS FUNNEL GAP
-- Cross-analysis: estimated COPD E&M visits vs actual PFT tests
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: COPD Diagnosis Funnel Gap Analysis
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS copd_diagnosis_funnel_gap;

CREATE VIEW copd_diagnosis_funnel_gap AS
WITH em_stats AS (
    SELECT SUM(tot_srvcs) AS total_em_srvcs 
    FROM copd_em 
    WHERE geo_level = 'National'
),
pft_stats AS (
    SELECT
         SUM(tot_srvcs)    AS total_pft_srvcs,
         MAX(tot_benes)    AS max_pft_benes
     FROM copd_pft
     WHERE geo_level = 'National' 
       AND hcpcs_cd = '94729' 
       AND place_of_srvc = 'O'
)
SELECT
    ROUND(em.total_em_srvcs * 0.08, 0)                          AS copd_em_visits_est,
    pft.total_pft_srvcs                                         AS pft_tests_exact,
    ROUND((em.total_em_srvcs * 0.08) / pft.total_pft_srvcs, 1)  AS visit_to_test_ratio,
    6200000                                                     AS known_medicare_copd_patients,
    pft.max_pft_benes                                           AS pft_tested_patients,
    ROUND((6200000 - pft.max_pft_benes) * 100.0 / 6200000, 1)   AS pct_never_tested,
    6200000 - pft.max_pft_benes                                 AS never_tested_count
FROM em_stats em, pft_stats pft;

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 4: 99214 COMPLEXITY SIGNAL BY STATE
-- States with highest 99214 % = highest acuity = best B2B targets
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: State-Level E&M Complexity & B2B Priority
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS em_state_complexity_b2b_priority;

CREATE VIEW em_state_complexity_b2b_priority AS
SELECT
    geo_desc                                                    AS state,
    SUM(CASE WHEN hcpcs_cd = '99213' THEN tot_srvcs ELSE 0 END) AS srvcs_99213,
    SUM(CASE WHEN hcpcs_cd = '99214' THEN tot_srvcs ELSE 0 END) AS srvcs_99214,
    SUM(tot_srvcs)                                              AS total_services,
    ROUND(
        SUM(CASE WHEN hcpcs_cd = '99214' THEN tot_srvcs ELSE 0 END) * 100.0 /
        NULLIF(SUM(tot_srvcs), 0), 1
    )                                                           AS pct_99214,
    CASE
        WHEN SUM(CASE WHEN hcpcs_cd = '99214' THEN tot_srvcs ELSE 0 END) * 100.0 /
             NULLIF(SUM(tot_srvcs), 0) >= 62 THEN 'HIGH PRIORITY'
        WHEN SUM(CASE WHEN hcpcs_cd = '99214' THEN tot_srvcs ELSE 0 END) * 100.0 /
             NULLIF(SUM(tot_srvcs), 0) >= 60 THEN 'STRONG'
        ELSE 'STANDARD'
    END                                                         AS b2b_priority
FROM copd_em
WHERE geo_level = 'State'
GROUP BY geo_desc
HAVING SUM(tot_srvcs) > 1000000;

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5: TOP 15 STATES BY TOTAL ALLOWED
-- ─────────────────────────────────────────────────────────────────────────────
-- =============================================================================
-- MEDIGY DISEASE STATE DATABASE (MDSD)
-- Core Analytical Views: COPD PFT & E&M Market Context
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. VIEW: National E&M All-Condition Market Summary
-- Provides the "denominator" for the entire Medicare E&M market.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS em_national_all_condition_summary;
CREATE VIEW em_national_all_condition_summary AS
SELECT
    SUM(tot_srvcs)                                          AS total_all_condition_services,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)           AS total_allowed,
    ROUND(SUM(avg_sbmtd_chrg     * tot_srvcs), 2)           AS total_submitted,
    ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) / 
          SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)           AS system_markup_x,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) / 1000000000.0, 2) AS total_allowed_billions,
    SUM(tot_rndrng_prvdrs)                                  AS total_providers_sum
FROM copd_em
WHERE geo_level = 'National';


-- -----------------------------------------------------------------------------
-- 2. VIEW: National COPD Estimated E&M Share (8% Baseline)
-- Applies the epidemiological constant to define the target COPD visit market.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS em_national_copd_estimated_share;
CREATE VIEW em_national_copd_estimated_share AS
SELECT
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08, 2)       AS copd_est_allowed,
    ROUND(SUM(tot_srvcs) * 0.08, 0)                             AS copd_est_services,
    0.08                                                        AS copd_share_pct,
    'Epidemiological estimate — 8% of Medicare E&M'             AS basis
FROM copd_em
WHERE geo_level = 'National';


-- -----------------------------------------------------------------------------
-- 3. VIEW: COPD Diagnosis Funnel Gap Analysis
-- Cross-references E&M visits vs. PFT tests to identify the "Never Tested" gap.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS copd_diagnosis_funnel_gap;
CREATE VIEW copd_diagnosis_funnel_gap AS
WITH em_data AS (
    SELECT SUM(tot_srvcs) AS total_em_srvcs FROM copd_em WHERE geo_level = 'National'
),
pft_data AS (
    SELECT SUM(tot_srvcs) AS total_pft_srvcs, MAX(tot_benes) AS max_pft_benes
    FROM copd_pft
    WHERE geo_level = 'National' AND hcpcs_cd = '94729' AND place_of_srvc = 'O'
)
SELECT
    ROUND(em.total_em_srvcs * 0.08, 0)                          AS copd_em_visits_est,
    pft.total_pft_srvcs                                         AS pft_tests_exact,
    ROUND((em.total_em_srvcs * 0.08) / pft.total_pft_srvcs, 1)  AS visit_to_test_ratio,
    6200000                                                     AS known_medicare_copd_patients,
    pft.max_pft_benes                                           AS pft_tested_patients,
    ROUND((6200000 - pft.max_pft_benes) * 100.0 / 6200000, 1)   AS pct_never_tested,
    6200000 - pft.max_pft_benes                                 AS never_tested_count
FROM em_data em, pft_data pft;


-- -----------------------------------------------------------------------------
-- 4. VIEW: National PFT Key Findings Summary
-- The Executive Dashboard view for top-line results.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS pft_national_key_findings_summary;
CREATE VIEW pft_national_key_findings_summary AS
SELECT '1' AS finding_no, 'Medicare-only diagnostic market' AS metric, ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 0) AS value_raw, '$' || ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) / 1000000, 2) || 'M' AS formatted
FROM copd_pft WHERE geo_level = 'National'
UNION ALL
SELECT '2', 'Total procedures billed', SUM(tot_srvcs), CAST(SUM(tot_srvcs) AS TEXT)
FROM copd_pft WHERE geo_level = 'National'
UNION ALL
SELECT '3', 'System markup (submitted / allowed)', ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) / SUM(avg_mdcr_alowd_amt * tot_srvcs), 2), ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) / SUM(avg_mdcr_alowd_amt * tot_srvcs), 2) || 'x'
FROM copd_pft WHERE geo_level = 'National'
UNION ALL
SELECT '4', 'Billing friction ($)', ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) - SUM(avg_mdcr_alowd_amt * tot_srvcs), 0), '$' || ROUND((SUM(avg_sbmtd_chrg * tot_srvcs) - SUM(avg_mdcr_alowd_amt * tot_srvcs)) / 1000000, 1) || 'M'
FROM copd_pft WHERE geo_level = 'National'
UNION ALL
SELECT '5', 'Office share of all services (%)', ROUND(SUM(CASE WHEN place_of_srvc='O' THEN tot_srvcs ELSE 0 END) * 100.0 / SUM(tot_srvcs), 1), ROUND(SUM(CASE WHEN place_of_srvc='O' THEN tot_srvcs ELSE 0 END) * 100.0 / SUM(tot_srvcs), 1) || '%'
FROM copd_pft WHERE geo_level = 'National'
UNION ALL
SELECT '6', 'Office share of all spend (%)', ROUND(SUM(CASE WHEN place_of_srvc='O' THEN avg_mdcr_alowd_amt * tot_srvcs ELSE 0 END) * 100.0 / SUM(avg_mdcr_alowd_amt * tot_srvcs), 1), ROUND(SUM(CASE WHEN place_of_srvc='O' THEN avg_mdcr_alowd_amt * tot_srvcs ELSE 0 END) * 100.0 / SUM(avg_mdcr_alowd_amt * tot_srvcs), 1) || '%'
FROM copd_pft WHERE geo_level = 'National';

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 7: B2B PROVIDER MARKET SIZING
-- ─────────────────────────────────────────────────────────────────────────────

-- Total active COPD-managing providers (from PFT dataset as proxy)
-- -----------------------------------------------------------------------------
-- VIEW: PFT B2B SaaS Revenue Projections (Subscription Model)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS pft_b2b_saas_revenue_projections;

CREATE VIEW pft_b2b_saas_revenue_projections AS
SELECT
    SUM(tot_rndrng_prvdrs)                                      AS total_copd_providers_pft,
    
    -- Assumption: 60% of PCPs currently do not have in-house spirometry
    ROUND(SUM(tot_rndrng_prvdrs) * 0.60, 0)                     AS estimated_pcp_without_spirometry,
    
    -- Conservative Tier: 0.5% conversion at $299/month
    ROUND(SUM(tot_rndrng_prvdrs) * 0.60 * 0.005, 0)             AS conservative_conversion_05pct,
    ROUND(SUM(tot_rndrng_prvdrs) * 0.60 * 0.005 * 299 * 12, 0)  AS conservative_annual_arr,
    
    -- Moderate Tier: 2.0% conversion at $299/month
    ROUND(SUM(tot_rndrng_prvdrs) * 0.60 * 0.02, 0)              AS moderate_conversion_2pct,
    ROUND(SUM(tot_rndrng_prvdrs) * 0.60 * 0.02 * 299 * 12, 0)   AS moderate_annual_arr,
    
    'B2B SaaS Model: $299/mo subscription'                      AS business_model_basis
FROM copd_pft
WHERE geo_level = 'National'
  AND hcpcs_cd = '94010'
  AND place_of_srvc = 'O';

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 8: AGGREGATE MARKET VALUE — ALL LAYERS
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: COPD Total Addressable Market (TAM) Valuation Stack
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS copd_market_tam_valuation_stack;

CREATE VIEW copd_market_tam_valuation_stack AS
-- Layer 1: PFT Diagnostics
SELECT
    'Layer 1: PFT Diagnostics (exact)'                          AS market_layer,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 0)               AS medicare_allowed,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 3, 0)           AS all_payer_low,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 5, 0)           AS all_payer_high,
    'Exact CMS'                                                 AS data_basis
FROM copd_pft WHERE geo_level = 'National'

UNION ALL
-- Layer 2: E&M Visits (Estimated)
SELECT
    'Layer 2: E&M Visits (COPD est. 8%)',
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08, 0),
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08 * 3, 0),
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08 * 5, 0),
    'Estimated — 8% COPD share'
FROM copd_em WHERE geo_level = 'National'

UNION ALL
-- Layer 3: Oxygen DME (Exact)
SELECT
    'Layer 3: Oxygen DME (exact)',
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 0),
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) * 3, 0),
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) * 5, 0),
    'Exact CMS'
FROM copd_oxygen;

-- =============================================================================
-- COPD ANALYTICS
-- Script 05: Oxygen DME Queries (E0434 / E1392)
-- Source table: copd_oxygen
-- Covers every KPI, table, and observation in the Oxygen DME Evidence Report
-- IMPORTANT: tot_suplr_srvcs = rental MONTHS billed (not individual procedures)
--            tot_suplr_benes = NULL for rows with <11 beneficiaries (CMS suppression)
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 1: NATIONAL TOP-LINE KPIs
-- ─────────────────────────────────────────────────────────────────────────────
-- -----------------------------------------------------------------------------
-- VIEW: National Oxygen DME Market Financial Summary
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS oxygen_national_market_financial_summary;

CREATE VIEW oxygen_national_market_financial_summary AS
WITH raw_totals AS (
    SELECT
        SUM(tot_suplr_srvcs) AS rental_months,
        SUM(tot_suplr_clms) AS claims,
        SUM(tot_suplr_benes) AS benes,
        COUNT(DISTINCT rfrg_npi) AS referring_providers,
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) AS allowed_raw,
        SUM(avg_suplr_sbmtd_chrg * tot_suplr_srvcs) AS submitted_raw,
        SUM(avg_suplr_mdcr_pymt_amt * tot_suplr_srvcs) AS payment_raw
    FROM copd_oxygen
)
SELECT
    rental_months AS total_rental_months,
    claims AS total_claims,
    benes AS total_benes_non_suppressed,
    referring_providers AS unique_referring_providers,
    ROUND(allowed_raw, 2) AS total_allowed,
    ROUND(submitted_raw, 2) AS total_submitted,
    ROUND(payment_raw, 2) AS total_payment,
    ROUND(submitted_raw / allowed_raw, 2) AS system_markup_x,
    ROUND(submitted_raw - allowed_raw, 2) AS billing_friction,
    ROUND(allowed_raw / rental_months, 2) AS avg_allowed_per_rental_month
FROM raw_totals;

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 2.1: CODE-LEVEL BREAKDOWN — E0434 vs E1392
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: Oxygen DME HCPCS Economic Breakdown
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS oxygen_hcpcs_economic_breakdown;

CREATE VIEW oxygen_hcpcs_economic_breakdown AS
WITH hcpcs_totals AS (
    SELECT
        hcpcs_cd,
        MAX(hcpcs_desc) AS description,
        SUM(tot_suplr_srvcs) AS rental_months,
        SUM(tot_suplr_clms) AS claims,
        SUM(tot_suplr_benes) AS benes,
        COUNT(DISTINCT rfrg_npi) AS unique_providers,
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) AS allowed_raw,
        SUM(avg_suplr_sbmtd_chrg * tot_suplr_srvcs) AS submitted_raw
    FROM copd_oxygen
    GROUP BY hcpcs_cd
)
SELECT
    hcpcs_cd,
    description,
    rental_months AS total_rental_months,
    ROUND(rental_months * 100.0 / (SELECT SUM(rental_months) FROM hcpcs_totals), 1) AS pct_volume,
    claims AS total_claims,
    benes AS benes_non_suppressed,
    unique_providers,
    ROUND(allowed_raw, 2) AS total_allowed,
    ROUND(submitted_raw, 2) AS total_submitted,
    ROUND(allowed_raw / rental_months, 2) AS avg_allowed_per_month,
    ROUND(submitted_raw / allowed_raw, 2) AS markup_x,
    -- Annual and 36-month per-patient economics
    ROUND((allowed_raw / rental_months) * 12, 2) AS annual_per_patient,
    ROUND((allowed_raw / rental_months) * 36, 2) AS medicare_36mo_cap
FROM hcpcs_totals
ORDER BY total_rental_months DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 3: PER-PATIENT RENTAL ECONOMICS — 36-MONTH RULE
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: Oxygen E1392 (Portable Concentrator) Lifecycle Economics
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS oxygen_e1392_lifecycle_economics;

CREATE VIEW oxygen_e1392_lifecycle_economics AS
WITH e1392_averages AS (
    SELECT 
        AVG(avg_suplr_mdcr_alowd_amt) AS avg_allowed,
        AVG(avg_suplr_sbmtd_chrg)     AS avg_submitted
    FROM copd_oxygen 
    WHERE hcpcs_cd = 'E1392'
)
SELECT 'Monthly allowed (E1392)' AS metric, ROUND(avg_allowed, 2) AS value FROM e1392_averages
UNION ALL 
SELECT 'Annual allowed per patient (x12)', ROUND(avg_allowed * 12, 2) FROM e1392_averages
UNION ALL 
SELECT '36-month Medicare cap per patient', ROUND(avg_allowed * 36, 2) FROM e1392_averages
UNION ALL 
SELECT 'Monthly submitted (billed) E1392', ROUND(avg_submitted, 2) FROM e1392_averages
UNION ALL 
SELECT '36-month submitted cap per patient', ROUND(avg_submitted * 36, 2) FROM e1392_averages
UNION ALL 
SELECT 'Friction per patient over 36 months', ROUND((avg_submitted - avg_allowed) * 36, 2) FROM e1392_averages;

-- -----------------------------------------------------------------------------
-- VIEW: Oxygen E1392 (Portable Concentrator) 36-Month Amortization Model
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS oxygen_e1392_amortization_model;

CREATE VIEW oxygen_e1392_amortization_model AS
WITH monthly_rates AS (
    SELECT 
        AVG(avg_suplr_mdcr_alowd_amt) AS avg_allowed,
        AVG(avg_suplr_sbmtd_chrg)     AS avg_submitted
    FROM copd_oxygen 
    WHERE hcpcs_cd = 'E1392'
),
periods AS (
    SELECT 'Month 01-12 (Year 1)' AS period, 12 AS months UNION ALL
    SELECT 'Month 13-24 (Year 2)', 12 UNION ALL
    SELECT 'Month 25-36 (Year 3)', 12 UNION ALL
    SELECT 'Full 36-Month Cap',     36
)
SELECT
    period,
    months,
    ROUND(avg_allowed   * months, 2) AS medicare_pays,
    ROUND(avg_submitted * months, 2) AS provider_bills,
    ROUND((avg_submitted - avg_allowed) * months, 2) AS billing_friction
FROM monthly_rates, periods;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 4.1: TOP 15 STATES BY TOTAL ALLOWED
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: State-Level Oxygen DME Market Prioritization
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS oxygen_state_market_prioritization;

CREATE VIEW oxygen_state_market_prioritization AS
WITH state_metrics AS (
    SELECT
        prvdr_state,
        SUM(tot_suplr_srvcs)                                    AS total_rental_months,
        SUM(tot_suplr_benes)                                    AS benes_non_suppressed,
        COUNT(DISTINCT rfrg_npi)                                AS total_providers,
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs)         AS allowed_raw,
        SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs)         AS submitted_raw
    FROM copd_oxygen
    GROUP BY prvdr_state
)
SELECT
    prvdr_state                                                 AS state,
    total_rental_months,
    benes_non_suppressed,
    total_providers,
    ROUND(allowed_raw, 2)                                       AS total_allowed,
    ROUND(submitted_raw, 2)                                     AS total_submitted,
    ROUND(allowed_raw / total_rental_months, 2)                  AS avg_allowed_per_month,
    ROUND(submitted_raw / allowed_raw, 2)                        AS markup_x,
    CASE
        WHEN RANK() OVER (ORDER BY allowed_raw DESC) <= 4  THEN 'TIER 1'
        WHEN RANK() OVER (ORDER BY allowed_raw DESC) <= 10 THEN 'TIER 2'
        ELSE 'TIER 3'
    END                                                         AS market_tier
FROM state_metrics;


-- Top 5 states share of national
-- -----------------------------------------------------------------------------
-- VIEW: Oxygen DME Market Share of Top 5 States
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS oxygen_top_5_states_market_share;

CREATE VIEW oxygen_top_5_states_market_share AS
SELECT
    ROUND(SUM(state_allowed) * 100.0 /
        (SELECT SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) FROM copd_oxygen), 1)
                                                                AS top5_pct_of_national,
    ROUND(SUM(state_allowed) / 1000000, 2)                      AS top5_allowed_millions
FROM (
    SELECT SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) AS state_allowed
    FROM copd_oxygen
    GROUP BY prvdr_state
    ORDER BY state_allowed DESC
    LIMIT 5
);


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 4.2: URBAN vs RURAL
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: Oxygen DME Urban/Rural Market Split (RUCA)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS oxygen_market_urban_rural_split;

CREATE VIEW oxygen_market_urban_rural_split AS
WITH national_totals AS (
    SELECT 
        SUM(tot_suplr_srvcs) AS total_srvcs,
        COUNT(DISTINCT rfrg_npi) AS total_providers,
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) AS total_allowed_raw
    FROM copd_oxygen
)
SELECT
    ruca_cat,
    SUM(tot_suplr_srvcs)                                        AS total_rental_months,
    ROUND(SUM(tot_suplr_srvcs) * 100.0 / 
        (SELECT total_srvcs FROM national_totals), 1)           AS pct_rental_months,
    SUM(tot_suplr_benes)                                        AS benes_non_suppressed,
    COUNT(DISTINCT rfrg_npi)                                    AS unique_providers,
    ROUND(COUNT(DISTINCT rfrg_npi) * 100.0 / 
        (SELECT total_providers FROM national_totals), 1)       AS pct_providers,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS total_allowed,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) * 100.0 / 
        (SELECT total_allowed_raw FROM national_totals), 1)     AS pct_allowed
FROM copd_oxygen, national_totals
WHERE ruca_cat IS NOT NULL
GROUP BY ruca_cat;


-- Top rural states by oxygen rental allowed
-- -----------------------------------------------------------------------------
-- VIEW: Rural Oxygen High-Burden State Analysis
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS oxygen_rural_high_burden_states;

CREATE VIEW oxygen_rural_high_burden_states AS
SELECT
    prvdr_state                                                 AS state,
    SUM(tot_suplr_srvcs)                                        AS rural_rental_months,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS rural_allowed,
    COUNT(DISTINCT rfrg_npi)                                    AS rural_providers,
    CASE prvdr_state
        WHEN 'TN' THEN 'HIGH BURDEN — Tobacco/Coal'
        WHEN 'KY' THEN 'HIGH BURDEN — Coal Mining'
        WHEN 'MS' THEN 'HIGH BURDEN — Tobacco/Agriculture'
        WHEN 'WV' THEN 'HIGH BURDEN — Coal Mining'
        ELSE 'MODERATE'
    END                                                         AS rural_access_signal
FROM copd_oxygen
WHERE ruca_cat = 'Rural'
GROUP BY prvdr_state;

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5: REFERRING SPECIALTY ANALYSIS — B2B TARGET MAP
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: Oxygen DME Specialty B2B Channels
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS oxygen_specialty_b2b_channels;

CREATE VIEW oxygen_specialty_b2b_channels AS
WITH national_total AS (
    SELECT SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) AS total_allowed_raw
    FROM copd_oxygen
)
SELECT
    specialty_desc,
    SUM(tot_suplr_srvcs)                                        AS total_rental_months,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS total_allowed,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) * 100.0 / 
        (SELECT total_allowed_raw FROM national_total), 1)      AS pct_of_market,
    COUNT(DISTINCT rfrg_npi)                                    AS unique_providers,
    CASE specialty_desc
        WHEN 'Pulmonary Disease'   THEN 'PRIMARY B2B'
        WHEN 'Internal Medicine'   THEN 'PRIMARY B2B'
        WHEN 'Family Practice'     THEN 'PRIMARY B2B'
        WHEN 'Nurse Practitioner'  THEN 'KEY CHANNEL'
        WHEN 'Sleep Medicine'      THEN 'STRATEGIC BRIDGE'
        ELSE 'SECONDARY'
    END                                                         AS b2b_tier
FROM copd_oxygen, national_total
WHERE specialty_desc IS NOT NULL
GROUP BY specialty_desc;

-- Pulmonology + IM + Family Practice combined share
-- -----------------------------------------------------------------------------
-- VIEW: Oxygen DME Market Concentration (Top 3 Specialties)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS oxygen_top_3_specialty_dominance;

CREATE VIEW oxygen_top_3_specialty_dominance AS
WITH totals AS (
    SELECT 
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) AS total_market_allowed,
        SUM(CASE 
            WHEN specialty_desc IN ('Pulmonary Disease','Internal Medicine','Family Practice') 
            THEN avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs 
            ELSE 0 
        END) AS top3_allowed,
        COUNT(DISTINCT CASE 
            WHEN specialty_desc IN ('Pulmonary Disease','Internal Medicine','Family Practice') 
            THEN rfrg_npi 
        END) AS providers_top3
    FROM copd_oxygen
)
SELECT
    ROUND(top3_allowed * 100.0 / NULLIF(total_market_allowed, 0), 1) AS pct_top3_specialties,
    providers_top3                                                  AS providers_top3,
    'Pulmonary, Internal Medicine, Family Practice'                AS core_specialties
FROM totals;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 6: E0434 LIQUID OXYGEN — STATE ADOPTION PATTERN
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: State-Level Oxygen Tech Transition (Liquid vs. Concentrator)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS oxygen_state_tech_transition_status;

CREATE VIEW oxygen_state_tech_transition_status AS
SELECT
    prvdr_state                                                         AS state,
    SUM(CASE WHEN hcpcs_cd = 'E0434' THEN tot_suplr_srvcs ELSE 0 END)   AS e0434_months,
    SUM(CASE WHEN hcpcs_cd = 'E1392' THEN tot_suplr_srvcs ELSE 0 END)   AS e1392_months,
    SUM(tot_suplr_srvcs)                                                AS total_months,
    ROUND(
        SUM(CASE WHEN hcpcs_cd = 'E0434' THEN tot_suplr_srvcs ELSE 0 END) * 100.0 /
        NULLIF(SUM(tot_suplr_srvcs), 0), 1
    )                                                                   AS e0434_pct,
    CASE
        WHEN SUM(CASE WHEN hcpcs_cd = 'E0434' THEN tot_suplr_srvcs ELSE 0 END) * 100.0 /
             NULLIF(SUM(tot_suplr_srvcs), 0) >= 5 THEN 'Legacy liquid O2 dominant'
        WHEN SUM(CASE WHEN hcpcs_cd = 'E0434' THEN tot_suplr_srvcs ELSE 0 END) * 100.0 /
             NULLIF(SUM(tot_suplr_srvcs), 0) >= 2 THEN 'Transitioning to concentrators'
        ELSE 'Concentrator dominant'
    END                                                                 AS transition_status
FROM copd_oxygen
GROUP BY prvdr_state
HAVING SUM(tot_suplr_srvcs) > 100;

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 7: PROVIDER VOLUME DISTRIBUTION (E1392)
-- ─────────────────────────────────────────────────────────────────────────────

-- Bucket providers by rental month volume
-- -----------------------------------------------------------------------------
-- VIEW: Oxygen E1392 Provider Volume Segmentation
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS oxygen_e1392_provider_volume_segmentation;

CREATE VIEW oxygen_e1392_provider_volume_segmentation AS
WITH provider_totals AS (
    SELECT
        rfrg_npi,
        SUM(tot_suplr_srvcs)                                    AS total_months,
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs)         AS total_allowed
    FROM copd_oxygen
    WHERE hcpcs_cd = 'E1392'
    GROUP BY rfrg_npi
),
national_provider_count AS (
    SELECT COUNT(DISTINCT rfrg_npi) AS total_count 
    FROM copd_oxygen 
    WHERE hcpcs_cd = 'E1392'
)
SELECT
    CASE
        WHEN total_months <= 10  THEN '1-10 months'
        WHEN total_months <= 30  THEN '11-30 months'
        WHEN total_months <= 100 THEN '31-100 months'
        WHEN total_months <= 300 THEN '101-300 months'
        ELSE '300+ months'
    END                                                         AS volume_bucket,
    COUNT(*)                                                    AS provider_count,
    ROUND(COUNT(*) * 100.0 / (SELECT total_count FROM national_provider_count), 1)
                                                                AS pct_providers,
    SUM(total_months)                                           AS total_rental_months,
    ROUND(SUM(total_allowed), 2)                                AS total_allowed
FROM provider_totals
GROUP BY volume_bucket
ORDER BY MIN(total_months);

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 8: STATE MARKUP COMPARISON
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: State-Level Oxygen Reimbursement Efficiency (Markup & Allowed)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS oxygen_state_reimbursement_efficiency;

CREATE VIEW oxygen_state_reimbursement_efficiency AS
SELECT
    prvdr_state                                                 AS state,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS total_allowed,
    ROUND(SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs), 2)   AS total_submitted,
    ROUND(SUM(avg_suplr_sbmtd_chrg * tot_suplr_srvcs) / 
          NULLIF(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 0), 2) AS markup_x,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) / 
          NULLIF(SUM(tot_suplr_srvcs), 0), 2)                   AS avg_monthly_allowed
FROM copd_oxygen
GROUP BY prvdr_state;
-- Script 06: Integrated Market Stack & LTV Model
-- Source tables: copd_pft + copd_em + copd_oxygen (all three combined)
-- Covers: Master market overview, full LTV stack, geographic composite,
--         markup comparison across layers, key findings summary
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 1: MASTER MARKET STACK — ALL THREE LAYERS
-- (Report: Master Market Overview — Section 1)
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: COPD Total Market Economic Stack
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS copd_total_market_economic_stack;

CREATE VIEW copd_total_market_economic_stack AS
WITH raw_layers AS (
    -- Layer 1: PFT Diagnostics
    SELECT 
        1 AS layer_no, 
        'DIAGNOSTIC' AS layer_type,
        'PFT Tests (94010/94060/94726/94729)' AS market_name,
        SUM(avg_mdcr_alowd_amt * tot_srvcs) AS medicare_allowed,
        SUM(avg_sbmtd_chrg     * tot_srvcs) AS medicare_submitted,
        'Exact CMS' AS data_basis
    FROM copd_pft WHERE geo_level = 'National'

    UNION ALL
    -- Layer 2: E&M Visits
    SELECT 
        2, 
        'VISIT',
        'E&M Office Visits 99213/99214 (COPD est. 8%)',
        SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08,
        SUM(avg_sbmtd_chrg     * tot_srvcs) * 0.08,
        'Estimated — 8% COPD share'
    FROM copd_em WHERE geo_level = 'National'

    UNION ALL
    -- Layer 3: Oxygen DME
    SELECT 
        3, 
        'DME',
        'Oxygen DME (E0434 / E1392)',
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs),
        SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs),
        'Exact CMS DMEPOS PUF'
    FROM copd_oxygen
)
SELECT
    layer_no,
    layer_type,
    market_name,
    ROUND(medicare_allowed, 0)                                  AS medicare_allowed,
    ROUND(medicare_submitted, 0)                                AS submitted_charges,
    ROUND(CASE WHEN medicare_allowed > 0 
               THEN medicare_submitted / medicare_allowed 
               ELSE NULL END, 2)                                AS markup_x,
    ROUND(medicare_submitted - medicare_allowed, 0)             AS billing_friction,
    ROUND(medicare_allowed * 3, 0)                              AS all_payer_low,
    ROUND(medicare_allowed * 5, 0)                              AS all_payer_high,
    data_basis
FROM raw_layers;

-- Grand total row
-- -----------------------------------------------------------------------------
-- VIEW: COPD National Market Grand Total Rollup
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS copd_national_market_grand_total;

CREATE VIEW copd_national_market_grand_total AS
WITH segment_totals AS (
    SELECT
        (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) FROM copd_pft WHERE geo_level='National')
            AS pft_allowed,
        (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08 FROM copd_em WHERE geo_level='National')
            AS em_allowed,
        (SELECT SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) FROM copd_oxygen)
            AS o2_allowed,
        (SELECT SUM(avg_sbmtd_chrg * tot_srvcs) FROM copd_pft WHERE geo_level='National')
            AS pft_submitted,
        (SELECT SUM(avg_sbmtd_chrg * tot_srvcs) * 0.08 FROM copd_em WHERE geo_level='National')
            AS em_submitted,
        (SELECT SUM(avg_suplr_sbmtd_chrg * tot_suplr_srvcs) FROM copd_oxygen)
            AS o2_submitted
)
SELECT
    'COPD MARKET TOTAL'                                         AS layer,
    ROUND(pft_allowed + em_allowed + o2_allowed, 0)             AS total_medicare_allowed,
    ROUND((pft_allowed + em_allowed + o2_allowed) * 3, 0)       AS all_payer_low,
    ROUND((pft_allowed + em_allowed + o2_allowed) * 5, 0)       AS all_payer_high,
    ROUND((pft_submitted + em_submitted + o2_submitted) / 
          NULLIF((pft_allowed + em_allowed + o2_allowed), 0), 2) AS blended_markup_x
FROM segment_totals;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 2: FULL PER-PATIENT LTV STACK
-- Combines PFT + E&M + Oxygen into single patient economics model
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: COPD Patient 36-Month LTV Model
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS copd_patient_36mo_ltv_model;

CREATE VIEW copd_patient_36mo_ltv_model AS
WITH base_rates AS (
    SELECT
        -- PFT confirmation rate (94060 office)
        (SELECT ROUND(AVG(avg_mdcr_alowd_amt), 2)
         FROM copd_pft WHERE geo_level='National' AND hcpcs_cd='94060' AND place_of_srvc='O')
             AS pft_94060,
        -- E&M initial visit (99214 office)
        (SELECT ROUND(AVG(avg_mdcr_alowd_amt), 2)
         FROM copd_em WHERE geo_level='National' AND hcpcs_cd='99214' AND place_of_srvc='O')
             AS em_99214,
        -- E&M follow-up (99213 office x 2.2/yr)
        (SELECT ROUND(AVG(avg_mdcr_alowd_amt) * 2.2, 2)
         FROM copd_em WHERE geo_level='National' AND hcpcs_cd='99213' AND place_of_srvc='O')
             AS em_99213_annual,
        -- Oxygen monthly (E1392)
        (SELECT ROUND(AVG(avg_suplr_mdcr_alowd_amt), 2) FROM copd_oxygen WHERE hcpcs_cd='E1392')
             AS o2_monthly
),
stack AS (
    SELECT
        99.00                                   AS voice_screen,
        pft_94060                               AS pft_confirmation,
        em_99214                                AS initial_visit,
        em_99213_annual                         AS annual_followup_visits,
        em_99214                                AS annual_review,
        o2_monthly * 12                         AS annual_o2,
        o2_monthly * 36                         AS o2_36mo_cap
    FROM base_rates
)
SELECT
    -- One-time items
    voice_screen,
    pft_confirmation,
    initial_visit                           AS initial_visit_99214,
    -- Annual recurring
    annual_followup_visits                  AS annual_99213_visits,
    annual_review                           AS annual_review_99214,
    annual_o2                               AS annual_o2_rental,
    -- LTV calculations
    ROUND(annual_followup_visits + annual_review, 2)            AS total_annual_em,
    ROUND(annual_followup_visits + annual_review + annual_o2, 2) AS total_annual_em_plus_o2,
    -- Year 1 total (one-time + first-year recurring)
    ROUND(voice_screen + pft_confirmation + initial_visit + 
          annual_followup_visits + annual_review + annual_o2, 2) AS year1_ltv,
    -- Year 2-3 recurring
    ROUND(annual_followup_visits + annual_review + annual_o2, 2) AS year2_plus_annual,
    -- 36-month total LTV
    ROUND(voice_screen + pft_confirmation + initial_visit + 
          (annual_followup_visits + annual_review + annual_o2) * 3, 2) AS ltv_36_months_with_o2,
    -- 36-month LTV without oxygen (70% of patients)
    ROUND(voice_screen + pft_confirmation + initial_visit + 
          (annual_followup_visits + annual_review) * 3, 2)            AS ltv_36_months_no_o2,
    o2_36mo_cap
FROM stack;

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 3: MARKUP COMPARISON ACROSS ALL LAYERS
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: COPD Market Friction & Markup Severity Index
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS copd_market_friction_severity_index;

CREATE VIEW copd_market_friction_severity_index AS
WITH market_stack AS (
    -- Layer 1: PFT Diagnostics
    SELECT 
        1 AS layer_no, 
        'PFT Diagnostics (exact)' AS market_layer,
        SUM(avg_sbmtd_chrg     * tot_srvcs) AS total_submitted,
        SUM(avg_mdcr_alowd_amt * tot_srvcs) AS total_allowed
    FROM copd_pft WHERE geo_level = 'National'

    UNION ALL
    -- Layer 2: E&M Visits
    SELECT 
        2, 
        'E&M Visits (est. 8% COPD)',
        SUM(avg_sbmtd_chrg     * tot_srvcs) * 0.08,
        SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08
    FROM copd_em WHERE geo_level = 'National'

    UNION ALL
    -- Layer 3: Oxygen DME
    SELECT 
        3, 
        'Oxygen DME (exact)',
        SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs),
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs)
    FROM copd_oxygen
)
SELECT
    layer_no,
    market_layer,
    ROUND(total_submitted, 0)                                   AS total_submitted,
    ROUND(total_allowed, 0)                                     AS total_allowed,
    ROUND(total_submitted - total_allowed, 0)                   AS friction,
    ROUND(total_submitted / total_allowed, 2)                   AS markup_x,
    CASE
        WHEN total_submitted / total_allowed >= 5   THEN 'EXTREME'
        WHEN total_submitted / total_allowed >= 3.5 THEN 'HIGH'
        WHEN total_submitted / total_allowed >= 2.5 THEN 'MODERATE'
        ELSE 'LOW'
    END                                                         AS markup_severity
FROM market_stack;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 4: GEOGRAPHIC COMPOSITE RANKING
-- States ranked across all three data layers
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: State-Level Composite Market Tiering (PFT + E&M + Oxygen)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS copd_state_composite_market_tiering;

CREATE VIEW copd_state_composite_market_tiering AS
WITH pft_state AS (
    SELECT geo_desc AS state,
           SUM(avg_mdcr_alowd_amt * tot_srvcs) AS pft_allowed,
           RANK() OVER (ORDER BY SUM(avg_mdcr_alowd_amt * tot_srvcs) DESC) AS pft_rank
    FROM copd_pft WHERE geo_level = 'State'
    GROUP BY geo_desc
),
em_state AS (
    SELECT geo_desc AS state,
           SUM(avg_mdcr_alowd_amt * tot_srvcs) AS em_allowed,
           RANK() OVER (ORDER BY SUM(avg_mdcr_alowd_amt * tot_srvcs) DESC) AS em_rank
    FROM copd_em WHERE geo_level = 'State'
    GROUP BY geo_desc
),
o2_state AS (
    SELECT prvdr_state AS state,
           SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) AS o2_allowed,
           RANK() OVER (ORDER BY SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) DESC) AS o2_rank
    FROM copd_oxygen
    GROUP BY prvdr_state
)
SELECT
    p.state,
    p.pft_rank,
    e.em_rank,
    o.o2_rank,
    (p.pft_rank + COALESCE(e.em_rank, 99) + COALESCE(o.o2_rank, 99)) AS composite_rank_sum,
    ROUND(p.pft_allowed, 0)                                     AS pft_allowed,
    ROUND(COALESCE(e.em_allowed, 0), 0)                         AS em_allowed_all_cond,
    ROUND(COALESCE(o.o2_allowed, 0), 0)                         AS o2_allowed,
    CASE
        WHEN (p.pft_rank + COALESCE(e.em_rank, 99) + COALESCE(o.o2_rank, 99)) <= 12 THEN 'TIER 1'
        WHEN (p.pft_rank + COALESCE(e.em_rank, 99) + COALESCE(o.o2_rank, 99)) <= 25 THEN 'TIER 2'
        ELSE 'TIER 3'
    END                                                         AS composite_tier
FROM pft_state p
LEFT JOIN em_state e ON p.state = e.state
LEFT JOIN o2_state o ON p.state = o.state;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5: SCALE CONTEXT — 10,000-PATIENT COHORT MODEL
-- (Report: "At scale: 10,000 diagnosed patients, 30% qualifying for O2")
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- VIEW: COPD Cohort 36-Month Economic Projection (10k Patient Scale)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS copd_cohort_36mo_economic_projection;

CREATE VIEW copd_cohort_36mo_economic_projection AS
WITH rates AS (
    SELECT
        99.00 AS screen_price,
        (SELECT AVG(avg_mdcr_alowd_amt) FROM copd_pft WHERE geo_level='National' AND hcpcs_cd='94060' AND place_of_srvc='O') AS pft_rate,
        (SELECT AVG(avg_mdcr_alowd_amt) FROM copd_em  WHERE geo_level='National' AND hcpcs_cd='99213' AND place_of_srvc='O') AS rate_99213,
        (SELECT AVG(avg_mdcr_alowd_amt) FROM copd_em  WHERE geo_level='National' AND hcpcs_cd='99214' AND place_of_srvc='O') AS rate_99214,
        (SELECT AVG(avg_suplr_mdcr_alowd_amt) FROM copd_oxygen WHERE hcpcs_cd='E1392') AS o2_monthly
)
SELECT
    10000                                                       AS cohort_size,
    0.30                                                        AS o2_qualifying_rate,
    -- Immediate Revenue
    ROUND(10000 * screen_price, 0)                              AS total_screen_revenue,
    ROUND(10000 * pft_rate, 0)                                  AS total_pft_revenue,
    -- Annual Operational Revenue
    ROUND(10000 * (rate_99213 * 2.2 + rate_99214), 0)           AS total_annual_em_revenue,
    -- Long-term DME Revenue (30% of cohort)
    ROUND(10000 * 0.30 * o2_monthly * 36, 0)                    AS total_o2_36mo_revenue,
    -- 36-Month Combined Cohort Value
    ROUND(
        (10000 * screen_price) +                                -- Screening
        (10000 * pft_rate) +                                    -- Diagnostics
        (10000 * rate_99214) +                                  -- Initial Dx Visit
        (10000 * (rate_99213 * 2.2 + rate_99214) * 3) +         -- 3 Years of Mgmt
        (10000 * 0.30 * o2_monthly * 36), 0                     -- 3 Years of Oxygen
    )                                                           AS cohort_36mo_total_revenue,
    'Operational Truth™: 10k patient cohort at 30% O2 use'      AS modeling_basis
FROM rates;

-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 6: COMPLETE KEY FINDINGS DASHBOARD
-- All 12 findings from the integrated report in one query set
-- ─────────────────────────────────────────────────────────────────────────────

-- Finding 1: Total COPD Medicare market (3 layers)
-- -----------------------------------------------------------------------------
-- VIEW: COPD Executive Summary & Key Findings (7 Layers)
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS copd_executive_summary_kpis;

CREATE VIEW copd_executive_summary_kpis AS

-- Finding 1: Total Market
SELECT 
    '1' AS finding_no, 
    'Total COPD Medicare market (3 layers)' AS metric, 
    ROUND(
        (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) FROM copd_pft WHERE geo_level='National') +
        (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08 FROM copd_em WHERE geo_level='National') +
        (SELECT SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) FROM copd_oxygen)
    , 0) AS value, 
    'Total Allowed ($)' AS unit

UNION ALL
-- Finding 2: Funnel Gap
SELECT 
    '2', 
    'Visit-to-test funnel ratio (estimated)', 
    ROUND(
        (SELECT SUM(tot_srvcs) * 0.08 FROM copd_em WHERE geo_level='National') /
        NULLIF((SELECT SUM(tot_srvcs) FROM copd_pft WHERE geo_level='National'), 0)
    , 1), 
    'x Ratio'

UNION ALL
-- Finding 3: Untested Population
SELECT 
    '3', 
    '% Medicare COPD patients untested (estimated)', 
    ROUND((6200000 - 
        (SELECT MAX(tot_benes) FROM copd_pft WHERE geo_level='National' AND hcpcs_cd='94729' AND place_of_srvc='O')
    ) * 100.0 / 6200000, 1), 
    '%'

UNION ALL
-- Finding 4: Oxygen Market
SELECT 
    '4', 
    'Oxygen DME total allowed', 
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 0), 
    'Total Allowed ($)'
FROM copd_oxygen

UNION ALL
-- Finding 5: 36-Month LTV
SELECT 
    '5', 
    '36-month patient LTV (with O2)', 
    ROUND(
        99 + 
        (SELECT AVG(avg_mdcr_alowd_amt) FROM copd_pft WHERE geo_level='National' AND hcpcs_cd='94060' AND place_of_srvc='O') + 
        (SELECT AVG(avg_mdcr_alowd_amt) FROM copd_em WHERE geo_level='National' AND hcpcs_cd='99214' AND place_of_srvc='O') + 
        (SELECT (AVG(avg_mdcr_alowd_amt) * 2.2) + AVG(avg_mdcr_alowd_amt) FROM copd_em WHERE geo_level='National' AND hcpcs_cd='99213' AND place_of_srvc='O') * 3 + 
        (SELECT AVG(avg_suplr_mdcr_alowd_amt) * 36 FROM copd_oxygen WHERE hcpcs_cd='E1392')
    , 2), 
    'LTV ($)'

UNION ALL
-- Finding 6: Blended Markup
SELECT 
    '6', 
    'Blended system markup (all 3 layers)', 
    ROUND(
        (
            (SELECT SUM(avg_sbmtd_chrg * tot_srvcs) FROM copd_pft WHERE geo_level='National') +
            (SELECT SUM(avg_sbmtd_chrg * tot_srvcs) * 0.08 FROM copd_em WHERE geo_level='National') +
            (SELECT SUM(avg_suplr_sbmtd_chrg * tot_suplr_srvcs) FROM copd_oxygen)
        ) / 
        NULLIF(
            (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) FROM copd_pft WHERE geo_level='National') +
            (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08 FROM copd_em WHERE geo_level='National') +
            (SELECT SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) FROM copd_oxygen)
        , 0)
    , 2), 
    'x Blended Markup'

UNION ALL
-- Finding 7: Rural Share
SELECT 
    '7', 
    'Rural share of O2 rental spend', 
    ROUND(
        SUM(CASE WHEN ruca_cat = 'Rural' THEN avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs ELSE 0 END) * 100.0 / 
        NULLIF(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 0), 1
    ), 
    '%'
FROM copd_oxygen

UNION ALL
-- Finding 8: Primary 3 specialties share of O2 market
SELECT
    '8' AS finding_no,
    'Pulm + IM + Family Practice % of O2 market' AS metric,
    ROUND(
        SUM(CASE WHEN specialty_desc IN ('Pulmonary Disease','Internal Medicine','Family Practice')
            THEN avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs ELSE 0 END) * 100.0 /
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 1
    ) AS value,
    '%' AS unit
FROM copd_oxygen

UNION ALL
-- Finding 9: Primary 3 specialties share of PFT market
SELECT
    '9' AS finding_no,
    'Pulm + IM + Family Practice % of PFT market' AS metric,
    ROUND(
        SUM(CASE WHEN specialty_desc IN ('Pulmonary Disease','Internal Medicine','Family Practice')
            THEN avg_mdcr_alowd_amt * tot_srvcs ELSE 0 END) * 100.0 /
        SUM(avg_mdcr_alowd_amt * tot_srvcs), 1
    ) AS value,
    '%' AS unit
FROM copd_pft;

-- Script 07: Views — Pre-computed aggregations for dashboard use
-- Create these once; reference them in application queries.
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 1: PFT National Summary
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS pft_national;
CREATE VIEW IF NOT EXISTS pft_national AS
SELECT
    hcpcs_cd,
    MAX(hcpcs_desc)                                             AS hcpcs_desc,
    SUM(tot_srvcs)                                              AS total_services,
    MAX(tot_benes)                                              AS beneficiaries_approx,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)               AS total_allowed,
    ROUND(SUM(avg_sbmtd_chrg     * tot_srvcs), 2)               AS total_submitted,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) / SUM(tot_srvcs), 2) AS avg_allowed_per_test,
    ROUND(SUM(avg_sbmtd_chrg     * tot_srvcs) / SUM(tot_srvcs), 2) AS avg_submitted_per_test,
    ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) /
          SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)               AS markup_x,
    ROUND(SUM(tot_srvcs) * 1.0 / MAX(tot_benes), 2)             AS interaction_density,
    ROUND(SUM(tot_srvcs) * 100.0 /
         (SELECT SUM(tot_srvcs) FROM copd_pft WHERE geo_level='National'), 1) AS pct_volume
FROM copd_pft
WHERE geo_level = 'National'
GROUP BY hcpcs_cd;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 2: PFT State Summary
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS pft_state;
CREATE VIEW IF NOT EXISTS pft_state AS
SELECT
    geo_desc                                                    AS state,
    SUM(tot_srvcs)                                              AS total_services,
    SUM(tot_benes)                                              AS total_benes,
    SUM(tot_rndrng_prvdrs)                                      AS total_providers,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)               AS total_allowed,
    ROUND(SUM(avg_sbmtd_chrg     * tot_srvcs), 2)               AS total_submitted,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) / SUM(tot_srvcs), 2) AS avg_allowed_per_test,
    ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) /
          SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)               AS markup_x,
    ROUND(SUM(CASE WHEN place_of_srvc='O' THEN tot_srvcs ELSE 0 END) * 100.0 /
          NULLIF(SUM(tot_srvcs), 0), 1)                         AS office_pct,
    RANK() OVER (ORDER BY SUM(avg_mdcr_alowd_amt * tot_srvcs) DESC) AS allowed_rank
FROM copd_pft
WHERE geo_level = 'State'
GROUP BY geo_desc;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 3: E&M National Summary
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS em_national;
CREATE VIEW IF NOT EXISTS em_national AS
SELECT
    hcpcs_cd,
    MAX(hcpcs_desc)                                             AS hcpcs_desc,
    SUM(tot_srvcs)                                              AS total_services,
    MAX(tot_benes)                                              AS beneficiaries_approx,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)               AS total_allowed,
    ROUND(SUM(avg_sbmtd_chrg     * tot_srvcs), 2)               AS total_submitted,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) / SUM(tot_srvcs), 2) AS avg_allowed_per_visit,
    ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) /
          SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)               AS markup_x,
    ROUND(SUM(tot_srvcs) * 1.0 / MAX(tot_benes), 2)             AS interaction_density
FROM copd_em
WHERE geo_level = 'National'
GROUP BY hcpcs_cd;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 4: E&M State Summary with 99214 complexity signal
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS em_state;
CREATE VIEW IF NOT EXISTS em_state AS
SELECT
    geo_desc                                                    AS state,
    SUM(tot_srvcs)                                              AS total_services,
    SUM(tot_benes)                                              AS total_benes,
    SUM(tot_rndrng_prvdrs)                                      AS total_providers,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)               AS total_allowed,
    SUM(CASE WHEN hcpcs_cd = '99213' THEN tot_srvcs ELSE 0 END) AS srvcs_99213,
    SUM(CASE WHEN hcpcs_cd = '99214' THEN tot_srvcs ELSE 0 END) AS srvcs_99214,
    ROUND(
        SUM(CASE WHEN hcpcs_cd='99214' THEN tot_srvcs ELSE 0 END) * 100.0 /
        NULLIF(SUM(tot_srvcs), 0), 1
    )                                                           AS pct_99214,
    RANK() OVER (ORDER BY SUM(avg_mdcr_alowd_amt * tot_srvcs) DESC) AS allowed_rank
FROM copd_em
WHERE geo_level = 'State'
GROUP BY geo_desc;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 5: Oxygen DME National Summary
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS o2_national;
CREATE VIEW IF NOT EXISTS o2_national AS
SELECT
    hcpcs_cd,
    MAX(hcpcs_desc)                                             AS hcpcs_desc,
    SUM(tot_suplr_srvcs)                                        AS total_rental_months,
    SUM(tot_suplr_clms)                                         AS total_claims,
    SUM(tot_suplr_benes)                                        AS benes_non_suppressed,
    COUNT(DISTINCT rfrg_npi)                                    AS unique_providers,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS total_allowed,
    ROUND(SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs), 2)   AS total_submitted,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) /
          SUM(tot_suplr_srvcs), 2)                              AS avg_allowed_per_month,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) /
          SUM(tot_suplr_srvcs) * 12, 2)                         AS annual_per_patient,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) /
          SUM(tot_suplr_srvcs) * 36, 2)                         AS medicare_36mo_cap,
    ROUND(SUM(avg_suplr_sbmtd_chrg * tot_suplr_srvcs) /
          SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS markup_x,
    ROUND(SUM(tot_suplr_srvcs) * 100.0 /
         (SELECT SUM(tot_suplr_srvcs) FROM copd_oxygen), 1)     AS pct_volume
FROM copd_oxygen
GROUP BY hcpcs_cd;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 6: Oxygen DME State Summary
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS o2_state;
CREATE VIEW IF NOT EXISTS o2_state AS
SELECT
    prvdr_state                                                 AS state,
    SUM(tot_suplr_srvcs)                                        AS total_rental_months,
    SUM(tot_suplr_benes)                                        AS benes_non_suppressed,
    COUNT(DISTINCT rfrg_npi)                                    AS unique_providers,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS total_allowed,
    ROUND(SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs), 2)   AS total_submitted,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) /
          SUM(tot_suplr_srvcs), 2)                              AS avg_allowed_per_month,
    ROUND(SUM(avg_suplr_sbmtd_chrg * tot_suplr_srvcs) /
          SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS markup_x,
    SUM(CASE WHEN ruca_cat='Rural' THEN tot_suplr_srvcs ELSE 0 END) AS rural_rental_months,
    ROUND(
        SUM(CASE WHEN ruca_cat='Rural' THEN tot_suplr_srvcs ELSE 0 END) * 100.0 /
        NULLIF(SUM(tot_suplr_srvcs), 0), 1
    )                                                           AS rural_pct,
    RANK() OVER (ORDER BY SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) DESC) AS allowed_rank
FROM copd_oxygen
GROUP BY prvdr_state;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 7: Oxygen DME Specialty Summary
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS o2_specialty;
CREATE VIEW IF NOT EXISTS o2_specialty AS
SELECT
    specialty_desc,
    SUM(tot_suplr_srvcs)                                        AS total_rental_months,
    COUNT(DISTINCT rfrg_npi)                                    AS unique_providers,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS total_allowed,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) * 100.0 /
         (SELECT SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) FROM copd_oxygen), 1)
                                                                AS pct_of_market,
    CASE specialty_desc
        WHEN 'Pulmonary Disease'    THEN 'PRIMARY B2B'
        WHEN 'Internal Medicine'   THEN 'PRIMARY B2B'
        WHEN 'Family Practice'     THEN 'PRIMARY B2B'
        WHEN 'Nurse Practitioner'  THEN 'KEY CHANNEL'
        WHEN 'Sleep Medicine'      THEN 'STRATEGIC BRIDGE'
        ELSE 'SECONDARY'
    END                                                         AS b2b_tier,
    RANK() OVER (ORDER BY SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) DESC) AS market_rank
FROM copd_oxygen
WHERE specialty_desc IS NOT NULL
GROUP BY specialty_desc;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 8: Integrated Market Stack Summary (all three layers)
-- ─────────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS integrated_market;
CREATE VIEW IF NOT EXISTS integrated_market AS
SELECT
    layer_no,
    layer_type,
    market_name,
    ROUND(medicare_allowed, 0)          AS medicare_allowed,
    ROUND(medicare_submitted, 0)        AS medicare_submitted,
    ROUND(medicare_submitted - medicare_allowed, 0) AS friction,
    ROUND(CASE WHEN medicare_allowed > 0
               THEN medicare_submitted / medicare_allowed ELSE NULL END, 2) AS markup_x,
    ROUND(medicare_allowed * 3, 0)      AS all_payer_low,
    ROUND(medicare_allowed * 5, 0)      AS all_payer_high,
    data_basis
FROM (
    SELECT 1 AS layer_no, 'DIAGNOSTIC' AS layer_type,
           'PFT Tests 94010/94060/94726/94729' AS market_name,
           SUM(avg_mdcr_alowd_amt * tot_srvcs) AS medicare_allowed,
           SUM(avg_sbmtd_chrg     * tot_srvcs) AS medicare_submitted,
           'Exact CMS' AS data_basis
    FROM copd_pft WHERE geo_level = 'National'
    UNION ALL
    SELECT 2, 'VISIT', 'E&M Visits 99213/99214 (COPD est. 8%)',
           SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08,
           SUM(avg_sbmtd_chrg     * tot_srvcs) * 0.08,
           'Estimated — 8% COPD share'
    FROM copd_em WHERE geo_level = 'National'
    UNION ALL
    SELECT 3, 'DME', 'Oxygen DME E0434/E1392',
           SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs),
           SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs),
           'Exact CMS DMEPOS PUF'
    FROM copd_oxygen
);
