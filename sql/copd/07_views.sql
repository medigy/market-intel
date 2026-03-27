-- =============================================================================
-- VOXIA COPD ANALYTICS
-- Script 07: Views — Pre-computed aggregations for dashboard use
-- Create these once; reference them in application queries.
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW 1: PFT National Summary
-- ─────────────────────────────────────────────────────────────────────────────
CREATE VIEW IF NOT EXISTS vw_pft_national AS
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
CREATE VIEW IF NOT EXISTS vw_pft_state AS
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
CREATE VIEW IF NOT EXISTS vw_em_national AS
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
CREATE VIEW IF NOT EXISTS vw_em_state AS
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
CREATE VIEW IF NOT EXISTS vw_o2_national AS
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
CREATE VIEW IF NOT EXISTS vw_o2_state AS
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
CREATE VIEW IF NOT EXISTS vw_o2_specialty AS
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
CREATE VIEW IF NOT EXISTS vw_integrated_market AS
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
