-- =============================================================================
-- VOXIA COPD ANALYTICS
-- Script 04: E&M Visits Queries (99213 / 99214)
-- Source table: copd_em
-- Covers every KPI, table, and observation in the E&M Integrated Report
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 1: NATIONAL TOP-LINE KPIs
-- ─────────────────────────────────────────────────────────────────────────────

-- All-condition national totals (this data covers ALL diagnoses, not COPD-only)
SELECT
    SUM(tot_srvcs)                                              AS total_all_condition_services,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)               AS total_allowed,
    ROUND(SUM(avg_sbmtd_chrg     * tot_srvcs), 2)               AS total_submitted,
    ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) /
          SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)               AS system_markup_x,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) / 1000000000.0, 2)
                                                                AS total_allowed_billions,
    SUM(tot_rndrng_prvdrs)                                      AS total_providers_sum
FROM copd_em
WHERE geo_level = 'National';


-- COPD-estimated share (8% applied to all-condition E&M)
SELECT
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08, 2)       AS copd_est_allowed,
    ROUND(SUM(tot_srvcs) * 0.08, 0)                             AS copd_est_services,
    0.08                                                        AS copd_share_pct,
    'Epidemiological estimate — 8% of Medicare E&M'             AS basis
FROM copd_em
WHERE geo_level = 'National';


-- E&M market vs PFT diagnostic market ratio
SELECT
    ROUND(em.em_allowed / pft.pft_allowed, 1)                   AS em_vs_pft_multiplier,
    ROUND(em.em_allowed * 0.08, 2)                              AS copd_em_est,
    pft.pft_allowed                                             AS pft_exact
FROM
    (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) AS em_allowed
     FROM copd_em WHERE geo_level = 'National') em,
    (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) AS pft_allowed
     FROM copd_pft WHERE geo_level = 'National') pft;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 2: PER-CODE BREAKDOWN — 99213 vs 99214
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    hcpcs_cd,
    MAX(hcpcs_desc)                                             AS description,
    SUM(tot_srvcs)                                              AS total_services,
    ROUND(SUM(tot_srvcs) * 100.0 /
        (SELECT SUM(tot_srvcs) FROM copd_em WHERE geo_level = 'National'), 1)
                                                                AS pct_of_volume,
    MAX(tot_benes)                                              AS beneficiaries_approx,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)               AS total_allowed,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) /
          SUM(tot_srvcs), 2)                                    AS avg_allowed_per_visit,
    ROUND(SUM(avg_sbmtd_chrg     * tot_srvcs) /
          SUM(tot_srvcs), 2)                                    AS avg_submitted_per_visit,
    ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) /
          SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)               AS markup_x,
    ROUND(SUM(tot_srvcs) * 1.0 / MAX(tot_benes), 2)             AS interaction_density
FROM copd_em
WHERE geo_level = 'National'
GROUP BY hcpcs_cd
ORDER BY hcpcs_cd;


-- Office vs Facility split for E&M
SELECT
    place_of_srvc,
    CASE place_of_srvc WHEN 'F' THEN 'Facility' ELSE 'Office / Non-Facility' END AS setting,
    SUM(tot_srvcs)                                              AS total_services,
    ROUND(SUM(tot_srvcs) * 100.0 /
        (SELECT SUM(tot_srvcs) FROM copd_em WHERE geo_level = 'National'), 1)
                                                                AS pct_services,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)               AS total_allowed,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 100.0 /
        (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) FROM copd_em WHERE geo_level = 'National'), 1)
                                                                AS pct_allowed
FROM copd_em
WHERE geo_level = 'National'
GROUP BY place_of_srvc;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 3: THE DIAGNOSIS FUNNEL GAP
-- Cross-analysis: estimated COPD E&M visits vs actual PFT tests
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    ROUND(em.total_em_srvcs * 0.08, 0)                         AS copd_em_visits_est,
    pft.total_pft_srvcs                                         AS pft_tests_exact,
    ROUND((em.total_em_srvcs * 0.08) / pft.total_pft_srvcs, 1) AS visit_to_test_ratio,
    6200000                                                     AS known_medicare_copd_patients,
    pft.max_pft_benes                                           AS pft_tested_patients,
    ROUND((6200000 - pft.max_pft_benes) * 100.0 / 6200000, 1)  AS pct_never_tested,
    6200000 - pft.max_pft_benes                                 AS never_tested_count
FROM
    (SELECT SUM(tot_srvcs) AS total_em_srvcs FROM copd_em WHERE geo_level = 'National') em,
    (SELECT
         SUM(tot_srvcs)    AS total_pft_srvcs,
         MAX(tot_benes)    AS max_pft_benes
     FROM copd_pft
     WHERE geo_level = 'National' AND hcpcs_cd = '94729' AND place_of_srvc = 'O') pft;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 4: 99214 COMPLEXITY SIGNAL BY STATE
-- States with highest 99214 % = highest acuity = best B2B targets
-- ─────────────────────────────────────────────────────────────────────────────

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
HAVING total_services > 1000000       -- filter to meaningful volume states
ORDER BY pct_99214 DESC
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5: TOP 15 STATES BY TOTAL ALLOWED
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    geo_desc                                                    AS state,
    SUM(tot_srvcs)                                              AS total_services,
    SUM(tot_benes)                                              AS total_benes,
    SUM(tot_rndrng_prvdrs)                                      AS total_providers,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)               AS total_allowed,
    ROUND(SUM(avg_sbmtd_chrg     * tot_srvcs), 2)               AS total_submitted,
    ROUND(SUM(tot_srvcs) * 1.0 / NULLIF(SUM(tot_benes), 0), 2) AS density_visits_per_bene
FROM copd_em
WHERE geo_level = 'State'
GROUP BY geo_desc
ORDER BY total_allowed DESC
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5: GEOGRAPHIC CROSS-ANALYSIS
-- E&M visits vs PFT tests by state (funnel gap by geography)
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    em.state,
    em.em_total_srvcs                                           AS em_all_cond_services,
    ROUND(em.em_total_srvcs * 0.08, 0)                          AS copd_em_est,
    COALESCE(pft.pft_total_srvcs, 0)                            AS pft_tests,
    CASE
        WHEN COALESCE(pft.pft_total_srvcs, 0) > 0
        THEN ROUND(em.em_total_srvcs * 1.0 / pft.pft_total_srvcs, 1)
        ELSE NULL
    END                                                         AS em_to_pft_ratio,
    em.em_total_allowed                                         AS em_allowed,
    COALESCE(pft.pft_total_allowed, 0)                          AS pft_allowed
FROM (
    SELECT
        geo_desc                                                AS state,
        SUM(tot_srvcs)                                          AS em_total_srvcs,
        ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)           AS em_total_allowed
    FROM copd_em
    WHERE geo_level = 'State'
    GROUP BY geo_desc
) em
LEFT JOIN (
    SELECT
        geo_desc                                                AS state,
        SUM(tot_srvcs)                                          AS pft_total_srvcs,
        ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)           AS pft_total_allowed
    FROM copd_pft
    WHERE geo_level = 'State'
    GROUP BY geo_desc
) pft ON em.state = pft.state
ORDER BY em.em_total_allowed DESC
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 6: FULL REVENUE STACK PER PATIENT (Report Section 5.1)
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    layer_no,
    layer_type,
    revenue_item,
    timing,
    ROUND(value_per_patient, 2)                                 AS value_per_patient,
    data_basis
FROM (
    SELECT 1 AS layer_no, 'SCREEN' AS layer_type,
           'Voxia voice screen'                                 AS revenue_item,
           'One-time'                                           AS timing,
           99.00                                                AS value_per_patient,
           'Voxia DTC pricing'                                  AS data_basis

    UNION ALL SELECT 2, 'DIAG',
           'PFT confirmation (94060 avg allowed)',
           'One-time',
           (SELECT ROUND(AVG(avg_mdcr_alowd_amt), 2)
            FROM copd_pft WHERE geo_level = 'National'
              AND hcpcs_cd = '94060' AND place_of_srvc = 'O'),
           'Exact CMS — 94060 Office avg allowed'

    UNION ALL SELECT 3, 'VISIT',
           'Initial 99214 diagnosis visit',
           'One-time',
           (SELECT ROUND(AVG(avg_mdcr_alowd_amt), 2)
            FROM copd_em WHERE geo_level = 'National'
              AND hcpcs_cd = '99214' AND place_of_srvc = 'O'),
           'Exact CMS — 99214 Office avg allowed'

    UNION ALL SELECT 4, 'VISIT',
           'Follow-up 99213 visits x 2.2/yr',
           'Annual',
           (SELECT ROUND(AVG(avg_mdcr_alowd_amt) * 2.2, 2)
            FROM copd_em WHERE geo_level = 'National'
              AND hcpcs_cd = '99213' AND place_of_srvc = 'O'),
           'CMS avg x 2.2 estimated annual visits'

    UNION ALL SELECT 5, 'VISIT',
           'Annual 99214 COPD review x 1/yr',
           'Annual',
           (SELECT ROUND(AVG(avg_mdcr_alowd_amt), 2)
            FROM copd_em WHERE geo_level = 'National'
              AND hcpcs_cd = '99214' AND place_of_srvc = 'O'),
           'Exact CMS — 99214 Office avg allowed'
)
ORDER BY layer_no;


-- Year 1 and Year 2+ LTV rollup
WITH visit_values AS (
    SELECT
        ROUND(AVG(CASE WHEN hcpcs_cd='99213' AND place_of_srvc='O'
                  THEN avg_mdcr_alowd_amt END) * 2.2, 2) AS annual_99213,
        ROUND(AVG(CASE WHEN hcpcs_cd='99214' AND place_of_srvc='O'
                  THEN avg_mdcr_alowd_amt END), 2)        AS annual_99214_review,
        ROUND(AVG(CASE WHEN hcpcs_cd='99214' AND place_of_srvc='O'
                  THEN avg_mdcr_alowd_amt END), 2)        AS initial_99214,
        ROUND(AVG(CASE WHEN hcpcs_cd='94060' AND place_of_srvc='O'
                  THEN avg_mdcr_alowd_amt END), 2)        AS pft_confirmation
    FROM (
        SELECT hcpcs_cd, place_of_srvc, avg_mdcr_alowd_amt FROM copd_em  WHERE geo_level='National'
        UNION ALL
        SELECT hcpcs_cd, place_of_srvc, avg_mdcr_alowd_amt FROM copd_pft WHERE geo_level='National'
    )
)
SELECT
    ROUND(99 + pft_confirmation + initial_99214 + annual_99213 + annual_99214_review, 2)
                                                            AS year1_ltv,
    ROUND(annual_99213 + annual_99214_review, 2)            AS year2_plus_annual,
    ROUND(99 + pft_confirmation + initial_99214 +
          (annual_99213 + annual_99214_review) * 3, 2)      AS ltv_3yr_em_only
FROM visit_values;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 7: B2B PROVIDER MARKET SIZING
-- ─────────────────────────────────────────────────────────────────────────────

-- Total active COPD-managing providers (from PFT dataset as proxy)
SELECT
    SUM(tot_rndrng_prvdrs)                                      AS total_copd_providers_pft,
    ROUND(SUM(tot_rndrng_prvdrs) * 0.60, 0)                     AS estimated_pcp_without_spirometry,
    ROUND(SUM(tot_rndrng_prvdrs) * 0.60 * 0.005, 0)             AS conservative_conversion_05pct,
    ROUND(SUM(tot_rndrng_prvdrs) * 0.60 * 0.005 * 299 * 12, 0) AS conservative_annual_arr,
    ROUND(SUM(tot_rndrng_prvdrs) * 0.60 * 0.02, 0)              AS moderate_conversion_2pct,
    ROUND(SUM(tot_rndrng_prvdrs) * 0.60 * 0.02 * 299 * 12, 0)  AS moderate_annual_arr
FROM copd_pft
WHERE geo_level = 'National'
  AND hcpcs_cd = '94010'
  AND place_of_srvc = 'O';


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 8: AGGREGATE MARKET VALUE — ALL LAYERS
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    'Layer 1: PFT Diagnostics (exact)'                          AS market_layer,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 0)               AS medicare_allowed,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 3, 0)           AS all_payer_low,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 5, 0)           AS all_payer_high,
    'Exact CMS'                                                 AS data_basis
FROM copd_pft WHERE geo_level = 'National'

UNION ALL
SELECT
    'Layer 2: E&M Visits (COPD est. 8%)',
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08, 0),
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08 * 3, 0),
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08 * 5, 0),
    'Estimated — 8% COPD share'
FROM copd_em WHERE geo_level = 'National'

UNION ALL
SELECT
    'Layer 3: Oxygen DME (exact)',
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 0),
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) * 3, 0),
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) * 5, 0),
    'Exact CMS'
FROM copd_oxygen;
