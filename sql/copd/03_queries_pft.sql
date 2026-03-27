-- =============================================================================
-- VOXIA COPD ANALYTICS
-- Script 03: COPD PFT Diagnostic Queries
-- Source table: copd_pft
-- Covers every KPI, table, and observation in the COPD PFT Evidence Report
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 1: NATIONAL TOP-LINE KPIs
-- ─────────────────────────────────────────────────────────────────────────────

-- KPI 1: Total Medicare Allowed Payments
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

-- Table: Volume and Revenue by Procedure Code (all settings combined)
SELECT
    p.hcpcs_cd,
    MAX(p.hcpcs_desc)                                       AS description,
    SUM(p.tot_srvcs)                                        AS total_services,
    ROUND(SUM(p.tot_srvcs) * 100.0 /
        (SELECT SUM(tot_srvcs) FROM copd_pft WHERE geo_level = 'National'), 1)
                                                            AS pct_volume,
    MAX(p.tot_benes)                                        AS beneficiaries_approx,
    ROUND(SUM(p.avg_mdcr_alowd_amt * p.tot_srvcs), 2)       AS total_allowed,
    ROUND(
        SUM(p.avg_mdcr_alowd_amt * p.tot_srvcs) /
        SUM(p.tot_srvcs), 2
    )                                                       AS avg_allowed_per_test,
    ROUND(
        SUM(p.avg_sbmtd_chrg * p.tot_srvcs) /
        SUM(p.tot_srvcs), 2
    )                                                       AS avg_submitted_per_test,
    ROUND(
        SUM(p.avg_sbmtd_chrg * p.tot_srvcs) /
        SUM(p.avg_mdcr_alowd_amt * p.tot_srvcs), 2
    )                                                       AS markup_x,
    -- Interaction density: services / max benes per code
    ROUND(SUM(p.tot_srvcs) * 1.0 / MAX(p.tot_benes), 2)    AS interaction_density
FROM copd_pft p
WHERE geo_level = 'National'
GROUP BY p.hcpcs_cd
ORDER BY total_services DESC;


-- Table: Simple average allowed per code per setting (facility vs office)
-- Used in Report Section 2.1 footnote
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

SELECT
    place_of_srvc,
    CASE place_of_srvc WHEN 'F' THEN 'Facility' ELSE 'Office / Non-Facility' END AS setting,
    SUM(tot_srvcs)                                          AS total_services,
    ROUND(SUM(tot_srvcs) * 100.0 /
        (SELECT SUM(tot_srvcs) FROM copd_pft WHERE geo_level = 'National'), 1)
                                                            AS pct_services,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)           AS total_allowed,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) * 100.0 /
        (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) FROM copd_pft WHERE geo_level = 'National'), 1)
                                                            AS pct_allowed,
    SUM(tot_benes)                                          AS total_benes,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs) /
          SUM(tot_srvcs), 2)                                AS avg_allowed_per_test
FROM copd_pft
WHERE geo_level = 'National'
GROUP BY place_of_srvc
ORDER BY place_of_srvc;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 3: PRICING INEFFICIENCY
-- ─────────────────────────────────────────────────────────────────────────────

-- Per-code markup table (Report Section 3)
SELECT
    hcpcs_cd,
    ROUND(SUM(avg_sbmtd_chrg      * tot_srvcs) / SUM(tot_srvcs), 2) AS avg_submitted,
    ROUND(SUM(avg_mdcr_alowd_amt  * tot_srvcs) / SUM(tot_srvcs), 2) AS avg_allowed,
    ROUND(
        (SUM(avg_sbmtd_chrg * tot_srvcs) / SUM(tot_srvcs)) /
        (SUM(avg_mdcr_alowd_amt * tot_srvcs) / SUM(tot_srvcs)), 2
    )                                                                  AS markup_x,
    ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) -
          SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)                      AS total_friction
FROM copd_pft
WHERE geo_level = 'National'
GROUP BY hcpcs_cd
ORDER BY markup_x DESC;


-- Total system friction across all codes
SELECT
    ROUND(SUM(avg_sbmtd_chrg * tot_srvcs), 2)              AS total_submitted,
    ROUND(SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)           AS total_allowed,
    ROUND(SUM(avg_sbmtd_chrg * tot_srvcs) -
          SUM(avg_mdcr_alowd_amt * tot_srvcs), 2)           AS total_friction,
    ROUND(
        SUM(avg_sbmtd_chrg * tot_srvcs) /
        SUM(avg_mdcr_alowd_amt * tot_srvcs), 2
    )                                                       AS blended_markup_x
FROM copd_pft
WHERE geo_level = 'National';


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 4: INTERACTION DENSITY (Report Table Section 4)
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    hcpcs_cd,
    MAX(hcpcs_desc)                                         AS description,
    SUM(tot_srvcs)                                          AS total_services,
    MAX(tot_benes)                                          AS beneficiaries_approx,
    ROUND(SUM(tot_srvcs) * 1.0 / MAX(tot_benes), 2)         AS interaction_density,
    CASE hcpcs_cd
        WHEN '94010' THEN 'First-line screening'
        WHEN '94060' THEN 'Diagnostic confirmation'
        WHEN '94726' THEN 'Severity classification'
        WHEN '94729' THEN 'Ongoing monitoring'
    END                                                     AS clinical_role
FROM copd_pft
WHERE geo_level = 'National'
GROUP BY hcpcs_cd
ORDER BY interaction_density DESC;


-- Overall interaction density
SELECT
    ROUND(
        SUM(tot_srvcs) * 1.0 /
        (SELECT SUM(max_benes)
         FROM (SELECT MAX(tot_benes) AS max_benes FROM copd_pft
               WHERE geo_level = 'National' GROUP BY hcpcs_cd)), 2
    )                                                       AS overall_density
FROM copd_pft
WHERE geo_level = 'National';


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5.1: TOP 10 STATES BY TOTAL ALLOWED (Report Table 5.1)
-- ─────────────────────────────────────────────────────────────────────────────

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
GROUP BY geo_desc
ORDER BY total_allowed DESC
LIMIT 10;


-- Top 10 states share of national total
SELECT
    ROUND(
        SUM(state_allowed) * 100.0 /
        (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) FROM copd_pft WHERE geo_level = 'National'), 1
    )                                                       AS top10_pct_of_national
FROM (
    SELECT
        geo_desc,
        SUM(avg_mdcr_alowd_amt * tot_srvcs)                AS state_allowed
    FROM copd_pft
    WHERE geo_level = 'State'
    GROUP BY geo_desc
    ORDER BY state_allowed DESC
    LIMIT 10
);


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5.2: PROVIDER ACCESS GAP — LOWEST PROVIDER STATES
-- ─────────────────────────────────────────────────────────────────────────────

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
HAVING total_providers IS NOT NULL
ORDER BY total_providers ASC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5.3: MARKET READINESS — OFFICE TEST ADOPTION BY STATE
-- ─────────────────────────────────────────────────────────────────────────────

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
HAVING total_services > 1000
ORDER BY office_pct DESC
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 6.1: MARKET SIZING — DIAGNOSTIC-AS-PRODUCT MODEL
-- ─────────────────────────────────────────────────────────────────────────────

-- Revenue model assumptions (report Section 6.1)
SELECT
    'Medicare diagnosed/tested annually (DLCO proxy)'       AS assumption,
    MAX(tot_benes)                                          AS value
FROM copd_pft
WHERE geo_level = 'National' AND hcpcs_cd = '94729' AND place_of_srvc = 'O'

UNION ALL
SELECT 'Undiagnosed COPD (estimated)',          12000000
UNION ALL
SELECT 'Voice screen price ($)',                79
UNION ALL
SELECT 'Conservative capture 0.5% (tests)',    60000
UNION ALL
SELECT 'Conservative Year 1 revenue ($)',      4740000
UNION ALL
SELECT 'Moderate capture 2.0% (tests)',        240000
UNION ALL
SELECT 'Moderate Year 1 revenue ($)',          18960000;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 7: COMPETITIVE BENCHMARK TABLE (Report Section 7)
-- ─────────────────────────────────────────────────────────────────────────────

-- Pricing reference: what Voxia competes against (from CMS data)
SELECT
    'Spirometry in-lab (94010 Facility)'        AS diagnostic_method,
    ROUND(AVG(avg_sbmtd_chrg), 2)              AS avg_submitted_patient_facing,
    ROUND(AVG(avg_mdcr_alowd_amt), 2)          AS avg_medicare_allowed,
    ROUND(AVG(avg_sbmtd_chrg) /
          AVG(avg_mdcr_alowd_amt), 2)          AS markup_x,
    'Requires in-person visit + equipment'      AS friction
FROM copd_pft
WHERE geo_level = 'National' AND hcpcs_cd = '94010' AND place_of_srvc = 'F'

UNION ALL
SELECT
    'Spirometry office (94010 Office)',
    ROUND(AVG(avg_sbmtd_chrg), 2),
    ROUND(AVG(avg_mdcr_alowd_amt), 2),
    ROUND(AVG(avg_sbmtd_chrg) / AVG(avg_mdcr_alowd_amt), 2),
    'In-person, spirometer required'
FROM copd_pft
WHERE geo_level = 'National' AND hcpcs_cd = '94010' AND place_of_srvc = 'O'

UNION ALL
SELECT
    'Post-bronchodilator test (94060 Office)',
    ROUND(AVG(avg_sbmtd_chrg), 2),
    ROUND(AVG(avg_mdcr_alowd_amt), 2),
    ROUND(AVG(avg_sbmtd_chrg) / AVG(avg_mdcr_alowd_amt), 2),
    'In-person, spirometer + medication'
FROM copd_pft
WHERE geo_level = 'National' AND hcpcs_cd = '94060' AND place_of_srvc = 'O'

UNION ALL
SELECT
    'Voxia Voice Screen',
    NULL, 99.0, NULL,
    '60 seconds on phone, no equipment'

ORDER BY avg_medicare_allowed ASC;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 8: KEY FINDINGS SUMMARY — All metrics in one result set
-- ─────────────────────────────────────────────────────────────────────────────

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
