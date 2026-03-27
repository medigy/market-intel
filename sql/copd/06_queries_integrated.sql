-- =============================================================================
-- VOXIA COPD ANALYTICS
-- Script 06: Integrated Market Stack & LTV Model
-- Source tables: copd_pft + copd_em + copd_oxygen (all three combined)
-- Covers: Master market overview, full LTV stack, geographic composite,
--         markup comparison across layers, key findings summary
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 1: MASTER MARKET STACK — ALL THREE LAYERS
-- (Report: Master Market Overview — Section 1)
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    layer_no,
    layer_type,
    market_name,
    ROUND(medicare_allowed, 0)                                  AS medicare_allowed,
    ROUND(medicare_submitted, 0)                                AS submitted_charges,
    ROUND(CASE WHEN medicare_allowed > 0
               THEN medicare_submitted / medicare_allowed ELSE NULL END, 2)
                                                                AS markup_x,
    ROUND(medicare_submitted - medicare_allowed, 0)             AS billing_friction,
    ROUND(medicare_allowed * 3, 0)                              AS all_payer_low,
    ROUND(medicare_allowed * 5, 0)                              AS all_payer_high,
    data_basis
FROM (
    SELECT 1 AS layer_no, 'DIAGNOSTIC' AS layer_type,
           'PFT Tests (94010/94060/94726/94729)' AS market_name,
           SUM(avg_mdcr_alowd_amt * tot_srvcs)   AS medicare_allowed,
           SUM(avg_sbmtd_chrg     * tot_srvcs)   AS medicare_submitted,
           'Exact CMS'                            AS data_basis
    FROM copd_pft WHERE geo_level = 'National'

    UNION ALL
    SELECT 2, 'VISIT',
           'E&M Office Visits 99213/99214 (COPD est. 8%)',
           SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08,
           SUM(avg_sbmtd_chrg     * tot_srvcs) * 0.08,
           'Estimated — 8% COPD share'
    FROM copd_em WHERE geo_level = 'National'

    UNION ALL
    SELECT 3, 'DME',
           'Oxygen DME (E0434 / E1392)',
           SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs),
           SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs),
           'Exact CMS DMEPOS PUF'
    FROM copd_oxygen
)
ORDER BY layer_no;


-- Grand total row
SELECT
    'TOTAL'                                                     AS layer,
    ROUND(pft_allowed + em_allowed + o2_allowed, 0)             AS total_medicare_allowed,
    ROUND((pft_allowed + em_allowed + o2_allowed) * 3, 0)       AS all_payer_low,
    ROUND((pft_allowed + em_allowed + o2_allowed) * 5, 0)       AS all_payer_high,
    ROUND((pft_submitted + em_submitted + o2_submitted) /
          (pft_allowed + em_allowed + o2_allowed), 2)           AS blended_markup_x
FROM (
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
);


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 2: FULL PER-PATIENT LTV STACK
-- Combines PFT + E&M + Oxygen into single patient economics model
-- ─────────────────────────────────────────────────────────────────────────────

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
        99.00                               AS voice_screen,
        pft_94060                           AS pft_confirmation,
        em_99214                            AS initial_visit,
        em_99213_annual                     AS annual_followup_visits,
        em_99214                            AS annual_review,
        o2_monthly * 12                     AS annual_o2,
        o2_monthly * 36                     AS o2_36mo_cap
    FROM base_rates
)
SELECT
    -- One-time items
    voice_screen                                                AS voice_screen,
    pft_confirmation                                            AS pft_confirmation,
    initial_visit                                               AS initial_visit_99214,
    -- Annual recurring
    annual_followup_visits                                      AS annual_99213_visits,
    annual_review                                               AS annual_review_99214,
    annual_o2                                                   AS annual_o2_rental,
    -- LTV calculations
    ROUND(annual_followup_visits + annual_review, 2)            AS total_annual_em,
    ROUND(annual_followup_visits + annual_review + annual_o2, 2)
                                                                AS total_annual_em_plus_o2,
    -- Year 1 total (one-time + first-year recurring)
    ROUND(voice_screen + pft_confirmation + initial_visit +
          annual_followup_visits + annual_review + annual_o2, 2)
                                                                AS year1_ltv,
    -- Year 2-3 recurring (no screen/PFT/initial visit)
    ROUND(annual_followup_visits + annual_review + annual_o2, 2)
                                                                AS year2_plus_annual,
    -- 36-month total LTV
    ROUND(voice_screen + pft_confirmation + initial_visit +
         (annual_followup_visits + annual_review + annual_o2) * 3, 2)
                                                                AS ltv_36_months_with_o2,
    -- 36-month LTV without oxygen (70% of patients — SpO2 > 88%)
    ROUND(voice_screen + pft_confirmation + initial_visit +
         (annual_followup_visits + annual_review) * 3, 2)
                                                                AS ltv_36_months_no_o2,
    o2_36mo_cap                                                 AS o2_36mo_cap
FROM stack;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 3: MARKUP COMPARISON ACROSS ALL LAYERS
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    layer_no,
    market_layer,
    ROUND(total_submitted, 0)                                   AS total_submitted,
    ROUND(total_allowed, 0)                                     AS total_allowed,
    ROUND(total_submitted - total_allowed, 0)                   AS friction,
    ROUND(total_submitted / total_allowed, 2)                   AS markup_x,
    CASE
        WHEN total_submitted / total_allowed >= 5    THEN 'EXTREME'
        WHEN total_submitted / total_allowed >= 3.5  THEN 'HIGH'
        WHEN total_submitted / total_allowed >= 2.5  THEN 'MODERATE'
        ELSE 'LOW'
    END                                                         AS markup_severity
FROM (
    SELECT 1 AS layer_no, 'PFT Diagnostics (exact)' AS market_layer,
           SUM(avg_sbmtd_chrg     * tot_srvcs) AS total_submitted,
           SUM(avg_mdcr_alowd_amt * tot_srvcs) AS total_allowed
    FROM copd_pft WHERE geo_level = 'National'

    UNION ALL
    SELECT 2, 'E&M Visits (est. 8% COPD)',
           SUM(avg_sbmtd_chrg     * tot_srvcs) * 0.08,
           SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08
    FROM copd_em WHERE geo_level = 'National'

    UNION ALL
    SELECT 3, 'Oxygen DME (exact)',
           SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs),
           SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs)
    FROM copd_oxygen
)
ORDER BY layer_no;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 4: GEOGRAPHIC COMPOSITE RANKING
-- States ranked across all three data layers
-- ─────────────────────────────────────────────────────────────────────────────

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
LEFT JOIN o2_state o ON p.state = o.state
ORDER BY composite_rank_sum ASC
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5: SCALE CONTEXT — 10,000-PATIENT COHORT MODEL
-- (Report: "At scale: 10,000 diagnosed patients, 30% qualifying for O2")
-- ─────────────────────────────────────────────────────────────────────────────

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
    -- Revenue across cohort
    ROUND(10000 * screen_price, 0)                              AS total_screen_revenue,
    ROUND(10000 * pft_rate, 0)                                  AS total_pft_revenue,
    ROUND(10000 * (rate_99213 * 2.2 + rate_99214), 0)           AS total_annual_em_revenue,
    ROUND(10000 * 0.30 * o2_monthly * 36, 0)                    AS total_o2_36mo_revenue,
    -- 36-month cohort LTV
    ROUND(
        10000 * screen_price +
        10000 * pft_rate +
        10000 * rate_99214 +    -- initial visit
        10000 * (rate_99213 * 2.2 + rate_99214) * 3 +  -- 3 years E&M
        10000 * 0.30 * o2_monthly * 36, 0              -- O2 for qualifying patients
    )                                                           AS cohort_36mo_total_revenue
FROM rates;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 6: COMPLETE KEY FINDINGS DASHBOARD
-- All 12 findings from the integrated report in one query set
-- ─────────────────────────────────────────────────────────────────────────────

-- Finding 1: Total COPD Medicare market (3 layers)
SELECT
    '1' AS finding_no,
    'Total COPD Medicare market (3 layers)' AS metric,
    ROUND(
        (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) FROM copd_pft WHERE geo_level='National') +
        (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08 FROM copd_em WHERE geo_level='National') +
        (SELECT SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) FROM copd_oxygen)
    , 0) AS value,
    '$1.59B' AS formatted_value;


-- Finding 2: Funnel gap — COPD E&M visits vs PFT tests
SELECT
    '2' AS finding_no,
    'Visit-to-test funnel ratio (estimated)' AS metric,
    ROUND(
        (SELECT SUM(tot_srvcs) * 0.08 FROM copd_em WHERE geo_level='National') /
        (SELECT SUM(tot_srvcs) FROM copd_pft WHERE geo_level='National')
    , 1) AS value,
    'x ratio' AS unit;


-- Finding 3: % of Medicare COPD patients never tested
SELECT
    '3' AS finding_no,
    '% Medicare COPD patients untested (estimated)' AS metric,
    ROUND((6200000 -
        (SELECT MAX(tot_benes) FROM copd_pft WHERE geo_level='National' AND hcpcs_cd='94729' AND place_of_srvc='O')
    ) * 100.0 / 6200000, 1) AS value,
    '%' AS unit;


-- Finding 4: Oxygen market metrics
SELECT
    '4' AS finding_no,
    'Oxygen DME total allowed' AS metric,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 0) AS value,
    '$' || ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) / 1000000, 1) || 'M' AS formatted
FROM copd_oxygen;


-- Finding 5: 36-month LTV with oxygen
SELECT
    '5' AS finding_no,
    '36-month patient LTV (with O2)' AS metric,
    ROUND(
        99 +
        (SELECT AVG(avg_mdcr_alowd_amt) FROM copd_pft WHERE geo_level='National' AND hcpcs_cd='94060' AND place_of_srvc='O') +
        (SELECT AVG(avg_mdcr_alowd_amt) FROM copd_em WHERE geo_level='National' AND hcpcs_cd='99214' AND place_of_srvc='O') +
        (SELECT AVG(avg_mdcr_alowd_amt) * 2.2 + AVG(avg_mdcr_alowd_amt) FROM copd_em WHERE geo_level='National' AND hcpcs_cd='99213' AND place_of_srvc='O') * 3 +
        (SELECT AVG(avg_suplr_mdcr_alowd_amt) * 36 FROM copd_oxygen WHERE hcpcs_cd='E1392')
    , 2) AS value,
    '$' AS unit;


-- Finding 6: Blended system markup across all layers
SELECT
    '6' AS finding_no,
    'Blended system markup (all 3 layers)' AS metric,
    ROUND(
        (
            (SELECT SUM(avg_sbmtd_chrg * tot_srvcs) FROM copd_pft WHERE geo_level='National') +
            (SELECT SUM(avg_sbmtd_chrg * tot_srvcs) * 0.08 FROM copd_em WHERE geo_level='National') +
            (SELECT SUM(avg_suplr_sbmtd_chrg * tot_suplr_srvcs) FROM copd_oxygen)
        ) /
        (
            (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) FROM copd_pft WHERE geo_level='National') +
            (SELECT SUM(avg_mdcr_alowd_amt * tot_srvcs) * 0.08 FROM copd_em WHERE geo_level='National') +
            (SELECT SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) FROM copd_oxygen)
        )
    , 2) AS value,
    'x blended markup' AS unit;


-- Finding 7: Oxygen rural share
SELECT
    '7' AS finding_no,
    'Rural share of O2 rental spend' AS metric,
    ROUND(
        SUM(CASE WHEN ruca_cat = 'Rural' THEN avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs ELSE 0 END) * 100.0 /
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 1
    ) AS value,
    '%' AS unit
FROM copd_oxygen;


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
FROM copd_oxygen;
