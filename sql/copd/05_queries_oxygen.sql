-- =============================================================================
-- VOXIA COPD ANALYTICS
-- Script 05: Oxygen DME Queries (E0434 / E1392)
-- Source table: copd_oxygen
-- Covers every KPI, table, and observation in the Oxygen DME Evidence Report
-- IMPORTANT: tot_suplr_srvcs = rental MONTHS billed (not individual procedures)
--            tot_suplr_benes = NULL for rows with <11 beneficiaries (CMS suppression)
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 1: NATIONAL TOP-LINE KPIs
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    SUM(tot_suplr_srvcs)                                        AS total_rental_months,
    SUM(tot_suplr_clms)                                         AS total_claims,
    SUM(tot_suplr_benes)                                        AS total_benes_non_suppressed,
    COUNT(DISTINCT rfrg_npi)                                    AS unique_referring_providers,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS total_allowed,
    ROUND(SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs), 2)   AS total_submitted,
    ROUND(SUM(avg_suplr_mdcr_pymt_amt  * tot_suplr_srvcs), 2)   AS total_payment,
    ROUND(
        SUM(avg_suplr_sbmtd_chrg * tot_suplr_srvcs) /
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2
    )                                                           AS system_markup_x,
    ROUND(
        SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs) -
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2
    )                                                           AS billing_friction,
    ROUND(
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) /
        SUM(tot_suplr_srvcs), 2
    )                                                           AS avg_allowed_per_rental_month
FROM copd_oxygen;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 2.1: CODE-LEVEL BREAKDOWN — E0434 vs E1392
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    hcpcs_cd,
    MAX(hcpcs_desc)                                             AS description,
    SUM(tot_suplr_srvcs)                                        AS total_rental_months,
    ROUND(SUM(tot_suplr_srvcs) * 100.0 /
        (SELECT SUM(tot_suplr_srvcs) FROM copd_oxygen), 1)      AS pct_volume,
    SUM(tot_suplr_clms)                                         AS total_claims,
    SUM(tot_suplr_benes)                                        AS benes_non_suppressed,
    COUNT(DISTINCT rfrg_npi)                                    AS unique_providers,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS total_allowed,
    ROUND(SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs), 2)   AS total_submitted,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) /
          SUM(tot_suplr_srvcs), 2)                              AS avg_allowed_per_month,
    ROUND(SUM(avg_suplr_sbmtd_chrg * tot_suplr_srvcs) /
          SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS markup_x,
    -- Annual and 36-month per-patient economics
    ROUND((SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) /
           SUM(tot_suplr_srvcs)) * 12, 2)                       AS annual_per_patient,
    ROUND((SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) /
           SUM(tot_suplr_srvcs)) * 36, 2)                       AS medicare_36mo_cap
FROM copd_oxygen
GROUP BY hcpcs_cd
ORDER BY total_rental_months DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 3: PER-PATIENT RENTAL ECONOMICS — 36-MONTH RULE
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    'Monthly allowed (E1392)'                                   AS metric,
    ROUND(AVG(avg_suplr_mdcr_alowd_amt), 2)                     AS value
FROM copd_oxygen WHERE hcpcs_cd = 'E1392'

UNION ALL SELECT 'Annual allowed per patient (x12)',
    ROUND(AVG(avg_suplr_mdcr_alowd_amt) * 12, 2)
FROM copd_oxygen WHERE hcpcs_cd = 'E1392'

UNION ALL SELECT '36-month Medicare cap per patient',
    ROUND(AVG(avg_suplr_mdcr_alowd_amt) * 36, 2)
FROM copd_oxygen WHERE hcpcs_cd = 'E1392'

UNION ALL SELECT 'Monthly submitted (billed) E1392',
    ROUND(AVG(avg_suplr_sbmtd_chrg), 2)
FROM copd_oxygen WHERE hcpcs_cd = 'E1392'

UNION ALL SELECT '36-month submitted cap per patient',
    ROUND(AVG(avg_suplr_sbmtd_chrg) * 36, 2)
FROM copd_oxygen WHERE hcpcs_cd = 'E1392'

UNION ALL SELECT 'Friction per patient over 36 months',
    ROUND((AVG(avg_suplr_sbmtd_chrg) - AVG(avg_suplr_mdcr_alowd_amt)) * 36, 2)
FROM copd_oxygen WHERE hcpcs_cd = 'E1392';


-- Year-by-year rental table
WITH monthly_rates AS (
    SELECT ROUND(AVG(avg_suplr_mdcr_alowd_amt), 2) AS monthly_allowed,
           ROUND(AVG(avg_suplr_sbmtd_chrg), 2)     AS monthly_submitted
    FROM copd_oxygen WHERE hcpcs_cd = 'E1392'
)
SELECT
    period,
    months,
    ROUND(monthly_allowed  * months, 2) AS medicare_pays,
    ROUND(monthly_submitted * months, 2) AS provider_bills,
    ROUND((monthly_submitted - monthly_allowed) * months, 2) AS friction
FROM monthly_rates,
(VALUES ('Month 1-12 (Year 1)', 12),
        ('Month 13-24 (Year 2)', 12),
        ('Month 25-36 (Year 3)', 12),
        ('Full 36-Month Cap', 36)) AS periods(period, months);


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 4.1: TOP 15 STATES BY TOTAL ALLOWED
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    prvdr_state,
    SUM(tot_suplr_srvcs)                                        AS total_rental_months,
    SUM(tot_suplr_benes)                                        AS benes_non_suppressed,
    COUNT(DISTINCT rfrg_npi)                                    AS total_providers,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS total_allowed,
    ROUND(SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs), 2)   AS total_submitted,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) /
          SUM(tot_suplr_srvcs), 2)                              AS avg_allowed_per_month,
    ROUND(SUM(avg_suplr_sbmtd_chrg * tot_suplr_srvcs) /
          SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS markup_x,
    CASE
        WHEN RANK() OVER (ORDER BY SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) DESC) <= 4  THEN 'TIER 1'
        WHEN RANK() OVER (ORDER BY SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) DESC) <= 10 THEN 'TIER 2'
        ELSE 'TIER 3'
    END                                                         AS market_tier
FROM copd_oxygen
GROUP BY prvdr_state
ORDER BY total_allowed DESC
LIMIT 15;


-- Top 5 states share of national
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

SELECT
    ruca_cat,
    SUM(tot_suplr_srvcs)                                        AS total_rental_months,
    ROUND(SUM(tot_suplr_srvcs) * 100.0 /
        (SELECT SUM(tot_suplr_srvcs) FROM copd_oxygen), 1)      AS pct_rental_months,
    SUM(tot_suplr_benes)                                        AS benes_non_suppressed,
    COUNT(DISTINCT rfrg_npi)                                    AS unique_providers,
    ROUND(COUNT(DISTINCT rfrg_npi) * 100.0 /
        (SELECT COUNT(DISTINCT rfrg_npi) FROM copd_oxygen), 1)  AS pct_providers,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS total_allowed,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) * 100.0 /
        (SELECT SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) FROM copd_oxygen), 1)
                                                                AS pct_allowed
FROM copd_oxygen
WHERE ruca_cat IS NOT NULL
GROUP BY ruca_cat
ORDER BY total_allowed DESC;


-- Top rural states by oxygen rental allowed
SELECT
    prvdr_state,
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
GROUP BY prvdr_state
ORDER BY rural_allowed DESC
LIMIT 12;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 5: REFERRING SPECIALTY ANALYSIS — B2B TARGET MAP
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    specialty_desc,
    SUM(tot_suplr_srvcs)                                        AS total_rental_months,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS total_allowed,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) * 100.0 /
        (SELECT SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) FROM copd_oxygen), 1)
                                                                AS pct_of_market,
    COUNT(DISTINCT rfrg_npi)                                    AS unique_providers,
    CASE specialty_desc
        WHEN 'Pulmonary Disease'    THEN 'PRIMARY B2B'
        WHEN 'Internal Medicine'   THEN 'PRIMARY B2B'
        WHEN 'Family Practice'     THEN 'PRIMARY B2B'
        WHEN 'Nurse Practitioner'  THEN 'KEY CHANNEL'
        WHEN 'Sleep Medicine'      THEN 'STRATEGIC BRIDGE'
        ELSE 'SECONDARY'
    END                                                         AS b2b_tier
FROM copd_oxygen
WHERE specialty_desc IS NOT NULL
GROUP BY specialty_desc
ORDER BY total_allowed DESC
LIMIT 12;


-- Pulmonology + IM + Family Practice combined share
SELECT
    ROUND(SUM(CASE WHEN specialty_desc IN
        ('Pulmonary Disease','Internal Medicine','Family Practice')
        THEN avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs ELSE 0 END) * 100.0 /
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 1)         AS pct_top3_specialties,
    COUNT(DISTINCT CASE WHEN specialty_desc IN
        ('Pulmonary Disease','Internal Medicine','Family Practice')
        THEN rfrg_npi END)                                          AS providers_top3
FROM copd_oxygen;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 6: E0434 LIQUID OXYGEN — STATE ADOPTION PATTERN
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    prvdr_state,
    SUM(CASE WHEN hcpcs_cd = 'E0434' THEN tot_suplr_srvcs ELSE 0 END) AS e0434_months,
    SUM(CASE WHEN hcpcs_cd = 'E1392' THEN tot_suplr_srvcs ELSE 0 END) AS e1392_months,
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
HAVING total_months > 100
ORDER BY e0434_pct DESC
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 7: PROVIDER VOLUME DISTRIBUTION (E1392)
-- ─────────────────────────────────────────────────────────────────────────────

-- Bucket providers by rental month volume
SELECT
    CASE
        WHEN total_months <= 10  THEN '1–10 months'
        WHEN total_months <= 30  THEN '11–30 months'
        WHEN total_months <= 100 THEN '31–100 months'
        WHEN total_months <= 300 THEN '101–300 months'
        ELSE '300+ months'
    END                                                         AS volume_bucket,
    COUNT(*)                                                    AS provider_count,
    ROUND(COUNT(*) * 100.0 /
        (SELECT COUNT(DISTINCT rfrg_npi) FROM copd_oxygen WHERE hcpcs_cd = 'E1392'), 1)
                                                                AS pct_providers,
    SUM(total_months)                                           AS total_rental_months,
    ROUND(SUM(total_allowed), 2)                                AS total_allowed
FROM (
    SELECT
        rfrg_npi,
        SUM(tot_suplr_srvcs)                                    AS total_months,
        SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs)         AS total_allowed
    FROM copd_oxygen
    WHERE hcpcs_cd = 'E1392'
    GROUP BY rfrg_npi
) provider_totals
GROUP BY volume_bucket
ORDER BY MIN(total_months);


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 8: STATE MARKUP COMPARISON
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    prvdr_state,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS total_allowed,
    ROUND(SUM(avg_suplr_sbmtd_chrg     * tot_suplr_srvcs), 2)   AS total_submitted,
    ROUND(SUM(avg_suplr_sbmtd_chrg * tot_suplr_srvcs) /
          SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs), 2)   AS markup_x,
    ROUND(SUM(avg_suplr_mdcr_alowd_amt * tot_suplr_srvcs) /
          SUM(tot_suplr_srvcs), 2)                              AS avg_monthly_allowed
FROM copd_oxygen
GROUP BY prvdr_state
ORDER BY markup_x DESC
LIMIT 15;
