-- =========================================================================
-- CMS Sleep Apnea Market Analysis SQL Report
-- =========================================================================
-- This script contains queries designed to reproduce the metrics found in
-- the "Voxia_Complete_CMS_Evidence" report and the associated dashboard.
--
-- Data sources assumed:
-- 1. uniform_resource_cms_bygeography (from Rndrng_Prvdr_Geo_Lvl,Rndrng_Prvdr_G.csv)
-- 2. uniform_resource_dme_cpap_e0601_e0470_e0471 (from DME_CPAP_E0601_E0470_E0471.csv)
-- =========================================================================


-- -- =========================================================================
-- -- PART 1: DIAGNOSTIC MARKET ECONOMICS
-- -- =========================================================================

-- 1. Top-Line Metrics (National Level)
-- Calculates total allowed payments, procedures, beneficiaries, and submitted charges
DROP TABLE IF EXISTS top_line_metrics;
CREATE Table top_line_metrics As
SELECT 
    SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt) AS Total_Medicare_Allowed_Payments,
    SUM(Tot_Srvcs) AS Total_Procedures_Billed,
    SUM(Tot_Benes) AS Unique_Medicare_Beneficiaries,
    SUM(Tot_Srvcs * Avg_Sbmtd_Chrg) AS Total_Submitted_Charges
FROM uniform_resource_cms_bygeography
WHERE Rndrng_Prvdr_Geo_Lvl = 'National';

-- 2. Procedure-Level Breakdown (National Level)
-- Calculates volumes and average allowed amounts grouped by HCPCS Code
DROP TABLE IF EXISTS procedure_level_breakdown;
CREATE Table procedure_level_breakdown As
SELECT 
    HCPCS_Cd,
    HCPCS_Desc,
    SUM(Tot_Srvcs) AS Total_Services,
    SUM(Tot_Benes) AS Total_Beneficiaries,
    AVG(Avg_Mdcr_Alowd_Amt) AS Avg_Allowed_Per_Test_Simple,
    SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt) / SUM(Tot_Srvcs) AS Avg_Allowed_Per_Test_Weighted,
    SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt) AS Total_Allowed
FROM uniform_resource_cms_bygeography
WHERE Rndrng_Prvdr_Geo_Lvl = 'National'
GROUP BY HCPCS_Cd, HCPCS_Desc
ORDER BY Total_Allowed DESC;

-- 3. In-Lab vs Home Sleep Test Market Split (National Level)
-- Groups allowed amounts by In-Lab and HST categories using volume-weighted averages
DROP TABLE IF EXISTS inlab_home_sleep_market_split;
CREATE Table inlab_home_sleep_market_split As
SELECT 
    CASE 
        WHEN HCPCS_Cd IN ('95810', '95811') THEN 'In-Lab'
        WHEN HCPCS_Cd IN ('G0398', 'G0399', 'G0400', '95800', '95806') THEN 'HST'
        ELSE 'Other'
    END AS Test_Category,
    SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt) AS Total_Allowed
FROM uniform_resource_cms_bygeography
WHERE Rndrng_Prvdr_Geo_Lvl = 'National'
GROUP BY 
    CASE 
        WHEN HCPCS_Cd IN ('95810', '95811') THEN 'In-Lab'
        WHEN HCPCS_Cd IN ('G0398', 'G0399', 'G0400', '95800', '95806') THEN 'HST'
        ELSE 'Other'
    END;

-- 4. Interaction Density
-- Calculates the ratio of total services per unique beneficiary
DROP TABLE IF EXISTS sleep_interaction_density;
CREATE Table sleep_interaction_density As
SELECT 
    HCPCS_Cd,
    SUM(Tot_Srvcs) AS Total_Services,
    SUM(Tot_Benes) AS Total_Benes,
    SUM(Tot_Srvcs) * 1.0 / SUM(Tot_Benes) AS Density_Ratio
FROM uniform_resource_cms_bygeography
WHERE Rndrng_Prvdr_Geo_Lvl = 'National'
GROUP BY HCPCS_Cd
ORDER BY HCPCS_Cd;

-- 5. Geographic Analysis (State Level)
-- Provides a state-by-state breakdown of diagnostic volumes and payments

DROP TABLE IF EXISTS geographic_analysis_state_study;
CREATE Table geographic_analysis_state_study As
SELECT 
    Rndrng_Prvdr_Geo_Desc AS State,
    SUM(Tot_Srvcs) AS Total_Services,
    SUM(Tot_Benes) AS Total_Beneficiaries,
    SUM(Tot_Srvcs * Avg_Mdcr_Alowd_Amt) AS Total_Allowed
FROM uniform_resource_cms_bygeography
WHERE Rndrng_Prvdr_Geo_Lvl = 'State'
GROUP BY Rndrng_Prvdr_Geo_Desc
ORDER BY Total_Allowed DESC;


-- =========================================================================
-- PART 2: TREATMENT MARKET ECONOMICS (DME PUF Data)
-- =========================================================================

-- 1. Device Billing Breakdown (HCPCS E0601, E0470, E0471)
-- Details volumes, charges, and payments by specific CPAP/BiPAP device code
DROP TABLE IF EXISTS device_billing_breakdown;
create table device_billing_breakdown AS
SELECT 
    HCPCS_CD AS Device_Code,
    HCPCS_Desc AS Device_Description,
    SUM(Tot_Suplr_Srvcs) AS Services_Rentals,
    SUM(Tot_Suplr_Benes) AS Beneficiaries,
    SUM(Tot_Suplr_Srvcs * Avg_Suplr_Sbmtd_Chrg) AS Total_Submitted_Charges,
    SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt) AS Total_Allowed_Amount,
    SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Pymt_Amt) AS Total_Medicare_Payment
FROM uniform_resource_dme_cpap_e0601_e0470_e0471
WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471')
GROUP BY HCPCS_CD, HCPCS_Desc
ORDER BY Total_Allowed_Amount DESC;

-- 2. System Friction and Grand Totals
-- Quantifies the gap between submitted charges and allowed amounts (System Friction)

DROP TABLE IF EXISTS system_friction_grandtotal;
create table system_friction_grandtotal AS
SELECT 
    SUM(Tot_Suplr_Srvcs) AS Grand_Total_Services,
    SUM(Tot_Suplr_Benes) AS Grand_Total_Beneficiaries,
    SUM(Tot_Suplr_Srvcs * Avg_Suplr_Sbmtd_Chrg) AS Grand_Total_Submitted,
    SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt) AS Grand_Total_Allowed,
    SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Pymt_Amt) AS Grand_Total_Payment,
    SUM(Tot_Suplr_Srvcs * Avg_Suplr_Sbmtd_Chrg) - SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt) AS System_Friction_Amount,
    (SUM(Tot_Suplr_Srvcs * Avg_Suplr_Mdcr_Alowd_Amt) / SUM(Tot_Suplr_Srvcs * Avg_Suplr_Sbmtd_Chrg)) * 100 AS Allowed_To_Submitted_Percentage
FROM uniform_resource_dme_cpap_e0601_e0470_e0471
WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471');


DROP TABLE IF EXISTS market_analysis_summary;
CREATE TABLE market_analysis_summary AS
WITH Raw_Metrics AS (
    SELECT 
        'Part B — Diagnostic (PSG + home sleep tests)' AS Dataset,
        SUM(CAST(Tot_Srvcs AS FLOAT) * CAST(Avg_Mdcr_Alowd_Amt AS FLOAT)) AS Medicare_Allowed,
        SUM(CAST(Tot_Benes AS FLOAT)) AS Beneficiaries,
        SUM(CAST(Tot_Srvcs AS FLOAT)) AS Services_Claims,
        1 AS Sort_Order
    FROM uniform_resource_cms_bygeography
    WHERE Rndrng_Prvdr_Geo_Lvl = 'National'
      AND HCPCS_Cd IN ('95810', '95811', 'G0398', 'G0399', 'G0400', '95800', '95806')

    UNION ALL

    SELECT 
        CASE 
            WHEN HCPCS_CD = 'E0601' THEN 'DME — E0601 CPAP device'
            WHEN HCPCS_CD = 'E0470' THEN 'DME — E0470 BiPAP (no backup)'
            WHEN HCPCS_CD = 'E0471' THEN 'DME — E0471 BiPAP (with backup)'
        END AS Dataset,
        SUM(CAST(Tot_Suplr_Srvcs AS FLOAT) * CAST(Avg_Suplr_Mdcr_Alowd_Amt AS FLOAT)) AS Medicare_Allowed,
        SUM(CAST(COALESCE(Tot_Suplr_Benes, 0) AS FLOAT)) AS Beneficiaries,
        SUM(CAST(Tot_Suplr_Srvcs AS FLOAT)) AS Services_Claims,
        2 AS Sort_Order
    FROM uniform_resource_dme_cpap_e0601_e0470_e0471
    WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471')
    GROUP BY HCPCS_CD
),
Total_Market AS (
    SELECT SUM(Medicare_Allowed) AS Grand_Total_Allowed FROM Raw_Metrics
)
SELECT 
    Dataset,
    PRINTF('$%,.1fM', Medicare_Allowed / 1000000.0) AS "Medicare Allowed",
    PRINTF('%,.0f', Beneficiaries) AS "Beneficiaries",
    PRINTF('%,.0f', Services_Claims) AS "Services/Claims",
    PRINTF('%.1f%%', (Medicare_Allowed / (SELECT Grand_Total_Allowed FROM Total_Market)) * 100) AS "Mkt Share",
    CASE WHEN Dataset = 'COMBINED TOTAL' THEN 'font-weight-bold table-primary' END AS _sqlpage_css_class
FROM (
    SELECT * FROM Raw_Metrics
    UNION ALL
    SELECT 'COMBINED TOTAL', SUM(Medicare_Allowed), SUM(Beneficiaries), SUM(Services_Claims), 3 FROM Raw_Metrics
)
ORDER BY Sort_Order;


-- 1. SQL for Section 2.1: Code-Level Summary
-- This query calculates the totals, the average allowed amount per service, and includes the static 13-month cap values for comparison.

DROP VIEW IF EXISTS dme_code_summary;
CREATE VIEW dme_code_summary AS
WITH Metrics AS (
    SELECT 
        HCPCS_CD,
        CASE 
            WHEN HCPCS_CD = 'E0601' THEN 'CPAP — standard sleep apnea treatment'
            WHEN HCPCS_CD = 'E0470' THEN 'BiPAP without backup rate'
            WHEN HCPCS_CD = 'E0471' THEN 'BiPAP with backup rate'
        END AS Device,
        SUM(CAST(COALESCE(Tot_Suplr_Benes, 0) AS FLOAT)) AS Beneficiaries,
        SUM(CAST(Tot_Suplr_Srvcs AS FLOAT) * CAST(Avg_Suplr_Mdcr_Alowd_Amt AS FLOAT)) AS Total_Allowed,
        SUM(CAST(Tot_Suplr_Srvcs AS FLOAT)) AS Total_Services,
        CASE 
            WHEN HCPCS_CD = 'E0601' THEN 671
            WHEN HCPCS_CD = 'E0470' THEN 1655
            WHEN HCPCS_CD = 'E0471' THEN 4008
        END AS Thirteen_Month_Cap
    FROM uniform_resource_dme_cpap_e0601_e0470_e0471
    WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471')
      AND Suplr_Rentl_Ind = 'Y'
    GROUP BY HCPCS_CD
)
SELECT 
    HCPCS_CD AS Code,
    Device,
    Beneficiaries,
    Total_Allowed,
    (Total_Allowed / Total_Services) AS Avg_Per_Service,
    Thirteen_Month_Cap
FROM Metrics

UNION ALL

SELECT 
    'TOTAL' AS Code,
    'All three codes' AS Device,
    SUM(Beneficiaries),
    SUM(Total_Allowed),
    NULL AS Avg_Per_Service,
    NULL AS Thirteen_Month_Cap
FROM Metrics;


--  SQL for Section 2.2: Interaction Density
-- This query combines data from both the Diagnostic (Part B) and DME tables to calculate the **Density** (Claims per Beneficiary), which illustrates the "One-and-done" vs. "Monthly Rental" business models.


-- Fix Section 2.2: Interaction Density
DROP VIEW IF EXISTS dme_interaction_density;
CREATE VIEW dme_interaction_density AS
-- 1. Diagnostic Density (Part B)
SELECT 
    CASE 
        WHEN HCPCS_Cd IN ('95810', '95811') THEN 'Part B PSG (95810, 95811)'
        ELSE 'Part B Home tests (all HST)'
    END AS Data_Code,
    SUM(CAST(Tot_Srvcs AS FLOAT)) AS Claims,
    SUM(CAST(Tot_Benes AS FLOAT)) AS Beneficiaries,
    SUM(CAST(Tot_Srvcs AS FLOAT)) / SUM(CAST(Tot_Benes AS FLOAT)) AS Density,
    CASE 
        WHEN HCPCS_Cd IN ('95810', '95811') THEN 'One-and-done. Each patient = new acquisition'
        ELSE 'Same — volume acquisition game'
    END AS Revenue_Implication
FROM uniform_resource_cms_bygeography
WHERE Rndrng_Prvdr_Geo_Lvl = 'National'
  AND HCPCS_Cd IN ('95810', '95811', 'G0398', 'G0399', 'G0400', '95800', '95806')
GROUP BY 1, 5; -- Grouping by positional columns (Data_Code and Revenue_Implication)

-- Fix Section 4: Specialty Analysis
DROP VIEW IF EXISTS referring_specialty_analysis;
CREATE VIEW referring_specialty_analysis AS
WITH Specialty_Totals AS (
    SELECT 
        Rfrg_Prvdr_Spclty_Desc AS Specialty,
        COUNT(DISTINCT Rfrg_NPI) AS Providers,
        SUM(CAST(Tot_Suplr_Srvcs AS FLOAT)) AS Services,
        SUM(CAST(Tot_Suplr_Srvcs AS FLOAT) * CAST(Avg_Suplr_Mdcr_Alowd_Amt AS FLOAT)) AS Allowed_Amount
    FROM uniform_resource_dme_cpap_e0601_e0470_e0471
    WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471')
    GROUP BY 1
),
Market_Total AS (
    SELECT SUM(Allowed_Amount) AS Grand_Total FROM Specialty_Totals
)
SELECT 
    s.Specialty,
    s.Providers,
    s.Services,
    (s.Allowed_Amount / m.Grand_Total) * 100 AS Pct_Allowed,
    CASE 
        WHEN s.Specialty = 'Pulmonary Disease' THEN 'B2B embed — voice screen in pulmonology workflow'
        WHEN s.Specialty = 'Nurse Practitioner' THEN 'DTC allies — NPs prescribe Zepbound directly'
        WHEN s.Specialty = 'Family Practice' THEN 'Highest provider count — referral target'
        WHEN s.Specialty = 'Internal Medicine' THEN 'Embed screening tool in EHR workflow'
        WHEN s.Specialty = 'Sleep Medicine' THEN 'Partial competitor — manage carefully'
        WHEN s.Specialty = 'Physician Assistant' THEN 'DTC allies — can prescribe Zepbound'
        WHEN s.Specialty = 'Neurology' THEN 'Parkinson''s overlap — secondary partnership'
        WHEN s.Specialty = 'Otolaryngology' THEN 'MAD pathway partners for non-Zepbound patients'
        ELSE 'General referral network'
    END AS Voxia_Strategy
FROM Specialty_Totals s, Market_Total m
ORDER BY s.Allowed_Amount DESC;


--  Section 5: Geographic Access (Urban vs. Rural)
-- This query utilizes the `Rfrg_Prvdr_RUCA_Cat` column to identify access disparities.

DROP VIEW IF EXISTS geographic_access_analysis;
CREATE VIEW geographic_access_analysis AS
WITH Geo_Metrics AS (
    SELECT 
        COALESCE(Rfrg_Prvdr_RUCA_Cat, 'Unknown') AS RUCA_Category,
        COUNT(DISTINCT Rfrg_NPI) AS Providers,
        SUM(CAST(Tot_Suplr_Srvcs AS FLOAT)) AS Services,
        SUM(CAST(Tot_Suplr_Srvcs AS FLOAT) * CAST(Avg_Suplr_Mdcr_Alowd_Amt AS FLOAT)) AS Total_Allowed
    FROM uniform_resource_dme_cpap_e0601_e0470_e0471
    WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471')
    GROUP BY 1
),
Market_Total AS (
    SELECT SUM(Total_Allowed) AS Grand_Total FROM Geo_Metrics
)
SELECT 
    RUCA_Category,
    Providers,
    Services,
    Total_Allowed,
    (Total_Allowed / Grand_Total) * 100 AS Pct_Of_Market,
    (Total_Allowed / Services) AS Avg_Allowed_Per_Service
FROM Geo_Metrics, Market_Total;


-- Section 6: Complete Evidence Summary
-- This view acts as the "Master Dashboard" by aggregating metrics from both the Diagnostic and DME tables into a single summary table.


DROP VIEW IF EXISTS complete_evidence_summary;
CREATE VIEW complete_evidence_summary AS
-- 1. Combined Market
SELECT 1 AS ID, 'Combined Medicare market' AS Metric, 
       PRINTF('$%,.1fM annually', (
           (SELECT SUM(CAST(Tot_Srvcs AS FLOAT) * CAST(Avg_Mdcr_Alowd_Amt AS FLOAT)) FROM uniform_resource_cms_bygeography WHERE Rndrng_Prvdr_Geo_Lvl = 'National' AND HCPCS_Cd IN ('95810', '95811', 'G0398', 'G0399', 'G0400', '95800', '95806')) +
           (SELECT SUM(CAST(Tot_Suplr_Srvcs AS FLOAT) * CAST(Avg_Suplr_Mdcr_Alowd_Amt AS FLOAT)) FROM uniform_resource_dme_cpap_e0601_e0470_e0471 WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471'))
       ) / 1000000.0) AS CMS_Value,
       'All-payer est. $1.5–2.5B' AS Voxia_Implication

UNION ALL
-- 2. CPAP Cap
SELECT 2, 'CPAP rental revenue ceiling', '$671 lifetime/patient (13 months)', 'Zepbound: $3,588–$5,388/yr — 5.3–8.0×'

UNION ALL
-- 4. DME Markup (System Friction)
SELECT 4, 'DME markup ratio', 
       PRINTF('%.1fx ($%,.2fB submitted)', 
       SUM(CAST(Tot_Suplr_Srvcs AS FLOAT) * CAST(Avg_Suplr_Sbmtd_Chrg AS FLOAT)) / SUM(CAST(Tot_Suplr_Srvcs AS FLOAT) * CAST(Avg_Suplr_Mdcr_Alowd_Amt AS FLOAT)),
       SUM(CAST(Tot_Suplr_Srvcs AS FLOAT) * CAST(Avg_Suplr_Sbmtd_Chrg AS FLOAT)) / 1000000000.0),
       'Voxia''s $99 flat price eliminates this layer'
FROM uniform_resource_dme_cpap_e0601_e0470_e0471
WHERE HCPCS_CD IN ('E0601', 'E0470', 'E0471')

UNION ALL
-- 6. NP Referrals
SELECT 6, 'NPs = 20.2% of CPAP referrals', 
       (SELECT PRINTF('%,.0f NPs across 46,316 providers', COUNT(DISTINCT Rfrg_NPI)) FROM uniform_resource_dme_cpap_e0601_e0470_e0471 WHERE Rfrg_Prvdr_Spclty_Desc = 'Nurse Practitioner'),
       'NPs prescribe Zepbound — #1 VI partnership target'

UNION ALL
-- 7. Rural Premium
SELECT 7, 'Rural CPAP premium', 
       '$65.27 rural vs $46.63 urban (+40%)', 
       'Voxia''s uniform $99 more accessible than rural options';


