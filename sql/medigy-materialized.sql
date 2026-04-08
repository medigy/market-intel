
-- =============================================================================
-- SECTION 4 — MATERIALIZED ANALYTICS TABLES
-- 
-- WHY: Views like executive_kpis, condition_national_summary, and
--      opportunity_score perform multi-million-row aggregations on every page
--      load. Materializing them as tables with indexes reduces query time from
--      seconds to milliseconds after pipeline runs.
--
-- HOW TO REFRESH: Re-run medigy-unified-v2.sql (pipeline drops/recreates these).
--      The DROP + CREATE TABLE AS pattern below is idempotent and safe to rerun.
-- =============================================================================


-- ── mat_executive_kpis ────────────────────────────────────────────────────────
-- Replaces: executive_kpis (view)
-- Used by: executive-dashboard.sql, home-overview.sql
-- Refresh: Drops and recreates from fact_utilization_unified on every pipeline run.

DROP TABLE IF EXISTS mat_executive_kpis;
CREATE TABLE mat_executive_kpis AS
SELECT
    COUNT(DISTINCT f.condition_name)    AS total_conditions,
    COUNT(DISTINCT f.state_abbr)        AS total_states,
    COUNT(DISTINCT f.hcpcs_code)        AS total_procedures,
    SUM(f.total_beneficiaries)          AS total_beneficiaries,
    SUM(f.total_allowed_amt)            AS total_allowed_amt,
    SUM(f.total_medicare_payment)       AS total_medicare_payment,
    COUNT(DISTINCT f.source_type)       AS active_data_sources
FROM fact_utilization_unified f;

-- Single-row table, no index needed. Access via: SELECT * FROM mat_executive_kpis;


-- ── mat_opportunity_score ─────────────────────────────────────────────────────
-- Replaces: opportunity_score (view)
-- Used by: opportunity-scoring.sql, condition_national_summary
-- Score = normalized(beneficiaries) × 0.4 + normalized(allowed_amt) × 0.4 + tier weight × 0.2

DROP TABLE IF EXISTS mat_opportunity_score;
CREATE TABLE mat_opportunity_score AS
WITH base AS (
    SELECT
        condition_name, specialty_domain, tier, b2b_tier_primary, icon, color,
        SUM(total_beneficiaries) AS total_benes,
        SUM(total_allowed_amt)   AS total_allowed
    FROM fact_utilization_unified
    GROUP BY 1, 2, 3, 4, 5, 6
),
maxvals AS (
    SELECT MAX(total_benes) AS max_benes, MAX(total_allowed) AS max_allowed
    FROM base
)
SELECT
    b.condition_name, b.specialty_domain, b.tier, b.b2b_tier_primary, b.icon, b.color,
    b.total_benes, b.total_allowed,
    ROUND(
        (CAST(b.total_benes   AS REAL) / NULLIF(m.max_benes,   0)) * 40
      + (CAST(b.total_allowed AS REAL) / NULLIF(m.max_allowed, 0)) * 40
      + (4 - b.tier) * 20
    , 1) AS opportunity_score
FROM base b, maxvals m
ORDER BY opportunity_score DESC;

CREATE INDEX IF NOT EXISTS idx_mat_opp_cond  ON mat_opportunity_score (condition_name);
CREATE INDEX IF NOT EXISTS idx_mat_opp_score ON mat_opportunity_score (opportunity_score DESC);
CREATE INDEX IF NOT EXISTS idx_mat_opp_tier  ON mat_opportunity_score (tier);


-- ── mat_condition_national_summary ────────────────────────────────────────────
-- Replaces: condition_national_summary (view)
-- Used by: home-overview.sql, executive-dashboard.sql, conditions.sql,
--          condition-hub.sql, procedure-drilldown.sql
-- This is the MOST QUERIED aggregate — materializing gives the biggest speedup.

DROP TABLE IF EXISTS mat_condition_national_summary;
CREATE TABLE mat_condition_national_summary AS
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
    ROUND(SUM(f.total_allowed_amt) / NULLIF(SUM(f.total_beneficiaries), 0), 2) AS allowed_per_patient,
    ROUND(SUM(f.total_services)    / NULLIF(SUM(f.total_beneficiaries), 0), 2) AS services_per_patient
FROM fact_utilization_unified f
LEFT JOIN mat_opportunity_score os 
    ON TRIM(LOWER(f.condition_name)) = TRIM(LOWER(os.condition_name))
GROUP BY 1, 2, 3, 4, 5, 6, 7;

CREATE INDEX IF NOT EXISTS idx_mat_cns_cond  ON mat_condition_national_summary (condition_name);
CREATE INDEX IF NOT EXISTS idx_mat_cns_tier  ON mat_condition_national_summary (tier);
CREATE INDEX IF NOT EXISTS idx_mat_cns_allow ON mat_condition_national_summary (total_allowed_amt DESC);
CREATE INDEX IF NOT EXISTS idx_mat_cns_benes ON mat_condition_national_summary (total_beneficiaries DESC);


-- ── mat_condition_source_breakdown ───────────────────────────────────────────
-- Replaces: condition_source_breakdown (view)
-- Used by: condition-hub.sql donut chart

DROP TABLE IF EXISTS mat_condition_source_breakdown;
CREATE TABLE mat_condition_source_breakdown AS
SELECT
    f.condition_name,
    f.source_type,
    SUM(f.total_beneficiaries)    AS total_beneficiaries,
    SUM(f.total_services)         AS total_services,
    SUM(f.total_allowed_amt)      AS total_allowed_amt,
    SUM(f.total_medicare_payment) AS total_medicare_payment
FROM fact_utilization_unified f
GROUP BY 1, 2;

CREATE INDEX IF NOT EXISTS idx_mat_csb_cond ON mat_condition_source_breakdown (condition_name);
CREATE INDEX IF NOT EXISTS idx_mat_csb_src  ON mat_condition_source_breakdown (source_type);


-- ── mat_condition_hcpcs_detail ────────────────────────────────────────────────
-- Replaces: condition_hcpcs_detail (view)
-- Used by: condition-hub.sql procedure table, procedure-drilldown.sql
-- This view has the most OFFSET-heavy pagination queries — materialization helps greatly.

DROP TABLE IF EXISTS mat_condition_hcpcs_detail;
CREATE TABLE mat_condition_hcpcs_detail AS
SELECT
    f.condition_name,
    f.hcpcs_code,
    COALESCE(p.procedure_description, 'Inpatient DRG ' || f.hcpcs_code) AS procedure_description,
    COALESCE(p.procedure_category, 'Hospital Inpatient')                 AS procedure_category,
    f.source_type,
    SUM(f.total_beneficiaries) AS total_beneficiaries,
    SUM(f.total_services)      AS total_services,
    SUM(f.total_allowed_amt)   AS total_allowed_amt,
    ROUND(SUM(f.total_allowed_amt) / NULLIF(SUM(f.total_services), 0), 2) AS avg_allowed_per_service
FROM fact_utilization_unified f
LEFT JOIN dim_procedure p ON f.hcpcs_code = p.hcpcs_code
GROUP BY 1, 2, 3, 4, 5;

CREATE INDEX IF NOT EXISTS idx_mat_chd_cond  ON mat_condition_hcpcs_detail (condition_name);
CREATE INDEX IF NOT EXISTS idx_mat_chd_hcpcs ON mat_condition_hcpcs_detail (hcpcs_code);
CREATE INDEX IF NOT EXISTS idx_mat_chd_allow ON mat_condition_hcpcs_detail (total_allowed_amt DESC);
CREATE INDEX IF NOT EXISTS idx_mat_chd_src   ON mat_condition_hcpcs_detail (source_type);
-- Composite for paginated condition-filtered queries
CREATE INDEX IF NOT EXISTS idx_mat_chd_cond_allow 
    ON mat_condition_hcpcs_detail (condition_name, total_allowed_amt DESC);


-- ── mat_condition_state_breakdown ─────────────────────────────────────────────
-- Replaces: condition_state_breakdown (view)
-- Used by: condition-hub.sql geo table, geography.sql

DROP TABLE IF EXISTS mat_condition_state_breakdown;
CREATE TABLE mat_condition_state_breakdown AS
SELECT
    f.condition_name,
    f.state_abbr,
    g.locality_name,
    g.cost_tier,
    g.pw_gpci,
    SUM(f.total_beneficiaries)    AS total_beneficiaries,
    SUM(f.total_services)         AS total_services,
    SUM(f.total_allowed_amt)      AS total_allowed_amt,
    SUM(f.total_medicare_payment) AS total_medicare_payment,
    ROUND(SUM(f.total_allowed_amt) / NULLIF(SUM(f.total_beneficiaries), 0), 2) AS allowed_per_patient
FROM fact_utilization_unified f
LEFT JOIN dim_geography g ON TRIM(UPPER(f.state_abbr)) = TRIM(UPPER(g.state_abbr))
GROUP BY 1, 2, 3, 4, 5;

CREATE INDEX IF NOT EXISTS idx_mat_csb2_cond  ON mat_condition_state_breakdown (condition_name);
CREATE INDEX IF NOT EXISTS idx_mat_csb2_state ON mat_condition_state_breakdown (state_abbr);
CREATE INDEX IF NOT EXISTS idx_mat_csb2_allow ON mat_condition_state_breakdown (total_allowed_amt DESC);
-- Composite for condition+state pivot queries
CREATE INDEX IF NOT EXISTS idx_mat_csb2_cond_state 
    ON mat_condition_state_breakdown (condition_name, state_abbr);


-- ── mat_top_states_by_condition ───────────────────────────────────────────────
-- Replaces: top_states_by_condition (view)
-- Pre-ranked so the window function runs once at pipeline time, not on every request.

DROP TABLE IF EXISTS mat_top_states_by_condition;
CREATE TABLE mat_top_states_by_condition AS
SELECT
    condition_name, state_abbr,
    total_beneficiaries, total_allowed_amt, total_medicare_payment,
    allowed_per_patient,
    ROW_NUMBER() OVER (PARTITION BY condition_name ORDER BY total_allowed_amt DESC) AS state_rank
FROM mat_condition_state_breakdown;

CREATE INDEX IF NOT EXISTS idx_mat_tsbc_cond ON mat_top_states_by_condition (condition_name);
CREATE INDEX IF NOT EXISTS idx_mat_tsbc_rank ON mat_top_states_by_condition (condition_name, state_rank);


-- =============================================================================
-- SECTION 5 — COMPATIBILITY VIEWS
-- These keep the original view names working so existing SQL pages need
-- minimal changes. They simply SELECT from the new materialized tables.
-- =============================================================================

DROP VIEW IF EXISTS executive_kpis;
CREATE VIEW executive_kpis AS SELECT * FROM mat_executive_kpis;

DROP VIEW IF EXISTS opportunity_score;
CREATE VIEW opportunity_score AS SELECT * FROM mat_opportunity_score;

DROP VIEW IF EXISTS condition_national_summary;
CREATE VIEW condition_national_summary AS SELECT * FROM mat_condition_national_summary;

DROP VIEW IF EXISTS condition_source_breakdown;
CREATE VIEW condition_source_breakdown AS SELECT * FROM mat_condition_source_breakdown;

DROP VIEW IF EXISTS condition_hcpcs_detail;
CREATE VIEW condition_hcpcs_detail AS SELECT * FROM mat_condition_hcpcs_detail;

DROP VIEW IF EXISTS condition_state_breakdown;
CREATE VIEW condition_state_breakdown AS SELECT * FROM mat_condition_state_breakdown;

DROP VIEW IF EXISTS top_states_by_condition;
CREATE VIEW top_states_by_condition AS SELECT * FROM mat_top_states_by_condition;