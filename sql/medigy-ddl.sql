-- =============================================================================
-- Medigy Opportunity Atlas — DDL v3 (Enhanced)
-- Adds:
--   1. data_provenance table (unchanged)
--   2. data_tables_derived view (unchanged)
--   3. data_dictionary_indexes view (unchanged)
--   4. NEW: Materialized analytics tables replacing slow views
--      - mat_executive_kpis           (replaces executive_kpis view)
--      - mat_opportunity_score        (replaces opportunity_score view)
--      - mat_condition_national_summary (replaces condition_national_summary view)
--      - mat_condition_source_breakdown (replaces condition_source_breakdown view)
--   5. NEW: Performance indexes on all materialized tables
-- =============================================================================


-- =============================================================================
-- SECTION 1 — DATA PROVENANCE (unchanged)
-- =============================================================================

-- Create the provenance table with a unique constraint
CREATE TABLE IF NOT EXISTS data_provenance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    link TEXT NOT NULL,
    version_year INTEGER,
    ingested_at DATETIME,
    object_type TEXT NOT NULL, -- 'external_source'
    description TEXT,
    UNIQUE(title, link, version_year)
);

-- Using INSERT OR IGNORE: Only adds if the record doesn't exist.
-- If it exists, it does nothing (preserving the original ingested_at).

INSERT OR IGNORE INTO data_provenance (
    title, 
    link, 
    version_year, 
    ingested_at, 
    object_type, 
    description
)
VALUES (
    'Medicare Physician & Other Practitioners - by Provider',
    'https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider',
    2023,
    '2026-03-24 09:00:00', -- Formatted for SQLite compatibility
    'external_source',
    'Primary source for provider-level utilization and payment metrics.'
);

-- Seeding the other two resources with the same date
INSERT OR IGNORE INTO data_provenance (title, link, version_year, ingested_at, object_type, description)
VALUES (
    'Medicare Physician & Other Practitioners - by Geography and Service',
    'https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-geography-and-service',
    2023,
    '2026-03-24 09:00:00',
    'external_source',
    'Aggregated metrics by State and HCPCS code for market sizing.'
);

INSERT OR IGNORE INTO data_provenance (title, link, version_year, ingested_at, object_type, description)
VALUES (
    'Medicare Durable Medical Equipment, Devices & Supplies',
    'https://data.cms.gov/provider-summary-by-type-of-service/medicare-durable-medical-equipment-devices-supplies/medicare-durable-medical-equipment-devices-supplies-by-referring-provider-and-service',
    2023,
    '2026-03-24 09:00:00',
    'external_source',
    'DME and device referral data by provider and service.'
);
INSERT OR IGNORE INTO data_provenance (title, link, version_year, ingested_at, object_type, description)
VALUES (
    'Medicare Durable Medical Equipment, Devices & Supplies - by Geography and Service',
    'https://data.cms.gov/provider-summary-by-type-of-service/medicare-durable-medical-equipment-devices-supplies/medicare-durable-medical-equipment-devices-supplies-by-geography-and-service',
    2023,
    '2026-03-31 16:00:00',
    'external_source',
    'DMEPOS non-institutional claims aggregated by State and HCPCS, including rental indicators.'
);
-- Source 5: Medicare Outpatient Hospitals
INSERT OR IGNORE INTO data_provenance (title, link, version_year, ingested_at, object_type, description)
VALUES (
    'Medicare Outpatient Hospitals - by Provider and Service',
    'https://data.cms.gov/provider-summary-by-type-of-service/medicare-outpatient-hospitals/medicare-outpatient-hospitals-by-provider-and-service',
    2023,
    '2026-04-01 10:00:00',
    'external_source',
    'Institutional outpatient claims aggregated by Provider and APC code.'
);

-- Source 6: Medicare Diagnostics Data
INSERT OR IGNORE INTO data_provenance (title, link, version_year, ingested_at, object_type, description)
VALUES (
    'Medicare Physician & Other Practitioners - Diagnostics Data',
    'https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-geography-and-service',
    2023,
    '2026-04-01 10:00:00',
    'external_source',
    'Specialized diagnostics utilization and payment metrics by HCPCS.'
);

-- Source 7: Medicare Inpatient Hospitals
INSERT OR IGNORE INTO data_provenance (title, link, version_year, ingested_at, object_type, description)
VALUES (
    'Medicare Inpatient Hospitals - by Provider and Service',
    'https://data.cms.gov/provider-summary-by-type-of-service/medicare-inpatient-hospitals/medicare-inpatient-hospitals-by-provider-and-service',
    2023,
    '2026-04-01 10:00:00',
    'external_source',
    'Institutional inpatient claims aggregated by Provider and MS-DRG code.'
);



-- =============================================================================
-- SECTION 2 — DATA TABLES DERIVED VIEW 
-- =============================================================================

CREATE VIEW IF NOT EXISTS data_tables_derived AS
SELECT 
    s.name AS object_name,
    s.type AS object_type,
    CASE 
        WHEN s.name LIKE 'dim_%' THEN 'Dimensional Table'
        WHEN s.name LIKE 'uniform_resource_ref_%' THEN 'Master Reference'
        WHEN s.name LIKE 'fact_%' THEN 'Core Fact'
        WHEN s.name LIKE 'mat_%' THEN 'Materialized Table'
        WHEN s.type = 'view' THEN 'Analytical View'
        ELSE 'Derived Table'
    END AS category
FROM sqlite_schema s
WHERE s.type IN ('table', 'view')
  AND s.name NOT LIKE 'sqlite_%'
  AND s.name NOT LIKE 'uniform_resource_transform%'
  AND s.name NOT LIKE 'uniform_resource_edge%'
  AND s.name NOT LIKE 'orchestration_%'
  AND s.name NOT LIKE 'device_%'
  AND s.name NOT LIKE 'navigation_%'
  AND s.name NOT LIKE 'rssd_%'
  AND s.name NOT LIKE 'snmp_%'
  AND s.name NOT LIKE 'code_notebook_%'
  AND s.name NOT LIKE 'console_%'
  AND s.name NOT LIKE 'surveilr_%'
  AND s.name NOT LIKE 'ur_ingest_%'
  AND s.name NOT IN (
    'sqlean_define', 'assurance_schema', 'behavior', 'device', 
    'party_type', 'party', 'gender_type', 'organization', 
    'organization_role_type', 'organization_role', 'osquery_policy', 
    'party_relation_type', 'party_relation', 'person_type', 
    'sex_type', 'person', 'sqlpage_aide_navigation', 'sqlpage_files', 
    'uniform_resource', 'uniform_resource_graph', 'session_state_ephemeral',
    'email_messages_with_timezone', 'filesystem_graph', 'imap_graph','uniform_resource_file','uniform_resource_imap','uniform_resource_imap_content'  );



-- =============================================================================
-- SECTION 3 — DATA DICTIONARY INDEXES VIEW 
-- =============================================================================

DROP VIEW IF EXISTS data_dictionary_indexes;
CREATE VIEW data_dictionary_indexes AS
SELECT 
    name AS index_name,
    tbl_name AS table_name,
    'Index on ' || tbl_name AS description
FROM sqlite_schema 
WHERE type = 'index' 
  AND name NOT LIKE 'sqlite_%'
  -- Exclude Pipeline/Transform & System Noise
  AND tbl_name NOT LIKE 'uniform_resource_transform%'
  AND tbl_name NOT LIKE 'uniform_resource_edge%'
  AND tbl_name NOT LIKE 'surveilr_%'
  -- Exclude Ingestion & Session Tables
  AND tbl_name NOT LIKE 'ur_ingest_session%'
  AND tbl_name NOT LIKE 'uniform_resource%'
  -- Exclude Orchestration, Identity, & Role Noise
  AND tbl_name NOT LIKE 'device%'
  AND tbl_name NOT LIKE 'party%'
  AND tbl_name NOT LIKE 'orchestration_%'
  AND tbl_name NOT LIKE 'organization_%'
  -- Exclude Navigation & UI System tables
  AND tbl_name NOT LIKE 'navigation_%'
  AND tbl_name NOT LIKE 'code_notebook_%' 
  -- Exact name exclusions
  AND tbl_name NOT IN (
    'behavior',
    'sex_type',
    'person',
    'person_type',
    'gender_type',
    'ur_ingest_resource_path_match_rule','sqlpage_aide_navigation','ur_ingest_resource_path_rewrite_rule','osquery_policy'
  )
ORDER BY tbl_name ASC;


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