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