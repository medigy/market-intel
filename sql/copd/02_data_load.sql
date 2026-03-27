-- =============================================================================
-- VOXIA COPD ANALYTICS
-- Script 02: Data Load
-- Database: SQLite
-- =============================================================================
-- SQLite does not support LOAD DATA INFILE. Use one of these approaches:
--
-- OPTION A — sqlite3 CLI (recommended for large files):
--   sqlite3 voxia_copd.db
--   .mode csv
--   .headers on
--   .import cms_bygeography_copd_core_diagnosis.csv uniform_resource_cms_bygeography_copd_core_diagnosis
--   .import cms_bygeography_copd_visits.csv         uniform_resource_cms_bygeography_copd_visits
--   .import E0434_E1392_Extract.csv                 uniform_resource_copd_oxygen
--
-- OPTION B — Python (use if you need column mapping / type coercion):
--   See comments at end of this file for the Python snippet.
--
-- OPTION C — DB Browser for SQLite:
--   File > Import > Table from CSV file
--
-- This script assumes raw import tables exist and maps them into the
-- clean schema tables defined in 01_schema.sql.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- STEP 1: Migrate raw -> clean tables with type casting
-- -----------------------------------------------------------------------------

-- PFT: filter only the 4 COPD CPT codes and cast types
INSERT INTO copd_pft (
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
FROM uniform_resource_cms_bygeography_copd_core_diagnosis
WHERE TRIM(HCPCS_Cd) IN ('94010','94060','94726','94729');


-- E&M: filter only 99213 and 99214
INSERT INTO copd_em (
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
FROM uniform_resource_cms_bygeography_copd_visits
WHERE TRIM(HCPCS_Cd) IN ('99213','99214');


-- Oxygen DME: filter E0434 and E1392 only
INSERT INTO copd_oxygen (
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



-- -----------------------------------------------------------------------------
-- STEP 2: Verify row counts after load
-- -----------------------------------------------------------------------------
SELECT 'copd_pft'    AS tbl, COUNT(*) AS rows FROM copd_pft
UNION ALL
SELECT 'copd_em'     AS tbl, COUNT(*) AS rows FROM copd_em
UNION ALL
SELECT 'copd_oxygen' AS tbl, COUNT(*) AS rows FROM copd_oxygen;

-- Expected approximate counts:
--   copd_pft    ~429 rows  (8 national + ~421 state rows)
--   copd_em     ~242 rows  (4 national + ~238 state rows)
--   copd_oxygen ~40,371 rows (provider-level DMEPOS)


-- =============================================================================
-- PYTHON IMPORT ALTERNATIVE (for large CSVs with BOM / encoding issues)
-- Save as load_data.py and run: python3 load_data.py
-- =============================================================================
/*
import pandas as pd, sqlite3

DB = 'voxia_copd.db'
conn = sqlite3.connect(DB)

files = {
    'uniform_resource_cms_bygeography_copd_core_diagnosis':    'cms_bygeography_copd_core_diagnosis.csv',
    'uniform_resource_cms_bygeography_copd_visits':     'cms_bygeography_copd_visits.csv',
    'uniform_resource_copd_oxygen': 'E0434_E1392_Extract.csv',
}

for table, path in files.items():
    df = pd.read_csv(path, encoding='utf-8-sig', low_memory=False, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    df.to_sql(table, conn, if_exists='replace', index=False)
    print(f'Loaded {len(df):,} rows -> {table}')

conn.close()
*/
