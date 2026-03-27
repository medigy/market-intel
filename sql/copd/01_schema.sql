-- =============================================================================
-- VOXIA COPD ANALYTICS
-- Script 01: Schema Creation
-- Database: SQLite
-- Covers: COPD PFT Diagnostics | E&M Visits | Oxygen DME
-- =============================================================================

-- -----------------------------------------------------------------------------
-- TABLE 1: COPD PFT DIAGNOSTICS
-- Source: CMS Geographic Variation Public Use File
-- CPT Codes: 94010, 94060, 94726, 94729
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS copd_pft (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    geo_level           TEXT    NOT NULL,   -- 'National' or 'State'
    geo_code            TEXT,
    geo_desc            TEXT    NOT NULL,   -- State name or 'National'
    hcpcs_cd            TEXT    NOT NULL,   -- 94010 | 94060 | 94726 | 94729
    hcpcs_desc          TEXT    NOT NULL,
    place_of_srvc       TEXT    NOT NULL,   -- 'F' = Facility | 'O' = Office
    tot_rndrng_prvdrs   INTEGER,
    tot_benes           INTEGER,
    tot_srvcs           INTEGER,
    tot_bene_day_srvcs  INTEGER,
    avg_sbmtd_chrg      REAL,
    avg_mdcr_alowd_amt  REAL,
    avg_mdcr_pymt_amt   REAL,
    avg_mdcr_stdzd_amt  REAL
);

CREATE INDEX IF NOT EXISTS idx_pft_geo_level  ON copd_pft (geo_level);
CREATE INDEX IF NOT EXISTS idx_pft_geo_desc   ON copd_pft (geo_desc);
CREATE INDEX IF NOT EXISTS idx_pft_hcpcs      ON copd_pft (hcpcs_cd);
CREATE INDEX IF NOT EXISTS idx_pft_place      ON copd_pft (place_of_srvc);


-- -----------------------------------------------------------------------------
-- TABLE 2: COPD E&M VISITS
-- Source: CMS Geographic Variation Public Use File
-- CPT Codes: 99213, 99214
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS copd_em (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    geo_level           TEXT    NOT NULL,
    geo_code            TEXT,
    geo_desc            TEXT    NOT NULL,
    hcpcs_cd            TEXT    NOT NULL,   -- 99213 | 99214
    hcpcs_desc          TEXT    NOT NULL,
    place_of_srvc       TEXT    NOT NULL,
    tot_rndrng_prvdrs   INTEGER,
    tot_benes           INTEGER,
    tot_srvcs           INTEGER,
    tot_bene_day_srvcs  INTEGER,
    avg_sbmtd_chrg      REAL,
    avg_mdcr_alowd_amt  REAL,
    avg_mdcr_pymt_amt   REAL,
    avg_mdcr_stdzd_amt  REAL
);

CREATE INDEX IF NOT EXISTS idx_em_geo_level   ON copd_em (geo_level);
CREATE INDEX IF NOT EXISTS idx_em_geo_desc    ON copd_em (geo_desc);
CREATE INDEX IF NOT EXISTS idx_em_hcpcs       ON copd_em (hcpcs_cd);


-- -----------------------------------------------------------------------------
-- TABLE 3: OXYGEN DME
-- Source: CMS Referring Provider DMEPOS Public Use File
-- HCPCS Codes: E0434 (portable liquid oxygen), E1392 (portable concentrator)
-- Note: tot_suplr_benes is NULL for rows with <11 beneficiaries (CMS suppression)
--       tot_suplr_srvcs = rental MONTHS billed (not individual procedures)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS copd_oxygen (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    rfrg_npi                 TEXT    NOT NULL,
    prvdr_last_name          TEXT,
    prvdr_first_name         TEXT,
    prvdr_credentials        TEXT,
    prvdr_state              TEXT    NOT NULL,   -- 2-letter state abbreviation
    prvdr_state_fips         TEXT,
    prvdr_zip5               TEXT,
    ruca_cat                 TEXT,               -- 'Urban' | 'Rural' | 'Unknown'
    ruca_code                TEXT,
    ruca_desc                TEXT,
    country                  TEXT,
    specialty_cd             TEXT,
    specialty_desc           TEXT,
    rbcs_lvl                 TEXT,
    rbcs_id                  TEXT,
    rbcs_desc                TEXT,
    hcpcs_cd                 TEXT    NOT NULL,   -- E0434 | E1392
    hcpcs_desc               TEXT    NOT NULL,
    suplr_rentl_ind          TEXT,               -- 'Y' = rental
    tot_suplrs               INTEGER,
    tot_suplr_benes          INTEGER,            -- NULL when <11 (suppressed)
    tot_suplr_clms           INTEGER,
    tot_suplr_srvcs          INTEGER,            -- rental months billed
    avg_suplr_sbmtd_chrg     REAL,
    avg_suplr_mdcr_alowd_amt REAL,
    avg_suplr_mdcr_pymt_amt  REAL,
    avg_suplr_mdcr_stdzd_amt REAL
);

CREATE INDEX IF NOT EXISTS idx_o2_npi         ON copd_oxygen (rfrg_npi);
CREATE INDEX IF NOT EXISTS idx_o2_state       ON copd_oxygen (prvdr_state);
CREATE INDEX IF NOT EXISTS idx_o2_hcpcs       ON copd_oxygen (hcpcs_cd);
CREATE INDEX IF NOT EXISTS idx_o2_ruca        ON copd_oxygen (ruca_cat);
CREATE INDEX IF NOT EXISTS idx_o2_specialty   ON copd_oxygen (specialty_desc);
