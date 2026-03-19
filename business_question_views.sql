DROP VIEW IF EXISTS opportunity_scoring_view;
DROP VIEW IF EXISTS economic_intensity_index;
DROP VIEW IF EXISTS proxy_condition_activity;
DROP VIEW IF EXISTS condition_to_icd_mapping;
DROP VIEW IF EXISTS top_procedures_per_specialty;
DROP VIEW IF EXISTS procedure_volume_by_specialty;
DROP VIEW IF EXISTS procedure_specialty_proxy_map;

CREATE VIEW procedure_specialty_proxy_map AS
WITH code_reference AS (
    SELECT
        c.HCPCS_Cd AS hcpcs_code,
        MIN(c.HCPCS_Desc) AS procedure_description,
        CASE WHEN c.HCPCS_Cd GLOB '[0-9][0-9][0-9][0-9][0-9]' THEN CAST(c.HCPCS_Cd AS INTEGER) END AS code_num,
        lower(MIN(COALESCE(c.HCPCS_Desc, ''))) AS desc_lc
    FROM uniform_resource_cms_bygeography c
    WHERE c.Rndrng_Prvdr_Geo_Lvl = 'National'
    GROUP BY c.HCPCS_Cd
)
SELECT
    hcpcs_code,
    procedure_description,
    CASE
        WHEN code_num BETWEEN 100 AND 1999 OR desc_lc LIKE 'anes %' THEN 'Anesthesiology'
        WHEN desc_lc LIKE '%ambulance%' OR desc_lc LIKE '%transport%' OR desc_lc LIKE '%travel allowance%' OR hcpcs_code LIKE 'A04%' THEN 'Emergency Transport / Logistics'
        WHEN hcpcs_code IN ('90480', 'G0008', 'G0009') OR hcpcs_code LIKE '906%' OR hcpcs_code LIKE '913%' OR desc_lc LIKE '%vaccine%' OR desc_lc LIKE '%immunization%' THEN 'Preventive / Vaccination'
        WHEN code_num BETWEEN 90785 AND 90899 OR desc_lc LIKE '%psychotherapy%' OR desc_lc LIKE '%psychiatric%' OR desc_lc LIKE '%depression screening%' OR desc_lc LIKE '%stimulate nerve cells in brain%' THEN 'Behavioral Health'
        WHEN code_num BETWEEN 95004 AND 95199 OR desc_lc LIKE '%allergy%' OR desc_lc LIKE '%allergenic extract%' OR desc_lc LIKE '%antigen%' THEN 'Allergy / Immunology'
        WHEN code_num BETWEEN 98940 AND 98942 OR desc_lc LIKE '%chiropractic%' THEN 'Chiropractic / Manual Therapy'
        WHEN code_num BETWEEN 92002 AND 92499 OR desc_lc LIKE '%ophthalm%' OR desc_lc LIKE '%retina%' OR desc_lc LIKE '%glaucoma%' OR desc_lc LIKE '%cataract%' OR desc_lc LIKE '%intravitreal%' OR desc_lc LIKE '%eye %' THEN 'Ophthalmology'
        WHEN desc_lc LIKE '%cardiac%' OR desc_lc LIKE '%cardio%' OR desc_lc LIKE '%heart%' OR desc_lc LIKE '%vascular%' OR desc_lc LIKE '%coronary%' OR desc_lc LIKE '%artery%' OR desc_lc LIKE '%arrhythm%' OR desc_lc LIKE '%electrocardio%' OR desc_lc LIKE '%echocardio%' OR desc_lc LIKE '%pacemaker%' OR desc_lc LIKE '%anticoagulant%' THEN 'Cardiology / Vascular'
        WHEN desc_lc LIKE '%renal%' OR desc_lc LIKE '%kidney%' OR desc_lc LIKE '%dialysis%' OR desc_lc LIKE '%nephro%' OR desc_lc LIKE '%urolog%' OR desc_lc LIKE '%urinary%' OR desc_lc LIKE '%bladder%' OR desc_lc LIKE '%prostate%' THEN 'Nephrology / Urology'
        WHEN desc_lc LIKE '%diabetes%' OR desc_lc LIKE '%insulin%' OR desc_lc LIKE '%glucose%' OR desc_lc LIKE '%thyroid%' OR desc_lc LIKE '%endocrin%' OR desc_lc LIKE '%metabolic%' OR desc_lc LIKE '%parathormone%' THEN 'Endocrinology / Metabolic'
        WHEN code_num BETWEEN 70010 AND 79999 OR hcpcs_code LIKE 'A95%' OR hcpcs_code LIKE 'Q99%' OR desc_lc LIKE '%contrast material%' OR desc_lc LIKE '%radiology%' OR desc_lc LIKE '%mri%' OR desc_lc LIKE 'ct %' OR desc_lc LIKE '% ct %' OR desc_lc LIKE '%computed tomography%' OR desc_lc LIKE '%ultrasound%' OR desc_lc LIKE '%mammograph%' OR desc_lc LIKE '%diagnostic, per study dose%' THEN 'Imaging / Diagnostics'
        WHEN hcpcs_code LIKE 'J%' OR hcpcs_code LIKE 'C%' OR hcpcs_code LIKE 'Q%' OR hcpcs_code LIKE 'A96%' OR desc_lc LIKE '%injection%' OR desc_lc LIKE '%infusion%' OR desc_lc LIKE '%chemotherap%' OR desc_lc LIKE '%therapeutic, per microcurie%' OR desc_lc LIKE '%therapeutic, 1 millicurie%' THEN 'Injectables / Infusion / Specialty Drugs'
        WHEN code_num BETWEEN 97000 AND 97799 OR hcpcs_code = 'G0283' OR desc_lc LIKE '%therapy procedure%' OR desc_lc LIKE '%manual technique%' OR desc_lc LIKE '%physical therapy%' OR desc_lc LIKE '%occupational therapy%' OR desc_lc LIKE '%joint%' OR desc_lc LIKE '%knee%' OR desc_lc LIKE '%hip%' OR desc_lc LIKE '%shoulder%' OR desc_lc LIKE '%spine%' OR desc_lc LIKE '%pain%' OR desc_lc LIKE '%orthop%' OR desc_lc LIKE '%electrical stimulation%' THEN 'Orthopedics / Pain / Rehab'
        WHEN code_num BETWEEN 99202 AND 99499 OR desc_lc LIKE '%office or other outpatient visit%' OR desc_lc LIKE '%hospital care%' OR desc_lc LIKE '%nursing facility%' OR desc_lc LIKE '%home visit%' OR desc_lc LIKE '%subsequent hospital care%' OR desc_lc LIKE '%annual wellness%' THEN 'Primary Care / General Medicine'
        WHEN code_num BETWEEN 80047 AND 89398 OR desc_lc LIKE '%blood sample%' OR desc_lc LIKE '%laboratory%' OR desc_lc LIKE '%pathology%' OR desc_lc LIKE '%specimen%' OR desc_lc LIKE '%infectious agent detection%' OR desc_lc LIKE '%test, nonprescription self-administered%' THEN 'Laboratory / Pathology'
        WHEN desc_lc LIKE '%pulmonary%' OR desc_lc LIKE '%respiratory%' OR desc_lc LIKE '%lung%' OR desc_lc LIKE '%bronch%' OR desc_lc LIKE '%airway%' OR desc_lc LIKE '%sleep apnea%' OR desc_lc LIKE '%ventilation%' OR desc_lc LIKE '%spirometry%' THEN 'Pulmonology / Respiratory'
        WHEN code_num BETWEEN 10004 AND 69990 THEN 'Surgery / Procedural Specialties'
        WHEN substr(hcpcs_code, 1, 1) IN ('A', 'E', 'K', 'L') THEN 'DME / Supplies / Temporary Codes'
        ELSE 'Other / Unclassified'
    END AS specialty_proxy,
    CASE
        WHEN code_num BETWEEN 100 AND 1999 OR desc_lc LIKE 'anes %' THEN 'anesthesia_code_range'
        WHEN desc_lc LIKE '%ambulance%' OR desc_lc LIKE '%transport%' OR desc_lc LIKE '%travel allowance%' OR hcpcs_code LIKE 'A04%' THEN 'ambulance_transport_keywords'
        WHEN hcpcs_code IN ('90480', 'G0008', 'G0009') OR hcpcs_code LIKE '906%' OR hcpcs_code LIKE '913%' OR desc_lc LIKE '%vaccine%' OR desc_lc LIKE '%immunization%' THEN 'vaccination_keywords_and_codes'
        WHEN code_num BETWEEN 90785 AND 90899 OR desc_lc LIKE '%psychotherapy%' OR desc_lc LIKE '%psychiatric%' OR desc_lc LIKE '%depression screening%' OR desc_lc LIKE '%stimulate nerve cells in brain%' THEN 'behavioral_health_keywords_and_codes'
        WHEN code_num BETWEEN 95004 AND 95199 OR desc_lc LIKE '%allergy%' OR desc_lc LIKE '%allergenic extract%' OR desc_lc LIKE '%antigen%' THEN 'allergy_immunology_keywords_and_codes'
        WHEN code_num BETWEEN 98940 AND 98942 OR desc_lc LIKE '%chiropractic%' THEN 'chiropractic_codes'
        WHEN code_num BETWEEN 92002 AND 92499 OR desc_lc LIKE '%ophthalm%' OR desc_lc LIKE '%retina%' OR desc_lc LIKE '%glaucoma%' OR desc_lc LIKE '%cataract%' OR desc_lc LIKE '%intravitreal%' OR desc_lc LIKE '%eye %' THEN 'ophthalmology_keywords_and_codes'
        WHEN desc_lc LIKE '%cardiac%' OR desc_lc LIKE '%cardio%' OR desc_lc LIKE '%heart%' OR desc_lc LIKE '%vascular%' OR desc_lc LIKE '%coronary%' OR desc_lc LIKE '%artery%' OR desc_lc LIKE '%arrhythm%' OR desc_lc LIKE '%electrocardio%' OR desc_lc LIKE '%echocardio%' OR desc_lc LIKE '%pacemaker%' OR desc_lc LIKE '%anticoagulant%' THEN 'cardiology_keywords'
        WHEN desc_lc LIKE '%renal%' OR desc_lc LIKE '%kidney%' OR desc_lc LIKE '%dialysis%' OR desc_lc LIKE '%nephro%' OR desc_lc LIKE '%urolog%' OR desc_lc LIKE '%urinary%' OR desc_lc LIKE '%bladder%' OR desc_lc LIKE '%prostate%' THEN 'nephrology_urology_keywords'
        WHEN desc_lc LIKE '%diabetes%' OR desc_lc LIKE '%insulin%' OR desc_lc LIKE '%glucose%' OR desc_lc LIKE '%thyroid%' OR desc_lc LIKE '%endocrin%' OR desc_lc LIKE '%metabolic%' OR desc_lc LIKE '%parathormone%' THEN 'endocrinology_keywords'
        WHEN code_num BETWEEN 70010 AND 79999 OR hcpcs_code LIKE 'A95%' OR hcpcs_code LIKE 'Q99%' OR desc_lc LIKE '%contrast material%' OR desc_lc LIKE '%radiology%' OR desc_lc LIKE '%mri%' OR desc_lc LIKE 'ct %' OR desc_lc LIKE '% ct %' OR desc_lc LIKE '%computed tomography%' OR desc_lc LIKE '%ultrasound%' OR desc_lc LIKE '%mammograph%' OR desc_lc LIKE '%diagnostic, per study dose%' THEN 'imaging_keywords_and_codes'
        WHEN hcpcs_code LIKE 'J%' OR hcpcs_code LIKE 'C%' OR hcpcs_code LIKE 'Q%' OR hcpcs_code LIKE 'A96%' OR desc_lc LIKE '%injection%' OR desc_lc LIKE '%infusion%' OR desc_lc LIKE '%chemotherap%' OR desc_lc LIKE '%therapeutic, per microcurie%' OR desc_lc LIKE '%therapeutic, 1 millicurie%' THEN 'injectable_drug_keywords_and_codes'
        WHEN code_num BETWEEN 97000 AND 97799 OR hcpcs_code = 'G0283' OR desc_lc LIKE '%therapy procedure%' OR desc_lc LIKE '%manual technique%' OR desc_lc LIKE '%physical therapy%' OR desc_lc LIKE '%occupational therapy%' OR desc_lc LIKE '%joint%' OR desc_lc LIKE '%knee%' OR desc_lc LIKE '%hip%' OR desc_lc LIKE '%shoulder%' OR desc_lc LIKE '%spine%' OR desc_lc LIKE '%pain%' OR desc_lc LIKE '%orthop%' OR desc_lc LIKE '%electrical stimulation%' THEN 'ortho_rehab_keywords_and_codes'
        WHEN code_num BETWEEN 99202 AND 99499 OR desc_lc LIKE '%office or other outpatient visit%' OR desc_lc LIKE '%hospital care%' OR desc_lc LIKE '%nursing facility%' OR desc_lc LIKE '%home visit%' OR desc_lc LIKE '%subsequent hospital care%' OR desc_lc LIKE '%annual wellness%' THEN 'evaluation_management_keywords_and_codes'
        WHEN code_num BETWEEN 80047 AND 89398 OR desc_lc LIKE '%blood sample%' OR desc_lc LIKE '%laboratory%' OR desc_lc LIKE '%pathology%' OR desc_lc LIKE '%specimen%' OR desc_lc LIKE '%infectious agent detection%' OR desc_lc LIKE '%test, nonprescription self-administered%' THEN 'laboratory_keywords_and_codes'
        WHEN desc_lc LIKE '%pulmonary%' OR desc_lc LIKE '%respiratory%' OR desc_lc LIKE '%lung%' OR desc_lc LIKE '%bronch%' OR desc_lc LIKE '%airway%' OR desc_lc LIKE '%sleep apnea%' OR desc_lc LIKE '%ventilation%' OR desc_lc LIKE '%spirometry%' THEN 'pulmonology_keywords'
        WHEN code_num BETWEEN 10004 AND 69990 THEN 'surgery_code_range'
        WHEN substr(hcpcs_code, 1, 1) IN ('A', 'E', 'K', 'L') THEN 'supply_dme_prefix'
        ELSE 'fallback_unclassified'
    END AS classification_rule
FROM code_reference;

CREATE VIEW procedure_volume_by_specialty AS
WITH base_claims AS (
    SELECT
        c.HCPCS_Cd,
        CAST(c.Tot_Rndrng_Prvdrs AS REAL) AS providers,
        CAST(c.Tot_Benes AS REAL) AS beneficiaries,
        CAST(c.Tot_Srvcs AS REAL) AS services,
        CAST(c.Tot_Bene_Day_Srvcs AS REAL) AS bene_day_services,
        CAST(c.Avg_Sbmtd_Chrg AS REAL) AS avg_submitted_charge,
        CAST(c.Avg_Mdcr_Alowd_Amt AS REAL) AS avg_allowed_amount,
        CAST(c.Avg_Mdcr_Pymt_Amt AS REAL) AS avg_payment_amount
    FROM uniform_resource_cms_bygeography c
    WHERE c.Rndrng_Prvdr_Geo_Lvl = 'National'
)
SELECT
    m.specialty_proxy,
    COUNT(DISTINCT b.HCPCS_Cd) AS distinct_procedure_count,
    ROUND(SUM(b.providers), 2) AS total_rendering_providers,
    ROUND(SUM(b.beneficiaries), 2) AS total_beneficiaries,
    ROUND(SUM(b.services), 2) AS total_services,
    ROUND(SUM(b.bene_day_services), 2) AS total_bene_day_services,
    ROUND(SUM(b.services * b.avg_submitted_charge), 2) AS estimated_total_submitted_charge,
    ROUND(SUM(b.services * b.avg_allowed_amount), 2) AS estimated_total_allowed_amount,
    ROUND(SUM(b.services * b.avg_payment_amount), 2) AS estimated_total_payment_amount,
    ROUND(SUM(b.services) / NULLIF(SUM(b.beneficiaries), 0), 4) AS services_per_beneficiary
FROM base_claims b
JOIN procedure_specialty_proxy_map m
    ON m.hcpcs_code = b.HCPCS_Cd
GROUP BY m.specialty_proxy
ORDER BY total_services DESC;

CREATE VIEW top_procedures_per_specialty AS
WITH base_claims AS (
    SELECT
        c.HCPCS_Cd,
        CAST(c.Tot_Benes AS REAL) AS beneficiaries,
        CAST(c.Tot_Srvcs AS REAL) AS services,
        CAST(c.Avg_Mdcr_Alowd_Amt AS REAL) AS avg_allowed_amount
    FROM uniform_resource_cms_bygeography c
    WHERE c.Rndrng_Prvdr_Geo_Lvl = 'National'
), procedure_rollup AS (
    SELECT
        m.specialty_proxy,
        m.classification_rule,
        b.HCPCS_Cd,
        MIN(m.procedure_description) AS procedure_description,
        ROUND(SUM(b.services), 2) AS total_services,
        ROUND(SUM(b.beneficiaries), 2) AS total_beneficiaries,
        ROUND(SUM(b.services * b.avg_allowed_amount), 2) AS estimated_total_allowed_amount
    FROM base_claims b
    JOIN procedure_specialty_proxy_map m
        ON m.hcpcs_code = b.HCPCS_Cd
    GROUP BY m.specialty_proxy, m.classification_rule, b.HCPCS_Cd
)
SELECT
    specialty_proxy,
    classification_rule,
    ROW_NUMBER() OVER (PARTITION BY specialty_proxy ORDER BY total_services DESC, estimated_total_allowed_amount DESC, HCPCS_Cd) AS procedure_rank,
    HCPCS_Cd AS hcpcs_code,
    procedure_description,
    total_services,
    total_beneficiaries,
    estimated_total_allowed_amount
FROM procedure_rollup;

CREATE VIEW condition_to_icd_mapping AS
WITH seed(condition_group, primary_specialty_proxy, icd_prefix_rule) AS (
    VALUES
        ('Cardiovascular disease', 'Cardiology / Vascular', 'I10'),
        ('Cardiovascular disease', 'Cardiology / Vascular', 'I11'),
        ('Cardiovascular disease', 'Cardiology / Vascular', 'I12'),
        ('Cardiovascular disease', 'Cardiology / Vascular', 'I13'),
        ('Cardiovascular disease', 'Cardiology / Vascular', 'I20'),
        ('Cardiovascular disease', 'Cardiology / Vascular', 'I21'),
        ('Cardiovascular disease', 'Cardiology / Vascular', 'I25'),
        ('Cardiovascular disease', 'Cardiology / Vascular', 'I48'),
        ('Cardiovascular disease', 'Cardiology / Vascular', 'I50'),
        ('Cardiovascular disease', 'Cardiology / Vascular', 'I63'),
        ('Cardiovascular disease', 'Cardiology / Vascular', 'I70'),
        ('Diabetes & metabolic disease', 'Endocrinology / Metabolic', 'E08'),
        ('Diabetes & metabolic disease', 'Endocrinology / Metabolic', 'E09'),
        ('Diabetes & metabolic disease', 'Endocrinology / Metabolic', 'E10'),
        ('Diabetes & metabolic disease', 'Endocrinology / Metabolic', 'E11'),
        ('Diabetes & metabolic disease', 'Endocrinology / Metabolic', 'E13'),
        ('Diabetes & metabolic disease', 'Endocrinology / Metabolic', 'E66'),
        ('Diabetes & metabolic disease', 'Endocrinology / Metabolic', 'E78'),
        ('Chronic kidney disease', 'Nephrology / Urology', 'N18'),
        ('Chronic kidney disease', 'Nephrology / Urology', 'N19'),
        ('Pulmonary disease', 'Pulmonology / Respiratory', 'J41'),
        ('Pulmonary disease', 'Pulmonology / Respiratory', 'J42'),
        ('Pulmonary disease', 'Pulmonology / Respiratory', 'J43'),
        ('Pulmonary disease', 'Pulmonology / Respiratory', 'J44'),
        ('Pulmonary disease', 'Pulmonology / Respiratory', 'J45'),
        ('Pulmonary disease', 'Pulmonology / Respiratory', 'J47'),
        ('Pulmonary disease', 'Pulmonology / Respiratory', 'J84'),
        ('Oncology', 'Injectables / Infusion / Specialty Drugs', 'C'),
        ('Oncology', 'Injectables / Infusion / Specialty Drugs', 'D0'),
        ('Oncology', 'Injectables / Infusion / Specialty Drugs', 'D1'),
        ('Oncology', 'Injectables / Infusion / Specialty Drugs', 'D2'),
        ('Oncology', 'Injectables / Infusion / Specialty Drugs', 'D3'),
        ('Oncology', 'Injectables / Infusion / Specialty Drugs', 'D4'),
        ('Musculoskeletal & pain', 'Orthopedics / Pain / Rehab', 'M15'),
        ('Musculoskeletal & pain', 'Orthopedics / Pain / Rehab', 'M16'),
        ('Musculoskeletal & pain', 'Orthopedics / Pain / Rehab', 'M17'),
        ('Musculoskeletal & pain', 'Orthopedics / Pain / Rehab', 'M19'),
        ('Musculoskeletal & pain', 'Orthopedics / Pain / Rehab', 'M47'),
        ('Musculoskeletal & pain', 'Orthopedics / Pain / Rehab', 'M48'),
        ('Musculoskeletal & pain', 'Orthopedics / Pain / Rehab', 'M50'),
        ('Musculoskeletal & pain', 'Orthopedics / Pain / Rehab', 'M51'),
        ('Musculoskeletal & pain', 'Orthopedics / Pain / Rehab', 'M54'),
        ('Musculoskeletal & pain', 'Orthopedics / Pain / Rehab', 'M79'),
        ('Ophthalmic disease', 'Ophthalmology', 'H25'),
        ('Ophthalmic disease', 'Ophthalmology', 'H26'),
        ('Ophthalmic disease', 'Ophthalmology', 'H34'),
        ('Ophthalmic disease', 'Ophthalmology', 'H35'),
        ('Ophthalmic disease', 'Ophthalmology', 'H40'),
        ('Ophthalmic disease', 'Ophthalmology', 'H43'),
        ('Mental health & neurocognitive', 'Behavioral Health', 'F01'),
        ('Mental health & neurocognitive', 'Behavioral Health', 'F03'),
        ('Mental health & neurocognitive', 'Behavioral Health', 'F32'),
        ('Mental health & neurocognitive', 'Behavioral Health', 'F33'),
        ('Mental health & neurocognitive', 'Behavioral Health', 'G20'),
        ('Mental health & neurocognitive', 'Behavioral Health', 'G30'),
        ('Mental health & neurocognitive', 'Behavioral Health', 'G31'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D50'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D51'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D52'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D53'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D55'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D56'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D57'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D58'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D59'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D60'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D61'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D62'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D63'),
        ('Hematology / anemia', 'Injectables / Infusion / Specialty Drugs', 'D64')
)
SELECT DISTINCT
    s.condition_group,
    s.primary_specialty_proxy,
    s.icd_prefix_rule,
    d.icd10_code,
    d.is_billable,
    d.description_short,
    d.description_long,
    'manual_prefix_seed' AS mapping_method
FROM uniform_resource_ref_icd10_diagnosis d
JOIN seed s
    ON d.icd10_code LIKE s.icd_prefix_rule || '%';

CREATE VIEW proxy_condition_activity AS
WITH condition_summary AS (
    SELECT
        condition_group,
        primary_specialty_proxy,
        COUNT(*) AS mapped_icd_codes,
        SUM(CASE WHEN is_billable = '1' THEN 1 ELSE 0 END) AS billable_icd_codes
    FROM condition_to_icd_mapping
    GROUP BY condition_group, primary_specialty_proxy
), specialty_totals AS (
    SELECT
        SUM(total_services) AS all_specialty_services,
        SUM(total_beneficiaries) AS all_specialty_beneficiaries
    FROM procedure_volume_by_specialty
)
SELECT
    cs.condition_group,
    cs.primary_specialty_proxy,
    cs.mapped_icd_codes,
    cs.billable_icd_codes,
    COALESCE(p.distinct_procedure_count, 0) AS specialty_distinct_procedures,
    COALESCE(p.total_rendering_providers, 0) AS proxy_rendering_providers,
    COALESCE(p.total_services, 0) AS proxy_services,
    COALESCE(p.total_beneficiaries, 0) AS proxy_beneficiaries,
    COALESCE(p.estimated_total_allowed_amount, 0) AS proxy_allowed_amount,
    ROUND(COALESCE(p.total_services / NULLIF(p.total_beneficiaries, 0), 0), 4) AS procedure_density,
    ROUND(COALESCE(p.total_services / NULLIF((SELECT all_specialty_services FROM specialty_totals), 0), 0), 6) AS specialty_concentration,
    ROUND(COALESCE(p.total_beneficiaries / NULLIF((SELECT all_specialty_beneficiaries FROM specialty_totals), 0), 0), 6) AS beneficiary_share_proxy
FROM condition_summary cs
LEFT JOIN procedure_volume_by_specialty p
    ON p.specialty_proxy = cs.primary_specialty_proxy;

CREATE VIEW economic_intensity_index AS
WITH normalized AS (
    SELECT
        specialty_proxy,
        distinct_procedure_count,
        total_rendering_providers,
        total_beneficiaries,
        total_services,
        estimated_total_allowed_amount,
        CASE
            WHEN MAX(total_services) OVER () = MIN(total_services) OVER () THEN 0.0
            ELSE 1.0 * (total_services - MIN(total_services) OVER ()) / NULLIF(MAX(total_services) OVER () - MIN(total_services) OVER (), 0)
        END AS service_score,
        CASE
            WHEN MAX(total_beneficiaries) OVER () = MIN(total_beneficiaries) OVER () THEN 0.0
            ELSE 1.0 * (total_beneficiaries - MIN(total_beneficiaries) OVER ()) / NULLIF(MAX(total_beneficiaries) OVER () - MIN(total_beneficiaries) OVER (), 0)
        END AS beneficiary_score,
        CASE
            WHEN MAX(estimated_total_allowed_amount) OVER () = MIN(estimated_total_allowed_amount) OVER () THEN 0.0
            ELSE 1.0 * (estimated_total_allowed_amount - MIN(estimated_total_allowed_amount) OVER ()) / NULLIF(MAX(estimated_total_allowed_amount) OVER () - MIN(estimated_total_allowed_amount) OVER (), 0)
        END AS allowed_amount_score
    FROM procedure_volume_by_specialty
)
SELECT
    specialty_proxy,
    distinct_procedure_count,
    total_rendering_providers,
    total_beneficiaries,
    total_services,
    estimated_total_allowed_amount,
    ROUND(service_score, 6) AS service_score,
    ROUND(beneficiary_score, 6) AS beneficiary_score,
    ROUND(allowed_amount_score, 6) AS allowed_amount_score,
    ROUND(100 * ((0.35 * service_score) + (0.25 * beneficiary_score) + (0.40 * allowed_amount_score)), 2) AS economic_intensity_index
FROM normalized
ORDER BY economic_intensity_index DESC;

CREATE VIEW opportunity_scoring_view AS
WITH normalized AS (
    SELECT
        condition_group,
        primary_specialty_proxy,
        mapped_icd_codes,
        billable_icd_codes,
        specialty_distinct_procedures,
        proxy_rendering_providers,
        proxy_services,
        proxy_beneficiaries,
        proxy_allowed_amount,
        procedure_density,
        specialty_concentration,
        beneficiary_share_proxy,
        CASE
            WHEN MAX(billable_icd_codes) OVER () = MIN(billable_icd_codes) OVER () THEN 0.0
            ELSE 1.0 * (billable_icd_codes - MIN(billable_icd_codes) OVER ()) / NULLIF(MAX(billable_icd_codes) OVER () - MIN(billable_icd_codes) OVER (), 0)
        END AS icd_breadth_score,
        CASE
            WHEN MAX(beneficiary_share_proxy) OVER () = MIN(beneficiary_share_proxy) OVER () THEN 0.0
            ELSE 1.0 * (beneficiary_share_proxy - MIN(beneficiary_share_proxy) OVER ()) / NULLIF(MAX(beneficiary_share_proxy) OVER () - MIN(beneficiary_share_proxy) OVER (), 0)
        END AS beneficiary_share_score,
        CASE
            WHEN MAX(procedure_density) OVER () = MIN(procedure_density) OVER () THEN 0.0
            ELSE 1.0 * (procedure_density - MIN(procedure_density) OVER ()) / NULLIF(MAX(procedure_density) OVER () - MIN(procedure_density) OVER (), 0)
        END AS procedure_density_score,
        CASE
            WHEN MAX(specialty_concentration) OVER () = MIN(specialty_concentration) OVER () THEN 0.0
            ELSE 1.0 * (specialty_concentration - MIN(specialty_concentration) OVER ()) / NULLIF(MAX(specialty_concentration) OVER () - MIN(specialty_concentration) OVER (), 0)
        END AS specialty_concentration_score
    FROM proxy_condition_activity
), scored AS (
    SELECT
        *,
        ((0.45 * icd_breadth_score) + (0.55 * beneficiary_share_score)) AS prevalence_score
    FROM normalized
)
SELECT
    condition_group,
    primary_specialty_proxy,
    mapped_icd_codes,
    billable_icd_codes,
    specialty_distinct_procedures,
    proxy_rendering_providers,
    proxy_services,
    proxy_beneficiaries,
    proxy_allowed_amount,
    procedure_density,
    specialty_concentration,
    beneficiary_share_proxy,
    'billable_icd_breadth_plus_beneficiary_share_proxy_until_ccw_or_cdc_loaded' AS prevalence_basis,
    ROUND(icd_breadth_score, 6) AS icd_breadth_score,
    ROUND(beneficiary_share_score, 6) AS beneficiary_share_score,
    ROUND(prevalence_score, 6) AS prevalence_score,
    ROUND(procedure_density_score, 6) AS procedure_density_score,
    ROUND(specialty_concentration_score, 6) AS specialty_concentration_score,
    ROUND(100 * ((0.35 * prevalence_score) + (0.40 * procedure_density_score) + (0.25 * specialty_concentration_score)), 2) AS opportunity_score
FROM scored
ORDER BY opportunity_score DESC;