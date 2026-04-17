# Copilot Workspace Instructions

These instructions apply to all chats in this workspace.

## MCP and data-query rules
1. **Strict Tooling**: Use **Surveilr MCP** only for all data queries. Do not use Pylance MCP or direct `sqlite3` fallbacks.
2. **Data Tools**: Use `mcp_surveilr_query_sql`, `mcp_surveilr_get_table_sample`, `mcp_surveilr_get_table_metadata`, `mcp_surveilr_get_schema`, `mcp_surveilr_list_tables`, `mcp_surveilr_get_table_columns`, `mcp_surveilr_get_table_stats`, and `mcp_surveilr_get_schema_compact` as needed.
3. **Ontology Tools**: Use `mcp_surveilr_list_ontology` to discover available clinical classes and `mcp_surveilr_query_ontology` when searching for specific medical concept relationships or definitions.
4. **Direct Results**: Run queries and show results directly in chat. If no rows match, explicitly state `row_count=0` and suggest a broader query.
5. **Conciseness**: Keep responses actionable and focused on the business question.
6. **Visualizations**: For charts, first fetch category/count data via Surveilr MCP, then render a Mermaid chart.


## Medigy Schema

This guide outlines the available tables and views in the SQLite database, structured for analysts and developers using the **Model Context Protocol (MCP)** to query Medicare utilization, DME, and clinical reference data.

The SQLite database contains many **platform and infrastructure tables** (orchestration, ingestion, device management, etc.). For analytical and clinical queries, use **only the tables and views listed below**.

> **Primary join hub:** `fact_utilization_unified` is the central fact table. It links to:
> - `dim_procedure` via `hcpcs_code`
> - `dim_geography` via `state_abbr`
> - `dim_specialty` via `specialty_domain`
> - `dim_condition_registry` via `condition_name`

---

### 1. Clinical Dimensions

Lookup/dimension tables that classify procedures, diagnoses, geographies, and specialties.

| Table Name | Description |
| --- | --- |
| **`dim_condition_registry`** | Registry of all tracked clinical conditions. PK: `condition_id`. Key columns: `condition_name` (UNIQUE), `body_system`, `tier` (1=Flagship, 2=Core). |
| **`dim_diagnosis`** | ICD-10 diagnosis code dimension. Key columns: `icd10_code`, `diagnosis_description`, `disease_state`, `body_system`. |
| **`dim_geography`** | Geographic dimension with MAC mapping and cost adjustment. Key columns: `state_abbr`, `mac_name`, `locality_name`, `pw_gpci` (Practice Work GPCI), `cost_tier`. |
| **`dim_procedure`** | HCPCS procedure code dimension. Key columns: `hcpcs_code`, `procedure_description`, `procedure_category`, `procedure_signal`, `linked_condition`. |
| **`dim_specialty`** | Provider specialty dimension. Key columns: `raw_specialty_name`, `specialty_name`, `specialty_domain`. |

---

### 2. Core Fact Table

The central unified utilization fact table integrating events across all CMS data sources.

| Table Name | Description |
| --- | --- |
| **`fact_utilization_unified`** | Unified utilization events (services, beneficiaries, payments) across conditions and specialties. Key columns: `source_type`, `state_abbr`, `hcpcs_code`, `condition_name`, `body_system`, `specialty_domain`, `tier`, `b2b_tier_primary`, `icon`, `color`, `total_beneficiaries`, `total_services`. |

---

### 3. Materialized Analytics

Pre-computed tables for condition summaries, opportunity scoring, state breakdowns, and executive KPIs. These are the **primary tables for dashboards and MCP chat outputs**.

| Table Name | Description |
| --- | --- |
| **`mat_condition_hcpcs_detail`** | HCPCS procedure utilization per condition. Columns: `condition_name`, `hcpcs_code`, `procedure_description`, `procedure_category`, `source_type`, `total_beneficiaries`, `total_services`, `total_allowed_amt`, `avg_allowed_per_service`. |
| **`mat_condition_national_summary`** | National-level rollup per condition with opportunity score and multi-source aggregation. Columns: `condition_name`, `specialty_domain`, `tier`, `b2b_tier_primary`, `icon`, `color`, `opportunity_score`, `data_sources`, `states_with_data`, `total_beneficiaries`, `total_services`, `total_allowed_amt`. |
| **`mat_condition_source_breakdown`** | Condition utilization broken down by CMS data source type. Columns: `condition_name`, `source_type`, `total_beneficiaries`, `total_services`, `total_allowed_amt`, `total_medicare_payment`. |
| **`mat_condition_state_breakdown`** | State-level condition utilization with GPCI cost adjustment and per-patient amounts. Columns: `condition_name`, `state_abbr`, `locality_name`, `cost_tier`, `pw_gpci`, `total_beneficiaries`, `total_services`, `total_allowed_amt`, `total_medicare_payment`, `allowed_per_patient`. |
| **`mat_executive_kpis`** | Single-row executive KPI snapshot. Columns: `total_conditions`, `total_states`, `total_procedures`, `total_beneficiaries`, `total_allowed_amt`, `total_medicare_payment`, `active_data_sources`. |
| **`mat_opportunity_score`** | Ranked commercial opportunity scores per condition with specialty and financial totals. Columns: `condition_name`, `specialty_domain`, `tier`, `b2b_tier_primary`, `icon`, `color`, `total_benes`, `total_allowed`, `opportunity_score`. |
| **`mat_top_states_by_condition`** | Ranked top states by condition utilization volume and per-patient cost. Columns: `condition_name`, `state_abbr`, `total_beneficiaries`, `total_allowed_amt`, `total_medicare_payment`, `allowed_per_patient`, `state_rank`. |

**View aliases** (thin wrappers over the `mat_*` tables above — prefer the `mat_*` tables for queries):

| View Name | Maps To |
| --- | --- |
| `condition_hcpcs_detail` | `mat_condition_hcpcs_detail` |
| `condition_national_summary` | `mat_condition_national_summary` |
| `condition_source_breakdown` | `mat_condition_source_breakdown` |
| `condition_state_breakdown` | `mat_condition_state_breakdown` |
| `executive_kpis` | `mat_executive_kpis` |
| `opportunity_score` | `mat_opportunity_score` |
| `top_states_by_condition` | `mat_top_states_by_condition` |

---

### 4. CMS Aggregated Data

Raw CMS ingested tables covering provider, geographic, inpatient, and outpatient utilization.

| Table Name | Description |
| --- | --- |
| **`uniform_resource_cms_bygeography`** | CMS Medicare utilization aggregated by geographic locality and HCPCS code. Key CMS columns: `Rndrng_Prvdr_Geo_Lvl`, `Rndrng_Prvdr_Geo_Cd`, `Rndrng_Prvdr_Geo_Desc`, `HCPCS_Cd`, `HCPCS_Desc`, `Place_Of_Srvc`, `Tot_Benes`. |
| **`uniform_resource_cms_provider`** | CMS Medicare provider-level utilization with NPI, specialty, and payment totals. Key columns: `Rndrng_NPI`, `Rndrng_Prvdr_Last_Org_Name`, `Rndrng_Prvdr_First_Name`, `Rndrng_Prvdr_Crdntls`, `Rndrng_Prvdr_Ent_Cd`. |
| **`uniform_resource_cms_inpatienthospitals_byproviderandservice`** | CMS inpatient hospital utilization by provider (CCN) and DRG. Key columns: `Rndrng_Prvdr_CCN`, `Rndrng_Prvdr_Org_Name`, `Rndrng_Prvdr_City`, `Rndrng_Prvdr_St`, `Rndrng_Prvdr_Zip5`. |
| **`uniform_resource_cms_outpatienthospitals_byproviderandservice`** | CMS outpatient hospital utilization by provider (CCN) and APC service. Key columns: `Rndrng_Prvdr_CCN`, `Rndrng_Prvdr_Org_Name`, `Rndrng_Prvdr_St`, `Rndrng_Prvdr_State_Abrvtn`. |
| **`uniform_resource_cms_bygeo_place_of_service_mapping`** | Maps CMS place-of-service codes to descriptive categories. Columns: `pos_code`, `pos_description`, `pos_category`. |
| **`uniform_resource_cms_providerandservice_pos`** | CMS place-of-service reference mapping for provider and service records. Columns: `Place of Service Code`, `Place of Service Description`, `Place of Service`. |
| **`uniform_resource_diagnostics_data`** | CMS diagnostics/imaging utilization aggregated by geography and HCPCS. Same column structure as `uniform_resource_cms_bygeography`. |

---

### 5. DME (Durable Medical Equipment) Data

DME referral and utilization tables by referring provider NPI.

| Table Name | Description |
| --- | --- |
| **`uniform_resource_dme_data`** | General DME referral utilization by referring provider NPI. Key columns: `Rfrg_NPI`, `Rfrg_Prvdr_Last_Name_Org`, `Rfrg_Prvdr_First_Name`, `Rfrg_Prvdr_Crdntls`, `Rfrg_Prvdr_Ent_Cd`. |
| **`uniform_resource_copd_oxygen`** | CMS DME referral data for COPD home oxygen therapy (HCPCSs E0431, E1390, etc.) by referring provider NPI. Same referring-provider column structure as `uniform_resource_dme_data`. |
| **`uniform_resource_dme_cpap_e0601_e0470_e0471`** | CMS DME referral data for CPAP/BiPAP equipment (E0601, E0470, E0471). Same referring-provider column structure as `uniform_resource_dme_data`. |

---

### 6. Clinical Reference Tables

CMS/HCPCS/ICD-10/RVU/QPP reference code and pricing tables.

| Table Name | Description |
| --- | --- |
| **`uniform_resource_ref_procedure_code`** | HCPCS procedure codes with Medicare payment, Work RVU, PE RVU, and facility/non-facility pricing. Key columns: `HCPCS`, `MOD`, `DESCRIPTION`, `STATUS CODE`, `MEDICARE PAYMENT`, `WORK RVU`. |
| **`uniform_resource_ref_rvu_qpp`** | RVU and QPP data including MIPS/APM indicators per HCPCS code. Same column structure as `uniform_resource_ref_procedure_code`. |
| **`uniform_resource_ref_hcpcs_level_two_procedures`** | HCPCS Level II (non-physician) procedure codes with long and short descriptions. Columns: `hcpcs_code`, `seq_num`, `record_id`, `long_description`, `short_description`. |
| **`uniform_resource_ref_icd10_diagnosis`** | Full ICD-10 diagnosis code list with billable flag and short/long descriptions. Columns: `sort_order`, `icd10_code`, `is_billable`, `description_short`, `description_long`. |
| **`uniform_resource_ref_geo_adjustment`** | 2026 Practice Work GPCI adjustments by MAC, state, and locality (with/without 1.0 floor). Columns: `Medicare Administrative Contractor (MAC)`, `State`, `Locality Number`, `Locality Name`, `2026 PW GPCI (without 1.0 Floor)`, `2026 PW GPCI (with 1.0 Floor)`. |
| **`uniform_resource_ref_medicare_localities`** | Medicare locality definitions with fee schedule areas and county mappings. Columns: `Medicare Adminstrative Contractor`, `Locality Number`, `State`, `Fee Schedule Area`, `Counties`. |
| **`uniform_resource_ref_opps_price_cap`** | OPPS (Outpatient Prospective Payment System) price caps by HCPCS, modifier, carrier, and locality. Columns: `HCPCS`, `MOD`, `PROCSTAT`, `CARRIER`, `LOCALITY`, `FACILITY PRICE`, `NON-FACILTY PRICE`. |
| **`uniform_resource_ref_anes_conversion_factor`** | Anesthesia conversion factors by contractor and locality (Qualifying vs Non-Qualifying APM). Columns: `Contractor`, `Locality`, `Locality Name`, plus APM CF columns. |

---

### 7. Policy Monitoring & Data Provenance

| Table Name | Description |
| --- | --- |
| **`data_provenance`** | Lineage tracking for all ingested external data sources (CMS files, HCPCS, ICD-10 references). PK: `id`. Columns: `title`, `link`, `version_year`, `ingested_at`, `object_type`, `description`. |

---

### 8. Analytics & Data Dictionary Views

Utility views exposing schema metadata and cross-table analytics.

| View Name | Description |
| --- | --- |
| **`data_tables_derived`** | Classifies all tables/views by type (Dimensional Table, Master Reference, Materialized Analytics, etc.) based on naming convention. |
| **`data_dictionary_indexes`** | Lists all non-system indexes with their associated table names. |

---

### 9. Ontology
| Table Name | Description |
| --- | --- |
|`ontology_classes`|Primary view for clinical class hierarchies and semantic relationships within the Medigy ontology.|


## Workspace context
- Database path: `resource-surveillance.sqlite.db`
- MCP server name in config: `surveilr`
