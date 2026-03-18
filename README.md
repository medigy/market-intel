# medicare-dsd-evidence
Medigy Disease State Database (MDSD) Evidence Engine. A data pipeline using surveilr and spry to transform raw CMS Medicare claims into actionable evidence. It quantifies disease prevalence, procedure frequency, and economic intensity to move from qualitative prioritization to data-driven commercial validation.


## Overview
The **Medigy Disease State Database (MDSD)** is a data engineering initiative to provide evidence-based prioritization for clinical services. This repository contains the logic to validate "Disease States" using **actual Medicare Claims data**.

Using `surveilr`, we ingest CMS datasets into a **RSSD)** to enable SQL-based analysis and LLM-assisted workflows.

## Architecture & Tools
- **Orchestration**: `spry` (Executable Markdown blocks)
- **Ingestion**: `surveilr` (Extract & Load into SQLite RSSD)
- **Transformation**: `DuckDB` / SQLite SQL (Views and Materialized tables)

## Project Structure
```text
.
├── data/               # Local data storage (Git ignored)
│   └── raw/            # Source CSV/Parquet files from CMS
├── msd_orchestration.md # Executable MD files (The "Layout")│    
├── sql/                # SQL Transformation scripts
│   ├── ddl/            # Schema definitions
│   └── views/          # Logic for Evidence/Opportunity views
├── scripts/            # Bash utilities for remote data fetching
└── mdsd_evidence.rssd  # The resulting SQLite database
```

## Getting Started

### 1. Prerequisites
Ensure `surveilr` and `spry` are installed in your WSL path.
