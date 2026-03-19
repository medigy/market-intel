# Medicare DSD Evidence

This repository builds a Spry-managed SQLPage dashboard for exploring Medicare
data from CMS public-use files.

The dashboard focuses on:

- specialty proxy utilization
- top HCPCS/CPT procedures
- condition opportunity scoring
- classification QA for the HCPCS-to-specialty heuristic mapping

`Spryfile.md` is the source of truth for the SQLPage routes. Spry materializes
the executable SQL files into `dev-src.auto/`, and SQLPage serves those routes
against `resource-surveillance.sqlite.db`.

## Repository layout

- `Spryfile.md` — Spry playbook and embedded SQLPage routes
- `business_question_views.sql` — derived analytical SQLite views
- `medicare-ds/` — CMS source/reference CSV files used for ingestion
- `dev-src.auto/` — generated SQLPage web root for local development
- `sqlpage/sqlpage.json` — SQLPage runtime config
- `resource-surveillance.sqlite.db` — local SQLite database

## Dashboard routes

The Spry playbook generates these SQLPage pages:

- `index.sql` — overview / executive summary
- `specialties.sql` — specialty proxy utilization and economic intensity
- `procedures.sql` — top procedures and drilldowns by specialty proxy
- `conditions.sql` — condition opportunity scoring
- `classification.sql` — mapping coverage and unclassified code QA

## Prerequisites

Make sure these CLIs are available in your shell:

- `spry`
- `sqlpage`
- `surveilr`
- `sqlite3`

## 1. Build or refresh the SQLite database

Run from the repository root:

```bash
rm -f resource-surveillance.sqlite.db
surveilr ingest files -r medicare-ds/
surveilr orchestrate transform-csv
surveilr shell --engine rusqlite business_question_views.sql -d resource-surveillance.sqlite.db
```

This ingests the CSV resources and creates the derived views used by the
dashboard, including:

- `procedure_specialty_proxy_map`
- `procedure_volume_by_specialty`
- `top_procedures_per_specialty`
- `condition_to_icd_mapping`
- `proxy_condition_activity`
- `economic_intensity_index`
- `opportunity_scoring_view`

## 2. Generate the SQLPage app from Spry

Materialize the SQLPage routes into `dev-src.auto/`:

```bash
spry sp spc -m Spryfile.md --fs dev-src.auto --destroy-first --conf sqlpage/sqlpage.json
```

To confirm the playbook parses and exposes the expected routes:

```bash
spry sp spc ls -m Spryfile.md
```

## 3. Run SQLPage

Start SQLPage from the repository root with the generated web root and config:

```bash
sqlpage
```

Then open:

- `http://127.0.0.1:8080/index.sql`

## Development workflow

Regenerate the app whenever you change `Spryfile.md`:

```bash
spry sp spc -m Spryfile.md --fs dev-src.auto --destroy-first --conf sqlpage/sqlpage.json
```

For watch mode:

```bash
spry sp spc -m Spryfile.md --fs dev-src.auto --destroy-first --conf sqlpage/sqlpage.json --watch
```

If you want Spry to restart SQLPage automatically after rebuilds:

```bash
spry sp spc -m Spryfile.md --fs dev-src.auto --destroy-first --conf sqlpage/sqlpage.json --watch --with-sqlpage
```

## Cleaning generated artifacts

Remove generated development output:

```bash
rm -rf dev-src.auto
```

## Notes

- The analytics are built against national aggregates (`Rndrng_Prvdr_Geo_Lvl = 'National'`).
- `dev-src.auto/` is generated output and can be recreated from `Spryfile.md`.
- `spry.d/` content inside `dev-src.auto/` is generated metadata used by Spry/SQLPage.

## Typical local run

```bash
rm -f resource-surveillance.sqlite.db
surveilr ingest files -r medicare-ds/
surveilr orchestrate transform-csv
surveilr shell --engine rusqlite business_question_views.sql -d resource-surveillance.sqlite.db
spry sp spc -m Spryfile.md --fs dev-src.auto --destroy-first --conf sqlpage/sqlpage.json
sqlpage
```
