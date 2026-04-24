
# Ontology Ingestion Guide

> Ingest the `medigy_mcp.owl` schema into the local database to enable clinical intelligence in the chat

---

## Prerequisites

Ensure the following file exists in your workspace before proceeding:

* `ontology/medigy_mcp.owl`

---

## Step 1 — Ingest the Schema File

Run the following command in your terminal to load the ontology into your local instance:

```bash
surveilr ingest files -r ontology/medigy_mcp.owl
```

**What this does:**

* Scans the `ontology/` directory for the RDF/XML schema
* Populates the raw ingestion tables in `resource-surveillance.sqlite.db`

---

## Step 2 — Transform for Clinical Intelligence

Once ingested, the raw data must be structured into queryable views for the chat tools:

```bash
surveilr orchestrate adapt-owl
```

**What this does:**

* Parses the OWL properties and relationships
* Creates and populates the `ontology_classes` view
* Activates the clinical mapping used by your workspace instructions

---

## Step 3 — Verification

Confirm the clinical classes are accessible by running this query in your terminal:

```bash
sqlite3 resource-surveillance.sqlite.db "SELECT * FROM ontology_classes LIMIT 5;"
```

---

## Dashboard Integration

The ingestion of the OWL file and the generation of ontology classes have been packaged as an automated task within your dashboard documentation.

**Direct Execution**: After completing the initial setup task block, you can execute the ontology workflow directly by running the following command in your terminal:

```bash
spry rb task ontology-setup moa-dashboard.md
```

---

## Troubleshooting

1.**File not found during ingestion**

* Ensure you are in the root directory
* Check that the path `ontology/medigy_mcp.owl` is correct.

2.**Ontology views are empty**

* Ensure the `adapt-owl` orchestration command finished without errors
* If views are missing, re-run Step 1 and Step 2 in sequence.
