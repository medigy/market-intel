# surveilr OWL Ingestion Guide

> Ingest the generated `medigy_mcp.owl` TBox schema into surveilr for MCP chat.

---

## Prerequisites

```bash
# Confirm surveilr is installed
surveilr --version

# Install surveilr (if not already installed)
curl -sL https://raw.githubusercontent.com/surveilr/packages/main/surveilr/install.sh | bash
```

Ensure `medigy_mcp.owl` is present in your working directory before proceeding.

---

## Step 1 — Ingest the OWL File

```bash
surveilr ingest files -r ./medigy_mcp.owl
```

This command:
- Reads `medigy_mcp.owl` from the current directory

---

## Step 2 — Adapt OWL for MCP

```bash
surveilr orchestrate adapt-owl
```

This command:
- Parses the RDF/XML and extracts classes, properties, and relationships
- Populates surveilr's orchestration tables to make the ontology queryable
- Makes the schema available to the MCP chat interface

**Verify adaptation:**

```bash
  SELECT * FROM ontology_classes
```

---

## Full Commands (copy-paste)

```bash
# 1. Ingest
surveilr ingest files -r ./medigy_mcp.owl

# 2. Adapt for MCP
surveilr orchestrate adapt-owl
```

---

## Troubleshooting

**`surveilr ingest files` — file not found**

```bash
# Run from the directory containing the OWL file
cd /path/to/project
surveilr ingest files -r ./medigy_mcp.owl
```

**`surveilr orchestrate adapt-owl` — no OWL found**

Confirm ingestion completed successfully before running adapt:

```bash
SELECT * FROM ontology_classes
```

If empty, re-run Step 1.
