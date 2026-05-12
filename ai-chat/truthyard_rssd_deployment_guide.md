# Truth Yard RSSD Deployment Guide for Tenant-Based Medigy Opportunity Atlas DB

## Overview

This document explains how to deploy tenant-specific RSSD SQLite databases using Truth Yard based on folder structure and environment scope configuration.

---

# Folder Structure

Truth Yard deploys RSSD databases based on the directory structure inside the `cargo.d` folder.

Environment (`env`) scope is limited to the parent folder only.  
Because of this limitation, each tenant must have its own folder containing:

- Tenant-specific environment configuration
- Tenant-specific SQLite RSSD DB file

---

# Base Structure

```text
cargo.d/
└── medigy/
    ├── opportunity-atlas/
    │   ├── env
    │   └── dsd.db
    │
    ├── tenant1/
    │   ├── env
    │   └── dsd.db
    │
    ├── tenant2/
    │   ├── env
    │   └── dsd.db
```

---

# Tenant Folder Naming

Each tenant requires a dedicated folder under:

```text
cargo.d/medigy/
```

Examples:

| Tenant | Folder |
|---|---|
| Default Medigy Opportunity Atlas | `opportunity-atlas` |
| Tenant 1 | `tenant1` |
| Tenant 2 | `tenant2` |

---

# Database Naming Convention

The same database name is used for all tenants:

```text
dsd.db
```

Even though the database file name remains the same, the database content differs based on the tenant-specific environment configuration.

---

# Required Files Per Tenant

Each tenant folder must contain:

| File | Purpose |
|---|---|
| `env` | Tenant-specific environment configuration |
| `dsd.db` | Tenant-specific RSSD SQLite database |

---

# RSSD Database Preparation Process

Before deploying a database:

1. Set the correct tenant ID in the `env` configuration.
2. Prepare/generate the RSSD SQLite database for that tenant.
3. Save the generated database as:

```text
dsd.db
```

4. Place both the `env` file and `dsd.db` inside the corresponding tenant folder.

---

# Deployment URLs

Truth Yard automatically exposes the deployed RSSD database using the folder structure.

## URL Pattern

```text
http://<host>:<port>/<parent-folder>/<tenant-folder>/<db-name>
```

---

## Example URLs

### Tenant 1

```text
http://127.0.0.1:8080/medigy/tenant1/dsd
```

### Default Opportunity Atlas Tenant

```text
http://127.0.0.1:8081/medigy/opportunity-atlas/dsd
```

---

# Deployment Workflow Summary

## Step 1 — Create Tenant Folder

Example:

```text
cargo.d/medigy/tenant1/
```

---

## Step 2 — Add Tenant Environment

Place the tenant-specific `env` file inside the folder.

Example:

```text
cargo.d/medigy/tenant1/env
```

---

## Step 3 — Generate Tenant RSSD DB

Prepare the RSSD SQLite DB using the correct tenant ID from the environment configuration.

---

## Step 4 — Add Database File

Copy the generated database:

```text
dsd.db
```

into the tenant folder.

Example:

```text
cargo.d/medigy/tenant1/dsd.db
```

---

## Step 5 — Access the Deployment

Example:

```text
http://127.0.0.1:8080/medigy/tenant1/dsd
```

---

# Important Notes

- Environment scope is restricted to the parent folder only.
- Each tenant must have its own dedicated folder.
- The same database file name (`dsd.db`) can be reused across tenants.
- Database content is tenant-specific based on the tenant ID configured during RSSD DB preparation.
- Truth Yard deployment routing is fully dependent on folder hierarchy.

---

# Example Complete Structure

```text
cargo.d/
└── medigy/
    ├── opportunity-atlas/
    │   ├── env
    │   └── dsd.db
    │
    ├── tenant1/
    │   ├── env
    │   └── dsd.db
    │
    ├── tenant2/
        ├── env
        └── dsd.db
```
