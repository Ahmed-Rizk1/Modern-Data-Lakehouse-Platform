# 🏗️ Master Engineering Plan: Enterprise Modern Data Lakehouse Platform
## Production Orchestration, ACID Table Storage, Distributed Querying & BI Cockpit

> **Project Reference:** `data-engineering-2/`  
> **Target Candidate Profile:** Senior Data Engineer / Modern Data Platform Architect  
> **Core Architecture:** Prefect 2/3 (Workflow & Pipeline Orchestrator) + Apache Iceberg (ACID Open Table Format) + Trino (Distributed ANSI SQL Query Engine) + MinIO (S3-Compatible Distributed Object Storage) + PostgreSQL (Iceberg REST Catalog & Prefect State Store) + Apache Superset (BI Analytics).  
> **Base Codebase:** `https://github.com/fraibacas/lakehouse-poc`

---

## Strategic Objective & Design Philosophy

The purpose of this project is to build and master a **production-grade, decoupled Modern Data Lakehouse** that demonstrates senior architectural competence in technical interviews:

1. **Storage/Compute Decoupling:** Data lands as immutable columnar Parquet files on S3 object storage (MinIO) governed by an open table format (Apache Iceberg) and queried on-demand by a distributed MPP engine (Trino).
2. **ACID Guarantees on Object Storage:** Eliminates partial writes, dirty reads, and orphaned data files via atomic snapshot commits.
3. **Resilient Orchestration:** Moves away from monolithic batch scripts to granular, observable, retryable Prefect tasks with dynamic mapping and caching.
4. **100% Local & Self-Contained:** Zero cloud dependency or monthly billing, fully reproducible on Docker Compose with cross-platform native management (Linux/macOS bash + Windows native PowerShell).

---

## Master Engineering Roadmap: 6 Structured Stages

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       STAGE 1: ENVIRONMENT & LIFECYCLE                      │
│   • Docker Compose v2 modernization   • .env.example governance             │
│   • Native Windows lakehouse.ps1      • Hardened service health checks      │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    STAGE 2: PREFECT PIPELINE REFACTORING                    │
│   • Deconstruct monolithic flow       • Granular @task decorations          │
│   • Dynamic task mapping (.map())     • Exponential backoff retries         │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                STAGE 3: ICEBERG INTERNALS & TABLE GOVERNANCE                │
│   • Dynamic partitioning specs        • Snapshot expiration maintenance     │
│   • Orphan file cleanup               • Time-travel query verification      │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    STAGE 4: DATA QUALITY & CONTRACT GATES                   │
│   • Schema contract enforcement       • Null threshold assertions           │
│   • End-to-end row reconciliation     • Descriptive pipeline halt triggers  │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      STAGE 5: AUTOMATED TESTING SUITE                       │
│   • PyIceberg client unit tests       • Prefect task & arrow mock tests     │
│   • Multi-service smoke tests         • Automated CI validation script      │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                 STAGE 6: DOCUMENTATION & INTERVIEW READINESS                │
│   • Recruiter-grade README.md         • Architectural diagrams              │
│   • Senior interview defense pack     • Benchmarks & failure mode analysis  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Stage 1: Environment Hardening & Cross-Platform Orchestration

* **Goal:** Make the entire lakehouse stack robust, idempotent, and seamlessly runnable on both Windows (PowerShell) and Unix environments (Bash).
* **Detailed Steps:**
  1. **Compose v2 Modernization:** Audit `docker-compose.yaml` and `lakehouse.sh` to transition all legacy `docker-compose` commands to modern `docker compose` syntax.
  2. **Native Windows Automation (`lakehouse.ps1`):** Develop a native PowerShell management script implementing the exact lifecycle methods (`start`, `stop`, `restart`, `status`, `reset`, `build`, `test`) without requiring WSL or Git Bash.
  3. **Configuration & Secrets Governance:**
     * Create a documented `.env.example` defining all variables (ports, credentials, bucket names, catalog URLs).
     * Replace brittle shell `eval "echo ..."` configuration generation with deterministic templating.
  4. **Health Checks & Dependency Ordering:** Ensure MinIO, PostgreSQL, and Iceberg REST catalog have rigorous health check probes so dependent services (Trino, Prefect Server, Prefect Worker) never start prematurely.

---

### Stage 2: Senior Refactoring of the Prefect Pipeline

* **Goal:** Transform `data/pipelines/ingestion.py` from a monolithic, un-tasked function into an observable, fault-tolerant, task-mapped orchestration pipeline.
* **Detailed Steps:**
  1. **Granular Task Decomposition:**
     * `@task validate_payload_contract`: Validates file format (`.parquet`, `.csv`), verifies file presence, and checks byte size.
     * `@task extract_to_arrow`: Ingests input data using DuckDB with memory limits, casting to a standardized PyArrow table schema.
     * `@task enforce_data_quality`: Executes data contract checks before touching the storage catalog.
     * `@task commit_to_iceberg`: Interacts with the Iceberg REST catalog, ensures namespace/table existence, and performs an atomic snapshot commit.
     * `@task verify_trino_queryability`: Executes an ANSI SQL query against Trino to guarantee the committed data is immediately queryable.
     * `@task sync_superset_bi`: Programmatically provisions Superset datasets, dashboards, and charts.
  2. **Retry Policies & Fault Tolerance:** Configure task retries with exponential backoffs and jitter for network-sensitive operations (REST catalog commits and Trino connections).
  3. **Dynamic Task Mapping (`.map()`):** Enable the ingestion flow to accept lists of files or partition directories, dynamically distributing extraction and validation across mapped tasks.
  4. **Prefect Deployment Standardization:** Audit and polish `prefect.yaml`, worker pool declarations, and flow run metadata tags.

---

### Stage 3: Lakehouse Engineering & Apache Iceberg Internals

* **Goal:** Deepen the storage layer to demonstrate senior mastery of open table format mechanics, partitioning, and metadata maintenance.
* **Detailed Steps:**
  1. **Table Partitioning Strategy:** Update `data/scripts/iceberg.py` to support explicit partitioning schemes (e.g. partition by day/month or trip category), enabling Trino query partition pruning.
  2. **Lakehouse Table Maintenance Flow (`data/pipelines/maintenance.py`):**
     * Build a standalone Prefect maintenance flow running scheduled table hygiene:
       * Snapshot expiration: Prunes old historical snapshots past a configurable retention threshold (e.g., retain last 7 snapshots).
       * Orphan file removal: Identifies and removes unreferenced Parquet files in MinIO storage.
  3. **Time-Travel Query Demonstration:** Update `data/notebooks/lakehouse.ipynb` with reproducible SQL queries showcasing:
     * Snapshot inspection (`table$snapshots`, `table$manifests`).
     * Time-travel querying in Trino using `FOR TIMESTAMP AS OF` and `FOR VERSION AS OF`.
     * Non-destructive schema evolution (adding/renaming columns without rewriting Parquet data).

---

### Stage 4: Data Quality & Contract Enforcement

* **Goal:** Guarantee data integrity at the lakehouse boundary and prevent data corruption in the warehouse tier.
* **Detailed Steps:**
  1. **Quality Module (`data/scripts/quality.py`):**
     * Build declarative data contract checks:
       * Primary/timestamp column non-null rate (100% required).
       * Numeric bounds validation (e.g. trip distance >= 0, fare amount >= 0).
       * Expected column schema conformity (types and allowable names).
  2. **Three-Way Row Count Reconciliation:**
     * Assert extracted raw rows == committed Iceberg rows == queried Trino rows.
  3. **Descriptive Exception Handling:** Ensure quality failures halt the flow before snapshot commit with descriptive error diagnostics in Prefect logs.

---

### Stage 5: Automated Testing Suite & Verification

* **Goal:** Provide automated verification guaranteeing that pipelines, catalog operations, and Docker services work deterministically.
* **Detailed Steps:**
  1. **Unit Testing (`tests/test_ingestion.py`):**
     * Mock PyArrow tables and test extraction, quality gates, and error handlers in isolation without requiring live Docker containers.
  2. **Catalog Client Testing (`tests/test_iceberg_client.py`):**
     * Test namespace creation, table creation, and schema mapping logic.
  3. **Integration Smoke Tests (`tests/test_smoke_services.py`):**
     * Programmatic health checks for MinIO S3 API, Iceberg REST catalog endpoint (`:8181`), Trino HTTP endpoint (`:8080`), and Prefect API (`:4200`).
  4. **Continuous Test Runner:** Provide a single command (`pytest tests/`) runnable locally and in CI.

---

### Stage 6: Recruiter-Grade Documentation & Interview Defense Pack

* **Goal:** Deliver an industry-standard, professional README and technical interview defense guide that proves deep technical expertise.
* **Detailed Steps:**
  1. **README.md Overhaul:**
     * Clean, publication-grade markdown with zero generic AI markers.
     * Architectural ASCII/Mermaid flowcharts displaying data landing, catalog interaction, and query paths.
     * Step-by-step setup and quickstart guides for both Windows and Linux.
  2. **Technical Interview Defense Guide:**
     * **Lakehouse vs. Traditional Warehouse:** Why decoupling storage and compute beats monolithic PostgreSQL for analytical workloads.
     * **Table Format Comparison:** Deep trade-off breakdown of Apache Iceberg vs. Delta Lake vs. Apache Hudi.
     * **Orchestration Paradigms:** Why Prefect (dynamic task graphs, native typing, lightweight deployment) over Airflow for this architecture.
     * **Query Optimization:** How Trino reads Iceberg metadata (manifest lists -> manifests -> data files) to prune partitions without scanning object storage.
     * **Failure Modes:** How atomic commits resolve write conflicts, network partition recovery, and snapshot rollbacks.

---

## Execution Protocol

* **No Code Without Review:** Each stage will be reviewed and approved before proceeding to implementation.
* **Verification at Every Step:** Every stage must be tested and verified against live services or test runners before being marked complete.
* **Progress Tracking:** All completions will be logged in `ANALYSIS_PROGRESS_2.md`.

