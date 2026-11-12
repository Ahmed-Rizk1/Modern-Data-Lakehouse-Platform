# 🏛️ Project 2: Enterprise Modern Data Lakehouse Platform
## Master Architecture, Tuning & Senior Interview-Readiness Plan

> **Tracking Reference:** Corresponds to `data-engineering-2/` in our Master Portfolio Context (`PROJECTS_MASTER_CONTEXT.md`).  
> **Target Profile:** Senior Data Engineer / Modern Data Platform Architect (Class of 2026, CGPA 3.83/4.00, Excellent with Honors).  
> **Core Architecture:** Prefect 2/3 (Workflow & Pipeline Orchestrator) + Apache Iceberg (ACID Open Table Format) + Trino (Distributed ANSI SQL Query Engine) + MinIO (S3-Compatible Distributed Object Storage) + PostgreSQL (REST Catalog & Prefect State Store) + Apache Superset (BI & Exploration).  
> **Source Base Repository:** `https://github.com/fraibacas/lakehouse-poc`

---

## 1. Executive Summary & The Senior Architectural Vision

In **Data Engineering Project 1** (`data-engineering-1-airflow`), we demonstrated production batch ETL fundamentals:
* Scheduled batch ingestion via Apache Airflow with Celery workers.
* Staging JSON files into a relational PostgreSQL warehouse.
* Declarative data contract enforcement via Soda Core.

While standard ETL into relational databases remains common in legacy systems, **tier-one tech companies and modern enterprises build decoupled Data Lakehouses**. The modern enterprise standard decouples storage from compute, lands columnar datasets onto object storage, and guarantees ACID transactions, snapshot isolation, and schema evolution through open table formats.

### The Strategic Value of This Stack
1. **Prefect over Airflow:** Dynamic DAGs at runtime, native Python typing and `async`/`await`, granular task-level retries and caching, lightweight execution pools, and seamless programmatic deployments.
2. **Apache Iceberg over Traditional Object Lakes:** Eliminates corrupted states and partial writes via atomic snapshot metadata trees. Provides hidden partitioning, partition evolution, non-destructive schema evolution, and time-travel querying.
3. **Trino over Monolithic Warehouses:** Distributed Massively Parallel Processing (MPP) query engine that evaluates queries directly across Iceberg metadata and MinIO Parquet files with sub-second latencies without moving data into proprietary databases.
4. **100% Local Reproducibility:** Entire lakehouse infrastructure runs in containerized Docker services, enabling zero-cloud-cost experimentation, automated end-to-end integration testing, and live demonstration during technical interviews.

---

## 2. High-Level Audit of the Cloned Base Repository

Our initial audit of `fraibacas/lakehouse-poc` identified strong architectural foundations alongside several critical gaps that prevent it from passing as a senior-level, interview-bulletproof system:

### Architectural Strengths:
* **True Modern Stack:** Combines MinIO S3 object storage, Iceberg REST catalog, PostgreSQL, Trino query engine, Prefect orchestration, and Superset BI.
* **Declarative Catalog Configs:** Clean templating for PyIceberg (`pyiceberg.template.yaml`) and Trino (`trino-catalog.template.properties`).
* **Automated BI Integration:** `data/scripts/superset.py` programmatically registers Trino databases, datasets, dashboards, and charts into Superset via its REST API upon successful pipeline execution.

### Critical Production & Engineering Gaps to Address:
1. **Monolithic Ingestion Flow (Zero Tasks):**
   * In `data/pipelines/ingestion.py`, `data_to_dashboard` is a monolithic `@flow` containing all logic in one function with **zero `@task` decorators**.
   * Consequence: If Superset configuration fails at step 4, the entire flow fails, and steps 1–3 must be rerun. There is no task granularity, no task-level retries, no caching, and no dynamic mapping.
2. **Missing Data Quality & Contract Enforcement:**
   * Raw data is read via DuckDB and immediately committed to Iceberg without schema contract verification, column profiling, null threshold checks, or row count reconciliation.
3. **Single File / Non-Dynamic Ingestion:**
   * Flow is hardcoded around a single NYC taxi dataset (`yellow_tripdata_2024-01.parquet`). It lacks dynamic task mapping (`.map()`) for handling batch directories or multi-partition ingestion.
4. **Lack of Lakehouse Table Maintenance:**
   * Iceberg tables accumulate metadata snapshots and orphan files over time. The codebase lacks snapshot expiration and maintenance routines necessary in production.
5. **Cross-Platform & Script Friction:**
   * Management script `lakehouse.sh` is bash-only and invokes legacy `docker-compose` instead of modern `docker compose`. Running on Windows requires a native PowerShell automation script (`lakehouse.ps1`).
6. **Zero Automated Testing:**
   * No unit tests or integration tests exist to validate PyIceberg catalog operations, Trino query execution, or Prefect flow execution.
7. **Documentation Depth:**
   * The repository README is a minimal 100-line POC guide lacking architectural diagrams, design trade-off explanations, or technical interview defense guides.

---

## 3. Target Project Architecture

```text
data-engineering-2/
├── .env                                # Local environment configuration
├── .env.example                        # Documented environment template
├── .dockerignore
├── .gitignore
├── docker-compose.yaml                 # Multi-service modern lakehouse specification
├── lakehouse.sh                        # Bash management script (Linux / macOS / WSL)
├── lakehouse.ps1                       # Native PowerShell management script (Windows)
│
├── build/                              # Custom Docker build contexts
│   ├── jupyter/Dockerfile              # Interactive analytics environment
│   ├── prefect/Dockerfile              # Prefect worker with PyIceberg, DuckDB, Trino driver
│   ├── spark/                          # Optional PySpark processing engine
│   └── superset/                       # Superset BI with Trino connector
│
├── config/                             # Declarative service configurations
│   ├── pyiceberg.template.yaml         # PyIceberg REST catalog configuration
│   ├── superset_config.py              # Superset security and database configuration
│   └── trino-catalog.template.properties # Trino Iceberg connector specification
│
├── data/
│   ├── datasets/                       # Raw input files (Parquet / CSV landing zone)
│   ├── notebooks/
│   │   └── lakehouse.ipynb             # Time-travel queries & Iceberg snapshot inspection
│   ├── pipelines/
│   │   ├── .prefectignore
│   │   ├── ingestion.py                # REFACTORED: Modular, task-mapped Prefect flow
│   │   ├── maintenance.py              # NEW: Iceberg snapshot expiration & orphan cleanup
│   │   ├── prefect.yaml                # Prefect deployment & work-pool specification
│   │   └── requirements.txt            # Locked pipeline dependencies
│   └── scripts/
│       ├── iceberg.py                  # PyIceberg client with partition & transaction support
│       ├── quality.py                  # NEW: Data contract and validation assertions
│       ├── setup_superset_db.py        # Superset database initialization script
│       └── superset.py                 # Superset REST API programmatic client
│
├── tests/                              # NEW: Automated testing suite
│   ├── test_ingestion.py               # Unit tests for Prefect tasks & schema validation
│   ├── test_iceberg_client.py          # Tests for PyIceberg namespace/table operations
│   └── test_smoke_services.py          # Service health and API connectivity checks
│
├── docs/                               # Architectural diagrams & specifications
│   ├── lakehouse_architecture.png      # End-to-end dataflow & storage layout
│   └── iceberg_metadata_tree.png       # Manifest lists, manifests, and data files hierarchy
│
├── README.md                           # Recruiter-grade documentation & interview defense guide
├── ANALYSIS_TUNING_PLAN_2.md           # This master strategic tuning roadmap
└── ANALYSIS_PROGRESS_2.md             # Execution progress and milestone tracker
```

---

## 4. Multi-Phase Tuning & Refactoring Roadmap

### Phase 1: Environment Hardening & Cross-Platform Scaffolding
* **Objective:** Ensure seamless, idempotent stack lifecycle management on both Windows and Linux.
* **Tasks:**
  1. Audit `.env` and create a clean, documented `.env.example` with sensible defaults.
  2. Modernize `lakehouse.sh` to use `docker compose` v2 consistently without hardcoded legacy commands.
  3. Create `lakehouse.ps1` for native Windows PowerShell execution (start, stop, status, reset, build).
  4. Ensure Docker container dependencies (`depends_on` with health checks) for MinIO, PostgreSQL, and Iceberg REST catalog are rock-solid.

### Phase 2: Senior Refactoring of the Prefect Pipeline (`ingestion.py`)
* **Objective:** Transform the monolithic flow into a decoupled, resilient, task-mapped orchestration pipeline.
* **Tasks:**
  1. Deconstruct `data_to_dashboard` into focused, single-responsibility Prefect tasks:
     * `@task validate_input_payload`: Format validation, file existence, and path resolution.
     * `@task extract_to_arrow`: Ingests Parquet/CSV via DuckDB into PyArrow table format with memory efficiency.
     * `@task validate_data_contract`: Programmatic data contract assertions (schema structure, null percentages, row count threshold).
     * `@task commit_to_iceberg`: Idempotent Iceberg table creation, partition registration, and atomic snapshot append.
     * `@task verify_trino_sync`: Executes a verification query through Trino to assert data availability.
     * `@task publish_to_superset`: Configures Superset datasets and dashboards via REST API.
  2. Implement task-level retry policies (`retries=3, retry_delay_seconds=exponential_backoff(...)`).
  3. Implement **Dynamic Task Mapping (`.map()`)** allowing the flow to ingest multiple files/partitions concurrently.
  4. Add Prefect tags, flow logging, and run state summaries.

### Phase 3: Lakehouse Engineering & Open Table Format Depth
* **Objective:** Showcase deep understanding of Apache Iceberg internals and table governance.
* **Tasks:**
  1. Enhance `data/scripts/iceberg.py` to support table partitioning specs (e.g., partitioning by timestamp or category).
  2. Implement `data/pipelines/maintenance.py`: A dedicated Prefect flow for table optimization:
     * Snapshot expiration (pruning old snapshots beyond retention window).
     * Orphan file cleanup (removing unreferenced Parquet files from MinIO).
  3. Document Iceberg metadata mechanics:
     * `Iceberg Catalog` -> `Current Metadata Pointer` -> `Snapshot List` -> `Manifest List` -> `Manifest Files` -> `Data Files (Parquet)`.
  4. Build a dedicated Jupyter notebook (`data/notebooks/lakehouse.ipynb`) demonstrating **Time-Travel Querying** in Trino (`FOR TIMESTAMP AS OF` and `FOR VERSION AS OF`) and schema evolution.

### Phase 4: Data Quality & Contract Enforcement (`quality.py`)
* **Objective:** Eliminate silent data corruption and enforce rigorous data contracts before Iceberg commit.
* **Tasks:**
  1. Create `data/scripts/quality.py` with declarative validation rules:
     * Required non-null primary/timestamp columns.
     * Row count reconciliation (extracted rows == committed rows == queryable rows in Trino).
     * Anomaly bounds (e.g. fare amount >= 0, trip distance >= 0).
  2. Integrate assertions directly into the Prefect pipeline with descriptive error states.

### Phase 5: Automated Testing & Verification Suite
* **Objective:** Provide automated verification of all core components.
* **Tasks:**
  1. Implement unit tests in `tests/` targeting the refactored Prefect tasks using mocked Arrow tables.
  2. Implement tests for `IcebergClient` catalog interactions.
  3. Implement smoke tests verifying MinIO bucket accessibility, Trino JDBC/HTTP endpoint, and Prefect API responsiveness.

### Phase 6: Recruiter-Grade Documentation & Interview Defense Pack
* **Objective:** Produce a publication-grade README and technical defense cheat sheet.
* **Tasks:**
  1. Overhaul `README.md` to reflect an enterprise-ready Data Lakehouse.
  2. Remove all placeholder text, generic emojis, and AI markers.
  3. Include an extensive **Technical Interview Defense Guide** covering:
     * **Orchestration:** Why Prefect vs. Airflow (dynamic graphs, Python-native debugging, lower infrastructure overhead).
     * **Table Formats:** Apache Iceberg vs. Delta Lake vs. Apache Hudi.
     * **Query Engines:** Why Trino vs. Spark for interactive OLAP.
     * **Catalog Architecture:** REST Catalog vs. Hive Metastore vs. Nessie.
     * **Failure Modes & Edge Cases:** Handling concurrent writes, catalog locks, S3 consistency, and snapshot rollbacks.

---

## 5. Verification Checklist & Success Criteria

- [ ] All containers spin up cleanly without manual intervention via `./lakehouse.sh start` or `.\lakehouse.ps1 start`.
- [ ] Prefect UI (`http://localhost:4200`) displays modular tasks with retries, parameters, and execution logs.
- [ ] MinIO (`http://localhost:9001`) displays partitioned Parquet files and Iceberg metadata JSON/AVRO files in `s3://warehouse/`.
- [ ] Trino (`http://localhost:8080`) queries the Iceberg catalog and supports time-travel queries.
- [ ] Apache Superset (`http://localhost:8088`) displays automatically registered datasets and charts.
- [ ] Automated test suite in `tests/` passes with 100% success rate.
- [ ] `README.md` delivers senior-level architectural depth and interview readiness.

