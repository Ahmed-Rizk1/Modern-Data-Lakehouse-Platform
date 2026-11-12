# Modern Data Lakehouse Platform
## ACID Table Storage, Distributed MPP Querying & Resilient Orchestration

[![Lakehouse CI](https://img.shields.io/badge/CI-Automated%20Test%20Suite-brightgreen?style=flat-square)](tests/)
[![Table Format](https://img.shields.io/badge/Table%20Format-Apache%20Iceberg%20v2-blue?style=flat-square)](https://iceberg.apache.org/)
[![Query Engine](https://img.shields.io/badge/Query%20Engine-Trino%20440-orange?style=flat-square)](https://trino.io/)
[![Orchestrator](https://img.shields.io/badge/Orchestrator-Prefect%202.18-red?style=flat-square)](https://www.prefect.io/)
[![Storage](https://img.shields.io/badge/Storage-MinIO%20S3-purple?style=flat-square)](https://min.io/)

A decoupled, modern analytical data platform built with **Apache Iceberg**, **Trino**, **MinIO**, **PostgreSQL (REST Catalog)**, **Prefect**, and **Apache Superset**. The architecture replaces monolithic legacy analytical warehouses with open, columnar, ACID-guaranteed object storage while decoupling compute from storage.

---

## 1. System Architecture

```
                  ┌────────────────────────────────────────────────────────┐
                  │                 RAW DATA LANDING ZONE                  │
                  │        (NYC Taxi / High-Volume Parquet & CSV)          │
                  └───────────────────────────┬────────────────────────────┘
                                              │
                                              ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                              PREFECT WORKFLOW ORCHESTRATION                                  │
│                                                                                              │
│   [validate_payload_contract] ──► [extract_to_arrow] ──► [enforce_data_quality]              │
│       • Schema & Size Check          • DuckDB 2GB Cap        • Null & Bounds Gate            │
│                                                                     │                        │
│   [reconcile_row_counts]     ◄── [verify_trino]     ◄── [commit_to_iceberg]                 │
│       • 3-Way Reconciliation         • Distributed SQL       • Atomic Snapshot Append        │
└─────────────────────────────────────────────┬────────────────────────────────────────────────┘
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
┌───────────────────────────────────────────┐   ┌──────────────────────────────────────────────┐
│          METADATA CATALOG TIER            │   │            STORAGE ENGINE TIER               │
│                                           │   │                                              │
│  ┌─────────────────────────────────────┐  │   │  ┌────────────────────────────────────────┐  │
│  │     Iceberg REST Catalog (:8181)    │  │   │  │           MinIO S3 (:9000)             │  │
│  │   (Tabular REST Spec / PostgreSQL)  │  │   │  │       s3a://warehouse/namespace/       │  │
│  └──────────────────┬──────────────────┘  │   │  └───────────────────▲────────────────────┘  │
│                     │                     │   │                      │                       │
│                     ▼                     │   │                      │                       │
│       Metadata Pointer (.metadata.json)   │   │                      │                       │
│                     │                     │   │                      │                       │
│                     ▼                     │   │                      │                       │
│             Snapshot Manifests            │───┼──────────────────────┘ (Columnar Parquet)    │
└─────────────────────┬─────────────────────┘   └──────────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 DISTRIBUTED QUERY & CONSUMPTION                              │
│                                                                                              │
│               ┌──────────────────────────────────────────────────────────────┐               │
│               │                      Trino MPP (:8080)                       │               │
│               │      • Partition Pruning via Manifest Min/Max Stats          │               │
│               │      • Time-Travel Querying (FOR VERSION / TIMESTAMP AS OF)  │               │
│               │      • Schema Evolution (In-Place, Zero File Rewrites)       │               │
│               └──────────────────────────────┬───────────────────────────────┘               │
│                                              │                                               │
│                                              ▼                                               │
│               ┌──────────────────────────────────────────────────────────────┐               │
│               │             Apache Superset BI Cockpit (:8088)               │               │
│               │                 Interactive Semantic Charts                  │               │
│               └──────────────────────────────────────────────────────────────┘               │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Apache Iceberg Storage Layout & Metadata Hierarchy

Apache Iceberg guarantees ACID transactions, snapshot isolation, and time-travel querying on object storage without locking or table-level file directory scans.

```
Catalog (PostgreSQL REST Endpoint)
   └── Current Table Pointer: s3a://warehouse/nyc_taxi/yellow_trips/metadata/v2.metadata.json
         ├── Schema: [VendorID: int, tpep_pickup_datetime: timestamp, fare_amount: double, ...]
         ├── Partition Spec: [field_id: 1000, source_id: 2, transform: day, name: pickup_day]
         └── Snapshot Log:
               ├── Snapshot S0 (Commit 101) ──► Manifest List: snap-101.avro
               └── Snapshot S1 (Commit 102) ──► Manifest List: snap-102.avro
                                                      │
                     ┌────────────────────────────────┴────────────────────────────────┐
                     ▼                                                                 ▼
           Manifest File M1.avro                                             Manifest File M2.avro
       (Partition: pickup_day=2024-01-01)                               (Partition: pickup_day=2024-01-02)
       ├── Data File: data_01.parquet                                    ├── Data File: data_03.parquet
       │    ├── Lower Bound: fare_amount = 2.50                          │    ├── Lower Bound: fare_amount = 3.00
       │    └── Upper Bound: fare_amount = 85.00                         │    └── Upper Bound: fare_amount = 120.00
       └── Data File: data_02.parquet                                    └── Data File: data_04.parquet
            ├── Lower Bound: fare_amount = 5.00                               ├── Lower Bound: fare_amount = 7.50
            └── Upper Bound: fare_amount = 45.00                              └── Upper Bound: fare_amount = 60.00
```

### Partition Pruning Mechanics
When Trino executes `WHERE pickup_day = DATE '2024-01-02' AND fare_amount > 100.00`:
1. Trino reads the current snapshot's **Manifest List**.
2. Trino evaluates partition bounds on manifests and **skips `Manifest File M1.avro` entirely**.
3. In `Manifest File M2.avro`, Trino reads file-level column statistics (min/max bounds) and **skips `data_04.parquet`** without reading object storage bytes.
4. Trino opens only `data_03.parquet` directly via S3 byte-range requests.

---

## 3. Key Technical Capabilities

### Declarative Data Quality Gates
- **Contract Enforcement:** PyArrow schema typing, mandatory column presence, and duplicate column rejection via `quality.py`.
- **Threshold Assertions:** Non-null guarantees (0% tolerance on pickup timestamps) and statistical domain bounding (non-negative fares and distances with <=0.5% anomaly ceilings).
- **Three-Way Row Count Reconciliation:** Asserts `Raw Ingested == Iceberg Committed == Trino Queryable` across every flow run. Any discrepancy immediately raises `RowReconciliationError` and halts pipeline execution.

### Non-Destructive Schema Evolution
- Columns can be added, dropped, or renamed (`Airport_fee` $\to$ `airport_surcharge`) via PyIceberg metadata transactions.
- Zero data files are rewritten. Old Parquet files retain their original physical column names while metadata tracks field IDs.

### Lakehouse Table Maintenance
- Standalone Prefect maintenance flow (`data/pipelines/maintenance.py`) automating:
  - **Snapshot Expiration:** Prunes historical snapshots older than retention windows.
  - **Orphan File Cleanup:** Identifies and cleans unreferenced Parquet files in MinIO storage.
  - **Manifest & File Compaction:** Consolidates small files via Trino `EXECUTE optimize`.

---

## 4. Environment & Quickstart

### Prerequisites
- Docker & Docker Compose v2 (`docker compose`)
- Minimum 8 GB RAM, 4 CPU Cores allocated to Docker Desktop/Daemon
- Python 3.10+ (for local test runner)

### Service Ports
| Service | External Port | Internal Port | Description |
| :--- | :--- | :--- | :--- |
| **Traefik Reverse Proxy** | `80` | `80` | Gateway & Subdomain Router |
| **Prefect Server UI** | `4200` | `4200` | Orchestration & Flow Run Observability |
| **Trino MPP Engine** | `8080` | `8080` | Distributed ANSI SQL Query Server |
| **Apache Superset** | `8088` | `8088` | BI Analytics Cockpit (`admin` / `admin`) |
| **MinIO S3 API** | `9000` | `9000` | Object Storage Engine |
| **MinIO Web Console** | `9001` | `9001` | S3 Storage Browser UI |
| **Iceberg REST Catalog** | `8181` | `8181` | Tabular REST Catalog Protocol |
| **JupyterLab** | `5006` | `8888` | Interactive Notebook Environment |
| **PostgreSQL** | `5432` | `5432` | State Store (Prefect, Superset, Iceberg) |

### Native Lifecycle Commands

#### Windows (PowerShell)
```powershell
# Start all services with automated health checks & schema initialization
.\lakehouse.ps1 start

# Check status of all containers
.\lakehouse.ps1 status

# Execute unit and integration tests
.\lakehouse.ps1 test

# Tear down environment
.\lakehouse.ps1 stop
```

#### Linux / macOS (Bash)
```bash
# Start all services
./lakehouse.sh start

# Inspect status
./lakehouse.sh status

# Rebuild images
./lakehouse.sh build

# Stop services
./lakehouse.sh stop
```

---

## 5. Automated Testing Suite

The repository includes a comprehensive 24-test verification suite runnable locally without Docker container overhead:

```bash
# Run all tests with verbose output
python -m pytest tests/ -v
```

### Test Hierarchy
- **`tests/test_ingestion.py` (12 tests):**
  - Payload file presence, extension validation, and zero-byte payload guards.
  - PyArrow table memory extraction and declarative contract assertions.
  - Null threshold violations and out-of-bounds anomaly rejection.
  - Three-way row reconciliation logic (`Raw == Committed == Queried`).
- **`tests/test_iceberg_client.py` (10 tests):**
  - Partition transform parsers (`day`, `month`, `identity`, `bucket[N]`, `truncate[N]`).
  - Partition spec builders mapping Iceberg field IDs (`field_id >= 1000`).
  - Catalog namespace management, table existence checks, and idempotent initialization.
  - Transactional non-destructive schema evolution (`add_column`, `rename_column`).
- **`tests/test_smoke_services.py` (2 tests):**
  - HTTP health probe resilience and multi-service connectivity reporting.

---

## 6. Senior Technical Interview Defense Guide

### Topic 1: Open Table Formats — Apache Iceberg vs. Delta Lake vs. Apache Hudi

| Dimension | Apache Iceberg | Delta Lake | Apache Hudi |
| :--- | :--- | :--- | :--- |
| **Metadata Architecture** | Tree of immutable JSON metadata $\to$ Manifest Lists $\to$ Manifests $\to$ Data Files. | Transaction Log (JSON commits in `_delta_log/`) checkpointed into Parquet. | Timeline metadata files with commit states stored in `.hoodie/`. |
| **Partitioning Design** | **Hidden Partitioning:** Partitions are defined as transforms on columns; queries don't need to specify partition columns. | Explicit Directory Partitioning (`/col=val/`). Partition changes require physical data restructuring. | Explicit directory-based partitioning; supports virtual key generation. |
| **Engine Neutrality** | **First-Class Multi-Engine:** Purpose-built for Trino, Spark, Flink, StarRocks, DuckDB, Snowflake via open REST Catalog. | Historically Spark-centric (UniForm bridge adds metadata translations for Iceberg/Hudi). | Spark and Flink-centric; secondary support on Trino/Presto. |
| **ACID Isolation** | Optimistic Concurrency Control (OCC) with atomic catalog snapshot commit swaps. | Optimistic Concurrency Control (OCC) managed through storage driver / catalog. | OCC with multi-version concurrency control (MVCC) designed for high-frequency streaming upserts. |
| **Evolution Capabilities** | Full in-place schema, partition spec, and sort-order evolution without data rewrites. | Schema evolution supported; partition evolution requires full data migration. | Schema evolution supported via Avro schemas; partition evolution is limited. |

**Interview Defense:**
> *"We selected Apache Iceberg because of its complete engine independence and hidden partitioning. Unlike Delta Lake, which historically couples partition layout to physical directory structures (`/year=2024/month=01/`), Iceberg abstracts partitioning into a metadata function over field IDs. If our query patterns shift from daily to monthly partitioning, we evolve the partition spec metadata instantly without rewriting gigabytes of Parquet files on S3. Furthermore, Iceberg's REST catalog specification allows Trino, DuckDB, and PyIceberg to access the exact same snapshot state via standard HTTP APIs."*

---

### Topic 2: Apache Iceberg REST Catalog Architecture

Traditional Hive Metastore (HMS) setups bottlenecked lakehouse scaling because they relied on file-system directory listings and database locks over relational tables.

```
┌─────────────────┐       GET /v1/namespaces/nyc_taxi/tables/yellow_trips
│  Trino / Client │ ────────────────────────────────────────────────────────┐
└─────────────────┘                                                         │
                                                                            ▼
                                                             ┌─────────────────────────────┐
                                                             │ Iceberg REST Catalog Server │
                                                             │   (tabular/iceberg-rest)    │
                                                             └──────────────┬──────────────┘
                                                                            │ Read Pointer
                                                                            ▼
                                                             ┌─────────────────────────────┐
                                                             │     PostgreSQL Backend      │
                                                             │  (Namespace & Table Index)  │
                                                             └──────────────┬──────────────┘
                                                                            │ Returns Pointer:
                                                                            │ s3a://warehouse/.../v2.metadata.json
                                                                            ▼
┌─────────────────┐       Direct Range GET to s3a://warehouse/...   ┌─────────────────────────────┐
│  Trino / Client │ ───────────────────────────────────────────────► │          MinIO S3           │
└─────────────────┘                                                  └─────────────────────────────┘
```

1. **Decoupled Control vs. Data Plane:** The REST Catalog never touches data files. It only tracks the URI of the current `metadata.json` file.
2. **Atomic Compare-And-Swap (CAS):** During a commit, the client presents the previous snapshot ID and the new snapshot metadata. The catalog validates no concurrent commit occurred and atomically updates the pointer in PostgreSQL. If a conflict occurs, the client refreshes metadata and retries.
3. **Storage Independence:** The catalog provides token vending and S3 endpoint configuration dynamically, abstracting storage topology from client engines.

---

### Topic 3: Distributed MPP Query Mechanics in Trino

Trino does not use MapReduce or write intermediate shuffle files to disk; it uses an in-memory, pipelined Massively Parallel Processing (MPP) execution engine:

1. **Coordinator Optimization:**
   - Parses ANSI SQL into an Abstract Syntax Tree (AST).
   - Contacts the Iceberg REST catalog to retrieve the current table snapshot pointer.
   - Reads manifest files directly from MinIO and evaluates predicate statistics (Min/Max values, null counts) to generate execution splits.
2. **Split Assignment & Pipelining:**
   - Each Parquet file (or block within a file) becomes a `Split`.
   - The coordinator schedules splits across active `Trino Worker` nodes based on network locality and cluster load.
3. **Streaming In-Memory Operators:**
   - Workers execute columnar vector processing via Java bytecode generation.
   - Hash joins and aggregations stream over memory buffers between worker stages using HTTP/REST streaming without disk serialization.

---

### Topic 4: Orchestration Trade-Offs — Prefect vs. Apache Airflow

| Architectural Dimension | Prefect (Modern Lakehouse) | Apache Airflow (Traditional Batch) |
| :--- | :--- | :--- |
| **Execution Paradigm** | Native Python code as workflows (`@flow`, `@task`); code executes dynamically like standard Python. | Static Directed Acyclic Graph (DAG) parsed repeatedly by a centralized scheduler process. |
| **Data In-Memory Handoff** | Returns native Python objects (e.g. `pyarrow.Table`) directly between tasks in the same process worker. | XCom relies on metadata database or external S3 blobs; inefficient for memory-heavy table pointers. |
| **Dynamic Task Mapping** | First-class `.map()` evaluates dynamically at runtime based on upstream payload lists. | Dynamic DAG generation or `expand()` requires scheduler re-serialization cycles. |
| **Deployment Footprint** | Lightweight worker pools and ephemeral runners; zero heavyweight Celery/RabbitMQ dependencies required. | Requires Webserver, Scheduler, Celery Workers, RabbitMQ/Redis, and PostgreSQL. |
| **Developer Ergonomics** | Flows and tasks are standard unit-testable Python functions; tests run via `pytest` without spinning up servers. | Requires specialized Airflow test fixtures, mock contexts, and DAG bag parsers. |

**Interview Defense:**
> *"We utilized Prefect for this lakehouse pipeline because of its native handling of in-memory data structures and dynamic task mapping. In our ingestion flow, DuckDB extracts raw files into PyArrow tables in memory. With Prefect, that PyArrow table reference is passed directly to the `@task enforce_data_quality` and `@task commit_to_iceberg` tasks without serializing intermediate states to disk or abusing an Airflow XCom database. Furthermore, Prefect's lightweight worker architecture drastically reduces infrastructure overhead while offering out-of-the-box exponential retry backoffs with jitter."*

---

## 7. Operational Failure Modes & Production Hardening

| Failure Scenario | Root Cause | Built-in Mitigation |
| :--- | :--- | :--- |
| **Concurrent Writer Conflict** | Two workers commit snapshots to the same Iceberg table simultaneously. | Iceberg uses Optimistic Concurrency Control. The second commit fails the CAS check, automatically re-reads the updated manifest list, checks for conflict overlaps, and rebases cleanly. |
| **Network Hiccup during S3 Append** | Transient MinIO / S3 latency or dropped socket during multipart Parquet upload. | Prefect task `@task commit_to_iceberg` implements exponential backoff retries (`retries=3, delay=[3, 8, 15]`). If uncommitted, unreferenced Parquet files are safely cleaned up by the maintenance flow. |
| **Silent Data Corruption / Schema Drift** | Source upstream modifies data types or introduces anomalous null values. | Prefect flow halts at `@task enforce_data_quality` before snapshot generation; zero bad records enter the Iceberg table or Trino analytics. |
| **Discrepancy Between S3 & Query Engine** | Query engine reads an uncommitted snapshot or cached stale metadata. | `@task reconcile_pipeline_row_counts` performs a 3-way check (`Raw == Committed == Trino`). Pipeline fails with `RowReconciliationError` if Trino does not return the exact row count committed. |
