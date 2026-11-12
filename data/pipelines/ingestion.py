from datetime import timedelta
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional, Union

try:
    import duckdb
except ImportError:
    duckdb = None
from pydantic import BaseModel
import pyarrow as pa
try:
    import trino
except ImportError:
    trino = None

try:
    from prefect import flow, get_run_logger, task, unmapped
    from prefect.tasks import task_input_hash
except ImportError:
    def flow(*args, **kwargs):
        def decorator(f):
            return f
        return decorator

    def task(*args, **kwargs):
        def decorator(f):
            f.map = lambda *m_args, **m_kwargs: [f(*item) if isinstance(item, tuple) else f(item) for item in m_args[0]]
            return f
        return decorator

    def unmapped(val):
        return val

    def get_run_logger():
        return logging.getLogger("prefect-ingestion")

    task_input_hash = None

# Ensure scripts directory is available for direct imports
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from iceberg import IcebergClient, day_partition_spec
from quality import (
    DataQualityEnforcer,
    DataQualityError,
    NYC_TAXI_CONTRACT,
    RowReconciliationError,
    TableContract,
    reconcile_row_counts,
)
from superset import SupersetConfigurator

DATA_FOLDER = os.getenv("LAKEHOUSE_DATA_PATH", "/lakehouse-poc")
DEFAULT_CATALOG_NAME = os.getenv("CATALOG_NAME", "lakehouse-poc")


class IngestionRequest(BaseModel):
    url: str
    db_name: str
    table_name: str
    has_header: Optional[bool] = True
    date_format: Optional[str] = None
    timestamp_format: Optional[str] = None
    normalize_column_names: Optional[bool] = True
    max_memory_gb: Optional[int] = 2
    partition_column: Optional[str] = "tpep_pickup_datetime"
    partition_transform: Optional[str] = "day"

    # Provide a stable representation for task caching
    def cache_key(self) -> str:
        return f"{self.url}:{self.db_name}:{self.table_name}"


default_ingestion_request = IngestionRequest(
    url=f"{DATA_FOLDER}/datasets/yellow_tripdata_2024-01.parquet",
    db_name="nyc-taxi",
    table_name="yellow_tripdata",
    partition_column="tpep_pickup_datetime",
    partition_transform="day",
)


# ==============================================================================
# Granular Prefect Tasks
# ==============================================================================

@task(
    name="validate_payload_contract",
    retries=3,
    retry_delay_seconds=[2, 5, 10],
    retry_jitter_factor=0.2,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=15),
)
def validate_payload_contract(request: IngestionRequest) -> Dict[str, Any]:
    """Validates the input file exists, has a supported format, and is non-empty."""
    logger = get_run_logger()
    logger.info(f"Validating payload contract for: {request.url}")

    path = Path(request.url)
    if not path.exists():
        raise FileNotFoundError(f"Source file does not exist at path: {request.url}")

    extension = path.suffix.lower()
    if extension not in [".parquet", ".csv"]:
        raise ValueError(
            f"Unsupported file format '{extension}'. Only .parquet and .csv are supported."
        )

    file_size_bytes = path.stat().st_size
    if file_size_bytes <= 0:
        raise ValueError(f"Payload file is empty (0 bytes): {request.url}")

    file_size_mb = round(file_size_bytes / (1024 * 1024), 2)
    logger.info(
        f"Contract verified for {path.name}: format={extension}, size={file_size_mb} MB"
    )

    return {
        "url": str(path),
        "filename": path.name,
        "extension": extension,
        "file_size_bytes": file_size_bytes,
        "file_size_mb": file_size_mb,
    }


@task(
    name="extract_to_arrow",
    retries=2,
    retry_delay_seconds=[3, 6],
    retry_jitter_factor=0.2,
)
def extract_to_arrow(request: IngestionRequest) -> pa.Table:
    """Extracts raw data via DuckDB with memory governance into an in-memory PyArrow Table."""
    logger = get_run_logger()
    logger.info(f"Extracting dataset into Arrow from: {request.url}")

    con = duckdb.connect(database=":memory:")
    try:
        mem_limit = f"{request.max_memory_gb or 2}GB"
        con.execute(f"PRAGMA max_memory='{mem_limit}';")
        con.execute("PRAGMA threads=4;")

        extension = Path(request.url).suffix.lower()
        if extension == ".csv":
            read_kwargs = {
                "header": request.has_header,
                "normalize_names": request.normalize_column_names,
            }
            if request.date_format:
                read_kwargs["date_format"] = request.date_format
            if request.timestamp_format:
                read_kwargs["timestamp_format"] = request.timestamp_format

            relation = con.read_csv(request.url, **read_kwargs)
        else:
            relation = con.read_parquet(request.url)

        arrow_table = relation.to_arrow_table()
    finally:
        con.close()

    logger.info(
        f"Extracted {arrow_table.num_rows:,} rows across {arrow_table.num_columns} columns "
        f"({round(arrow_table.nbytes / (1024 * 1024), 2)} MB Arrow memory)"
    )
    return arrow_table


@task(
    name="enforce_data_quality",
    retries=1,
)
def enforce_data_quality(arrow_table: pa.Table, request: IngestionRequest) -> Dict[str, Any]:
    """Applies declarative schema contracts, null limits, and bounds rules via quality.py."""
    logger = get_run_logger()
    logger.info(
        f"Enforcing declarative data quality contracts on {arrow_table.num_rows:,} rows for {request.table_name}..."
    )

    contract: Optional[TableContract] = None
    if "yellow_tripdata" in request.table_name:
        contract = NYC_TAXI_CONTRACT

    # Executes declarative quality checks; raises DataQualityError if violations found
    report = DataQualityEnforcer.validate(
        table=arrow_table,
        contract=contract,
        enforce=True,
    )

    logger.info(
        f"Data quality assertions PASSED for {request.table_name} ({report.row_count:,} rows)."
    )

    return {
        "status": report.status,
        "row_count": report.row_count,
        "column_count": report.column_count,
        "null_checks": report.null_checks,
        "bounds_checks": report.bounds_checks,
    }


@task(
    name="commit_to_iceberg",
    retries=3,
    retry_delay_seconds=[3, 8, 15],
    retry_jitter_factor=0.2,
)
def commit_to_iceberg(request: IngestionRequest, arrow_table: pa.Table) -> Dict[str, Any]:
    """Atomically commits the PyArrow dataset into an Apache Iceberg table with optional partitioning."""
    logger = get_run_logger()
    logger.info(f"Connecting to Iceberg REST Catalog for {request.db_name}.{request.table_name}...")

    iceberg = IcebergClient()

    # Idempotently create or verify namespace
    iceberg.create_namespace(
        request.db_name,
        properties={
            "customer_name": request.db_name,
            "created_by": "prefect-lakehouse-pipeline",
        },
        recreate_if_exists=False,
    )

    # Optional partition spec specification for new tables
    partition_spec = None
    if (
        request.partition_column
        and request.partition_column in arrow_table.column_names
        and not iceberg.table_exists(request.db_name, request.table_name)
    ):
        try:
            from pyiceberg.io.pyarrow import pyarrow_to_schema
            iceberg_schema = pyarrow_to_schema(arrow_table.schema)
            if request.partition_transform == "day":
                partition_spec = day_partition_spec(
                    schema=iceberg_schema,
                    timestamp_column=request.partition_column,
                )
                logger.info(
                    f"Configured Day partition spec on '{request.partition_column}' for {request.table_name}."
                )
        except Exception as e:
            logger.warning(f"Could not build partition spec ({e}); creating unpartitioned table.")

    # Load existing table or create new table with Arrow schema
    table = iceberg.load_or_create_table(
        namespace=request.db_name,
        table=request.table_name,
        schema=arrow_table.schema,
        partition_spec=partition_spec,
    )

    logger.info(f"Performing atomic append of {arrow_table.num_rows:,} rows to Iceberg table...")
    start_time = time.time()
    table.append(arrow_table)
    commit_duration = round(time.time() - start_time, 2)

    current_snapshot = getattr(table, "current_snapshot", lambda: None)()
    snapshot_id = current_snapshot.snapshot_id if current_snapshot else "unknown"

    logger.info(
        f"Iceberg ACID commit successful: Snapshot ID={snapshot_id} (took {commit_duration}s)"
    )

    return {
        "namespace": request.db_name,
        "table": request.table_name,
        "rows_committed": arrow_table.num_rows,
        "snapshot_id": snapshot_id,
        "commit_duration_seconds": commit_duration,
    }


@task(
    name="verify_trino_queryability",
    retries=4,
    retry_delay_seconds=[2, 5, 10, 20],
    retry_jitter_factor=0.2,
)
def verify_trino_queryability(
    request: IngestionRequest,
    expected_min_rows: int,
    catalog_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Executes a distributed SQL query against Trino to guarantee immediate queryability."""
    logger = get_run_logger()
    catalog = catalog_name or DEFAULT_CATALOG_NAME
    trino_host = os.getenv("TRINO_HOST", "trino")
    trino_port = int(os.getenv("TRINO_PORT", "8080"))
    trino_user = os.getenv("TRINO_USER", "lakehouse")

    logger.info(
        f"Verifying Trino queryability at {trino_host}:{trino_port} on catalog '{catalog}'..."
    )

    start_time = time.time()
    try:
        conn = trino.dbapi.connect(
            host=trino_host,
            port=trino_port,
            user=trino_user,
            catalog=catalog,
            schema=request.db_name,
        )
        cursor = conn.cursor()
        query = f'SELECT COUNT(*) FROM "{catalog}"."{request.db_name}"."{request.table_name}"'
        logger.info(f"Executing Trino verification query: {query}")
        cursor.execute(query)
        actual_rows = cursor.fetchone()[0]
        query_duration = round(time.time() - start_time, 2)

        logger.info(
            f"Trino query result: actual_rows={actual_rows:,} (expected >= {expected_min_rows:,}) in {query_duration}s"
        )

        return {
            "trino_query_status": "SUCCESS",
            "actual_rows": actual_rows,
            "expected_min_rows": expected_min_rows,
            "query_duration_seconds": query_duration,
        }
    except Exception as e:
        logger.warning(
            f"Trino verification warning: {e}. (Verify Trino service availability if running outside Docker)"
        )
        raise


@task(
    name="reconcile_pipeline_row_counts",
    retries=1,
)
def reconcile_pipeline_row_counts(
    raw_rows: int,
    commit_summary: Dict[str, Any],
    trino_summary: Dict[str, Any],
    table_identifier: str,
) -> Dict[str, Any]:
    """Enforces strict three-way reconciliation: Raw Ingested == Iceberg Committed == Trino Queryable."""
    logger = get_run_logger()
    logger.info(f"Enforcing three-way row reconciliation for {table_identifier}...")

    committed_rows = commit_summary["rows_committed"]
    trino_rows = trino_summary["actual_rows"]

    reconciliation_report = reconcile_row_counts(
        raw_rows=raw_rows,
        committed_rows=committed_rows,
        trino_rows=trino_rows,
        table_identifier=table_identifier,
    )

    logger.info(f"Reconciliation verified: {reconciliation_report}")
    return reconciliation_report


@task(
    name="sync_superset_bi",
    retries=3,
    retry_delay_seconds=[3, 6, 12],
    retry_jitter_factor=0.2,
)
def sync_superset_bi(
    request: IngestionRequest,
    superset_url: str = "http://superset:8088",
) -> Dict[str, Any]:
    """Provisions Superset BI datasets, analytics dashboards, and table charts."""
    logger = get_run_logger()
    logger.info(f"Synchronizing Superset BI at {superset_url}...")

    dashboard_name = f"LAKEHOUSE_POC.{request.db_name}.{request.table_name}"
    chart_name = f"{request.db_name}.{request.table_name}"

    configurator = SupersetConfigurator(superset_url)

    dataset = configurator.get_dataset(
        schema_name=request.db_name,
        table_name=request.table_name,
    )
    if dataset is None:
        dataset = configurator.create_dataset(
            schema_name=request.db_name,
            table_name=request.table_name,
        )
        logger.info(f"Superset dataset created for {request.table_name}.")

    dashboard = configurator.get_dashboard(dashboard_name)
    if dashboard is None:
        dashboard = configurator.create_dashboard(dashboard_name)
        logger.info(f"Superset dashboard created: {dashboard_name}")

    chart = configurator.get_table_chart(chart_name)
    if chart is None:
        chart = configurator.create_table_chart(
            name=chart_name,
            dataset_id=dataset["id"],
        )
        logger.info(f"Superset chart created: {chart_name}")
        configurator.add_table_chart_to_dashboard(dashboard, chart)
        logger.info("Chart attached to dashboard successfully.")

    return {
        "status": "SYNCED",
        "dashboard_name": dashboard_name,
        "chart_name": chart_name,
        "dataset_id": dataset.get("id"),
    }


# ==============================================================================
# Modular Flow with Dynamic Task Mapping (.map())
# ==============================================================================

@flow(name="lakehouse-ingestion-pipeline")
def data_to_dashboard(
    requests: Union[IngestionRequest, List[IngestionRequest]] = default_ingestion_request,
    superset_url: str = "http://superset:8088",
    verify_trino: bool = True,
    sync_bi: bool = True,
) -> Dict[str, Any]:
    """Orchestrates end-to-end lakehouse ingestion with dynamic task mapping, contracts, and three-way reconciliation."""
    logger = get_run_logger()

    request_list: List[IngestionRequest] = (
        requests if isinstance(requests, list) else [requests]
    )

    logger.info(
        f"Starting lakehouse ingestion pipeline for {len(request_list)} payload target(s)..."
    )

    # 1. Dynamic schema & payload contract validation
    validated_payloads = validate_payload_contract.map(request_list)

    # 2. Extract input sources to in-memory PyArrow tables
    extracted_tables = extract_to_arrow.map(request_list, wait_for=[validated_payloads])

    # 3. Enforce declarative data quality contracts before catalog mutation
    quality_reports = enforce_data_quality.map(extracted_tables, request_list)

    # 4. Atomic append to Apache Iceberg via REST catalog with partition specification
    commit_summaries = commit_to_iceberg.map(
        request_list,
        extracted_tables,
        wait_for=[quality_reports],
    )

    pipeline_results: Dict[str, Any] = {
        "status": "COMPLETED",
        "targets_processed": len(request_list),
        "commits": commit_summaries,
    }

    # 5. Distributed SQL verification & three-way row reconciliation via Trino
    if verify_trino:
        min_rows_list = [table.num_rows for table in extracted_tables]
        trino_summaries = verify_trino_queryability.map(
            request_list,
            min_rows_list,
            wait_for=[commit_summaries],
        )
        pipeline_results["trino_verifications"] = trino_summaries

        table_identifiers = [f"{req.db_name}.{req.table_name}" for req in request_list]
        raw_rows_list = [table.num_rows for table in extracted_tables]

        # Three-way reconciliation: Raw Ingested == Iceberg Committed == Trino Queryable
        reconciliations = reconcile_pipeline_row_counts.map(
            raw_rows_list,
            commit_summaries,
            trino_summaries,
            table_identifiers,
            wait_for=[trino_summaries],
        )
        pipeline_results["reconciliations"] = reconciliations

    # 6. BI metadata synchronization via Superset (optional, default True)
    if sync_bi:
        prior_deps = pipeline_results.get("reconciliations", commit_summaries)
        superset_summaries = sync_superset_bi.map(
            request_list,
            unmapped(superset_url),
            wait_for=[prior_deps],
        )
        pipeline_results["superset_syncs"] = superset_summaries

    logger.info(
        f"Lakehouse ingestion pipeline completed successfully for {len(request_list)} targets."
    )
    return pipeline_results


if __name__ == "__main__":
    data_to_dashboard(default_ingestion_request)
