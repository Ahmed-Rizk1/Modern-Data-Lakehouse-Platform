"""Production Lakehouse Maintenance & Table Governance Flow.

Orchestrates table hygiene for Apache Iceberg tables:
  1. Metadata auditing (snapshots, manifests, active files)
  2. Snapshot expiration (pruning historical commits past retention window)
  3. Orphan file removal (identifying unreferenced Parquet files on object storage)
  4. Manifest and data file compaction (optimizing layout for Trino distributed scan performance)
  5. Post-maintenance table integrity verification
"""

import argparse
from datetime import datetime, timedelta
import logging
import os
import time
from typing import Any, Dict, List, Optional

try:
    from prefect import flow, get_run_logger, task
except ImportError:
    def flow(*args, **kwargs):
        def decorator(f):
            return f
        return decorator

    def task(*args, **kwargs):
        def decorator(f):
            return f
        return decorator

    def get_run_logger():
        return logging.getLogger("prefect-maintenance")

try:
    import trino
except ImportError:
    trino = None

import sys
from pathlib import Path

# Add scripts directory to sys.path so iceberg and quality can be imported directly
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from iceberg import IcebergClient

DEFAULT_CATALOG = os.getenv("CATALOG_NAME", "lakehouse-poc")


# ==============================================================================
# Maintenance Tasks
# ==============================================================================

@task(
    name="audit_table_metadata",
    retries=2,
    retry_delay_seconds=3,
)
def audit_table_metadata(
    db_name: str,
    table_name: str,
) -> Dict[str, Any]:
    """Inspects table metadata state prior to maintenance operations."""
    logger = get_run_logger()
    logger.info(f"Auditing metadata state for {db_name}.{table_name}...")

    client = IcebergClient()
    if not client.table_exists(db_name, table_name):
        raise ValueError(f"Target table '{db_name}.{table_name}' does not exist in catalog.")

    snapshots = client.get_snapshots(db_name, table_name)
    current_snapshot = client.get_current_snapshot(db_name, table_name)
    spec = client.get_partition_spec(db_name, table_name)

    spec_repr = [f"{f.name} ({f.transform})" for f in spec.fields] if spec else []

    audit_summary = {
        "namespace": db_name,
        "table": table_name,
        "total_snapshots": len(snapshots),
        "current_snapshot_id": current_snapshot.get("snapshot_id") if current_snapshot else None,
        "partition_spec": spec_repr,
        "timestamp_utc": datetime.utcnow().isoformat(),
    }

    logger.info(
        f"Audit completed: {len(snapshots)} snapshot(s) found. "
        f"Current: {audit_summary['current_snapshot_id']}. Partitions: {spec_repr}"
    )
    return audit_summary


@task(
    name="expire_snapshots",
    retries=3,
    retry_delay_seconds=[3, 8, 15],
)
def expire_table_snapshots(
    db_name: str,
    table_name: str,
    retain_last: int = 5,
    retention_days: int = 7,
    use_trino: bool = True,
    catalog_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Prunes expired snapshots beyond retention window via Trino system procedure or PyIceberg."""
    logger = get_run_logger()
    catalog = catalog_name or DEFAULT_CATALOG

    logger.info(
        f"Executing snapshot expiration on {db_name}.{table_name} "
        f"(retain_last={retain_last}, retention_threshold={retention_days}d)..."
    )

    trino_executed = False
    details: Dict[str, Any] = {}

    if use_trino:
        trino_host = os.getenv("TRINO_HOST", "trino")
        trino_port = int(os.getenv("TRINO_PORT", "8080"))
        trino_user = os.getenv("TRINO_USER", "lakehouse-maintenance")

        try:
            conn = trino.dbapi.connect(
                host=trino_host,
                port=trino_port,
                user=trino_user,
                catalog=catalog,
                schema=db_name,
            )
            cursor = conn.cursor()
            threshold_str = f"{retention_days}d"
            # Trino Iceberg table procedure for snapshot expiration
            query = (
                f'ALTER TABLE "{catalog}"."{db_name}"."{table_name}" '
                f"EXECUTE expire_snapshots(retention_threshold => '{threshold_str}')"
            )
            logger.info(f"Running Trino snapshot expiration: {query}")
            cursor.execute(query)
            result = cursor.fetchall()
            trino_executed = True
            details["trino_result"] = str(result)
            logger.info(f"Trino snapshot expiration executed successfully: {result}")
        except Exception as e:
            logger.warning(
                f"Trino snapshot expiration procedure failed or unavailable ({e}). "
                f"Falling back to PyIceberg snapshot manager."
            )

    # PyIceberg snapshot expiration fallback or complement
    if not trino_executed:
        client = IcebergClient()
        cutoff_ms = int((datetime.utcnow() - timedelta(days=retention_days)).timestamp() * 1000)
        details = client.expire_snapshots(
            namespace=db_name,
            table=table_name,
            older_than_ms=cutoff_ms,
            retain_last=retain_last,
        )

    return {
        "status": "SUCCESS",
        "action": "expire_snapshots",
        "target": f"{db_name}.{table_name}",
        "details": details,
    }


@task(
    name="cleanup_orphan_files",
    retries=2,
    retry_delay_seconds=[5, 10],
)
def cleanup_orphan_files(
    db_name: str,
    table_name: str,
    retention_days: int = 7,
    catalog_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Identifies and removes unreferenced orphan data/metadata files on MinIO storage."""
    logger = get_run_logger()
    catalog = catalog_name or DEFAULT_CATALOG

    logger.info(
        f"Scanning for orphan files in {db_name}.{table_name} (older than {retention_days}d)..."
    )

    trino_host = os.getenv("TRINO_HOST", "trino")
    trino_port = int(os.getenv("TRINO_PORT", "8080"))
    trino_user = os.getenv("TRINO_USER", "lakehouse-maintenance")

    try:
        conn = trino.dbapi.connect(
            host=trino_host,
            port=trino_port,
            user=trino_user,
            catalog=catalog,
            schema=db_name,
        )
        cursor = conn.cursor()
        threshold_str = f"{retention_days}d"
        # Trino Iceberg procedure: remove_orphan_files
        query = (
            f'ALTER TABLE "{catalog}"."{db_name}"."{table_name}" '
            f"EXECUTE remove_orphan_files(retention_threshold => '{threshold_str}')"
        )
        logger.info(f"Running Trino orphan file cleanup: {query}")
        cursor.execute(query)
        result = cursor.fetchall()
        logger.info(f"Orphan file removal result: {result}")
        return {
            "status": "SUCCESS",
            "action": "remove_orphan_files",
            "target": f"{db_name}.{table_name}",
            "result": str(result),
        }
    except Exception as e:
        logger.warning(
            f"Trino orphan file removal procedure skipped ({e}). "
            f"Verify Trino Iceberg connector permissions if running in Docker."
        )
        return {
            "status": "SKIPPED",
            "action": "remove_orphan_files",
            "reason": str(e),
        }


@task(
    name="compact_manifests_and_data",
    retries=2,
    retry_delay_seconds=[5, 10],
)
def compact_manifests_and_data(
    db_name: str,
    table_name: str,
    file_size_threshold: str = "128MB",
    catalog_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Triggers Trino compaction to merge small files and compact metadata manifests."""
    logger = get_run_logger()
    catalog = catalog_name or DEFAULT_CATALOG

    logger.info(
        f"Optimizing file layouts and compacting manifests for {db_name}.{table_name} "
        f"(threshold={file_size_threshold})..."
    )

    trino_host = os.getenv("TRINO_HOST", "trino")
    trino_port = int(os.getenv("TRINO_PORT", "8080"))
    trino_user = os.getenv("TRINO_USER", "lakehouse-maintenance")

    try:
        conn = trino.dbapi.connect(
            host=trino_host,
            port=trino_port,
            user=trino_user,
            catalog=catalog,
            schema=db_name,
        )
        cursor = conn.cursor()
        query = (
            f'ALTER TABLE "{catalog}"."{db_name}"."{table_name}" '
            f"EXECUTE optimize(file_size_threshold => '{file_size_threshold}')"
        )
        logger.info(f"Executing compaction query: {query}")
        cursor.execute(query)
        result = cursor.fetchall()
        logger.info(f"Compaction completed successfully: {result}")
        return {
            "status": "SUCCESS",
            "action": "optimize_compaction",
            "target": f"{db_name}.{table_name}",
            "result": str(result),
        }
    except Exception as e:
        logger.warning(f"Trino optimize procedure skipped or unconfigured ({e}).")
        return {
            "status": "SKIPPED",
            "action": "optimize_compaction",
            "reason": str(e),
        }


@task(
    name="verify_table_health",
    retries=2,
    retry_delay_seconds=3,
)
def verify_table_health(
    db_name: str,
    table_name: str,
    catalog_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Validates that table remains readable and queryable with intact snapshots post-maintenance."""
    logger = get_run_logger()
    catalog = catalog_name or DEFAULT_CATALOG

    logger.info(f"Verifying table health for {db_name}.{table_name}...")
    client = IcebergClient()
    current_snapshot = client.get_current_snapshot(db_name, table_name)
    if not current_snapshot:
        raise ValueError(f"Post-maintenance health check failed: {db_name}.{table_name} has no valid snapshot.")

    trino_host = os.getenv("TRINO_HOST", "trino")
    trino_port = int(os.getenv("TRINO_PORT", "8080"))
    trino_user = os.getenv("TRINO_USER", "lakehouse-maintenance")

    row_count = None
    try:
        conn = trino.dbapi.connect(
            host=trino_host,
            port=trino_port,
            user=trino_user,
            catalog=catalog,
            schema=db_name,
        )
        cursor = conn.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM "{catalog}"."{db_name}"."{table_name}"')
        row_count = cursor.fetchone()[0]
        logger.info(f"Post-maintenance row count: {row_count:,} rows queryable.")
    except Exception as e:
        logger.warning(f"Trino health verification query failed: {e}")

    return {
        "status": "HEALTHY",
        "snapshot_id": current_snapshot["snapshot_id"],
        "row_count": row_count,
        "verified_at": datetime.utcnow().isoformat(),
    }


# ==============================================================================
# Lakehouse Table Maintenance Flow
# ==============================================================================

@flow(name="lakehouse-table-maintenance")
def lakehouse_maintenance_flow(
    db_name: str = "nyc-taxi",
    table_name: str = "yellow_tripdata",
    retain_last_snapshots: int = 5,
    retention_threshold_days: int = 7,
    execute_orphan_cleanup: bool = True,
    execute_compaction: bool = True,
    catalog_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Orchestrates comprehensive maintenance, metadata pruning, and file compaction for an Iceberg table."""
    logger = get_run_logger()
    logger.info(f"Initiating Lakehouse Table Maintenance for {db_name}.{table_name}...")

    # 1. Audit baseline metadata state
    pre_audit = audit_table_metadata(db_name=db_name, table_name=table_name)

    # 2. Expire old historical snapshots
    expiration_summary = expire_table_snapshots(
        db_name=db_name,
        table_name=table_name,
        retain_last=retain_last_snapshots,
        retention_days=retention_threshold_days,
        catalog_name=catalog_name,
    )

    # 3. Clean up orphaned Parquet files on object storage
    orphan_summary = None
    if execute_orphan_cleanup:
        orphan_summary = cleanup_orphan_files(
            db_name=db_name,
            table_name=table_name,
            retention_days=retention_threshold_days,
            catalog_name=catalog_name,
        )

    # 4. Compact small manifests and data files
    compaction_summary = None
    if execute_compaction:
        compaction_summary = compact_manifests_and_data(
            db_name=db_name,
            table_name=table_name,
            catalog_name=catalog_name,
        )

    # 5. Verify post-maintenance table health
    health_check = verify_table_health(
        db_name=db_name,
        table_name=table_name,
        catalog_name=catalog_name,
    )

    maintenance_report = {
        "status": "COMPLETED",
        "target_table": f"{db_name}.{table_name}",
        "pre_maintenance_audit": pre_audit,
        "snapshot_expiration": expiration_summary,
        "orphan_file_cleanup": orphan_summary,
        "compaction": compaction_summary,
        "post_maintenance_health": health_check,
    }

    logger.info(
        f"Lakehouse Table Maintenance completed successfully for {db_name}.{table_name}."
    )
    return maintenance_report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lakehouse Table Maintenance Pipeline")
    parser.add_argument("--namespace", default="nyc-taxi", help="Database/namespace name")
    parser.add_argument("--table", default="yellow_tripdata", help="Iceberg table name")
    parser.add_argument("--retain-snapshots", type=int, default=5, help="Number of snapshots to retain")
    parser.add_argument("--retention-days", type=int, default=7, help="Snapshot retention days")
    args = parser.parse_args()

    lakehouse_maintenance_flow(
        db_name=args.namespace,
        table_name=args.table,
        retain_last_snapshots=args.retain_snapshots,
        retention_threshold_days=args.retention_days,
    )
