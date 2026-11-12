"""Production PyIceberg Client & Partition Specification Engine.

Supports explicit table partitioning (Day, Month, Year, Identity, Bucket, Truncate)
to enable distributed Trino partition pruning, ACID snapshot inspection,
non-destructive schema evolution, and Lakehouse table maintenance.
"""

import logging
import re
import time
from typing import Any, Dict, List, Optional, Set, Union

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table import Table
    from pyiceberg.transforms import (
        BucketTransform,
        DayTransform,
        HourTransform,
        IdentityTransform,
        MonthTransform,
        TruncateTransform,
        YearTransform,
    )
    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False
    load_catalog = None
    Table = None
    Schema = None

    class TransformStub:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
        def __repr__(self):
            return f"{self.__class__.__name__}({self.kwargs})"

    class DayTransform(TransformStub): pass
    class MonthTransform(TransformStub): pass
    class YearTransform(TransformStub): pass
    class HourTransform(TransformStub): pass
    class IdentityTransform(TransformStub): pass
    class BucketTransform(TransformStub): pass
    class TruncateTransform(TransformStub): pass

    class PartitionField:
        def __init__(self, source_id, field_id, transform, name):
            self.source_id = source_id
            self.field_id = field_id
            self.transform = transform
            self.name = name
        def __repr__(self):
            return f"PartitionField(name={self.name}, source_id={self.source_id}, transform={self.transform})"

    class PartitionSpec:
        def __init__(self, *fields):
            self.fields = list(fields)
        def __repr__(self):
            return f"PartitionSpec({self.fields})"

logger = logging.getLogger(__name__)


# ==============================================================================
# Declarative Partition Specification Builders
# ==============================================================================

def parse_transform(transform_str: str):
    """Maps a transform string (e.g. 'day', 'month', 'year', 'identity', 'bucket[16]', 'truncate[10]')
    to its corresponding PyIceberg Transform instance.
    """
    clean_str = transform_str.strip().lower()

    if clean_str == "day":
        return DayTransform()
    elif clean_str == "month":
        return MonthTransform()
    elif clean_str == "year":
        return YearTransform()
    elif clean_str == "hour":
        return HourTransform()
    elif clean_str in ("identity", "none"):
        return IdentityTransform()

    bucket_match = re.match(r"bucket\[(\d+)\]", clean_str)
    if bucket_match:
        num_buckets = int(bucket_match.group(1))
        return BucketTransform(num_buckets=num_buckets)

    truncate_match = re.match(r"truncate\[(\d+)\]", clean_str)
    if truncate_match:
        width = int(truncate_match.group(1))
        return TruncateTransform(width=width)

    raise ValueError(
        f"Unsupported partition transform '{transform_str}'. "
        f"Supported: 'day', 'month', 'year', 'hour', 'identity', 'bucket[N]', 'truncate[N]'."
    )


def build_partition_spec(
    schema: Schema,
    partition_configs: List[Dict[str, Any]],
    start_field_id: int = 1000,
) -> PartitionSpec:
    """Builds an explicit Apache Iceberg PartitionSpec from declarative column definitions.
    
    Example partition_configs:
        [
            {"source_column": "tpep_pickup_datetime", "transform": "day", "target_name": "pickup_day"},
            {"source_column": "VendorID", "transform": "identity", "target_name": "vendor_id"},
        ]
    """
    fields: List[PartitionField] = []

    for index, config in enumerate(partition_configs):
        source_col = config["source_column"]
        field_in_schema = schema.find_field(source_col)
        if field_in_schema is None:
            raise KeyError(
                f"Cannot partition by '{source_col}': column not found in schema {schema}."
            )

        transform_obj = parse_transform(config.get("transform", "identity"))
        target_name = config.get("target_name") or f"{source_col}_{config.get('transform', 'part')}"
        partition_field_id = config.get("field_id", start_field_id + index)

        fields.append(
            PartitionField(
                source_id=field_in_schema.field_id,
                field_id=partition_field_id,
                transform=transform_obj,
                name=target_name,
            )
        )

    logger.info(
        f"Constructed PartitionSpec with {len(fields)} field(s): "
        f"{[f.name for f in fields]}"
    )
    return PartitionSpec(*fields)


def day_partition_spec(
    schema: Schema,
    timestamp_column: str,
    partition_name: Optional[str] = None,
) -> PartitionSpec:
    """Convenience builder: Partitions table by day on a timestamp column (e.g. for query pruning)."""
    return build_partition_spec(
        schema=schema,
        partition_configs=[
            {
                "source_column": timestamp_column,
                "transform": "day",
                "target_name": partition_name or f"{timestamp_column}_day",
            }
        ],
    )


def month_partition_spec(
    schema: Schema,
    timestamp_column: str,
    partition_name: Optional[str] = None,
) -> PartitionSpec:
    """Convenience builder: Partitions table by month on a timestamp column."""
    return build_partition_spec(
        schema=schema,
        partition_configs=[
            {
                "source_column": timestamp_column,
                "transform": "month",
                "target_name": partition_name or f"{timestamp_column}_month",
            }
        ],
    )


def identity_partition_spec(
    schema: Schema,
    column_name: str,
    partition_name: Optional[str] = None,
) -> PartitionSpec:
    """Convenience builder: Partitions table by exact identity value of a column."""
    return build_partition_spec(
        schema=schema,
        partition_configs=[
            {
                "source_column": column_name,
                "transform": "identity",
                "target_name": partition_name or column_name,
            }
        ],
    )


# ==============================================================================
# Production Iceberg Client
# ==============================================================================

class IcebergClient:
    """Production client wrapping PyIceberg catalog operations for the modern lakehouse."""

    def __init__(self, location: Optional[str] = "s3a://warehouse/"):
        self._catalog = None
        self.location = location

    @property
    def catalog(self):
        if self._catalog is None:
            self._catalog = load_catalog()
        return self._catalog

    @property
    def namespaces(self) -> Set[str]:
        try:
            return {x[0] for x in self.catalog.list_namespaces()}
        except Exception as e:
            logger.warning(f"Failed to list namespaces: {e}")
            return set()

    def namespace_properties(self, namespace: str) -> Dict:
        return dict(
            name=namespace,
            properties=self.catalog.load_namespace_properties(namespace),
        )

    def delete_namespace(self, namespace: str):
        try:
            for table in self.catalog.list_tables(namespace=namespace):
                logger.info(f"Dropping table {table} in namespace {namespace}...")
                self.catalog.drop_table(table)
            self.catalog.drop_namespace(namespace=namespace)
            logger.info(f"Dropped namespace: {namespace}")
        except Exception as e:
            logger.warning(f"Error dropping namespace {namespace}: {e}")

    def create_namespace(
        self,
        namespace: str,
        properties: Optional[Dict] = None,
        recreate_if_exists: bool = False,
    ):
        if namespace in self.namespaces:
            logger.info(f"Namespace '{namespace}' already exists.")
            if recreate_if_exists:
                logger.info(f"Recreating namespace '{namespace}'...")
                self.delete_namespace(namespace)
            else:
                return

        self.catalog.create_namespace(
            namespace=namespace,
            properties=properties or {},
        )
        logger.info(f"Namespace created: <{namespace}>")

    def table_exists(self, namespace: str, table: str) -> bool:
        try:
            return self.catalog.table_exists(f"{namespace}.{table}")
        except Exception:
            return False

    def load_table(self, namespace: str, table: str) -> Table:
        identifier = f"{namespace}.{table}"
        logger.info(f"Loading Iceberg table: {identifier}")
        return self.catalog.load_table(identifier)

    def create_table(
        self,
        namespace: str,
        table: str,
        schema: Schema,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> Table:
        identifier = f"{namespace}.{table}"
        location = f"{self.location.rstrip('/')}/{namespace}/{table}"
        logger.info(f"Creating Iceberg table: {identifier} at location: {location}")

        kwargs: Dict[str, Any] = {
            "identifier": identifier,
            "location": location,
            "schema": schema,
        }
        if partition_spec is not None:
            kwargs["partition_spec"] = partition_spec
            logger.info(f"Applied partition spec: {partition_spec}")
        if properties:
            kwargs["properties"] = properties

        return self.catalog.create_table(**kwargs)

    def load_or_create_table(
        self,
        namespace: str,
        table: str,
        schema: Schema,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> Table:
        identifier = f"{namespace}.{table}"
        if self.table_exists(namespace, table):
            logger.info(f"Found existing table: {identifier}")
            return self.load_table(namespace, table)
        return self.create_table(
            namespace=namespace,
            table=table,
            schema=schema,
            partition_spec=partition_spec,
            properties=properties,
        )

    # --------------------------------------------------------------------------
    # Snapshot & Metadata Inspection
    # --------------------------------------------------------------------------

    def get_snapshots(self, namespace: str, table: str) -> List[Dict[str, Any]]:
        """Returns ordered list of all committed snapshots for the given table."""
        tbl = self.load_table(namespace, table)
        snapshot_records = []
        for snap in tbl.snapshots():
            snapshot_records.append(
                {
                    "snapshot_id": snap.snapshot_id,
                    "parent_snapshot_id": snap.parent_snapshot_id,
                    "sequence_number": snap.sequence_number,
                    "timestamp_ms": snap.timestamp_ms,
                    "manifest_list": snap.manifest_list,
                    "summary": dict(snap.summary) if snap.summary else {},
                }
            )
        return snapshot_records

    def get_current_snapshot(self, namespace: str, table: str) -> Optional[Dict[str, Any]]:
        """Returns details for the current active snapshot pointer."""
        tbl = self.load_table(namespace, table)
        current = tbl.current_snapshot()
        if not current:
            return None
        return {
            "snapshot_id": current.snapshot_id,
            "parent_snapshot_id": current.parent_snapshot_id,
            "sequence_number": current.sequence_number,
            "timestamp_ms": current.timestamp_ms,
            "manifest_list": current.manifest_list,
            "summary": dict(current.summary) if current.summary else {},
        }

    def get_partition_spec(self, namespace: str, table: str) -> Optional[PartitionSpec]:
        """Returns current partition specification of the table."""
        tbl = self.load_table(namespace, table)
        return tbl.spec()

    # --------------------------------------------------------------------------
    # Non-Destructive Schema Evolution (In-Place, Zero Data Rewrite)
    # --------------------------------------------------------------------------

    def add_column(
        self,
        namespace: str,
        table: str,
        name: str,
        field_type: Any,
        doc: Optional[str] = None,
        required: bool = False,
    ):
        """Adds a new column to the table schema without rewriting existing Parquet data files."""
        tbl = self.load_table(namespace, table)
        logger.info(f"Evolving schema for {namespace}.{table}: adding column '{name}'")
        with tbl.update_schema() as update:
            update.add_column(path=name, field_type=field_type, doc=doc, required=required)
        logger.info(f"Column '{name}' added successfully to {namespace}.{table}.")

    def rename_column(self, namespace: str, table: str, old_name: str, new_name: str):
        """Renames an existing column in-place without touching underlying Parquet storage."""
        tbl = self.load_table(namespace, table)
        logger.info(f"Evolving schema for {namespace}.{table}: renaming '{old_name}' -> '{new_name}'")
        with tbl.update_schema() as update:
            update.rename_column(old_name, new_name)
        logger.info(f"Column renamed '{old_name}' -> '{new_name}' in {namespace}.{table}.")

    # --------------------------------------------------------------------------
    # Table Maintenance & Hygiene
    # --------------------------------------------------------------------------

    def expire_snapshots(
        self,
        namespace: str,
        table: str,
        older_than_ms: Optional[int] = None,
        retain_last: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Expires historical snapshots beyond retention threshold to reclaim storage."""
        tbl = self.load_table(namespace, table)
        initial_snapshots = len(tbl.snapshots())

        # If retain_last is specified, calculate timestamp threshold or snapshot list
        target_timestamp = older_than_ms
        if target_timestamp is None and retain_last is not None:
            snaps = tbl.snapshots()
            if len(snaps) > retain_last:
                # Snapshots sorted chronologically; find cutoff timestamp
                cutoff_snap = snaps[-retain_last]
                target_timestamp = cutoff_snap.timestamp_ms

        if target_timestamp is not None:
            logger.info(
                f"Expiring snapshots for {namespace}.{table} older than {target_timestamp} ms..."
            )
            with tbl.manage_snapshots() as ms:
                ms.expire_older_than(older_than_ms=target_timestamp)

        remaining_snapshots = len(tbl.snapshots())
        pruned_count = initial_snapshots - remaining_snapshots

        logger.info(
            f"Snapshot expiration completed for {namespace}.{table}: "
            f"pruned {pruned_count} snapshot(s), {remaining_snapshots} remaining."
        )

        return {
            "namespace": namespace,
            "table": table,
            "initial_snapshots": initial_snapshots,
            "pruned_snapshots": pruned_count,
            "remaining_snapshots": remaining_snapshots,
        }
