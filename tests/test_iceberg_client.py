"""Unit Tests for Iceberg Client, Partition Spec Builders, and Schema Evolution.

Tests catalog operations, table creation, partition specs, and schema migrations
using mock catalog/table backends to enable fast, zero-container execution.
"""

from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

import pytest

# Ensure data/scripts is on sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = BASE_DIR / "data" / "scripts"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from iceberg import (
    IcebergClient,
    build_partition_spec,
    day_partition_spec,
    identity_partition_spec,
    month_partition_spec,
    parse_transform,
)


class MockSchemaField:
    """Mock representing an Iceberg NestedField."""
    def __init__(self, field_id: int, name: str):
        self.field_id = field_id
        self.name = name


class MockIcebergSchema:
    """Mock representing an Iceberg Schema."""
    def __init__(self, fields):
        self.fields = fields

    def find_field(self, name: str):
        for f in self.fields:
            if f.name == name:
                return f
        return None


class TestPartitionSpecEngine(unittest.TestCase):
    """Tests partition transformation parsers and partition specification builders."""

    def setUp(self):
        self.schema = MockIcebergSchema([
            MockSchemaField(1, "VendorID"),
            MockSchemaField(2, "tpep_pickup_datetime"),
            MockSchemaField(3, "trip_distance"),
        ])

    def test_parse_transforms(self):
        t_day = parse_transform("day")
        self.assertEqual(t_day.__class__.__name__, "DayTransform")

        t_month = parse_transform("month")
        self.assertEqual(t_month.__class__.__name__, "MonthTransform")

        t_year = parse_transform("year")
        self.assertEqual(t_year.__class__.__name__, "YearTransform")

        t_hour = parse_transform("hour")
        self.assertEqual(t_hour.__class__.__name__, "HourTransform")

        t_ident = parse_transform("identity")
        self.assertEqual(t_ident.__class__.__name__, "IdentityTransform")

        t_bucket = parse_transform("bucket[16]")
        self.assertEqual(t_bucket.__class__.__name__, "BucketTransform")

        t_truncate = parse_transform("truncate[10]")
        self.assertEqual(t_truncate.__class__.__name__, "TruncateTransform")

    def test_day_partition_spec_generation(self):
        spec = day_partition_spec(
            schema=self.schema,
            timestamp_column="tpep_pickup_datetime",
            partition_name="pickup_day",
        )
        self.assertIsNotNone(spec)
        self.assertEqual(len(spec.fields), 1)
        field = spec.fields[0]
        self.assertEqual(field.source_id, 2)
        self.assertEqual(field.field_id, 1000)
        self.assertEqual(field.name, "pickup_day")
        self.assertEqual(field.transform.__class__.__name__, "DayTransform")

    def test_month_partition_spec_generation(self):
        spec = month_partition_spec(
            schema=self.schema,
            timestamp_column="tpep_pickup_datetime",
        )
        field = spec.fields[0]
        self.assertEqual(field.source_id, 2)
        self.assertEqual(field.name, "tpep_pickup_datetime_month")
        self.assertEqual(field.transform.__class__.__name__, "MonthTransform")

    def test_identity_partition_spec_generation(self):
        spec = identity_partition_spec(
            schema=self.schema,
            column_name="VendorID",
        )
        field = spec.fields[0]
        self.assertEqual(field.source_id, 1)
        self.assertEqual(field.name, "VendorID")
        self.assertEqual(field.transform.__class__.__name__, "IdentityTransform")

    def test_partition_spec_missing_column_raises_error(self):
        with pytest.raises(KeyError, match="Cannot partition by 'non_existent_col'"):
            day_partition_spec(
                schema=self.schema,
                timestamp_column="non_existent_col",
            )


class TestIcebergClient(unittest.TestCase):
    """Tests the IcebergClient operations with mocked PyIceberg catalog backend."""

    def setUp(self):
        self.client = IcebergClient(location="s3a://test-warehouse/")
        self.mock_catalog = MagicMock()
        self.client._catalog = self.mock_catalog

    def test_namespace_management(self):
        self.mock_catalog.list_namespaces.return_value = [("nyc_taxi",), ("analytics",)]

        self.assertEqual(self.client.namespaces, {"nyc_taxi", "analytics"})

        # Existing namespace should not recreate unless specified
        self.client.create_namespace("nyc_taxi", recreate_if_exists=False)
        self.mock_catalog.create_namespace.assert_not_called()

        # New namespace should invoke catalog.create_namespace
        self.client.create_namespace("finance")
        self.mock_catalog.create_namespace.assert_called_with(
            namespace="finance", properties={}
        )

    def test_table_exists(self):
        self.mock_catalog.table_exists.return_value = True
        self.assertTrue(self.client.table_exists("nyc_taxi", "yellow_trips"))
        self.mock_catalog.table_exists.assert_called_with("nyc_taxi.yellow_trips")

    def test_create_table_with_partition_spec(self):
        mock_schema = MagicMock()
        mock_spec = MagicMock()

        self.client.create_table(
            namespace="nyc_taxi",
            table="yellow_trips",
            schema=mock_schema,
            partition_spec=mock_spec,
            properties={"write.parquet.compression-codec": "zstd"},
        )

        self.mock_catalog.create_table.assert_called_once_with(
            identifier="nyc_taxi.yellow_trips",
            location="s3a://test-warehouse/nyc_taxi/yellow_trips",
            schema=mock_schema,
            partition_spec=mock_spec,
            properties={"write.parquet.compression-codec": "zstd"},
        )

    def test_load_or_create_table_branching(self):
        mock_schema = MagicMock()
        mock_table = MagicMock()

        # Branch 1: Table exists -> loads existing
        self.mock_catalog.table_exists.return_value = True
        self.mock_catalog.load_table.return_value = mock_table

        loaded_table = self.client.load_or_create_table(
            namespace="nyc_taxi",
            table="yellow_trips",
            schema=mock_schema,
        )
        self.assertEqual(loaded_table, mock_table)
        self.mock_catalog.load_table.assert_called_with("nyc_taxi.yellow_trips")
        self.mock_catalog.create_table.assert_not_called()

        # Branch 2: Table does not exist -> creates new table
        self.mock_catalog.reset_mock()
        self.mock_catalog.table_exists.return_value = False
        self.mock_catalog.list_namespaces.return_value = [("nyc_taxi",)]
        self.mock_catalog.create_table.return_value = mock_table

        created_table = self.client.load_or_create_table(
            namespace="nyc_taxi",
            table="yellow_trips",
            schema=mock_schema,
        )
        self.assertEqual(created_table, mock_table)
        self.mock_catalog.create_table.assert_called_once()

    def test_non_destructive_schema_evolution(self):
        mock_table = MagicMock()
        mock_update = MagicMock()
        mock_table.update_schema.return_value.__enter__.return_value = mock_update
        self.mock_catalog.load_table.return_value = mock_table

        # Add column
        self.client.add_column(
            namespace="nyc_taxi",
            table="yellow_trips",
            name="surge_multiplier",
            field_type="double",
            doc="Trip surge pricing factor",
        )
        mock_update.add_column.assert_called_once_with(
            path="surge_multiplier", field_type="double", doc="Trip surge pricing factor", required=False
        )

        # Rename column
        mock_update.reset_mock()
        self.client.rename_column(
            namespace="nyc_taxi",
            table="yellow_trips",
            old_name="Airport_fee",
            new_name="airport_surcharge",
        )
        mock_update.rename_column.assert_called_once_with("Airport_fee", "airport_surcharge")


if __name__ == "__main__":
    unittest.main()
