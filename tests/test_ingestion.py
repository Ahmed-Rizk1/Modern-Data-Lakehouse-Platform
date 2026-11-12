"""Unit Tests for Lakehouse Data Ingestion, Extraction, and Quality Validation.

Tests validate tasks, schema contracts, null boundaries, PyArrow table processing,
and three-way row count reconciliation without requiring container overhead or live Docker services.
"""

from datetime import datetime
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

# Ensure data/pipelines and data/scripts are on sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
PIPELINES_DIR = BASE_DIR / "data" / "pipelines"
SCRIPTS_DIR = BASE_DIR / "data" / "scripts"

if str(PIPELINES_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINES_DIR))
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from ingestion import (
    IngestionRequest,
    default_ingestion_request,
    enforce_data_quality,
    reconcile_pipeline_row_counts,
    validate_payload_contract,
)
from quality import (
    ColumnContract,
    DataQualityEnforcer,
    DataQualityError,
    NYC_TAXI_CONTRACT,
    RowReconciliationError,
    TableContract,
    reconcile_row_counts,
)


def create_mock_arrow_table(num_rows: int = 10, with_nulls: bool = False, negative_fare: bool = False) -> pa.Table:
    """Helper to generate deterministic PyArrow tables matching NYC_TAXI_CONTRACT schema."""
    vendor_ids = [1 if i % 2 == 0 else 2 for i in range(num_rows)]
    base_ts = 1704103200000000  # 2024-01-01 10:00:00 in microseconds
    pickups = [base_ts + i * 60000000 for i in range(num_rows)]
    dropoffs = [base_ts + (i + 10) * 60000000 for i in range(num_rows)]
    passenger_counts = [1 if not (with_nulls and i == 0) else None for i in range(num_rows)]
    trip_distances = [2.5 * (i + 1) for i in range(num_rows)]
    fare_amounts = [-15.0 if (negative_fare and i == 0) else (10.0 + i) for i in range(num_rows)]
    total_amounts = [fare + 3.0 if fare >= 0 else 0.0 for fare in fare_amounts]
    rate_codes = [1 for _ in range(num_rows)]
    pu_locations = [100 for _ in range(num_rows)]
    do_locations = [101 for _ in range(num_rows)]

    return pa.table({
        "VendorID": pa.array(vendor_ids, type=pa.int64()),
        "tpep_pickup_datetime": pa.array(pickups, type=pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array(dropoffs, type=pa.timestamp("us")),
        "passenger_count": pa.array(passenger_counts, type=pa.int64()),
        "trip_distance": pa.array(trip_distances, type=pa.float64()),
        "RatecodeID": pa.array(rate_codes, type=pa.int64()),
        "PULocationID": pa.array(pu_locations, type=pa.int64()),
        "DOLocationID": pa.array(do_locations, type=pa.int64()),
        "fare_amount": pa.array(fare_amounts, type=pa.float64()),
        "total_amount": pa.array(total_amounts, type=pa.float64()),
    })


class TestPayloadValidation(unittest.TestCase):
    """Test suite for payload format, presence, and size verification."""

    def test_valid_parquet_request(self):
        req = IngestionRequest(
            url="data/datasets/sample.parquet",
            db_name="nyc_taxi",
            table_name="yellow_trips",
        )
        with patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "stat") as mock_stat:
            mock_stat.return_value = MagicMock(st_size=1048576)
            result = validate_payload_contract(req)
            self.assertEqual(result["file_size_bytes"], 1048576)
            self.assertEqual(result["extension"], ".parquet")

    def test_unsupported_file_extension(self):
        req = IngestionRequest(
            url="data/datasets/sample.xml",
            db_name="nyc_taxi",
            table_name="yellow_trips",
        )
        with patch.object(Path, "exists", return_value=True):
            with pytest.raises(ValueError, match="Unsupported file format"):
                validate_payload_contract(req)

    def test_missing_file_raises_error(self):
        req = IngestionRequest(
            url="data/datasets/non_existent_file.parquet",
            db_name="nyc_taxi",
            table_name="yellow_trips",
        )
        with patch.object(Path, "exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="Source file does not exist"):
                validate_payload_contract(req)

    def test_zero_byte_file_raises_error(self):
        req = IngestionRequest(
            url="data/datasets/empty.csv",
            db_name="nyc_taxi",
            table_name="yellow_trips",
        )
        with patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "stat") as mock_stat:
            mock_stat.return_value = MagicMock(st_size=0)
            with pytest.raises(ValueError, match="Payload file is empty"):
                validate_payload_contract(req)


class TestDataQualityEnforcer(unittest.TestCase):
    """Unit tests for declarative data contracts, schema checks, and bounds validation."""

    def test_valid_table_passes_quality_contract(self):
        arrow_table = create_mock_arrow_table(num_rows=25)
        report = DataQualityEnforcer.validate(arrow_table, contract=NYC_TAXI_CONTRACT, enforce=True)
        self.assertTrue(report.passed)
        self.assertEqual(len(report.violations), 0)
        self.assertEqual(report.row_count, 25)

    def test_null_constraint_violation(self):
        # Pickup timestamp is configured with max_null_rate=0.0
        bad_table = pa.table({
            "VendorID": pa.array([1, 2], type=pa.int64()),
            "tpep_pickup_datetime": pa.array([None, 1704103200000000], type=pa.timestamp("us")),
            "tpep_dropoff_datetime": pa.array([1704103800000000, 1704104400000000], type=pa.timestamp("us")),
            "trip_distance": pa.array([1.5, 3.0], type=pa.float64()),
            "PULocationID": pa.array([100, 101], type=pa.int64()),
            "DOLocationID": pa.array([102, 103], type=pa.int64()),
            "fare_amount": pa.array([10.0, 15.0], type=pa.float64()),
            "total_amount": pa.array([13.0, 18.0], type=pa.float64()),
        })
        with pytest.raises(DataQualityError) as exc_info:
            DataQualityEnforcer.validate(bad_table, contract=NYC_TAXI_CONTRACT, enforce=True)
        self.assertIn("Null rate violation", str(exc_info.value))

    def test_numeric_boundary_violation(self):
        # Negative fare amount violates min_value=0.0
        arrow_table = create_mock_arrow_table(num_rows=10, negative_fare=True)
        with pytest.raises(DataQualityError) as exc_info:
            DataQualityEnforcer.validate(arrow_table, contract=NYC_TAXI_CONTRACT, enforce=True)
        self.assertIn("Numeric bounds violation", str(exc_info.value))

    def test_empty_table_fails_minimum_row_contract(self):
        empty_table = pa.table({
            "VendorID": pa.array([], type=pa.int64()),
            "tpep_pickup_datetime": pa.array([], type=pa.timestamp("us")),
            "tpep_dropoff_datetime": pa.array([], type=pa.timestamp("us")),
            "trip_distance": pa.array([], type=pa.float64()),
            "PULocationID": pa.array([], type=pa.int64()),
            "DOLocationID": pa.array([], type=pa.int64()),
            "fare_amount": pa.array([], type=pa.float64()),
            "total_amount": pa.array([], type=pa.float64()),
        })
        with pytest.raises(DataQualityError) as exc_info:
            DataQualityEnforcer.validate(empty_table, contract=NYC_TAXI_CONTRACT, enforce=True)
        self.assertIn("Row count assertion failed", str(exc_info.value))

    def test_pipeline_task_enforce_data_quality_raises_exception(self):
        bad_table = create_mock_arrow_table(num_rows=10, negative_fare=True)
        req = IngestionRequest(
            url="data/datasets/sample.parquet",
            db_name="nyc_taxi",
            table_name="yellow_tripdata",
        )
        with pytest.raises(DataQualityError):
            enforce_data_quality(bad_table, req)


class TestRowReconciliation(unittest.TestCase):
    """Unit tests for the three-way row reconciliation gate."""

    def test_perfect_three_way_match(self):
        result = reconcile_row_counts(
            raw_rows=5000,
            committed_rows=5000,
            trino_rows=5000,
            table_identifier="nyc_taxi.yellow_trips",
        )
        self.assertTrue(result["reconciled"])
        self.assertEqual(result["variance_rows"], 0)

    def test_row_count_mismatch_raises_reconciliation_error(self):
        with pytest.raises(RowReconciliationError, match="Three-way row reconciliation MISMATCH"):
            reconcile_row_counts(
                raw_rows=5000,
                committed_rows=4990,
                trino_rows=5000,
                table_identifier="nyc_taxi.yellow_trips",
            )

    def test_pipeline_task_reconcile_pipeline_row_counts(self):
        commit_summary = {"rows_committed": 100}
        trino_summary = {"actual_rows": 100}

        # Matching rows should pass cleanly
        res = reconcile_pipeline_row_counts(
            raw_rows=100,
            commit_summary=commit_summary,
            trino_summary=trino_summary,
            table_identifier="nyc_taxi.yellow_trips",
        )
        self.assertTrue(res["reconciled"])

        # Mismatched rows should raise RowReconciliationError
        mismatched_trino = {"actual_rows": 95}
        with pytest.raises(RowReconciliationError):
            reconcile_pipeline_row_counts(
                raw_rows=100,
                commit_summary=commit_summary,
                trino_summary=mismatched_trino,
                table_identifier="nyc_taxi.yellow_trips",
            )


if __name__ == "__main__":
    unittest.main()
