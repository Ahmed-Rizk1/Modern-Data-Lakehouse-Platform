"""Declarative Data Quality & Contract Enforcement for the Modern Lakehouse.

Enforces schema contracts, null rate thresholds, numeric domain bounds,
and three-way row count reconciliation (Raw Ingested == Iceberg Committed == Trino Queryable)
to eliminate silent data corruption before table snapshot commits.
"""

from dataclasses import dataclass, field
import logging
from typing import Any, Dict, List, Optional, Set, Union

import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when data fails schema contracts, null limits, or bounding rules."""
    pass


class RowReconciliationError(DataQualityError):
    """Raised when raw ingested, Iceberg committed, and Trino queryable row counts disagree."""
    pass


@dataclass
class ColumnContract:
    """Declarative specification for a single column's data contract."""
    name: str
    expected_type: Optional[str] = None
    required: bool = True
    max_null_rate: float = 0.0  # 0.0 = strictly non-null; 0.05 = up to 5% nulls allowed
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    max_anomaly_rate: float = 0.0  # Allowable fraction of out-of-bounds rows
    allowed_values: Optional[Set[Any]] = None


@dataclass
class TableContract:
    """Declarative contract for an entire table payload."""
    table_name: str
    columns: List[ColumnContract]
    min_row_count: int = 1
    max_row_count: Optional[int] = None
    disallow_duplicate_columns: bool = True

    def get_column(self, name: str) -> Optional[ColumnContract]:
        for col in self.columns:
            if col.name == name:
                return col
        return None


@dataclass
class DataQualityReport:
    """Diagnostic outcome of data quality contract verification."""
    table_name: str
    passed: bool
    row_count: int
    column_count: int
    schema_checks: Dict[str, Any] = field(default_factory=dict)
    null_checks: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    bounds_checks: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    violations: List[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        return "PASSED" if self.passed else "FAILED"

    def summary(self) -> str:
        header = f"=== Data Quality Report: {self.table_name} [{self.status}] ==="
        stats = f"Rows: {self.row_count:,} | Columns: {self.column_count}"
        if self.passed:
            return f"{header}\n{stats}\nAll schema contracts and quality rules passed cleanly."
        violations_text = "\n".join(f"  - {v}" for v in self.violations)
        return f"{header}\n{stats}\nViolations ({len(self.violations)}):\n{violations_text}"


class DataQualityEnforcer:
    """Validates PyArrow tables against declarative table contracts."""

    @classmethod
    def validate(
        cls,
        table: pa.Table,
        contract: Optional[TableContract] = None,
        enforce: bool = True,
    ) -> DataQualityReport:
        """Validates table against contract. Raises DataQualityError if enforce=True and failed."""
        table_name = contract.table_name if contract else "unnamed_table"
        violations: List[str] = []
        schema_checks: Dict[str, Any] = {}
        null_checks: Dict[str, Dict[str, Any]] = {}
        bounds_checks: Dict[str, Dict[str, Any]] = {}

        # 1. Row count assertions
        row_count = table.num_rows
        column_count = table.num_columns
        min_rows = contract.min_row_count if contract else 1

        if row_count < min_rows:
            violations.append(
                f"Row count assertion failed: received {row_count} rows, minimum required is {min_rows}."
            )

        if contract and contract.max_row_count is not None and row_count > contract.max_row_count:
            violations.append(
                f"Row count assertion failed: received {row_count} rows, maximum allowed is {contract.max_row_count}."
            )

        # 2. Duplicate column detection
        col_names = table.column_names
        if contract and contract.disallow_duplicate_columns:
            if len(col_names) != len(set(col_names)):
                duplicates = [c for c in col_names if col_names.count(c) > 1]
                violations.append(f"Duplicate columns detected in payload: {duplicates}")

        # If no contract is passed, perform fundamental sanity checks
        if not contract:
            passed = len(violations) == 0
            report = DataQualityReport(
                table_name=table_name,
                passed=passed,
                row_count=row_count,
                column_count=column_count,
                violations=violations,
            )
            if enforce and not passed:
                raise DataQualityError(report.summary())
            return report

        # 3. Column presence and type assertions
        existing_cols = set(col_names)
        for col_contract in contract.columns:
            col_name = col_contract.name

            if col_contract.required and col_name not in existing_cols:
                violations.append(f"Required column '{col_name}' is missing from payload.")
                continue

            if col_name not in existing_cols:
                continue

            col_data = table[col_name]
            actual_type = str(col_data.type).lower()

            if col_contract.expected_type:
                expected = col_contract.expected_type.lower()
                # Flexible type matching for timestamps / integers / floats
                type_matches = (
                    expected in actual_type
                    or (expected in ("int", "integer", "long") and "int" in actual_type)
                    or (expected in ("float", "double") and ("double" in actual_type or "float" in actual_type))
                    or (expected in ("str", "string") and ("string" in actual_type or "large_string" in actual_type))
                )
                schema_checks[col_name] = {
                    "expected": col_contract.expected_type,
                    "actual": str(col_data.type),
                    "match": type_matches,
                }
                if not type_matches:
                    violations.append(
                        f"Type mismatch on '{col_name}': expected {col_contract.expected_type}, got {col_data.type}."
                    )

            # 4. Null percentage assertion
            null_count = col_data.null_count
            null_rate = (null_count / row_count) if row_count > 0 else 0.0
            null_passed = null_rate <= (col_contract.max_null_rate + 1e-6)

            null_checks[col_name] = {
                "null_count": null_count,
                "null_rate": round(null_rate, 4),
                "threshold": col_contract.max_null_rate,
                "passed": null_passed,
            }

            if not null_passed:
                violations.append(
                    f"Null rate violation on '{col_name}': observed {null_rate:.2%} (max allowed: {col_contract.max_null_rate:.2%})."
                )

            # 5. Numeric bounds and anomaly rules
            if (col_contract.min_value is not None or col_contract.max_value is not None) and row_count > 0:
                try:
                    valid_mask = pc.is_valid(col_data)
                    valid_data = pc.filter(col_data, valid_mask)
                    valid_count = len(valid_data)

                    if valid_count > 0:
                        min_observed = pc.min(valid_data).as_py()
                        max_observed = pc.max(valid_data).as_py()

                        anomalies = 0
                        if col_contract.min_value is not None:
                            below_min = pc.sum(pc.less(valid_data, col_contract.min_value)).as_py() or 0
                            anomalies += below_min

                        if col_contract.max_value is not None:
                            above_max = pc.sum(pc.greater(valid_data, col_contract.max_value)).as_py() or 0
                            anomalies += above_max

                        anomaly_rate = anomalies / valid_count
                        bounds_passed = anomaly_rate <= (col_contract.max_anomaly_rate + 1e-6)

                        bounds_checks[col_name] = {
                            "min_observed": min_observed,
                            "max_observed": max_observed,
                            "anomalies": anomalies,
                            "anomaly_rate": round(anomaly_rate, 4),
                            "max_anomaly_rate": col_contract.max_anomaly_rate,
                            "passed": bounds_passed,
                        }

                        if not bounds_passed:
                            violations.append(
                                f"Numeric bounds violation on '{col_name}': {anomalies} out-of-bounds rows "
                                f"({anomaly_rate:.2%}, max allowable: {col_contract.max_anomaly_rate:.2%}). "
                                f"Observed [{min_observed}, {max_observed}], expected [{col_contract.min_value}, {col_contract.max_value}]."
                            )
                except Exception as e:
                    logger.warning(f"Bounds check on '{col_name}' skipped: {e}")

            # 6. Categorical allowed values check
            if col_contract.allowed_values and row_count > 0:
                try:
                    distinct_values = set(pc.unique(col_data).to_pylist())
                    distinct_values.discard(None)
                    invalid_values = distinct_values - set(col_contract.allowed_values)
                    if invalid_values:
                        violations.append(
                            f"Categorical domain violation on '{col_name}': unexpected values {invalid_values}."
                        )
                except Exception as e:
                    logger.warning(f"Categorical check on '{col_name}' skipped: {e}")

        passed = len(violations) == 0
        report = DataQualityReport(
            table_name=table_name,
            passed=passed,
            row_count=row_count,
            column_count=column_count,
            schema_checks=schema_checks,
            null_checks=null_checks,
            bounds_checks=bounds_checks,
            violations=violations,
        )

        if enforce and not passed:
            raise DataQualityError(report.summary())

        return report


def reconcile_row_counts(
    raw_rows: int,
    committed_rows: int,
    trino_rows: int,
    table_identifier: str = "",
) -> Dict[str, Any]:
    """Enforces three-way row count reconciliation: Raw Ingested == Iceberg Committed == Trino Queryable.
    
    Raises:
        RowReconciliationError: If any of the three counts disagree.
    """
    is_reconciled = (raw_rows == committed_rows == trino_rows)
    variance_raw_committed = abs(raw_rows - committed_rows)
    variance_committed_trino = abs(committed_rows - trino_rows)
    total_variance = variance_raw_committed + variance_committed_trino

    result = {
        "table": table_identifier,
        "reconciled": is_reconciled,
        "raw_ingested_rows": raw_rows,
        "iceberg_committed_rows": committed_rows,
        "trino_queryable_rows": trino_rows,
        "variance_rows": total_variance,
    }

    if not is_reconciled:
        msg = (
            f"Three-way row reconciliation MISMATCH for '{table_identifier}':\n"
            f"  [1] Raw Ingested      : {raw_rows:,}\n"
            f"  [2] Iceberg Committed : {committed_rows:,}\n"
            f"  [3] Trino Queryable   : {trino_rows:,}\n"
            f"  Variance (Raw vs Committed): {variance_raw_committed:,}\n"
            f"  Variance (Committed vs Trino): {variance_committed_trino:,}"
        )
        logger.error(msg)
        raise RowReconciliationError(msg)

    logger.info(
        f"Three-way row reconciliation PASSED for '{table_identifier}': "
        f"{raw_rows:,} == {committed_rows:,} == {trino_rows:,} (Variance: 0 rows)"
    )
    return result


# Standard declarative contract for NYC Taxi Yellow Tripdata
NYC_TAXI_CONTRACT = TableContract(
    table_name="yellow_tripdata",
    min_row_count=1,
    disallow_duplicate_columns=True,
    columns=[
        ColumnContract(
            name="VendorID",
            expected_type="int",
            required=True,
            max_null_rate=0.05,
        ),
        ColumnContract(
            name="tpep_pickup_datetime",
            expected_type="timestamp",
            required=True,
            max_null_rate=0.0,
        ),
        ColumnContract(
            name="tpep_dropoff_datetime",
            expected_type="timestamp",
            required=True,
            max_null_rate=0.0,
        ),
        ColumnContract(
            name="passenger_count",
            expected_type="int",
            required=False,
            max_null_rate=0.15,
            min_value=0.0,
            max_value=12.0,
            max_anomaly_rate=0.005,
        ),
        ColumnContract(
            name="trip_distance",
            expected_type="double",
            required=True,
            max_null_rate=0.01,
            min_value=0.0,
            max_value=2000.0,
            max_anomaly_rate=0.002,
        ),
        ColumnContract(
            name="RatecodeID",
            expected_type="int",
            required=False,
            max_null_rate=0.15,
        ),
        ColumnContract(
            name="PULocationID",
            expected_type="int",
            required=True,
            max_null_rate=0.01,
        ),
        ColumnContract(
            name="DOLocationID",
            expected_type="int",
            required=True,
            max_null_rate=0.01,
        ),
        ColumnContract(
            name="fare_amount",
            expected_type="double",
            required=True,
            max_null_rate=0.01,
            min_value=0.0,
            max_value=5000.0,
            max_anomaly_rate=0.005,
        ),
        ColumnContract(
            name="total_amount",
            expected_type="double",
            required=True,
            max_null_rate=0.01,
            min_value=0.0,
            max_value=10000.0,
            max_anomaly_rate=0.005,
        ),
    ],
)

