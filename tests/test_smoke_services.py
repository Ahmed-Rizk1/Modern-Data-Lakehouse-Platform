"""Multi-Service Integration Smoke Tests for Modern Data Lakehouse Services.

Probes endpoints across the infrastructure stack:
- MinIO S3 API (:9000) & Console (:9001)
- Apache Iceberg REST Catalog (:8181)
- Trino Distributed SQL Query Engine (:8080)
- Prefect Workflow Server (:4200)
- Apache Superset BI Cockpit (:8088)

Can be executed against live Docker services or gracefully skipped when services are offline.
"""

import os
import unittest
import urllib.error
import urllib.request
import pytest


SERVICES = {
    "minio_health": {
        "name": "MinIO S3 Health Check",
        "url": os.getenv("MINIO_HEALTH_URL", "http://localhost:9000/minio/health/live"),
        "expected_statuses": [200],
    },
    "iceberg_rest_catalog": {
        "name": "Iceberg REST Catalog Config",
        "url": os.getenv("ICEBERG_CATALOG_URL", "http://localhost:8181/v1/config"),
        "expected_statuses": [200],
    },
    "trino_server": {
        "name": "Trino MPP Query Engine",
        "url": os.getenv("TRINO_SERVER_URL", "http://localhost:8080/v1/info"),
        "expected_statuses": [200],
    },
    "prefect_api": {
        "name": "Prefect Orchestration API",
        "url": os.getenv("PREFECT_API_URL", "http://localhost:4200/api/health"),
        "expected_statuses": [200, 307],
    },
    "superset_health": {
        "name": "Superset BI Engine",
        "url": os.getenv("SUPERSET_HEALTH_URL", "http://localhost:8088/health"),
        "expected_statuses": [200],
    },
}


def probe_endpoint(url: str, timeout: float = 2.0) -> int:
    """Probes an HTTP endpoint and returns its HTTP status code, or 0 if unreachable."""
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Lakehouse-Smoke-Tester/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0


class TestLakehouseServicesSmoke(unittest.TestCase):
    """Smoke tests verifying connectivity to local or distributed Lakehouse service endpoints."""

    def test_probe_utility_handles_offline_gracefully(self):
        """Verifies that unreachable ports return 0 without uncaught crash."""
        status = probe_endpoint("http://127.0.0.1:59999/non_existent_service", timeout=0.5)
        self.assertEqual(status, 0)

    @pytest.mark.smoke
    def test_services_connectivity_report(self):
        """Probes all lakehouse services and prints an operational readiness report."""
        report = {}
        for key, info in SERVICES.items():
            status = probe_endpoint(info["url"], timeout=1.5)
            report[key] = {
                "name": info["name"],
                "url": info["url"],
                "status": status,
                "healthy": status in info["expected_statuses"],
            }

        # Print human-readable diagnostics for local CI/dev verification
        print("\n=== Lakehouse Infrastructure Smoke Test Report ===")
        for key, res in report.items():
            health_badge = "[ONLINE]" if res["healthy"] else "[OFFLINE/UNREACHABLE]"
            print(f" {health_badge} {res['name']} ({res['url']}) -> HTTP {res['status']}")
        print("==================================================")

        # Smoke assertion: If live services are flagged as required via ENV, assert all healthy
        require_live = os.getenv("REQUIRE_LIVE_LAKEHOUSE_SERVICES", "false").lower() in ("true", "1")
        if require_live:
            for key, res in report.items():
                self.assertTrue(
                    res["healthy"],
                    f"Required service {res['name']} failed health check at {res['url']}",
                )


if __name__ == "__main__":
    unittest.main()

