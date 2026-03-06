"""
Dune Analytics API client — shared by all Dune extraction scripts.

Usage:
    from dune_client import DuneClient
    client = DuneClient()
    rows = client.execute_and_fetch(query_id=6773409)
"""

import csv
import json
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Import config from parent
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DUNE_API_KEY, DATA_RAW, CSV_DELIMITER, CSV_ENCODING


class DuneClient:
    """Minimal Dune API client using only stdlib (no requests dependency)."""

    BASE_URL = "https://api.dune.com/api/v1"

    def __init__(self, api_key: str = DUNE_API_KEY):
        self.api_key = api_key
        self.headers = {"X-Dune-API-Key": api_key}

    def _get(self, path: str) -> dict:
        url = f"{self.BASE_URL}/{path}"
        req = urllib.request.Request(url, headers=self.headers)
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())

    def _post(self, path: str, data: dict = None) -> dict:
        url = f"{self.BASE_URL}/{path}"
        body = json.dumps(data or {}).encode()
        req = urllib.request.Request(url, data=body, headers={
            **self.headers, "Content-Type": "application/json"
        })
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())

    def create_query(self, name: str, query_sql: str, is_private: bool = True) -> int:
        """Create a new query on Dune. Returns the query_id.

        Use this to programmatically create queries instead of pasting SQL
        into the Dune web UI. The query will appear in your Dune dashboard.
        """
        print(f"  Creating Dune query: {name}...")
        result = self._post("query", data={
            "name": name,
            "query_sql": query_sql,
            "is_private": is_private,
        })
        query_id = result.get("query_id")
        if not query_id:
            raise RuntimeError(f"Failed to create query: {result}")
        print(f"  → Created query ID: {query_id}")
        return query_id

    def execute_query(self, query_id: int) -> str:
        """Execute a query, return execution_id."""
        result = self._post(f"query/{query_id}/execute")
        return result["execution_id"]

    def get_status(self, execution_id: str) -> str:
        """Get execution status."""
        result = self._get(f"execution/{execution_id}/status")
        return result.get("state", "")

    def get_results(self, execution_id: str, limit: int = 1000, offset: int = 0) -> dict:
        """Fetch results page."""
        return self._get(f"execution/{execution_id}/results?limit={limit}&offset={offset}")

    def execute_and_fetch(self, query_id: int, max_wait_sec: int = 1200, page_size: int = 1000) -> list[dict]:
        """Execute query, wait for completion, return all rows."""
        print(f"  Executing Dune query {query_id}...")
        exec_id = self.execute_query(query_id)
        print(f"  Execution ID: {exec_id}")

        # Poll for completion
        elapsed = 0
        while elapsed < max_wait_sec:
            state = self.get_status(exec_id)
            if state == "QUERY_STATE_COMPLETED":
                break
            if state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
                raise RuntimeError(f"Query {query_id} failed: {state}")
            print(f"    ... {state} ({elapsed}s)")
            time.sleep(5)
            elapsed += 5
        else:
            raise RuntimeError(f"Query {query_id} timed out after {max_wait_sec}s")

        # Fetch all pages
        all_rows = []
        offset = 0
        while True:
            result = self.get_results(exec_id, limit=page_size, offset=offset)
            rows = result.get("result", {}).get("rows", [])
            if not rows:
                break
            all_rows.extend(rows)
            total = result.get("result", {}).get("metadata", {}).get("total_row_count", 0)
            offset += len(rows)
            if offset >= total:
                break

        print(f"  Got {len(all_rows)} rows")
        return all_rows

    @staticmethod
    def save_csv(rows: list[dict], filename: str, columns: list[str], output_dir: Path = DATA_RAW):
        """Save rows to semicolon-delimited CSV."""
        output_dir.mkdir(parents=True, exist_ok=True)
        filepath = output_dir / filename
        with open(filepath, "w", encoding=CSV_ENCODING, newline="") as f:
            writer = csv.DictWriter(f, fieldnames=columns, delimiter=CSV_DELIMITER, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        print(f"  Saved: {filepath} ({len(rows)} rows)")
        return filepath
