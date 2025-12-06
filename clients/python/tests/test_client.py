from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Tuple

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from melian import MelianClient

DEFAULT_DSN = "unix:///tmp/melian.sock"
SCHEMA_SPEC = "table1#0|60|id#0:int,table2#1|60|id#0:int;hostname#1:string"

@pytest.fixture(scope="module")
def client() -> MelianClient:
    dsn = os.getenv("MELIAN_TEST_DSN", DEFAULT_DSN)
    client = MelianClient(dsn=dsn, timeout=1.0)
    yield client
    client.close()

def test_connection_loads_schema(client: MelianClient) -> None:
    schema = client.schema()
    assert "tables" in schema
    assert any(table.get("name") == "table1" for table in schema["tables"])
    assert any(table.get("name") == "table2" for table in schema["tables"])

def test_table1_fetch_by_id(client: MelianClient) -> None:
    table_id, index_id = resolve_index(client, "table1", "id")
    payload = client.fetch_by_int(table_id, index_id, 5)
    assert payload is not None
    assert payload["id"] == 5
    assert payload["name"] == "item_5"
    assert payload["category"] == "alpha"
    assert payload["value"] == "VAL_0005"
    assert payload["active"] == 1

def test_table2_fetch_by_id_and_hostname(client: MelianClient) -> None:
    table_id, id_index = resolve_index(client, "table2", "id")
    _, host_index = resolve_index(client, "table2", "hostname")

    expected = {
        "id": 2,
        "hostname": "host-00002",
        "ip": "10.0.2.0",
        "status": "maintenance",
    }

    by_id = client.fetch_by_int(table_id, id_index, 2)
    assert by_id == expected

    by_host = client.fetch_by_string(table_id, host_index, b"host-00002")
    assert by_host == expected

def test_named_fetch_helpers(client: MelianClient) -> None:
    direct = client.fetch_by_int_from("table1", "id", 5)
    assert direct is not None
    assert direct["name"] == "item_5"

    named = client.fetch_by_string_from("table2", "hostname", "host-00002")
    assert named is not None
    assert named["id"] == 2

def test_schema_spec_matches_live_description() -> None:
    live = MelianClient(dsn=os.getenv("MELIAN_TEST_DSN", DEFAULT_DSN)).schema()
    from_spec = MelianClient(
        dsn=os.getenv("MELIAN_TEST_DSN", DEFAULT_DSN),
        schema_spec=SCHEMA_SPEC,
    ).schema()

    assert normalize_schema(live) == normalize_schema(from_spec)

def test_resolve_invalid_index_raises(client: MelianClient) -> None:
    with pytest.raises(RuntimeError):
        client.resolve_index("table1", "nonexistent")

def resolve_index(client: MelianClient, table: str, column: str) -> Tuple[int, int]:
    resolved = client.resolve_index(table, column)
    return resolved[0], resolved[1]

def normalize_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    tables = schema.get("tables", [])
    sorted_tables = sorted(tables, key=lambda t: t.get("id", 0))
    normalized_tables = []
    for table in sorted_tables:
        indexes = table.get("indexes", [])
        sorted_indexes = sorted(indexes, key=lambda idx: idx.get("id", 0))
        normalized_indexes = [dict(sorted(index.items())) for index in sorted_indexes]
        normalized_tables.append(
            {
                key: table[key]
                for key in sorted(table.keys())
                if key != "indexes"
            }
        )
        normalized_tables[-1]["indexes"] = normalized_indexes
    return {"tables": normalized_tables}
