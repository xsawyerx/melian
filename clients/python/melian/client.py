from __future__ import annotations

import json
import os
import socket
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

@dataclass(frozen=True)
class Dsn:
    kind: str
    host: Optional[str] = None
    port: Optional[int] = None
    path: Optional[str] = None

class MelianClient:
    """Thin client for the Melian cache server."""

    HEADER_VERSION = 0x11
    ACTION_FETCH = 0x46  # 'F'
    ACTION_DESCRIBE = 0x44  # 'D'

    def __init__(
        self,
        dsn: str = "unix:///tmp/melian.sock",
        *,
        timeout: float = 1.0,
        schema: Optional[Dict[str, Any]] = None,
        schema_spec: Optional[str] = None,
        schema_file: Optional[str] = None,
    ) -> None:
        self._dsn = self._parse_dsn(dsn)
        self._timeout = timeout
        self._socket: Optional[socket.socket] = None
        self._schema = self._bootstrap_schema(schema, schema_spec, schema_file)

    def close(self) -> None:
        if self._socket is not None:
            self._socket.close()
            self._socket = None

    def schema(self) -> Dict[str, Any]:
        return self._schema

    def describe_schema(self) -> Dict[str, Any]:
        payload = self._send(self.ACTION_DESCRIBE, 0, 0, b"")
        if not payload:
            raise RuntimeError("Melian server returned empty schema description")
        decoded = json.loads(payload)
        if not isinstance(decoded, dict):
            raise RuntimeError("Schema description must be a JSON object")
        return decoded

    def fetch_raw(self, table_id: int, index_id: int, key: bytes) -> bytes:
        if not (0 <= table_id <= 255 and 0 <= index_id <= 255):
            raise ValueError("table_id and index_id must be between 0 and 255")
        return self._send(self.ACTION_FETCH, table_id, index_id, key)

    def fetch_by_string(self, table_id: int, index_id: int, key: bytes) -> Optional[Dict[str, Any]]:
        payload = self.fetch_raw(table_id, index_id, key)
        if not payload:
            return None
        decoded = json.loads(payload)
        if not isinstance(decoded, dict):
            raise RuntimeError("Expected JSON object from server")
        return decoded

    def fetch_by_int(self, table_id: int, index_id: int, record_id: int) -> Optional[Dict[str, Any]]:
        key = struct.pack("<I", record_id)
        return self.fetch_by_string(table_id, index_id, key)

    def fetch_by_string_from(
        self, table_name: str, column_name: str, key: bytes | bytearray | memoryview | str
    ) -> Optional[Dict[str, Any]]:
        table_id, index_id = self.resolve_index(table_name, column_name)
        if isinstance(key, str):
            key_bytes = key.encode("utf-8")
        elif isinstance(key, memoryview):
            key_bytes = key.tobytes()
        else:
            key_bytes = bytes(key)
        return self.fetch_by_string(table_id, index_id, key_bytes)

    def fetch_by_int_from(
        self, table_name: str, column_name: str, record_id: int
    ) -> Optional[Dict[str, Any]]:
        table_id, index_id = self.resolve_index(table_name, column_name)
        return self.fetch_by_int(table_id, index_id, record_id)

    def resolve_index(self, table_name: str, column: str) -> Tuple[int, int]:
        for table in self._schema.get("tables", []):
            if table.get("name") != table_name:
                continue
            for index in table.get("indexes", []):
                if index.get("column") == column:
                    return int(table["id"]), int(index["id"])
        raise RuntimeError(f"Unable to resolve index for {table_name}.{column}")

    def _ensure_connected(self) -> socket.socket:
        if self._socket is not None:
            return self._socket

        if self._dsn.kind == "unix":
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            assert self._dsn.path
            sock.connect(self._dsn.path)
        else:
            sock = socket.create_connection((self._dsn.host, int(self._dsn.port)))
        sock.settimeout(self._timeout)
        self._socket = sock
        return sock

    def _send(self, action: int, table_id: int, index_id: int, payload: bytes) -> bytes:
        sock = self._ensure_connected()
        header = struct.pack(
            "!BBBBI",
            self.HEADER_VERSION,
            action,
            table_id,
            index_id,
            len(payload),
        )
        sock.sendall(header + payload)
        length_bytes = self._read_exact(sock, 4)
        (length,) = struct.unpack("!I", length_bytes)
        if length == 0:
            return b""
        return self._read_exact(sock, length)

    def _read_exact(self, sock: socket.socket, size: int) -> bytes:
        chunks: List[bytes] = []
        remaining = size
        while remaining:
            chunk = sock.recv(remaining)
            if not chunk:
                raise RuntimeError("Socket closed while reading response")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _bootstrap_schema(
        self,
        schema: Optional[Dict[str, Any]],
        schema_spec: Optional[str],
        schema_file: Optional[str],
    ) -> Dict[str, Any]:
        provided = [bool(schema), bool(schema_spec), bool(schema_file)]
        if sum(provided) > 1:
            raise ValueError("Provide at most one of schema, schema_spec, schema_file")
        if schema is not None:
            return schema
        if schema_spec is not None:
            return self._load_schema_from_spec(schema_spec)
        if schema_file is not None:
            return self._load_schema_from_file(schema_file)
        return self.describe_schema()

    def _load_schema_from_file(self, path: str) -> Dict[str, Any]:
        file_path = Path(path)
        if not file_path.is_file():
            raise FileNotFoundError(path)
        contents = file_path.read_text(encoding="utf-8")
        data = json.loads(contents)
        if not isinstance(data, dict):
            raise RuntimeError("Schema file must contain a JSON object")
        return data

    def _load_schema_from_spec(self, spec: str) -> Dict[str, Any]:
        spec = spec.strip()
        if not spec:
            raise ValueError("schema_spec cannot be empty")

        tables: List[Dict[str, Any]] = []
        for chunk in spec.split(","):
            chunk = chunk.strip()
            if not chunk:
                continue
            parts = chunk.split("|")
            if len(parts) < 2:
                raise ValueError(f"Invalid table spec chunk: {chunk}")
            table_part = parts[0]
            period = int(parts[1]) if len(parts) >= 2 and parts[1] else 0
            columns_part = parts[2] if len(parts) >= 3 else ""

            table_name, table_id = self._split_with_hash(table_part, "table")
            table = {"name": table_name, "id": int(table_id), "period": period, "indexes": []}

            if not columns_part:
                raise ValueError(f"Table {table_name} must define at least one index")
            for idx_spec in columns_part.split(";"):
                idx_spec = idx_spec.strip()
                if not idx_spec:
                    continue
                column_with_id, *type_part = idx_spec.split(":", 1)
                column_name, column_id = self._split_with_hash(column_with_id, "index")
                index_type = type_part[0] if type_part else "int"
                table["indexes"].append(
                    {"column": column_name, "id": int(column_id), "type": index_type}
                )

            tables.append(table)

        if not tables:
            raise ValueError("schema_spec produced no tables")
        return {"tables": tables}

    def _split_with_hash(self, value: str, label: str) -> Tuple[str, str]:
        if "#" not in value:
            raise ValueError(f"Missing # delimiter for {label} specification: {value}")
        name, ident = value.split("#", 1)
        name = name.strip()
        ident = ident.strip()
        if not name or not ident:
            raise ValueError(f"Invalid {label} specification: {value}")
        return name, ident

    def _parse_dsn(self, dsn: str) -> Dsn:
        if dsn.startswith("unix://"):
            path = dsn[len("unix://") :]
            if not path:
                raise ValueError("unix DSN must include socket path")
            return Dsn("unix", path=path)
        if dsn.startswith("tcp://"):
            host_port = dsn[len("tcp://") :]
            host, _, port_str = host_port.rpartition(":")
            if not host or not port_str:
                raise ValueError("tcp DSN must include host:port")
            return Dsn("tcp", host=host, port=int(port_str))
        raise ValueError(f"Unsupported DSN: {dsn}")
