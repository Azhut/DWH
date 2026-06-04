"""Подключение к embedded DuckDB: схема, блокировка записи, выполнение в thread pool."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Optional, Sequence

import duckdb

from config.config import config

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS flat_data (
    form TEXT NOT NULL,
    file_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    reporter TEXT NOT NULL,
    section TEXT NOT NULL,
    "row" TEXT NOT NULL,
    "column" TEXT NOT NULL,
    value DOUBLE,
    UNIQUE (file_id, year, reporter, section, "row", "column")
);

CREATE TABLE IF NOT EXISTS flat_data_staging (
    upload_id TEXT NOT NULL,
    form TEXT NOT NULL,
    file_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    reporter TEXT NOT NULL,
    section TEXT NOT NULL,
    "row" TEXT NOT NULL,
    "column" TEXT NOT NULL,
    value DOUBLE
);

CREATE TABLE IF NOT EXISTS migration_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class DuckDBConnection:
    """Один процесс — одно соединение; все операции сериализуются asyncio.Lock."""

    def __init__(self) -> None:
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._lock = asyncio.Lock()

    @property
    def path(self) -> Path:
        return config.DUCKDB_PATH

    def _ensure_connection(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            path = self.path
            path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = duckdb.connect(str(path))
            logger.info("DuckDB подключён: %s", path)
        return self._conn

    def _execute_sync(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
    ) -> duckdb.DuckDBPyConnection:
        conn = self._ensure_connection()
        if params is None:
            conn.execute(sql)
        else:
            conn.execute(sql, params)
        return conn

    def _fetchall_sync(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
    ) -> list:
        conn = self._execute_sync(sql, params)
        if conn.description is None:
            return []
        return conn.fetchall()

    def _fetchone_sync(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
    ) -> Any:
        rows = self._fetchall_sync(sql, params)
        return rows[0] if rows else None

    async def execute(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
    ) -> None:
        async with self._lock:
            await asyncio.to_thread(self._execute_sync, sql, params)

    async def fetchall(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
    ) -> list:
        async with self._lock:
            return await asyncio.to_thread(self._fetchall_sync, sql, params)

    async def fetchone(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
    ) -> Any:
        async with self._lock:
            return await asyncio.to_thread(self._fetchone_sync, sql, params)

    async def run_write(self, fn):
        """Синхронная функция fn(conn) под единой блокировкой (batch write / promote)."""
        async with self._lock:
            return await asyncio.to_thread(fn)

    def initialize_schema_sync(self) -> None:
        conn = self._ensure_connection()
        conn.execute(_SCHEMA_SQL)

    async def initialize_schema(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self.initialize_schema_sync)

    async def ping(self) -> bool:
        try:
            row = await self.fetchone("SELECT 1")
            return row is not None and row[0] == 1
        except Exception as exc:
            logger.error("DuckDB ping failed: %s", exc)
            return False

    async def close(self) -> None:
        async with self._lock:
            if self._conn is not None:
                await asyncio.to_thread(self._conn.close)
                self._conn = None
                logger.info("DuckDB соединение закрыто")


duckdb_connection = DuckDBConnection()
