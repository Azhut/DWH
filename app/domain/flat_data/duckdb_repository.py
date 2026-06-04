"""Репозиторий FlatData на DuckDB: чтение, staging-загрузка, удаление."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Sequence, Tuple

from app.core.duckdb_database import duckdb_connection
from app.domain.flat_data.models import TABLE_FIELDS
from app.domain.flat_data.query_sql import mongo_filter_to_sql

logger = logging.getLogger(__name__)

_INSERT_STAGING_SQL = """
INSERT INTO flat_data_staging (
    upload_id, form, file_id, year, reporter, section, row, "column", value
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_PROMOTE_STAGING_SQL = """
INSERT INTO flat_data (
    form, file_id, year, reporter, section, row, "column", value
)
SELECT form, file_id, year, reporter, section, row, "column", value
FROM flat_data_staging
WHERE upload_id = ?
"""


class DuckDBFlatDataRepository:
    """Хранилище витрины FlatData в DuckDB со staging-протоколом загрузки."""

    supports_staging: bool = True
    TABLE_FIELDS = TABLE_FIELDS

    async def distinct(
        self,
        field: str,
        query: Dict[str, Any],
        session: Any = None,
    ) -> List[Any]:
        col = _quote_column(field)
        where_sql, params = mongo_filter_to_sql(query)
        sql = f'SELECT DISTINCT {col} FROM flat_data WHERE {where_sql} ORDER BY {col}'
        rows = await duckdb_connection.fetchall(sql, params)
        return [row[0] for row in rows]

    async def get_filtered_data(
        self,
        query: Dict[str, Any],
        limit: int,
        offset: int,
        session: Any = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        where_sql, params = mongo_filter_to_sql(query)
        cols = ", ".join(
            f'"{f}"' if f in ("row", "column") else f for f in self.TABLE_FIELDS
        )
        count_sql = f"SELECT COUNT(*) FROM flat_data WHERE {where_sql}"
        count_row = await duckdb_connection.fetchone(count_sql, list(params))
        total = int(count_row[0]) if count_row else 0

        data_sql = (
            f"SELECT {cols} FROM flat_data WHERE {where_sql} "
            f'ORDER BY year, reporter, section, row, "column" '
            f"LIMIT ? OFFSET ?"
        )
        data_params = list(params) + [limit, offset]
        rows = await duckdb_connection.fetchall(data_sql, data_params)
        docs = [
            {
                "year": r[0],
                "reporter": r[1],
                "section": r[2],
                "row": r[3],
                "column": r[4],
                "value": r[5],
            }
            for r in rows
        ]
        return docs, total

    async def count_documents(self, query: Dict[str, Any], session: Any = None) -> int:
        where_sql, params = mongo_filter_to_sql(query)
        row = await duckdb_connection.fetchone(
            f"SELECT COUNT(*) FROM flat_data WHERE {where_sql}",
            params,
        )
        return int(row[0]) if row else 0

    async def delete_by_file_id(self, file_id: str, session: Any = None) -> Any:
        def _work() -> None:
            conn = duckdb_connection._ensure_connection()
            conn.execute("DELETE FROM flat_data WHERE file_id = ?", [file_id])
            conn.execute("DELETE FROM flat_data_staging WHERE file_id = ?", [file_id])

        await duckdb_connection.run_write(_work)
        return _DeleteResult(deleted_count=-1)

    async def delete_by_form(self, form_id: str, session: Any = None) -> Any:
        def _work() -> None:
            conn = duckdb_connection._ensure_connection()
            conn.execute("DELETE FROM flat_data WHERE form = ?", [form_id])
            conn.execute("DELETE FROM flat_data_staging WHERE form = ?", [form_id])

        await duckdb_connection.run_write(_work)
        return _DeleteResult(deleted_count=-1)

    async def delete_staging_by_upload_id(self, upload_id: str) -> None:
        await duckdb_connection.execute(
            "DELETE FROM flat_data_staging WHERE upload_id = ?",
            [upload_id],
        )

    async def insert_staging_batch(
        self,
        upload_id: str,
        rows: Sequence[Dict[str, Any]],
    ) -> int:
        if not rows:
            return 0

        params_list = [
            (
                upload_id,
                row["form"],
                row["file_id"],
                int(row["year"]),
                row["reporter"],
                row["section"],
                row["row"],
                row["column"],
                row.get("value"),
            )
            for row in rows
        ]

        def _work() -> int:
            conn = duckdb_connection._ensure_connection()
            conn.executemany(_INSERT_STAGING_SQL, params_list)
            return len(params_list)

        return await duckdb_connection.run_write(_work)

    async def find_staging_duplicate_keys(self, upload_id: str) -> List[Dict[str, Any]]:
        sql = """
            SELECT file_id, year, reporter, section, row, "column", COUNT(*) AS cnt
            FROM flat_data_staging
            WHERE upload_id = ?
            GROUP BY file_id, year, reporter, section, row, "column"
            HAVING COUNT(*) > 1
            LIMIT 10
        """
        rows = await duckdb_connection.fetchall(sql, [upload_id])
        return [
            {
                "file_id": r[0],
                "year": r[1],
                "reporter": r[2],
                "section": r[3],
                "row": r[4],
                "column": r[5],
                "duplicates_count": r[6],
            }
            for r in rows
        ]

    async def count_staging_main_conflicts(self, upload_id: str) -> int:
        sql = """
            SELECT COUNT(*)
            FROM flat_data AS main
            INNER JOIN flat_data_staging AS staging
                ON main.file_id = staging.file_id
               AND main.year = staging.year
               AND main.reporter = staging.reporter
               AND main.section = staging.section
               AND main.row = staging.row
               AND main."column" = staging."column"
            WHERE staging.upload_id = ?
        """
        row = await duckdb_connection.fetchone(sql, [upload_id])
        return int(row[0]) if row else 0

    async def promote_staging(self, upload_id: str) -> int:
        def _work() -> int:
            conn = duckdb_connection._ensure_connection()
            count_row = conn.execute(
                "SELECT COUNT(*) FROM flat_data_staging WHERE upload_id = ?",
                [upload_id],
            ).fetchone()
            staging_count = int(count_row[0]) if count_row else 0
            conn.execute(_PROMOTE_STAGING_SQL, [upload_id])
            conn.execute(
                "DELETE FROM flat_data_staging WHERE upload_id = ?",
                [upload_id],
            )
            return staging_count

        return await duckdb_connection.run_write(_work)

    async def total_row_count(self) -> int:
        row = await duckdb_connection.fetchone("SELECT COUNT(*) FROM flat_data")
        return int(row[0]) if row else 0


class _DeleteResult:
    def __init__(self, deleted_count: int) -> None:
        self.deleted_count = deleted_count


def _quote_column(name: str) -> str:
    if name in ("column", "row"):
        return f'"{name}"'
    return name
