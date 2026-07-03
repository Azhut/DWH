#!/usr/bin/env python3
"""
Одноразовая фоновая миграция коллекции MongoDB FlatData → DuckDB flat_data.

Запускать при ОСТАНОВЛЕННОМ API (sport_api), чтобы не было конкурентной записи:

  cd /home/user/dashboards/DWH
  docker compose up -d mongo
  docker compose run --rm app python scripts/migrate_flatdata_mongo_to_duckdb.py

Или в фоне на VM:

  nohup docker compose run --rm app python scripts/migrate_flatdata_mongo_to_duckdb.py \
    > migration_flatdata.log 2>&1 &

Повторный запуск безопасен: продолжает с последнего _id (migration_state в DuckDB).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from bson import ObjectId

from app.core.database import mongo_connection
from app.core.duckdb_database import duckdb_connection
from app.domain.flat_data.value_utils import normalize_flat_value
from config.config import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("migrate_flatdata")

_STATE_STATUS = "mongo_flatdata_migration_status"
_STATE_LAST_ID = "mongo_flatdata_migration_last_id"
_STATE_INSERTED = "mongo_flatdata_migration_inserted"
_STATUS_COMPLETED = "completed"
_STATUS_RUNNING = "running"

_INSERT_SQL = """
INSERT INTO flat_data (
    form, file_id, year, reporter, section, row, "column", value
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
"""


def _doc_to_row(doc: Dict[str, Any]) -> Optional[tuple]:
    year = doc.get("year")
    reporter = doc.get("reporter")
    section = doc.get("section")
    row = doc.get("row")
    column = doc.get("column")
    file_id = doc.get("file_id")
    form = doc.get("form")

    if None in (year, reporter, section, row, column, file_id, form):
        return None

    if isinstance(year, float):
        year = int(year)

    value = normalize_flat_value(doc.get("value"))

    return (
        str(form),
        str(file_id),
        int(year),
        str(reporter),
        str(section),
        str(row),
        str(column),
        value,
    )


def _get_migration_state(conn) -> Dict[str, str]:
    rows = conn.execute("SELECT key, value FROM migration_state").fetchall()
    return {r[0]: r[1] for r in rows}


def _set_state(conn, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO migration_state (key, value) VALUES (?, ?)
        ON CONFLICT (key) DO UPDATE SET value = excluded.value
        """,
        [key, value],
    )


def _insert_batch_sync(rows: List[tuple]) -> int:
    conn = duckdb_connection._ensure_connection()
    conn.execute("BEGIN TRANSACTION")
    conn.executemany(_INSERT_SQL, rows)
    conn.execute("COMMIT")
    return len(rows)


async def migrate(
    *,
    reset: bool = False,
    batch_size: Optional[int] = None,
    dry_run: bool = False,
    dry_run_limit: Optional[int] = None,
) -> None:
    batch_size = batch_size or config.DUCKDB_MIGRATION_BATCH_SIZE
    duckdb_connection.initialize_schema_sync()
    conn = duckdb_connection._ensure_connection()

    if reset and dry_run:
        logger.warning("Dry run + reset: данные не будут удалены, только проверка состояния")
    elif reset:
        logger.warning("RESET: очищаем flat_data и состояние миграции")
        conn.execute("DELETE FROM flat_data")
        conn.execute("DELETE FROM migration_state")

    state = _get_migration_state(conn)
    if state.get(_STATE_STATUS) == _STATUS_COMPLETED and not reset:
        logger.info("Миграция уже завершена (status=completed). Выход.")
        duckdb_total = conn.execute("SELECT COUNT(*) FROM flat_data").fetchone()[0]
        logger.info("Строк в DuckDB flat_data: %s", duckdb_total)
        return

    db = mongo_connection.get_database()
    collection = db["FlatData"]

    mongo_total = await collection.count_documents({})
    logger.info("Документов в MongoDB FlatData: %s", mongo_total)

    last_id_str = state.get(_STATE_LAST_ID)
    processed_so_far = int(state.get(_STATE_INSERTED, "0") or 0)
    query: Dict[str, Any] = {}
    if last_id_str:
        query["_id"] = {"$gt": ObjectId(last_id_str)}
        logger.info(
            "Продолжение с _id > %s (уже обработано ~%s)",
            last_id_str,
            processed_so_far,
        )

    if not dry_run:
        _set_state(conn, _STATE_STATUS, _STATUS_RUNNING)
    else:
        logger.info("Dry run: изменения в DuckDB выполняться не будут")

    skipped_invalid = 0
    batch_rows: List[tuple] = []
    last_id: Optional[ObjectId] = None
    started = time.monotonic()

    cursor = collection.find(query, projection={"_id": 1, "form": 1, "file_id": 1, "year": 1,
        "reporter": 1, "section": 1, "row": 1, "column": 1, "value": 1}).sort("_id", 1)

    progress_count = 0
    async for doc in cursor:
        last_id = doc["_id"]
        row = _doc_to_row(doc)
        if row is None:
            skipped_invalid += 1
            continue
        batch_rows.append(row)
        progress_count += 1

        if dry_run and dry_run_limit is not None and progress_count >= dry_run_limit:
            logger.info("Достигнут dry-run лимит %s документов", dry_run_limit)
            break

        if len(batch_rows) >= batch_size:
            if not dry_run:
                n = await asyncio.to_thread(_insert_batch_sync, batch_rows)
            else:
                n = len(batch_rows)
            processed_so_far += n
            batch_rows = []
            if not dry_run:
                _set_state(conn, _STATE_LAST_ID, str(last_id))
                _set_state(conn, _STATE_INSERTED, str(processed_so_far))
            elapsed = time.monotonic() - started
            rate = processed_so_far / elapsed if elapsed > 0 else 0
            logger.info(
                "Прогресс: %s / %s (%.1f%%), %.0f rows/s, last_id=%s",
                processed_so_far,
                mongo_total,
                100.0 * processed_so_far / mongo_total if mongo_total else 0,
                rate,
                last_id,
            )

    if batch_rows:
        if not dry_run:
            n = await asyncio.to_thread(_insert_batch_sync, batch_rows)
        else:
            n = len(batch_rows)
        processed_so_far += n
        if last_id is not None and not dry_run:
            _set_state(conn, _STATE_LAST_ID, str(last_id))
        if not dry_run:
            _set_state(conn, _STATE_INSERTED, str(processed_so_far))

    duckdb_total = conn.execute("SELECT COUNT(*) FROM flat_data").fetchone()[0]
    if not dry_run:
        _set_state(conn, _STATE_STATUS, _STATUS_COMPLETED)

    elapsed = time.monotonic() - started
    logger.info(
        "%s за %.1f с",
        "Dry run завершён" if dry_run else "Миграция завершена",
        elapsed,
    )
    logger.info("Обработано документов Mongo: %s", processed_so_far)
    logger.info("Строк в DuckDB flat_data: %s", duckdb_total)
    logger.info("Пропущено битых документов: %s", skipped_invalid)

    if mongo_total and abs(duckdb_total - mongo_total) > skipped_invalid + 100:
        logger.warning(
            "Расхождение counts: mongo=%s duckdb=%s (допустимо при дублях/битых строках)",
            mongo_total,
            duckdb_total,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate MongoDB FlatData to DuckDB")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Очистить flat_data и начать миграцию заново",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help=f"Размер батча (по умолчанию {config.DUCKDB_MIGRATION_BATCH_SIZE})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Прогон без записи в DuckDB: проверка чтения Mongo + нормализации",
    )
    parser.add_argument(
        "--dry-run-limit",
        type=int,
        default=None,
        help="Остановиться после указанного числа документов в dry-run режиме",
    )
    args = parser.parse_args()
    if args.dry_run_limit is not None and not args.dry_run:
        parser.error("--dry-run-limit можно использовать только вместе с --dry-run")
    asyncio.run(
        migrate(
            reset=args.reset,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            dry_run_limit=args.dry_run_limit,
        )
    )


if __name__ == "__main__":
    main()
