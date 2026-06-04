"""Сервис агрегата FlatData: сохранение, удаление, фильтрация."""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Set, Tuple, Union

from pymongo import InsertOne

from app.core.exceptions import CriticalUploadError
from app.domain.flat_data.models import FILTER_MAP, FlatDataRecord, TABLE_FIELDS
from app.domain.flat_data.duckdb_repository import DuckDBFlatDataRepository
from app.domain.flat_data.repository import FlatDataRepository
from app.domain.flat_data.value_utils import normalize_flat_value
from config.config import config

logger = logging.getLogger(__name__)


def _to_builtin(obj: Any) -> Any:
    """Приводит значения к типам, пригодным для BSON (numpy/pandas и др.)."""
    try:
        if isinstance(obj, bool):
            return bool(obj)
        if hasattr(obj, "item"):
            try:
                return _to_builtin(obj.item())
            except Exception:
                pass
        try:
            if isinstance(obj, float) and math.isnan(obj):
                return None
        except Exception:
            pass
        if isinstance(obj, (int, float, str)):
            return obj
        if isinstance(obj, (bytes, bytearray)):
            try:
                return obj.decode("utf-8")
            except Exception:
                return str(obj)
        return str(obj)
    except Exception:
        try:
            return str(obj)
        except Exception:
            return None


def _normalize_record_for_storage(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Приводит документ FlatData к BSON/DuckDB-совместимым типам."""
    new_rec = {k: _to_builtin(v) for k, v in doc.items()}
    if "value" in new_rec:
        new_rec["value"] = normalize_flat_value(new_rec.get("value"))
    return new_rec


def _make_cache_key(
    filter_name: str,
    applied_filters: List[Dict[str, Any]],
    pattern: str,
    form_id: str | None,
) -> int:
    """Строит ключ кэша как хэш неизменяемой структуры.

    Заменяет JSON-сериализацию + MD5 на встроенный hash() от frozenset/tuple:
    операция занимает микросекунды вместо миллисекунд при большом числе
    вызовов в секунду и не требует импорта hashlib/json.
    """
    filters_key = frozenset(
        (f.get("filter-name", ""), tuple(sorted(str(v) for v in f.get("values", []))))
        for f in applied_filters
    )
    return hash((filter_name, filters_key, pattern, form_id))


class FlatDataService:
    """Сохранение плоских строк, удаление по file_id/form, выдача данных для фильтров и таблицы."""

    def __init__(self, repository: FlatDataRepository | DuckDBFlatDataRepository) -> None:
        self._repo = repository
        self._filter_cache: Dict[int, List[Union[str, int, float]]] = {}
        self._cache_max_size = 128

    async def get_filter_values(
        self,
        filter_name: str,
        applied_filters: List[Dict[str, Any]],
        pattern: str = "",
        form_id: str | None = None,
    ) -> List[Union[str, int, float]]:
        """
        Возвращает отсортированный список допустимых значений для имени фильтра
        с учётом уже выбранных условий.

        Результаты кэшируются в памяти процесса с ограничением размера кэша.
        """
        cache_key = _make_cache_key(filter_name, applied_filters, pattern, form_id)

        if cache_key in self._filter_cache:
            return self._filter_cache[cache_key]

        field = self._map_filter_name(filter_name)
        query = self._build_query(applied_filters, form_id)
        if pattern:
            query = {**query, field: {"$regex": pattern, "$options": "i"}}

        values = await self._repo.distinct(field, query)
        try:
            values = sorted(values)
        except Exception:
            pass

        if len(self._filter_cache) >= self._cache_max_size:
            oldest_key = next(iter(self._filter_cache))
            del self._filter_cache[oldest_key]

        self._filter_cache[cache_key] = values
        return values

    async def save_flat_data(self, records: List[FlatDataRecord], *, session: Any = None) -> int:
        """
        Пакетно сохраняет строки FlatData чанками фиксированного размера.

        Возвращает число фактически вставленных документов. При ошибке дубликата
        по уникальному индексу выбрасывает CriticalUploadError.

        Для DuckDB используется staging → проверки → promote.
        """
        if not records:
            logger.info("FlatDataService.save_flat_data: пустой список")
            return 0

        if isinstance(self._repo, DuckDBFlatDataRepository):
            return await self._save_flat_data_staged(records)

        normalized_records: List[Dict[str, Any]] = []
        file_ids: Set[str] = set()
        for rec in records:
            new_rec = _normalize_record_for_storage(rec.to_mongo_doc())
            normalized_records.append(new_rec)
            fid = new_rec.get("file_id")
            if fid:
                file_ids.add(str(fid))

        total_inserted = 0
        chunk_size = config.FLATDATA_BULK_CHUNK_SIZE

        for i in range(0, len(normalized_records), chunk_size):
            chunk = normalized_records[i : i + chunk_size]
            try:
                operations = [InsertOne(doc) for doc in chunk]
                result = await self._repo.bulk_write_ops(
                    operations,
                    ordered=False,
                    session=session,
                )

                if result.bulk_api_result.get("writeErrors"):
                    for error in result.bulk_api_result["writeErrors"]:
                        if error.get("code") == 11000:
                            error_doc = error.get("op", {})
                            duplicate_info = {
                                "file_id": error_doc.get("file_id"),
                                "year": error_doc.get("year"),
                                "reporter": error_doc.get("reporter"),
                                "section": error_doc.get("section"),
                                "row": error_doc.get("row"),
                                "column": error_doc.get("column"),
                                "value": error_doc.get("value"),
                            }
                            raise CriticalUploadError(
                                message=f"Duplicate data detected during bulk insert: {error.get('errmsg')}",
                                domain="upload.duplicate_data",
                                http_status=400,
                                meta={
                                    "error": error,
                                    "duplicate_document": duplicate_info,
                                    "chunk_range": f"{i}-{i + len(chunk) - 1}",
                                    "file_id": list(file_ids),
                                },
                            )
                        logger.error("Bulk write error: %s", error)

                chunk_inserted = result.inserted_count
                total_inserted += chunk_inserted
                logger.debug(
                    "FlatDataService: чанк %d..%d вставлен: %d записей",
                    i,
                    i + len(chunk) - 1,
                    chunk_inserted,
                )

            except Exception as e:
                if not isinstance(e, CriticalUploadError):
                    logger.error(
                        "FlatDataService: ошибка bulk_write для чанка %d..%d: %s",
                        i,
                        i + len(chunk) - 1,
                        e,
                    )
                else:
                    raise

                for j, doc in enumerate(chunk):
                    try:
                        await self._repo.insert_one(doc, session=session)
                        total_inserted += 1
                        logger.debug("FlatDataService: документ %d вставлен поштучно", i + j)
                    except Exception as e2:
                        if "duplicate key" in str(e2).lower() or e2.__class__.__name__ == "DuplicateKeyError":
                            duplicate_info = {
                                "file_id": doc.get("file_id"),
                                "year": doc.get("year"),
                                "reporter": doc.get("reporter"),
                                "section": doc.get("section"),
                                "row": doc.get("row"),
                                "column": doc.get("column"),
                                "value": doc.get("value"),
                            }
                            raise CriticalUploadError(
                                message=f"Duplicate data detected during individual insert: {e2}",
                                domain="upload.duplicate_data",
                                http_status=400,
                                meta={
                                    "error": str(e2),
                                    "duplicate_document": duplicate_info,
                                    "document_index": i + j,
                                    "file_id": doc.get("file_id"),
                                },
                            ) from e2
                        logger.error("FlatDataService: не удалось вставить документ %d: %s", i + j, e2)

        if file_ids:
            for fid in file_ids:
                count = await self._repo.count_documents({"file_id": fid}, session=session)
                if count == 0:
                    raise RuntimeError(f"Post-insert verification failed: file_id={fid} -> 0 documents in DB")
        return total_inserted

    async def _save_flat_data_staged(self, records: List[FlatDataRecord]) -> int:
        """DuckDB: staging, дедупликация, promote в flat_data."""
        repo: DuckDBFlatDataRepository = self._repo  # type: ignore[assignment]

        normalized_records: List[Dict[str, Any]] = []
        file_ids: Set[str] = set()
        for rec in records:
            new_rec = _normalize_record_for_storage(rec.to_mongo_doc())
            if new_rec.get("file_id"):
                file_ids.add(str(new_rec["file_id"]))
            normalized_records.append(new_rec)

        if not file_ids:
            raise CriticalUploadError(
                message="FlatData records must include file_id",
                domain="upload.persist",
                http_status=400,
            )

        upload_id = next(iter(file_ids))
        if len(file_ids) > 1:
            raise CriticalUploadError(
                message="Single upload must not mix multiple file_id values",
                domain="upload.persist",
                http_status=400,
                meta={"file_ids": list(file_ids)},
            )

        chunk_size = config.FLATDATA_BULK_CHUNK_SIZE
        try:
            for i in range(0, len(normalized_records), chunk_size):
                chunk = normalized_records[i : i + chunk_size]
                await repo.insert_staging_batch(upload_id, chunk)

            dupes = await repo.find_staging_duplicate_keys(upload_id)
            if dupes:
                raise CriticalUploadError(
                    message="Duplicate rows detected in staging",
                    domain="upload.duplicate_data",
                    http_status=400,
                    meta={"duplicates": dupes, "file_id": upload_id},
                )

            conflicts = await repo.count_staging_main_conflicts(upload_id)
            if conflicts > 0:
                raise CriticalUploadError(
                    message="Data conflicts with existing flat_data",
                    domain="upload.duplicate_data",
                    http_status=400,
                    meta={"conflict_count": conflicts, "file_id": upload_id},
                )

            inserted_total = await repo.promote_staging(upload_id)
        except CriticalUploadError:
            await repo.delete_staging_by_upload_id(upload_id)
            raise
        except Exception as exc:
            await repo.delete_staging_by_upload_id(upload_id)
            try:
                await repo.delete_by_file_id(upload_id)
            except Exception as cleanup_exc:
                logger.error(
                    "Cleanup flat_data after staged save failure for %s: %s",
                    upload_id,
                    cleanup_exc,
                )
            raise CriticalUploadError(
                message=f"Failed to save flat data via staging: {exc}",
                domain="upload.persist",
                http_status=500,
                meta={"file_id": upload_id, "error": str(exc)},
                show_traceback=True,
            ) from exc

        expected_count = len(normalized_records)
        if expected_count > 0 and inserted_total < expected_count:
            logger.warning(
                "Staged promote count %s < expected %s for file_id=%s",
                inserted_total,
                expected_count,
                upload_id,
            )

        count = await repo.count_documents({"file_id": upload_id})
        if count == 0 and expected_count > 0:
            raise RuntimeError(
                f"Post-promote verification failed: file_id={upload_id} -> 0 rows in flat_data"
            )
        return inserted_total

    async def delete_by_file_id(self, file_id: str, *, session: Any = None) -> int:
        """Удаляет все документы FlatData с указанным file_id."""
        res = await self._repo.delete_by_file_id(file_id, session=session)
        return getattr(res, "deleted_count", 0) or 0

    async def delete_by_form_id(self, form_id: str, *, session: Any = None) -> int:
        """Удаляет все документы FlatData, относящиеся к форме."""
        res = await self._repo.delete_by_form(form_id, session=session)
        return getattr(res, "deleted_count", 0) or 0

    def _map_filter_name(self, name: str) -> str:
        """Сопоставляет имя фильтра из API с полем в коллекции."""
        return FILTER_MAP[name.lower()]

    def _build_query(self, filters: List[Dict[str, Any]], form_id: str | None) -> Dict[str, Any]:
        """Собирает MongoDB-фильтр из списка условий фильтрации."""
        conditions: List[Dict[str, Any]] = []
        if form_id is not None:
            conditions.append({"form": form_id})
        for f in filters or []:
            fname = f.get("filter-name")
            values = f.get("values", []) or []
            if not fname or not values:
                continue
            field = self._map_filter_name(fname)
            if field == "reporter":
                values = [v.upper() if isinstance(v, str) else v for v in values]
            conditions.append({field: {"$in": values}})
        if not conditions:
            return {}
        if len(conditions) == 1:
            return conditions[0]
        return {"$and": conditions}

    async def get_filtered_data(
        self,
        filters: List[Dict[str, Any]],
        limit: int,
        offset: int,
        form_id: str | None = None,
    ) -> Tuple[List[List[Union[str, int, float, None]]], int]:
        """
        Возвращает таблицу строк (год, респондент, раздел, строка, колонка, значение)
        и общее число строк по фильтру.
        """
        query = self._build_query(filters, form_id)
        docs, total = await self._repo.get_filtered_data(query, limit, offset)
        table = self._process_docs_to_table(docs)
        return table, total

    def _process_docs_to_table(
        self, docs: List[Dict[str, Any]]
    ) -> List[List[Union[str, int, float, None]]]:
        """Преобразует сырые документы в строки таблицы для API.

        Прямой доступ к словарю вместо FlatDataRecord.from_mongo_doc() на каждой
        строке устраняет оверхед создания объектов при возврате тысяч записей.
        Логика нормализации value идентична оригиналу.
        """
        rows: List[List[Union[str, int, float, None]]] = []
        for doc in docs:
            year     = doc.get("year")
            reporter = doc.get("reporter")
            section  = doc.get("section")
            row      = doc.get("row")
            column   = doc.get("column")

            if year is None or reporter is None or section is None or row is None or column is None:
                raise ValueError("Документ не содержит обязательных полей")

            value = doc.get("value")
            if isinstance(value, float):
                if math.isnan(value):
                    value = None
                elif value == int(value):
                    value = int(value)
                else:
                    value = round(value, 2)

            rows.append([year, reporter, section, row, column, value])
        return rows