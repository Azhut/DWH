"""Сервис агрегата FlatData: сохранение, удаление, фильтрация (значения фильтров, отфильтрованные данные)."""
import logging
import math
from typing import Any, Dict, List, Set, Tuple, Union

from pymongo import InsertOne

from app.domain.flat_data.models import FILTER_MAP, FlatDataRecord, TABLE_FIELDS
from app.domain.flat_data.repository import FlatDataRepository

logger = logging.getLogger(__name__)


def _to_builtin(obj: Any) -> Any:
    """Приводит numpy/pandas и прочие типы к встроенным Python для записи в MongoDB."""
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


class FlatDataService:
    """Вся бизнес-логика по сущности FlatData: сохранение, удаление по file_id, фильтрация."""

    def __init__(self, repository: FlatDataRepository):
        self._repo = repository

    async def save_flat_data(self, records: List[FlatDataRecord]) -> int:
        """Сохраняет flat_data чанками. Возвращает количество вставленных документов."""
        if not records:
            logger.info("FlatDataService.save_flat_data: пустой список")
            return 0

        normalized_records = []
        file_ids: Set[str] = set()
        for rec in records:
            # Конвертируем FlatDataRecord в dict для MongoDB
            doc = rec.to_mongo_doc()
            new_rec = {k: _to_builtin(v) for k, v in doc.items()}
            normalized_records.append(new_rec)
            fid = new_rec.get("file_id")
            if fid:
                file_ids.add(str(fid))

        total_inserted = 0
        CHUNK_SIZE = 5000
        for i in range(0, len(normalized_records), CHUNK_SIZE):
            chunk = normalized_records[i : i + CHUNK_SIZE]
            try:
                operations = [InsertOne(doc) for doc in chunk]
                await self._repo.collection.bulk_write(
                    operations,
                    ordered=False,
                    bypass_document_validation=True,
                )
                total_inserted += len(chunk)
            except Exception as e:
                logger.exception("FlatDataService: ошибка bulk_write для чанка %d..%d: %s", i, i + len(chunk) - 1, e)
                for j, doc in enumerate(chunk):
                    try:
                        await self._repo.insert_one(doc)
                        total_inserted += 1
                    except Exception as e2:
                        logger.exception("FlatDataService: не удалось вставить документ %d: %s", j, e2)

        if file_ids:
            for fid in file_ids:
                count = await self._repo.count_documents({"file_id": fid})
                if count == 0:
                    raise RuntimeError(f"Post-insert verification failed: file_id={fid} -> 0 documents in DB")
        return total_inserted

    async def delete_by_file_id(self, file_id: str) -> int:
        res = await self._repo.delete_by_file_id(file_id)
        return getattr(res, "deleted_count", 0) or 0

    def _map_filter_name(self, name: str) -> str:
        key = name.lower()
        if key not in FILTER_MAP:
            raise ValueError(f"Неизвестный фильтр: {name}")
        return FILTER_MAP[key]

    def _build_query(self, filters: List[Dict[str, Any]], form_id: str | None) -> Dict[str, Any]:
        conditions: List[Dict[str, Any]] = []
        if form_id is not None:
            conditions.append({"form": form_id})
        for f in filters or []:
            fname = f.get("filter-name")
            values = f.get("values", []) or []
            if not fname or not values:
                continue
            field = self._map_filter_name(fname)
            if field == "city":
                values = [v.upper() if isinstance(v, str) else v for v in values]
            conditions.append({field: {"$in": values}})
        if not conditions:
            return {}
        if len(conditions) == 1:
            return conditions[0]
        return {"$and": conditions}

    async def get_filter_values(
        self,
        filter_name: str,
        applied_filters: List[Dict[str, Any]],
        pattern: str = "",
        form_id: str | None = None,
    ) -> List[Union[str, int, float]]:
        field = self._map_filter_name(filter_name)
        query = self._build_query(applied_filters, form_id)
        if pattern:
            query = {**query, field: {"$regex": pattern, "$options": "i"}}
        values = await self._repo.collection.distinct(field, filter=query)
        try:
            values = sorted(values)
        except Exception:
            pass
        return values

    async def get_filtered_data(
        self,
        filters: List[Dict[str, Any]],
        limit: int,
        offset: int,
        form_id: str | None = None,
    ) -> Tuple[List[List[Union[str, int, float, None]]], int]:
        query = self._build_query(filters, form_id)
        docs, total = await self._repo.get_filtered_data(query, limit, offset)
        table = self._process_docs_to_table(docs)
        return table, total

    def _process_docs_to_table(self, docs: List[Dict[str, Any]]) -> List[List[Union[str, int, float, None]]]:
        rows: List[List[Union[str, int, float, None]]] = []
        for doc in docs:
            record = FlatDataRecord.from_mongo_doc(doc)
            if (
                record.year is None
                or record.city is None
                or record.section is None
                or record.row is None
                or record.column is None
            ):
                raise ValueError("Документ не содержит обязательных полей")
            value = record.value
            if isinstance(value, float):
                value = None if math.isnan(value) else (int(value) if value == int(value) else round(value, 2))
            rows.append([record.year, record.city, record.section, record.row, record.column, value])
        return rows
