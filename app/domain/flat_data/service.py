"""Сервис агрегата FlatData: сохранение, удаление, фильтрация (значения фильтров, отфильтрованные данные)."""
import hashlib
import json
import logging
import math
from functools import lru_cache
from typing import Any, Dict, List, Set, Tuple, Union

from pymongo import InsertOne

from app.core.exceptions import CriticalUploadError
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
        self._filter_cache: Dict[str, List[Union[str, int, float]]] = {}
        self._cache_max_size = 128

    def _generate_cache_key(
        self, 
        filter_name: str, 
        applied_filters: List[Dict[str, Any]], 
        pattern: str, 
        form_id: str | None
    ) -> str:
        """Генерирует ключ для кэширования результатов фильтрации."""
        cache_data = {
            "filter_name": filter_name,
            "applied_filters": sorted(applied_filters, key=lambda x: x.get("filter-name", "")),
            "pattern": pattern,
            "form_id": form_id
        }
        cache_str = json.dumps(cache_data, sort_keys=True, default=str)
        return hashlib.md5(cache_str.encode()).hexdigest()

    async def get_filter_values(
        self,
        filter_name: str,
        applied_filters: List[Dict[str, Any]],
        pattern: str = "",
        form_id: str | None = None,
    ) -> List[Union[str, int, float]]:
        cache_key = self._generate_cache_key(filter_name, applied_filters, pattern, form_id)
        
        # Проверяем кэш
        if cache_key in self._filter_cache:
            return self._filter_cache[cache_key]
        
        # Получаем данные из БД
        field = self._map_filter_name(filter_name)
        query = self._build_query(applied_filters, form_id)
        if pattern:
            query = {**query, field: {"$regex": pattern, "$options": "i"}}
        values = await self._repo.collection.distinct(field, filter=query)
        try:
            values = sorted(values)
        except Exception:
            pass
        
        # Сохраняем в кэш
        if len(self._filter_cache) >= self._cache_max_size:
            # Удаляем самый старый элемент (простая реализация)
            oldest_key = next(iter(self._filter_cache))
            del self._filter_cache[oldest_key]
        
        self._filter_cache[cache_key] = values
        return values

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
                result = await self._repo.collection.bulk_write(
                    operations,
                    ordered=False,
                    bypass_document_validation=True,
                )
                
                # Проверяем на ошибки вставки, включая дубликаты
                if result.bulk_api_result.get('writeErrors'):
                    for error in result.bulk_api_result['writeErrors']:
                        if error.get('code') == 11000:  # MongoDB duplicate key error
                            # Извлекаем информацию о дублирующемся документе
                            error_doc = error.get('op', {})
                            duplicate_info = {
                                'file_id': error_doc.get('file_id'),
                                'year': error_doc.get('year'),
                                'reporter': error_doc.get('reporter'),
                                'section': error_doc.get('section'),
                                'row': error_doc.get('row'),
                                'column': error_doc.get('column'),
                                'value': error_doc.get('value')
                            }
                            
                            raise CriticalUploadError(
                                message=f"Duplicate data detected during bulk insert: {error.get('errmsg')}",
                                domain="upload.duplicate_data",
                                http_status=400,
                                meta={
                                    "error": error,
                                    "duplicate_document": duplicate_info,
                                    "chunk_range": f"{i}-{i + len(chunk) - 1}",
                                    "file_id": list(file_ids)
                                }
                            )
                        else:
                            logger.error("Bulk write error: %s", error)
                
                chunk_inserted = result.inserted_count
                total_inserted += chunk_inserted
                logger.debug("FlatDataService: чанк %d..%d успешно вставлен: %d записей", 
                           i, i + len(chunk) - 1, chunk_inserted)
                
            except Exception as e:
                # Если это не наша критическая ошибка, логируем и пробуем поштучно
                if not isinstance(e, CriticalUploadError):
                    logger.error("FlatDataService: ошибка bulk_write для чанка %d..%d: %s", i, i + len(chunk) - 1, e)
                else:
                    # Пробрасываем критическую ошибку (дубликаты)
                    raise
                
                # Поштучная вставка для резилвенции проблем
                for j, doc in enumerate(chunk):
                    try:
                        await self._repo.insert_one(doc)
                        total_inserted += 1
                        logger.debug("FlatDataService: документ %d успешно вставлен поштучно", i + j)
                    except Exception as e2:
                        if "duplicate key" in str(e2).lower() or e2.__class__.__name__ == 'DuplicateKeyError':
                            # Собираем информацию о дублирующемся документе
                            duplicate_info = {
                                'file_id': doc.get('file_id'),
                                'year': doc.get('year'),
                                'reporter': doc.get('reporter'),
                                'section': doc.get('section'),
                                'row': doc.get('row'),
                                'column': doc.get('column'),
                                'value': doc.get('value')
                            }
                            
                            raise CriticalUploadError(
                                message=f"Duplicate data detected during individual insert: {e2}",
                                domain="upload.duplicate_data",
                                http_status=400,
                                meta={
                                    "error": str(e2),
                                    "duplicate_document": duplicate_info,
                                    "document_index": i + j,
                                    "file_id": doc.get("file_id")
                                }
                            )
                        logger.error("FlatDataService: не удалось вставить документ %d: %s", i + j, e2)

        if file_ids:
            for fid in file_ids:
                count = await self._repo.count_documents({"file_id": fid})
                if count == 0:
                    raise RuntimeError(f"Post-insert verification failed: file_id={fid} -> 0 documents in DB")
        return total_inserted

    async def delete_by_file_id(self, file_id: str) -> int:
        res = await self._repo.delete_by_file_id(file_id)
        return getattr(res, "deleted_count", 0) or 0

    async def delete_by_form_id(self, form_id: str) -> int:
        res = await self._repo.delete_by_form(form_id)
        return getattr(res, "deleted_count", 0) or 0

    def _map_filter_name(self, name: str) -> str:
        """Маппит имя фильтра API в имя поля БД. Валидация фильтра выполняется в API слое."""
        return FILTER_MAP[name.lower()]

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
            if field == "reporter":
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
                or record.reporter is None
                or record.section is None
                or record.row is None
                or record.column is None
            ):
                raise ValueError("Документ не содержит обязательных полей")
            value = record.value
            if isinstance(value, float):
                value = None if math.isnan(value) else (int(value) if value == int(value) else round(value, 2))
            rows.append([record.year, record.reporter, record.section, record.row, record.column, value])
        return rows
