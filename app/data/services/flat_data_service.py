import logging
import math
from typing import List, Dict, Any, Set

from pymongo import InsertOne
from app.data.repositories.flat_data import FlatDataRepository

logger = logging.getLogger(__name__)


def _to_builtin(obj):
    """
    Привести возможные numpy/pandas типы и прочие к "чистым" Python-типам,
    пригодным для записи в MongoDB через motor/pymongo
    """
    try:
        # bool is subclass of int — обрабатываем отдельно
        if isinstance(obj, bool):
            return bool(obj)
        # numpy scalar (has .item)
        if hasattr(obj, "item"):
            try:
                primitive = obj.item()
                return _to_builtin(primitive)
            except Exception:
                pass
        # nan -> None
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
        # fallback
        return str(obj)
    except Exception:
        try:
            return str(obj)
        except Exception:
            return None


class FlatDataService:
    def __init__(self, flat_data_repo: FlatDataRepository):
        self.flat_data_repo = flat_data_repo

    async def save_flat_data(self, records: List[Dict[str, Any]]) -> int:
        """
        Сохранение flat_data chunk'ами с последующей верификацией по file_id
        Возвращает общее количество вставленных документов (int)
        """
        if not records:
            logger.info("FlatDataService.save_flat_data: пустой список — ничего не вставляем")
            return 0

        # Нормализация всех записей
        normalized_records = []
        file_ids: Set[str] = set()
        for rec in records:
            if not isinstance(rec, dict):
                logger.warning("FlatDataService: пропущена не-dict запись: %s", type(rec))
                continue
            new_rec = {}
            for k, v in rec.items():
                new_rec[k] = _to_builtin(v)
            normalized_records.append(new_rec)
            fid = new_rec.get("file_id")
            if fid:
                file_ids.add(str(fid))

        total_inserted = 0
        CHUNK_SIZE = 5000
        logger.info("FlatDataService: попытка вставить %d записей (chunksize=%d). Коллекция: %s.%s",
                    len(normalized_records),
                    CHUNK_SIZE,
                    getattr(self.flat_data_repo.collection, "database", None),
                    getattr(self.flat_data_repo.collection, "name", None)
                    )

        # разбиваем на чанки и вставляем
        for i in range(0, len(normalized_records), CHUNK_SIZE):
            chunk = normalized_records[i:i + CHUNK_SIZE]
            try:
                # используем insert_many через bulk_write для совместимости
                operations = [InsertOne(doc) for doc in chunk]
                result = await self.flat_data_repo.collection.bulk_write(
                    operations,
                    ordered=False,
                    bypass_document_validation=True
                )
                # bulk_write не возвращает inserted_ids, но возвращает BulkWriteResult,
                # у него можно примерно посчитать вставленные по количеству операций - ошибок
                # поэтому считаем вставленные как len(chunk) если не упало
                inserted = len(chunk)
                total_inserted += inserted
                logger.info("FlatDataService: вставлено %d документов в чанке (индексы %d..%d)",
                            inserted, i, i + len(chunk) - 1)
            except Exception as e:
                logger.exception("FlatDataService: ошибка при bulk_write для чанка %d..%d: %s",
                                 i, i + len(chunk) - 1, e)
                # Попробуем вставлять по одному (чтобы логировать проблемные документы)
                for j, doc in enumerate(chunk):
                    try:
                        await self.flat_data_repo.collection.insert_one(doc)
                        total_inserted += 1
                    except Exception as e2:
                        logger.exception("FlatDataService: не удалось вставить документ (индекс %d в чанке): %s; документ: %s",
                                         j, e2, doc)

        logger.info("FlatDataService: итого вставлено %d документов", total_inserted)

        # Верификация: если у всех записей есть file_id и он один — проверяем count_documents
        if file_ids:
            for fid in file_ids:
                try:
                    count = await self.flat_data_repo.collection.count_documents({"file_id": fid})
                    logger.info("FlatDataService: в коллекции найдено %d документов для file_id=%s", count, fid)
                    # Если для конкретного file_id мы вставили >0, а Mongo показывает 0 — выбрасываем ошибку
                    # (это поможет локализовать проблему в runtime)
                    if count == 0:
                        raise RuntimeError(f"Post-insert verification failed: file_id={fid} -> 0 documents in DB")
                except Exception as e:
                    # логируем и пробрасываем — это явно ошибка вставки/видимости
                    logger.exception("FlatDataService: ошибка проверки вставки для file_id=%s: %s", fid, e)
                    raise

        else:
            # нет file_id в вставленных документах — это подозрительно, логим
            logger.warning("FlatDataService: вставленные документы не содержат file_id — невозможно выполнить post-insert verification")

        return total_inserted

    async def delete_by_file_id(self, file_id: str) -> int:
        """
        Удаляет все flat записи, связанные с file_id
        Возвращает deleted_count
        """
        res = await self.flat_data_repo.delete_many({"file_id": file_id})
        deleted = getattr(res, "deleted_count", None)
        logger.info("FlatDataService.delete_by_file_id: удалено %s документов для file_id=%s", deleted, file_id)
        return deleted or 0
