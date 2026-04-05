"""Сценарий сохранения загрузки: согласованность Files, FlatData и журналов."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

from app.core.exceptions import CriticalUploadError
from app.core.mongo_transactions import run_in_transaction
from app.domain.file.models import FileModel, FileStatus
from app.domain.file.service import FileService
from app.domain.flat_data.models import FlatDataRecord
from app.domain.flat_data.service import FlatDataService
from app.domain.log.service import LogService
from config.config import config

logger = logging.getLogger(__name__)


class DataSaveService:
    """Координирует запись файла и FlatData с транзакцией при допустимом объёме или с компенсирующей очисткой."""

    def __init__(
        self,
        file_service: FileService,
        flat_data_service: FlatDataService,
        log_service: LogService,
    ) -> None:
        self._file_service = file_service
        self._flat_data_service = flat_data_service
        self._log_service = log_service

    def _should_use_transaction_for_flat_count(self, flat_count: int) -> bool:
        """Возвращает True, если объём FlatData укладывается в лимит одной много-документной транзакции."""
        if not config.MONGO_USE_TRANSACTIONS:
            return False
        return flat_count <= config.MONGO_TRANSACTION_MAX_FLAT_RECORDS

    async def process_and_save_all(
        self,
        file_model: FileModel,
        flat_data: Optional[List[FlatDataRecord]] = None,
    ) -> None:
        """Атомарно или пошагово сохраняет файл и строки FlatData, затем пишет записи в Logs."""
        flat_list = flat_data or []
        use_tx = self._should_use_transaction_for_flat_count(len(flat_list))
        try:
            if use_tx:
                await self._persist_in_transaction(file_model, flat_list)
            else:
                await self._persist_with_cleanup_on_failure(file_model, flat_list)
        except CriticalUploadError:
            raise
        except Exception as exc:
            raise CriticalUploadError(
                message="Error while saving uploaded file data",
                domain="upload.persist",
                http_status=500,
                meta={"file_id": file_model.file_id, "error": str(exc)},
                show_traceback=True,
            ) from exc

    async def _persist_in_transaction(
        self,
        file_model: FileModel,
        flat_list: List[FlatDataRecord],
    ) -> None:
        """Выполняет обновление Files и вставку FlatData в одной транзакции (при поддержке сервером)."""

        async def work(session):
            await self._file_service.update_or_create(file_model, session=session)
            inserted_total = 0
            if flat_list:
                inserted_total = await self._flat_data_service.save_flat_data(flat_list, session=session)
            file_model.status = FileStatus.SUCCESS
            file_model.flat_data_size = inserted_total
            await self._file_service.update_or_create(file_model, session=session)
            return inserted_total

        inserted_total = await run_in_transaction(work)

        if not flat_list:
            await self._log_service.save_log(
                scenario="upload",
                message=f"FlatData is empty for {file_model.file_id}",
                level="warning",
                meta={"file_id": file_model.file_id},
            )

        expected_count = len(flat_list)
        if expected_count > 0 and inserted_total < expected_count:
            discrepancy = expected_count - inserted_total
            await self._log_service.save_log(
                scenario="upload",
                message=(
                    f"Data discrepancy detected for {file_model.file_id}: "
                    f"expected {expected_count}, inserted {inserted_total}"
                ),
                level="warning",
                meta={
                    "file_id": file_model.file_id,
                    "expected_count": expected_count,
                    "inserted_count": inserted_total,
                    "discrepancy": discrepancy,
                },
            )
            logger.warning(
                "DataSaveService discrepancy for %s: expected %s, inserted %s (missing %s)",
                file_model.file_id,
                expected_count,
                inserted_total,
                discrepancy,
            )

        await self._log_service.save_log(
            scenario="upload",
            message=f"Data saved successfully for {file_model.file_id}; flat_inserted={inserted_total}",
            meta={"file_id": file_model.file_id, "flat_inserted": inserted_total},
        )
        logger.info(
            "DataSaveService completed for %s; flat_inserted=%s",
            file_model.file_id,
            inserted_total,
        )

    async def _persist_with_cleanup_on_failure(
        self,
        file_model: FileModel,
        flat_list: List[FlatDataRecord],
    ) -> None:
        """
        Сохраняет большой объём без одной транзакции (ограничение размера транзакции MongoDB).

        При ошибке после частичной вставки удаляет FlatData по file_id, чтобы не оставлять мусор.
        """
        try:
            await self._file_service.update_or_create(file_model)
            await self._log_service.save_log(
                scenario="upload",
                message=f"Start processing file {file_model.file_id}",
                meta={"file_id": file_model.file_id},
            )

            inserted_total = 0
            if flat_list:
                inserted_total = await self._flat_data_service.save_flat_data(flat_list)
            else:
                await self._log_service.save_log(
                    scenario="upload",
                    message=f"FlatData is empty for {file_model.file_id}",
                    level="warning",
                    meta={"file_id": file_model.file_id},
                )

            file_model.status = FileStatus.SUCCESS
            file_model.flat_data_size = inserted_total

            expected_count = len(flat_list)
            if expected_count > 0 and inserted_total < expected_count:
                discrepancy = expected_count - inserted_total
                await self._log_service.save_log(
                    scenario="upload",
                    message=(
                        f"Data discrepancy detected for {file_model.file_id}: "
                        f"expected {expected_count}, inserted {inserted_total}"
                    ),
                    level="warning",
                    meta={
                        "file_id": file_model.file_id,
                        "expected_count": expected_count,
                        "inserted_count": inserted_total,
                        "discrepancy": discrepancy,
                    },
                )
                logger.warning(
                    "DataSaveService discrepancy for %s: expected %s, inserted %s (missing %s)",
                    file_model.file_id,
                    expected_count,
                    inserted_total,
                    discrepancy,
                )

            await self._file_service.update_or_create(file_model)
            await self._log_service.save_log(
                scenario="upload",
                message=f"Data saved successfully for {file_model.file_id}; flat_inserted={inserted_total}",
                meta={"file_id": file_model.file_id, "flat_inserted": inserted_total},
            )
            logger.info(
                "DataSaveService completed for %s; flat_inserted=%s",
                file_model.file_id,
                inserted_total,
            )
        except Exception:
            try:
                await self._flat_data_service.delete_by_file_id(file_model.file_id)
            except Exception as cleanup_exc:
                logger.error(
                    "Не удалось очистить FlatData после сбоя сохранения для %s: %s",
                    file_model.file_id,
                    cleanup_exc,
                )
            raise

    async def rollback(self, file_model: FileModel, error: str) -> None:
        """Откатывает загрузку: удаляет FlatData и помечает файл как FAILED в одной транзакции, затем пишет лог."""

        async def work(session):
            await self._flat_data_service.delete_by_file_id(file_model.file_id, session=session)
            file_model.status = FileStatus.FAILED
            file_model.error = error
            file_model.updated_at = datetime.now()
            await self._file_service.update_or_create(file_model, session=session)

        try:
            await run_in_transaction(work)
        except Exception as exc:
            await self._log_service.save_log(
                scenario="upload",
                message=f"Failed transactional rollback for {file_model.file_id}: {exc}",
                level="error",
                meta={"file_id": file_model.file_id, "error": str(exc)},
            )
            try:
                await self._flat_data_service.delete_by_file_id(file_model.file_id)
                file_model.status = FileStatus.FAILED
                file_model.error = error
                file_model.updated_at = datetime.now()
                await self._file_service.update_or_create(file_model)
            except Exception as fallback_exc:
                await self._log_service.save_log(
                    scenario="upload",
                    message=f"Failed non-transactional rollback for {file_model.file_id}: {fallback_exc}",
                    level="error",
                    meta={"file_id": file_model.file_id, "error": str(fallback_exc)},
                )
                await self._log_service.save_log(
                    scenario="upload",
                    message=f"Rollback file {file_model.file_id}: {error} (неполный откат)",
                    level="error",
                    meta={"file_id": file_model.file_id, "error": error},
                )
                return

        await self._log_service.save_log(
            scenario="upload",
            message=f"Rollback file {file_model.file_id}: {error}",
            level="error",
            meta={"file_id": file_model.file_id, "error": error},
        )

    async def save_file(self, file_model: FileModel) -> None:
        """Сохраняет только запись файла (упрощённый сценарий без разбора листов)."""
        try:
            await self._file_service.update_or_create(file_model)
            await self._log_service.save_log(
                scenario="upload",
                message=f"Saved stub file {file_model.file_id}",
                level="warning",
                meta={"file_id": file_model.file_id},
            )
        except Exception as exc:
            logger.exception("Failed to save stub file %s: %s", file_model.file_id, exc)
            raise
