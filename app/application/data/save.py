"""Координатор сохранения файла и flat_data (сценарий upload). Использует агрегаты file, flat_data, log."""
import logging
from typing import List, Optional

from app.core.exceptions import log_and_raise_http
from app.domain.file.models import FileModel, FileStatus
from app.domain.file.service import FileService
from app.domain.flat_data.models import FlatDataRecord
from app.domain.flat_data.service import FlatDataService
from app.domain.log.service import LogService

logger = logging.getLogger(__name__)


class DataSaveService:
    """Координирует сохранение файла, flat_data и логов. При ошибке — откат."""

    def __init__(
        self,
        file_service: FileService,
        flat_data_service: FlatDataService,
        log_service: LogService,
    ):
        self._file_service = file_service
        self._flat_data_service = flat_data_service
        self._log_service = log_service

    async def process_and_save_all(
        self, file_model: FileModel, flat_data: Optional[List[FlatDataRecord]] = None
    ) -> None:
        try:
            await self._file_service.update_or_create(file_model)
            await self._log_service.save_log(f"Начало обработки файла {file_model.file_id}")

            inserted_total = 0
            if flat_data:
                inserted_total = await self._flat_data_service.save_flat_data(flat_data)
            else:
                await self._log_service.save_log(f"FlatData пуст для {file_model.file_id}", level="warning")

            file_model.status = FileStatus.SUCCESS
            try:
                file_model.size = inserted_total if inserted_total else getattr(file_model, "size", 0)
            except Exception:
                pass

            await self._file_service.update_or_create(file_model)
            await self._log_service.save_log(
                f"Успешно сохранены данные для {file_model.file_id}; flat_inserted={inserted_total}"
            )
            logger.info(
                "DataSaveService: сохранение завершено для %s; flat_inserted=%s",
                file_model.file_id,
                inserted_total,
            )
        except Exception as e:
            try:
                await self.rollback(file_model, str(e))
            except Exception as inner:
                await self._log_service.save_log(
                    f"Ошибка при откате для {file_model.file_id}: {inner}",
                    level="error",
                )
            log_and_raise_http(500, "Ошибка при сохранении данных", e)

    async def rollback(self, file_model: FileModel, error: str) -> None:
        try:
            await self._flat_data_service.delete_by_file_id(file_model.file_id)
        except Exception as e:
            await self._log_service.save_log(
                f"Не удалось удалить FlatData при откате для {file_model.file_id}: {e}",
                level="error",
            )

        file_model.status = FileStatus.FAILED
        file_model.error = error
        try:
            await self._file_service.update_or_create(file_model)
        except Exception as e:
            await self._log_service.save_log(
                f"Не удалось обновить Files при откате для {file_model.file_id}: {e}",
                level="error",
            )
        await self._log_service.save_log(f"Откат файла {file_model.file_id}: {error}", level="error")

    async def save_file(self, file_model: FileModel) -> None:
        """Сохраняет одиночную запись файла (stub при ошибках загрузки)."""
        try:
            await self._file_service.update_or_create(file_model)
            await self._log_service.save_log(f"Сохранён stub файл {file_model.file_id}", level="warning")
        except Exception as e:
            logger.exception("Не удалось сохранить stub файл %s: %s", file_model.file_id, e)
            raise

