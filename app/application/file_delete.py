"""Сценарий удаления файла: координация агрегатов file, flat_data, log."""
from fastapi import HTTPException

from app.domain.file.service import FileService
from app.domain.flat_data.service import FlatDataService
from app.domain.log.service import LogService


class DataDeleteService:
    """Удаление файла и связанных flat_data. Использует агрегаты file, flat_data, log."""

    def __init__(
        self,
        file_service: FileService,
        flat_data_service: FlatDataService,
        log_service: LogService,
    ):
        self._file_service = file_service
        self._flat_data_service = flat_data_service
        self._log_service = log_service

    async def delete_file(self, file_id: str) -> None:
        existing = await self._file_service.get_by_id(file_id)
        if not existing:
            raise HTTPException(404, f"Файл '{file_id}' не найден")

        try:
            await self._flat_data_service.delete_by_file_id(file_id)
        except Exception as e:
            await self._log_service.save_log(f"Ошибка при удалении FlatData для {file_id}: {e}", level="error")
            raise HTTPException(500, f"Ошибка при удалении связанных данных: {str(e)}")

        try:
            deleted = await self._file_service.delete_by_file_id(file_id)
            if deleted == 0:
                raise HTTPException(404, f"Файл '{file_id}' не найден при финальном удалении")
        except HTTPException:
            raise
        except Exception as e:
            await self._log_service.save_log(f"Ошибка при удалении записи Files для {file_id}: {e}", level="error")
            raise HTTPException(500, f"Ошибка при удалении записи файла: {str(e)}")

        await self._log_service.save_log(f"Удалён файл {file_id}", level="info")
