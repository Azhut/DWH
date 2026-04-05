"""Сценарий удаления файла: согласованное удаление FlatData и записи Files."""

from __future__ import annotations

from fastapi import HTTPException

from app.core.mongo_transactions import run_in_transaction
from app.domain.file.service import FileService
from app.domain.flat_data.service import FlatDataService
from app.domain.log.service import LogService


class DataDeleteService:
    """Удаляет связанные данные в одной транзакции при поддержке сервером."""

    def __init__(
        self,
        file_service: FileService,
        flat_data_service: FlatDataService,
        log_service: LogService,
    ) -> None:
        self._file_service = file_service
        self._flat_data_service = flat_data_service
        self._log_service = log_service

    async def delete_file(self, file_id: str) -> None:
        """Удаляет все FlatData по file_id и сам документ Files; пишет запись в Logs при успехе."""
        existing = await self._file_service.get_by_id(file_id)
        if not existing:
            raise HTTPException(404, f"Файл '{file_id}' не найден")

        async def work(session):
            flat_deleted = await self._flat_data_service.delete_by_file_id(file_id, session=session)
            files_deleted = await self._file_service.delete_by_file_id(file_id, session=session)
            if files_deleted == 0:
                raise HTTPException(404, f"Файл '{file_id}' не найден при финальном удалении")
            return flat_deleted, files_deleted

        try:
            flat_deleted, files_deleted = await run_in_transaction(work)
        except HTTPException:
            raise
        except Exception as exc:
            await self._log_service.save_log(
                scenario="deletion",
                message=f"Ошибка при удалении данных для {file_id}: {exc}",
                level="error",
                meta={
                    "deleted_type": "file",
                    "deleted_id": file_id,
                    "cascade": {"files_deleted": None, "flat_deleted": None},
                    "error": str(exc),
                },
            )
            raise HTTPException(500, f"Ошибка при удалении файла: {str(exc)}") from exc

        await self._log_service.save_log(
            scenario="deletion",
            message=f"Удалён файл {file_id}",
            level="info",
            meta={
                "deleted_type": "file",
                "deleted_id": file_id,
                "cascade": {
                    "files_deleted": files_deleted,
                    "flat_deleted": flat_deleted,
                },
            },
        )
