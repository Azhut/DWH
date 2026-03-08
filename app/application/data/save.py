"""Coordinator for saving file and flat_data entities for upload scenario."""

import logging
from datetime import datetime
from typing import List, Optional

from app.core.exceptions import CriticalUploadError
from app.domain.file.models import FileModel, FileStatus
from app.domain.file.service import FileService
from app.domain.flat_data.models import FlatDataRecord
from app.domain.flat_data.service import FlatDataService
from app.domain.log.service import LogService

logger = logging.getLogger(__name__)


class DataSaveService:
    """Coordinates file/flat_data/log persistence with rollback on errors."""

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
        self,
        file_model: FileModel,
        flat_data: Optional[List[FlatDataRecord]] = None,
    ) -> None:
        try:
            await self._file_service.update_or_create(file_model)
            await self._log_service.save_log(f"Start processing file {file_model.file_id}")

            inserted_total = 0
            if flat_data:
                inserted_total = await self._flat_data_service.save_flat_data(flat_data)
            else:
                await self._log_service.save_log(
                    f"FlatData is empty for {file_model.file_id}",
                    level="warning",
                )

            file_model.status = FileStatus.SUCCESS
            file_model.flat_data_size = inserted_total if inserted_total else file_model.flat_data_size

            await self._file_service.update_or_create(file_model)
            await self._log_service.save_log(
                f"Data saved successfully for {file_model.file_id}; flat_inserted={inserted_total}",
            )
            logger.info(
                "DataSaveService completed for %s; flat_inserted=%s",
                file_model.file_id,
                inserted_total,
            )

        except Exception as exc:
            raise CriticalUploadError(
                message="Error while saving uploaded file data",
                domain="upload.persist",
                http_status=500,
                meta={"file_id": file_model.file_id, "error": str(exc)},
                show_traceback=True,
            ) from exc

    async def rollback(self, file_model: FileModel, error: str) -> None:
        try:
            await self._flat_data_service.delete_by_file_id(file_model.file_id)
        except Exception as exc:
            await self._log_service.save_log(
                f"Failed to delete FlatData during rollback for {file_model.file_id}: {exc}",
                level="error",
            )

        file_model.status = FileStatus.FAILED
        file_model.error = error
        file_model.updated_at = datetime.now()
        try:
            await self._file_service.update_or_create(file_model)
        except Exception as exc:
            await self._log_service.save_log(
                f"Failed to update Files during rollback for {file_model.file_id}: {exc}",
                level="error",
            )

        await self._log_service.save_log(f"Rollback file {file_model.file_id}: {error}", level="error")

    async def save_file(self, file_model: FileModel) -> None:
        """Persist only file record (stub fallback scenario)."""
        try:
            await self._file_service.update_or_create(file_model)
            await self._log_service.save_log(f"Saved stub file {file_model.file_id}", level="warning")
        except Exception as exc:
            logger.exception("Failed to save stub file %s: %s", file_model.file_id, exc)
            raise
