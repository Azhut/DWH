# app/application/upload/response_builder.py
import logging
from typing import List, Optional

from app.api.v2.schemas.files import FileResponse
from app.api.v2.schemas.upload import UploadResponse
from app.domain.file.models import FileStatus

logger = logging.getLogger(__name__)


class UploadResponseBuilder:
    """
    Единственная точка формирования UploadResponse.

    build_response()         — финальный результат после обработки всех файлов
                               (вызывается из SSE-генератора в терминальном событии)
    build_accepted_response() — немедленный ответ 202 на POST /upload
                               (upload_id для подключения к SSE)
    """

    @staticmethod
    def build_response(
        file_responses: List[FileResponse],
        upload_id: Optional[str] = None,
    ) -> UploadResponse:
        """Формирует итоговый UploadResponse из результатов обработки файлов."""
        success_count = sum(1 for r in file_responses if r.status == FileStatus.SUCCESS)
        failure_count = len(file_responses) - success_count

        logger.info(
            "Upload complete: success=%d, failed=%d, upload_id=%s",
            success_count,
            failure_count,
            upload_id,
        )

        return UploadResponse(
            message=f"{success_count} files processed successfully, {failure_count} failed.",
            details=file_responses,
            upload_id=upload_id,
        )

    @staticmethod
    def build_accepted_response(upload_id: str) -> UploadResponse:
        """
        Формирует немедленный ответ 202 для POST /upload.
        Клиент использует upload_id для подключения к SSE-потоку прогресса.
        """
        logger.info("Upload accepted, background task started: upload_id=%s", upload_id)

        return UploadResponse(
            message=(
                f"Upload accepted. "
                f"Track progress at GET /api/v2/upload-progress/{upload_id}"
            ),
            details=[],
            upload_id=upload_id,
        )