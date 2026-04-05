import logging
from typing import List, Optional
from app.api.v2.schemas.upload import UploadResponse
from app.api.v2.schemas.files import FileResponse
from app.domain.file.models import FileStatus

logger = logging.getLogger(__name__)


class UploadResponseBuilder:
    """
    Строит ответ для эндпоинта загрузки.
    Формирует сообщение с итоговой статистикой.
    """

    @staticmethod
    def build_response(file_responses: List[FileResponse], upload_id: Optional[str] = None) -> UploadResponse:
        """Формирует UploadResponse из списка результатов обработки файлов."""
        success_count = sum(1 for r in file_responses if r.status == FileStatus.SUCCESS)
        failure_count = len(file_responses) - success_count

        logger.info("Обработка завершена. Успешно: %d, с ошибками: %d", success_count, failure_count)

        return UploadResponse(
            message=f"{success_count} files processed successfully, {failure_count} failed.",
            details=file_responses,
            upload_id=upload_id,
        )

    @staticmethod
    def build_pending_response(upload_id: str) -> UploadResponse:
        """Формирует UploadResponse для фоновой обработки — файлы ещё не обработаны."""
        logger.info("Upload accepted, processing in background. upload_id=%s", upload_id)

        return UploadResponse(
            message="Upload accepted. Track progress via /api/v2/upload-progress/{upload_id}",
            details=[],
            upload_id=upload_id,
        )