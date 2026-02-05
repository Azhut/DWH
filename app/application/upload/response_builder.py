import logging
from typing import List
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
    def build_response(file_responses: List[FileResponse]) -> UploadResponse:
        """
        Формирует UploadResponse из списка результатов обработки файлов.

        Args:
            file_responses: Список результатов обработки каждого файла

        Returns:
            UploadResponse с итоговой статистикой
        """
        success_count = sum(
            1 for r in file_responses if r.status == FileStatus.SUCCESS
        )
        failure_count = len(file_responses) - success_count

        logger.info(
            "Обработка завершена. Успешно: %d, с ошибками: %d",
            success_count,
            failure_count,
        )

        message = f"{success_count} files processed successfully, {failure_count} failed."

        return UploadResponse(
            message=message,
            details=file_responses,
        )