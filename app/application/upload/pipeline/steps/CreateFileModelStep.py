from app.application.upload.pipeline.context import UploadPipelineContext
from app.domain.file.models import FileModel
from app.core.exceptions import CriticalUploadError
import logging

logger = logging.getLogger(__name__)


class CreateFileModelStep:
    """Шаг: создание FileModel через модель агрегата File."""
    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.file_info:
            raise CriticalUploadError(
                message="Не удалось создать модель файла: отсутствуют метаданные (file_info)",
                domain="upload.create_file_model",
                http_status=500,
                meta={"file_name": getattr(ctx.file, "filename", None)},
            )


        try:
            file_model = FileModel.create_new(
                filename=ctx.file_info.filename,
                year=ctx.file_info.year,
                city=ctx.file_info.city,
                form_id=ctx.form_id,
            )
            file_model.form_id = ctx.form_id
            ctx.file_model = file_model
        except Exception as e:
            raise CriticalUploadError(
                message=f"Ошибка создания модели файла: {e}",
                domain="upload.create_file_model",
                http_status=500,
                meta={"file_name": getattr(ctx.file, "filename", None), "error": str(e)},
            )
