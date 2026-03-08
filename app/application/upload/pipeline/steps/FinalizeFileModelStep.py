"""Шаг: финализировать метаданные file_model после парсинга листов."""

from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError


class FinalizeFileModelStep:
    """Заполняет file_model метаданными распарсенных листов."""

    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.file_model:
            raise CriticalUploadError(
                message="file_model is not set before finalization",
                domain="upload.finalize_file_model",
                http_status=500,
                meta={"file_name": ctx.filename},
            )

        ctx.file_model.sheets = [
            sheet.sheet_name or sheet.sheet_fullname for sheet in ctx.sheets
        ]
        ctx.file_model.flat_data_size = len(ctx.flat_data)
