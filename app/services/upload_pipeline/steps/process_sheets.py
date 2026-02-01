"""Шаг: чтение и обработка листов. Делегирует сервису по работе с листами."""
from app.services.upload_pipeline.context import UploadPipelineContext
from app.services.sheet_handling_service import SheetHandlingService


class ProcessSheetsStep:
    """Обработка листов (чтение, округление по форме, парсинг) делегируется SheetHandlingService."""

    def __init__(self, sheet_handling_service: SheetHandlingService):
        self._sheet_service = sheet_handling_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        sheet_models, flat_data = await self._sheet_service.process_sheets(
            ctx.file,
            ctx.file_model,
            ctx.form_info,
        )
        ctx.sheet_models = sheet_models
        ctx.flat_data = flat_data
        ctx.file_model.size = len(sheet_models)
        ctx.file_model.sheets = [m.sheet_name for m in sheet_models]
