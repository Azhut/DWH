"""Шаг: обработка листов через сервис агрегата Sheet."""
from app.application.upload.pipeline.context import UploadPipelineContext
from app.domain.sheet.service import SheetService


class ProcessSheetsStep:
    def __init__(self, sheet_service: SheetService):
        self._sheet_service = sheet_service

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
