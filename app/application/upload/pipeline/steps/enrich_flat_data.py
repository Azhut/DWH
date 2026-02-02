"""Шаг: обогащение flat_data управляющими полями."""
from app.application.upload.pipeline.context import UploadPipelineContext


class EnrichFlatDataStep:
    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.flat_data or not ctx.file_model or not ctx.form_info:
            return
        for rec in ctx.flat_data:
            rec.file_id = ctx.file_model.file_id
            rec.form = ctx.form_id
            if rec.city and isinstance(rec.city, str):
                rec.city = rec.city.upper()
