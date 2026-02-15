from app.application.upload.pipeline.context import UploadPipelineContext


class EnrichFlatDataStep:
    """Шаг: обогащение flat_data управляющими полями."""
    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.flat_data or not ctx.file_model or not ctx.form_info:
            return
        for rec in ctx.flat_data:
            rec.file_id = ctx.file_model.file_id
            rec.form = ctx.form_id
            if rec.reporter and isinstance(rec.reporter, str):
                rec.reporter = rec.reporter.upper()
