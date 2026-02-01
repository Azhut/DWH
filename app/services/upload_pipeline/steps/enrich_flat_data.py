"""Шаг: добавление управляющих полей ко всем flat-записям."""
from app.services.upload_pipeline.context import UploadPipelineContext


class EnrichFlatDataStep:
    """Добавляет file_id, form, form_type, нормализует city."""

    async def execute(self, ctx: UploadPipelineContext) -> None:
        for rec in ctx.flat_data or []:
            rec["file_id"] = ctx.file_model.file_id
            rec["form"] = ctx.form_id
            if "city" in rec and isinstance(rec.get("city"), str):
                rec["city"] = rec["city"].upper()
            rec["form_type"] = ctx.form_info.type.value
