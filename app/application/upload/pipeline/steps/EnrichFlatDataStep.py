from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError
from app.core.profiling import profile_step


class EnrichFlatDataStep:
    """Обогащает flat_data-записи метаданными файла."""

    @profile_step()
    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.file_model:
            raise CriticalUploadError(
                message="file_model is not set before EnrichFlatDataStep",
                domain="upload.enrich_flat_data",
                http_status=500,
                meta={"file_name": ctx.filename},
            )

        if not ctx.sheets:
            raise CriticalUploadError(
                message="No parsed sheets found before data enrichment",
                domain="upload.enrich_flat_data",
                http_status=400,
                meta={"file_name": ctx.filename},
            )

        reporter = (ctx.file_model.reporter or "").upper()
        for rec in ctx.flat_data:
            rec.file_id = ctx.file_model.file_id
            rec.form = ctx.form_id
            rec.year = ctx.file_model.year
            rec.reporter = reporter
