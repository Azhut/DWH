from app.application.upload.pipeline.context import UploadPipelineContext


class EnrichFlatDataStep:
    """Шаг: обогащение flat_data метаданными файла.

    Единственное место, где file_id, form, year и reporter
    попадают в FlatDataRecord. GenerateFlatDataStep намеренно
    не заполняет эти поля — он знает только о структуре листа.
    """

    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.sheets or not ctx.file_model or not ctx.form_info:
            return
        for rec in ctx.flat_data:
            rec.file_id = ctx.file_model.file_id
            rec.form = ctx.form_id
            rec.year = ctx.file_model.year
            rec.reporter = (ctx.file_model.reporter or "").upper()