"""Step: read workbook sheets from in-memory file content."""

from app.application.upload.pipeline.context import UploadPipelineContext
from app.application.upload.pipeline.steps.ProcessSheetsStep.ExcelReader import ExcelReader
from app.core.exceptions import CriticalUploadError


class ReadWorkbookStep:
    """Convert file bytes into workbook sheets dict."""

    def __init__(self, excel_reader: ExcelReader | None = None) -> None:
        self._excel_reader = excel_reader or ExcelReader()

    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.file_content:
            raise CriticalUploadError(
                message="file_content is not set before workbook read",
                domain="upload.read_workbook",
                http_status=500,
                meta={"file_name": ctx.filename},
            )

        try:
            ctx.workbook_sheets = self._excel_reader.read(ctx.file_content, ctx.filename)
        except Exception as exc:
            raise CriticalUploadError(
                message=f"Failed to read workbook for '{ctx.filename}': {exc}",
                domain="upload.read_workbook",
                http_status=400,
                meta={"file_name": ctx.filename, "error": str(exc)},
            ) from exc
