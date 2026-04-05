"""Шаг парсинга листов рабочей книги через parsing-pipeline."""

import logging
from typing import Optional

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.registry import ParsingStrategyRegistry
from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalParsingError, CriticalUploadError
from app.core.profiling import profile_step
from app.domain.parsing.workbook_source import ParsingWorkbookSource
from app.domain.sheet.models import SheetModel

logger = logging.getLogger(__name__)


class ProcessSheetsStep:
    """Парсит листы рабочей книги и сохраняет список SheetModel в контекст."""

    def __init__(self, parsing_registry: ParsingStrategyRegistry | None = None) -> None:
        self._parsing_registry = parsing_registry

    @profile_step()
    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.file_model:
            raise CriticalUploadError(
                message="file_model must be set before ProcessSheetsStep",
                domain="upload.process_sheets",
                http_status=500,
                meta={"file_name": ctx.filename},
            )

        if self._parsing_registry is None:
            from app.application.parsing.registry import get_parsing_strategy_registry

            self._parsing_registry = get_parsing_strategy_registry()

        parsed_sheets: list[SheetModel] = []
        workbook_source: ParsingWorkbookSource | None = None
        if ctx.file_content is not None and ctx.file_info is not None:
            workbook_source = ParsingWorkbookSource(
                content=ctx.file_content,
                extension=ctx.file_info.extension or "",
            )

        for sheet_index, (sheet_name, dataframe) in enumerate(ctx.workbook_sheets.items()):
            parsed_sheet = await self._parse_sheet(
                ctx=ctx,
                sheet_name=sheet_name,
                sheet_index=sheet_index,
                dataframe=dataframe,
                workbook_source=workbook_source,
            )
            if parsed_sheet is not None:
                parsed_sheets.append(parsed_sheet)

        if not parsed_sheets:
            raise CriticalUploadError(
                message="Workbook does not contain supported/parsable sheets",
                domain="upload.process_sheets",
                http_status=400,
                meta={"file_name": ctx.filename},
            )

        ctx.sheets = parsed_sheets
        logger.info(
            "Sheet parsing completed. file='%s', parsed_sheets=%d, records=%d",
            ctx.filename,
            len(parsed_sheets),
            len(ctx.flat_data),
        )

    async def _parse_sheet(
        self,
        ctx: UploadPipelineContext,
        sheet_name: str,
        sheet_index: int,
        dataframe,
        workbook_source: ParsingWorkbookSource | None,
    ) -> Optional[SheetModel]:
        pipeline = self._parsing_registry.build_pipeline_for_sheet(
            form_info=ctx.form_info,
            sheet_name=sheet_name,
            sheet_index=sheet_index,
        )
        if pipeline is None:
            return None

        sheet_model = SheetModel(sheet_fullname=sheet_name)
        parsing_ctx = ParsingPipelineContext(
            sheet_model=sheet_model,
            raw_dataframe=dataframe,
            form_info=ctx.form_info,
            workbook_source=workbook_source,
        )

        try:
            await pipeline.run_for_sheet(parsing_ctx)
        except CriticalParsingError as exc:
            raise CriticalUploadError(
                message=(
                    f"Critical parsing error in sheet '{sheet_name}' "
                    f"for file '{ctx.filename}': {exc.message}"
                ),
                domain="upload.process_sheets",
                meta={
                    "sheet_name": sheet_name,
                    "file_name": ctx.filename,
                    "form_type": ctx.form_info.type.value,
                    **exc.meta,
                },
            ) from exc

        logger.debug(
            "Sheet parsed: '%s', records=%d, warnings=%d",
            sheet_model.sheet_name or sheet_name,
            len(sheet_model.flat_data_records),
            len(sheet_model.warnings),
        )
        return sheet_model
