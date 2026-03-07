
"""Шаг обработки листов Excel через parsing pipeline."""
import logging
from typing import List

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.registry import ParsingStrategyRegistry
from app.application.upload.pipeline.context import UploadPipelineContext
from app.application.upload.pipeline.steps.ProcessSheetsStep.excel_reader import ExcelReader
from app.core.exceptions import CriticalParsingError, CriticalUploadError
from app.domain.sheet.models import SheetModel

logger = logging.getLogger(__name__)


class ProcessSheetsStep:
    """
    Обрабатывает листы Excel через parsing pipeline.

    Ответственность:
    - Прочитать Excel файл через ExcelReader.
    - Для каждого листа: создать SheetModel, спросить стратегию, запустить pipeline.
    - Собрать заполненные SheetModel только после успеха всех листов.

    Инвариант целостности:
    SheetModel'ы накапливаются в локальном списке и записываются в ctx.sheets
    только после успешной обработки ВСЕХ листов. Если хотя бы один лист
    падает с CriticalParsingError — ctx остаётся пустым, rollback
    в UploadPipelineRunner работает с чистым состоянием.
    """

    def __init__(
        self,
        excel_reader: ExcelReader | None = None,
        parsing_registry: ParsingStrategyRegistry | None = None,
    ) -> None:
        self._excel_reader = excel_reader or ExcelReader()
        self._parsing_registry = parsing_registry

    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.form_info or not ctx.file_model:
            raise CriticalUploadError(
                message="form_info и file_model должны быть установлены перед ProcessSheetsStep",
                domain="upload.process_sheets",
                http_status=500,
                meta={"file_name": getattr(ctx.file, "filename", None)},
            )

        if not ctx.file_content:
            raise CriticalUploadError(
                message="file_content не установлен перед ProcessSheetsStep",
                domain="upload.process_sheets",
                http_status=500,
                meta={"file_name": getattr(ctx.file, "filename", None)},
            )

        if self._parsing_registry is None:
            from app.application.parsing.registry import get_parsing_strategy_registry
            self._parsing_registry = get_parsing_strategy_registry()



        # 1. Читаем Excel
        try:
            sheets = self._excel_reader.read(ctx.file_content, ctx.file.filename)
        except Exception as e:
            raise CriticalUploadError(
                message=f"Не удалось прочитать Excel файл '{ctx.file.filename}': {e}",
                domain="upload.process_sheets.excel_reader",
                http_status=400,
                meta={"file_name": ctx.file.filename, "error": str(e)},
                show_traceback=True,
            ) from e

        logger.info(
            "Начало обработки листов. Файл: '%s', форма: '%s', листов: %d",
            ctx.file.filename,
            ctx.form_info.type.value,
            len(sheets),
        )

        # 2. Локальный аккумулятор — в ctx попадёт только после успеха всех листов
        local_sheets: List[SheetModel] = []

        # 3. Обрабатываем каждый лист
        for sheet_index, (sheet_name, df) in enumerate(sheets.items()):

            pipeline = self._parsing_registry.build_pipeline_for_sheet(
                form_info=ctx.form_info,
                sheet_name=sheet_name,
                sheet_index=sheet_index,
            )

            if pipeline is None:
                continue


            sheet_model = SheetModel(sheet_fullname=sheet_name)

            parsing_ctx = ParsingPipelineContext(
                sheet_model=sheet_model,
                raw_dataframe=df,
                form_info=ctx.form_info,
            )

            # 4. Запускаем parsing pipeline
            try:
                await pipeline.run_for_sheet(parsing_ctx)
            except CriticalParsingError as e:
                raise CriticalUploadError(
                    message=(
                        f"Критическая ошибка парсинга листа '{sheet_name}' "
                        f"файла '{ctx.file.filename}': {e.message}"
                    ),
                    domain="upload.process_sheets",
                    meta={
                        "sheet_name": sheet_name,
                        "file_name": ctx.file.filename,
                        "form_type": ctx.form_info.type.value,
                        **e.meta,
                    },
                ) from e

            # 5. Успех листа — sheet_model заполнен pipeline'ом
            local_sheets.append(sheet_model)

            logger.debug(
                "Лист '%s' обработан: flat_data=%d, предупреждений=%d",
                sheet_model.sheet_name or sheet_name,
                len(sheet_model.flat_data_records),
                len(sheet_model.warnings),
            )

        # 6. Все листы успешны — записываем в ctx
        ctx.sheets = local_sheets
        ctx.file_model.size = len(local_sheets)
        ctx.file_model.sheets = [
            s.sheet_name or s.sheet_fullname for s in local_sheets
        ]

        logger.info(
            "Обработка листов завершена. Файл: '%s', листов: %d, flat_data: %d",
            ctx.file.filename,
            len(local_sheets),
            len(ctx.flat_data),
        )
