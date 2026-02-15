"""Шаг обработки листов Excel через parsing pipeline."""
import logging
from typing import List

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.registry import get_parsing_strategy_registry
from app.application.upload.pipeline.context import UploadPipelineContext
from app.application.upload.pipeline.steps.ProcessSheetsStep.excel_reader import ExcelReader
from app.core.exceptions import (
    CriticalParsingError,
    CriticalUploadError,
)
from app.domain.flat_data.models import FlatDataRecord
from app.domain.sheet.models import SheetModel

logger = logging.getLogger(__name__)


class ProcessSheetsStep:
    """
    Обрабатывает листы Excel через parsing pipeline.

    Ответственность:
    - Прочитать Excel файл через ExcelReader.
    - Для каждого листа: спросить стратегию, запустить pipeline.
    - Собрать SheetModel и flat_data только после успеха всех листов.

    Не содержит никакой форма-специфичной логики — она полностью
    инкапсулирована в стратегиях (AutoFormParsingStrategy, FK1FormParsingStrategy и др.)

    Инвариант целостности:
    Данные накапливаются в локальных переменных и записываются в ctx
    только после успешной обработки ВСЕХ листов. Если хотя бы один лист
    падает с CriticalParsingError — ctx остаётся пустым, rollback
    в UploadPipelineRunner работает с чистым состоянием.
    """

    def __init__(self, excel_reader: ExcelReader | None = None) -> None:
        self._excel_reader = excel_reader or ExcelReader()
        # Реестр НЕ инициализируется здесь — он получается лениво в execute().
        # Причина: ProcessSheetsStep создаётся в build_default_pipeline раньше,
        # чем UploadManager успевает проинициализировать реестр с sheet_service.

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


        registry = get_parsing_strategy_registry()

        # 1. Читаем Excel
        try:
            sheets = self._excel_reader.read(
                ctx.file_content,
                ctx.file.filename,
            )
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

        # 2. Локальные аккумуляторы — в ctx попадут только после успеха всех листов
        local_sheet_models: List[SheetModel] = []
        local_flat_data: List[FlatDataRecord] = []

        # 3. Обрабатываем каждый лист
        for sheet_index, (sheet_name, df) in enumerate(sheets.items()):

            pipeline = registry.build_pipeline_for_sheet(
                form_info=ctx.form_info,
                sheet_name=sheet_name,
                sheet_index=sheet_index,
            )

            if pipeline is None:
                continue

            parsing_ctx = ParsingPipelineContext(
                sheet_name=sheet_name,
                raw_dataframe=df,
                form_info=ctx.form_info,
                file_year=ctx.file_model.year,
                file_reporter=ctx.file_model.reporter,
                file_id=ctx.file_model.file_id,
                form_id=ctx.file_model.form_id,
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

            # 5. Успех листа — собираем SheetModel
            sheet_data = parsing_ctx.sheet_model_data or parsing_ctx.parsed_data
            if sheet_data:
                local_sheet_models.append(
                    SheetModel(
                        file_id=ctx.file_model.file_id,
                        sheet_name=sheet_name,
                        sheet_fullname=sheet_name,
                        year=ctx.file_model.year,
                        reporter=ctx.file_model.reporter,
                        headers=sheet_data.get("headers", {}),
                        data=sheet_data.get("data", []),
                    )
                )

            local_flat_data.extend(parsing_ctx.flat_data_records)

            logger.debug(
                "Лист '%s' обработан: flat_data=%d, предупреждений=%d",
                sheet_name,
                len(parsing_ctx.flat_data_records),
                len(parsing_ctx.warnings),
            )

        # 6. Все листы успешны — записываем результаты в ctx
        ctx.sheet_models = local_sheet_models
        ctx.flat_data = local_flat_data
        ctx.file_model.size = len(local_sheet_models)
        ctx.file_model.sheets = [s.sheet_name for s in local_sheet_models]

        logger.info(
            "Обработка листов завершена. Файл: '%s', листов: %d, flat_data: %d",
            ctx.file.filename,
            len(local_sheet_models),
            len(local_flat_data),
        )