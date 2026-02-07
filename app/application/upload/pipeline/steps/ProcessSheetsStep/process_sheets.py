import logging
from typing import List

from app.application.parsing import (
    ParsingPipelineContext,
    get_parsing_pipeline_registry,
)
from app.application.upload.pipeline.context import UploadPipelineContext
from app.application.upload.pipeline.steps.ProcessSheetsStep.excel_reader import ExcelReader
from app.domain.flat_data.models import FlatDataRecord
from app.domain.form.models import FormType
from app.domain.sheet.models import SheetModel
from app.domain.sheet.service import SheetService

logger = logging.getLogger(__name__)


class ProcessSheetsStep:
    """
    Обрабатывает листы Excel через parsing pipeline.

    ОТВЕТСТВЕННОСТЬ:
    - Прочитать Excel файл (через ExcelReader)
    - Запустить parsing pipeline для каждого листа
    - Собрать SheetModel и flat_data
    """

    def __init__(
        self,
        sheet_service: SheetService,
        excel_reader: ExcelReader | None = None,
    ):
        self._sheet_service = sheet_service
        self._excel_reader = excel_reader or ExcelReader()
        self._registry = get_parsing_pipeline_registry()

    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.form_info or not ctx.file_model:
            raise ValueError("form_info и file_model должны быть установлены")

        if not ctx.file_content:
            raise ValueError("file_content должен быть установлен")

        skip_sheets = ctx.form_info.requisites.get("skip_sheets", []) or []

        logger.info(
            "Начало обработки листов. Форма: %s, skip_sheets: %s",
            ctx.form_info.type.value,
            skip_sheets,
        )

        # 1. Чтение Excel
        sheets = self._excel_reader.read(
            ctx.file_content,
            ctx.filename,
        )

        sheet_models: List[SheetModel] = []
        flat_data: List[FlatDataRecord] = []

        # 2. Обработка листов
        for idx, (sheet_name, df) in enumerate(sheets.items()):
            if idx in skip_sheets:
                logger.debug(
                    "Пропуск листа %d (%s)",
                    idx,
                    sheet_name,
                )
                continue

            pipeline = self._registry.get_pipeline(
                ctx.form_info.type,
                sheet_name,
            )

            if not pipeline:
                logger.debug(
                    "Parsing pipeline не найден: форма=%s, лист=%s",
                    ctx.form_info.type.value,
                    sheet_name,
                )
                continue

            parsing_ctx = ParsingPipelineContext(
                sheet_name=sheet_name,
                raw_dataframe=df,
                form_info=ctx.form_info,
                file_year=ctx.file_model.year,
                file_city=ctx.file_model.city,
                file_id=ctx.file_model.file_id,
                form_id=ctx.file_model.form_id,
                apply_notes=(ctx.form_info.type == FormType.FK_1),
                deduplicate_columns=(ctx.form_info.type == FormType.FK_5),
            )

            # 3. Доменные операции над DataFrame
            parsing_ctx.raw_dataframe = self._sheet_service.round_dataframe(
                sheet_name,
                parsing_ctx.raw_dataframe,
                ctx.form_info,
            )

            # 4. Parsing pipeline
            await pipeline.run_for_sheet(parsing_ctx)

            if parsing_ctx.failed:
                logger.warning(
                    "Ошибка парсинга листа '%s': %s",
                    sheet_name,
                    "; ".join(parsing_ctx.errors),
                )
                continue

            # 5. SheetModel
            sheet_data = parsing_ctx.sheet_model_data or parsing_ctx.parsed_data
            if sheet_data:
                sheet_models.append(
                    SheetModel(
                        file_id=ctx.file_model.file_id,
                        sheet_name=sheet_name,
                        sheet_fullname=sheet_name,
                        year=ctx.file_model.year,
                        city=ctx.file_model.city,
                        headers=sheet_data.get("headers", {}),
                        data=sheet_data.get("data", []),
                    )
                )

            flat_data.extend(parsing_ctx.flat_data_records)

            logger.debug(
                "Лист '%s' обработан, flat_data: %d",
                sheet_name,
                len(parsing_ctx.flat_data_records),
            )


        ctx.sheet_models = sheet_models
        ctx.flat_data = flat_data
        ctx.file_model.size = len(sheet_models)
        ctx.file_model.sheets = [s.sheet_name for s in sheet_models]

        logger.info(
            "Обработка листов завершена. Листов: %d, flat_data: %d",
            len(sheet_models),
            len(flat_data),
        )
