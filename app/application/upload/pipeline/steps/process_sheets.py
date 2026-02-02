"""Шаг: обработка листов через parsing pipeline."""
import logging
from typing import List

from app.application.parsing import ParsingPipelineContext, get_parsing_pipeline_registry
from app.application.upload.pipeline.context import UploadPipelineContext
from app.domain.flat_data.models import FlatDataRecord
from app.domain.sheet.models import SheetModel
from app.domain.sheet.service import SheetService

logger = logging.getLogger(__name__)


class ProcessSheetsStep:
    """Обрабатывает листы через parsing pipeline вместо прямого вызова парсеров."""

    def __init__(self, sheet_service: SheetService):
        self._sheet_service = sheet_service
        self._registry = get_parsing_pipeline_registry()

    async def execute(self, ctx: UploadPipelineContext) -> None:
        """Читает листы и обрабатывает их через parsing pipeline."""
        if not ctx.form_info or not ctx.file_model:
            raise ValueError("form_info и file_model должны быть установлены перед ProcessSheetsStep")

        skip_sheets = ctx.form_info.requisites.get("skip_sheets", []) or []
        logger.info(
            "Начало обработки листов. Тип формы: %s, пропускаемые листы: %s",
            ctx.form_info.type.value,
            skip_sheets,
        )

        # Читаем листы через SheetService (только I/O)
        xls = await self._sheet_service.read_sheets(ctx.file)
        logger.info("Прочитано %d листов из файла %s", len(xls), ctx.file.filename)

        sheet_models: List[SheetModel] = []
        flat_data: List[FlatDataRecord] = []

        for idx, (sheet_name, df) in enumerate(xls.items()):
            if idx in skip_sheets:
                logger.debug("Пропускаем лист по индексу %d (%s)", idx, sheet_name)
                continue

            # Получаем pipeline для этого листа
            pipeline = self._registry.get_pipeline(ctx.form_info.type, sheet_name)
            if not pipeline:
                logger.error("Не найден parsing pipeline для формы %s, листа %s", ctx.form_info.type.value, sheet_name)
                continue

            # Создаём контекст для parsing pipeline
            parsing_ctx = ParsingPipelineContext(
                sheet_name=sheet_name,
                raw_dataframe=df,
                form_info=ctx.form_info,
                file_year=ctx.file_model.year,
                file_city=ctx.file_model.city,
                file_id=ctx.file_model.file_id,
                form_id=ctx.file_model.form_id,
            )

            # Применяем округление (если нужно)
            df_rounded = self._sheet_service.round_dataframe(sheet_name, df, ctx.form_info)
            parsing_ctx.raw_dataframe = df_rounded

            # Запускаем parsing pipeline
            await pipeline.run_for_sheet(parsing_ctx)

            # Проверяем результат
            if parsing_ctx.failed:
                logger.warning(
                    "Parsing pipeline завершился с ошибками для листа '%s': %s",
                    sheet_name,
                    "; ".join(parsing_ctx.errors),
                )
                # Продолжаем обработку других листов
                continue

            # Формируем SheetModel из результатов парсинга
            if parsing_ctx.sheet_model_data:
                sheet_model = SheetModel(
                    file_id=ctx.file_model.file_id,
                    sheet_name=sheet_name,
                    sheet_fullname=sheet_name,
                    year=ctx.file_model.year,
                    city=ctx.file_model.city,
                    headers=parsing_ctx.sheet_model_data.get("headers", {}),
                    data=parsing_ctx.sheet_model_data.get("data", []),
                )
                sheet_models.append(sheet_model)
            elif parsing_ctx.parsed_data:
                # Fallback: используем parsed_data если sheet_model_data не установлен
                sheet_model = SheetModel(
                    file_id=ctx.file_model.file_id,
                    sheet_name=sheet_name,
                    sheet_fullname=sheet_name,
                    year=ctx.file_model.year,
                    city=ctx.file_model.city,
                    headers=parsing_ctx.parsed_data.get("headers", {}),
                    data=parsing_ctx.parsed_data.get("data", []),
                )
                sheet_models.append(sheet_model)

            # Добавляем flat_data записи
            flat_data.extend(parsing_ctx.flat_data_records)

            logger.debug(
                "Лист '%s' обработан: %d записей flat_data",
                sheet_name,
                len(parsing_ctx.flat_data_records),
            )

        ctx.sheet_models = sheet_models
        ctx.flat_data = flat_data
        ctx.file_model.size = len(sheet_models)
        ctx.file_model.sheets = [m.sheet_name for m in sheet_models]

        logger.info(
            "Обработка листов завершена. Успешно: %d листов, записей flat_data: %d",
            len(sheet_models),
            len(flat_data),
        )
