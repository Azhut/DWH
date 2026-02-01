"""
Сервис по работе с листами при загрузке: чтение Excel, округление (опционально по форме), парсинг.
Методы переопределяемы в зависимости от обрабатываемой формы (наследование по типу формы).
Округление — часть pipeline, но реализовано в отдельном сервисе; шаг опционален и переопределяем по форме.
"""
import logging
from io import BytesIO
from typing import List, Tuple, Any

import pandas as pd
from fastapi import UploadFile

from app.models.sheet_model import SheetModel
from app.models.form_model import FormInfo, FormType
from app.parsers.parser_factory import ParserFactory
from app.services.rounding_service import RoundingService

logger = logging.getLogger(__name__)


class SheetHandlingService:
    """
    Базовый сервис обработки листов. Округление вызывается здесь и может быть
    переопределено в подклассах под конкретную форму (или отключено).
    """

    def round_dataframe(self, sheet_name: str, df: pd.DataFrame, form_info: FormInfo) -> pd.DataFrame:
        """
        Округление числовых данных по листу. Часть pipeline, опционально и переопределяемо по форме.
        По умолчанию делегирует RoundingService; подкласс может отключить или изменить логику.
        """
        return RoundingService.round_dataframe(sheet_name, df)

    async def read_sheets(self, file: UploadFile) -> dict:
        """Читает все листы из UploadFile в словарь {sheet_name: DataFrame}."""
        await file.seek(0)
        content = await file.read()
        await file.seek(0)
        buffer = BytesIO(content)
        return pd.read_excel(buffer, sheet_name=None, header=None, engine=None)

    async def process_sheets(
        self,
        file: UploadFile,
        file_model: Any,
        form_info: FormInfo,
    ) -> Tuple[List[SheetModel], List[dict]]:
        """
        Читает листы, применяет округление (через round_dataframe — переопределяемо по форме),
        парсит через ParserFactory и собирает sheet_models + flat_data.
        skip_sheets берётся из реквизитов формы: form_info.requisites.get("skip_sheets", []).
        """
        skip_sheets = form_info.requisites.get("skip_sheets", []) or []
        logger.info(
            "Начало обработки листов. Тип формы: %s, пропускаемые листы: %s",
            form_info.type.value,
            skip_sheets,
        )

        xls = await self.read_sheets(file)
        logger.info("Прочитано %d листов из файла %s", len(xls), file.filename)

        sheet_models: List[SheetModel] = []
        flat_data: List[dict] = []
        form_type: FormType = form_info.type

        for idx, (sheet_name, df) in enumerate(xls.items()):
            if idx in skip_sheets:
                logger.debug("Пропускаем лист по индексу %d (%s) согласно skip_sheets", idx, sheet_name)
                continue

            parser = ParserFactory.create_parser(sheet_name, form_type)
            logger.debug("Лист %d: '%s', парсер: %s", idx, sheet_name, parser.__class__.__name__)

            try:
                df_rounded = self.round_dataframe(sheet_name, df, form_info)
                parsed = parser.parse(df_rounded)
                if "form_type" not in parsed:
                    parsed["form_type"] = form_type.value

                records = parser.generate_flat_data(
                    file_model.year,
                    file_model.city,
                    sheet_name,
                    file_model.form_id,
                )

                sheet_model_doc = {
                    "file_id": file_model.file_id,
                    "sheet_name": sheet_name,
                    "sheet_fullname": sheet_name,
                    "year": file_model.year,
                    "city": file_model.city,
                    "form_type": form_type.value,
                    "headers": parsed.get("headers", {}),
                    "data": parsed.get("data", []),
                }
                sheet_models.append(SheetModel(**sheet_model_doc))

                for r in records:
                    flat_data.append({
                        "year": r.get("year", file_model.year),
                        "city": r.get("city", file_model.city),
                        "section": r.get("section", sheet_name),
                        "row": r.get("row"),
                        "column": r.get("column"),
                        "value": r.get("value"),
                        "form_type": form_type.value,
                    })

                logger.debug("Лист '%s' обработан: %d записей", sheet_name, len(records))
            except Exception as e:
                logger.exception("Ошибка обработки листа %s: %s", sheet_name, e)
                continue

        logger.info(
            "Обработка листов завершена. Успешно: %d листов, записей flat_data: %d",
            len(sheet_models),
            len(flat_data),
        )
        return sheet_models, flat_data
