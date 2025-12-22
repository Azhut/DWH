from io import BytesIO
import pandas as pd
import logging
from typing import List, Tuple, Optional
from fastapi import UploadFile
from app.models.sheet_model import SheetModel
from app.models.form_model import FormType
from app.core.database import mongo_connection
from app.parsers.parser_factory import ParserFactory

logger = logging.getLogger(__name__)


async def is_file_unique(filename: str) -> bool:
    """
    Проверка на дубликат по имени файла в коллекции Files.
    Возвращает True, если файла в базе нет.
    """
    db = mongo_connection.get_database()
    doc = await db.Files.find_one({"filename": filename})
    return doc is None


class SheetProcessor:
    """
    Задача: прочитать все листы из переданного UploadFile (pandas),
    для каждого листа подобрать соответствующий парсер через ParserFactory
    в зависимости от типа формы.
    Поддерживает пропуск листов по индексам (skip_sheets).
    """

    async def extract_and_process_sheets(
            self,
            file: UploadFile,
            file_model,
            form_type: FormType,
            skip_sheets: Optional[List[int]] = None
    ) -> Tuple[List[SheetModel], List[dict]]:
        """
        Исправленная версия с обработкой seekable() проблемы
        """
        if skip_sheets is None:
            skip_sheets = []
        logger.info(f"Начало обработки листов. Тип формы: {form_type.value}, "
                    f"пропускаемые листы: {skip_sheets}")

        try:
            # Считываем содержимое файла в память
            content = await file.read()
            await file.seek(0)  # Возвращаем указатель в начало

            # Создаем BytesIO объект, который имеет все необходимые методы
            file_buffer = BytesIO(content)

            # pandas теперь работает с BytesIO, который имеет seekable()
            xls = pd.read_excel(
                file_buffer,
                sheet_name=None,
                header=None,
                engine=None  # pandas сам выберет подходящий движок
            )
            logger.info(f"Прочитано {len(xls)} листов из файла {file.filename}")
        except Exception as e:
            logger.exception(f"Не удалось прочитать файл {file.filename} через pandas: %s", e)
            raise

        sheet_models: List[SheetModel] = []
        flat_data: List[dict] = []

        for idx, (sheet_name, df) in enumerate(xls.items()):
            if idx in skip_sheets:
                logger.debug(f"Пропускаем лист по индексу {idx} ({sheet_name}) согласно skip_sheets")
                continue

            parser = ParserFactory.create_parser(sheet_name, form_type)
            logger.debug(f"Лист {idx}: '{sheet_name}', выбран парсер: {parser.__class__.__name__}")

            try:
                parsed = parser.parse(df)
                if "form_type" not in parsed:
                    parsed["form_type"] = form_type.value

                records = parser.generate_flat_data(
                    file_model.year,
                    file_model.city,
                    sheet_name,
                    file_model.form_id
                )

                sheet_model_doc = {
                    "file_id": file_model.file_id,
                    "sheet_name": sheet_name,
                    "sheet_fullname": sheet_name,
                    "year": file_model.year,
                    "city": file_model.city,
                    "form_type": form_type.value,
                    "headers": parsed.get("headers", {}),
                    "data": parsed.get("data", [])
                }
                sheet_models.append(SheetModel(**sheet_model_doc))

                for r in records:
                    flat = {
                        "year": r.get("year", file_model.year),
                        "city": r.get("city", file_model.city),
                        "section": r.get("section", sheet_name),
                        "row": r.get("row"),
                        "column": r.get("column"),
                        "value": r.get("value"),
                        "form_type": form_type.value
                    }
                    flat_data.append(flat)

                logger.debug(f"Лист '{sheet_name}' обработан: {len(records)} записей")
            except Exception as e:
                logger.exception(f"Ошибка обработки листа {sheet_name}: {e}")
                continue

        logger.info(f"Обработка листов завершена. Успешно: {len(sheet_models)} листов, "
                    f"сгенерировано {len(flat_data)} записей flat_data")
        return sheet_models, flat_data