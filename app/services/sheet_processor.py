# app/services/sheet_processor.py
from typing import List, Tuple, Optional
from fastapi import UploadFile
import pandas as pd
import math
import logging
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

    def __init__(self):
        # ничего не храним в состоянии
        pass

    async def extract_and_process_sheets(
            self,
            file: UploadFile,
            file_model,
            form_type: FormType,  # Новый параметр: тип формы!
            skip_sheets: Optional[List[int]] = None
    ) -> Tuple[List[SheetModel], List[dict]]:
        """
        file: UploadFile (fastapi)
        file_model: экземпляр FileModel (для использования year/city/file_id)
        form_type: тип формы (1ФК, 5ФК и т.д.)
        skip_sheets: список индексов листов (0-based), которые нужно пропустить
        Возвращает: (sheet_models, flat_data)
        """
        if skip_sheets is None:
            skip_sheets = []

        logger.info(f"Начало обработки листов. Тип формы: {form_type.value}, "
                    f"пропускаемые листы: {skip_sheets}")

        # Переустанавливаем указатель на начало
        try:
            await file.seek(0)
        except Exception:
            try:
                file.file.seek(0)
            except Exception:
                pass

        # pandas может читать из file.file-like
        try:
            # Читаем все листы как DataFrame-ы без интерпретации заголовков
            # keep_default_na=False чтобы не поменять пустые ячейки на NaN (мы сами обработаем)
            xls = pd.read_excel(file.file, sheet_name=None, header=None, engine=None)
            logger.info(f"Прочитано {len(xls)} листов из файла {file.filename}")
        except Exception as e:
            logger.exception("Не удалось прочитать xls/xlsx через pandas: %s", e)
            raise

        sheet_models: List[SheetModel] = []
        flat_data: List[dict] = []

        # xls is dict: {sheet_name: DataFrame}
        for idx, (sheet_name, df) in enumerate(xls.items()):
            # пропускаем по индексам, если указано
            if idx in skip_sheets:
                logger.debug(f"Пропускаем лист по индексу {idx} ({sheet_name}) согласно skip_sheets")
                continue

            # Используем фабрику парсеров для получения подходящего парсера
            parser = ParserFactory.create_parser(sheet_name, form_type)

            logger.debug(f"Лист {idx}: '{sheet_name}', выбран парсер: {parser.__class__.__name__}")

            try:
                # Парсеры ожидают pandas.DataFrame с индексами строк/столбцов.
                # В проекте парсеры используют NotesProcessor.process_notes и т.п.
                parsed = parser.parse(df)

                # Добавляем информацию о типе формы в результат парсинга
                if "form_type" not in parsed:
                    parsed["form_type"] = form_type.value

                # parser.create_data и .data устанавливаются в parser.parse
                # Получим список плоских записей через generate_flat_data
                records = parser.generate_flat_data(file_model.year, file_model.city, sheet_name, file_model.form_id)

                # Создаём SheetModel-представление (совместимо с существующей моделью)
                sheet_model_doc = {
                    "file_id": file_model.file_id,
                    "sheet_name": sheet_name,
                    "sheet_fullname": sheet_name,
                    "year": file_model.year,
                    "city": file_model.city,
                    "form_type": form_type.value,  # Добавляем тип формы
                    "headers": parsed.get("headers", {}),
                    "data": parsed.get("data", [])
                }
                sheet_models.append(SheetModel(**sheet_model_doc))

                # records — список dict, у них ещё не должно быть file_id, мы добавим его позже в IngestionService
                for r in records:
                    # гарантируем, что year/city/section/row/column/value присутствуют в записи
                    flat = {
                        "year": r.get("year", file_model.year),
                        "city": r.get("city", file_model.city),
                        "section": r.get("section", sheet_name),
                        "row": r.get("row"),
                        "column": r.get("column"),
                        "value": r.get("value"),
                        "form_type": form_type.value  # Добавляем тип формы в flat_data
                    }
                    # file_id добавим в ingestion_service (там легче контролировать откат)
                    flat_data.append(flat)

                logger.debug(f"Лист '{sheet_name}' обработан: {len(records)} записей")

            except Exception as e:
                logger.exception(f"Ошибка обработки листа {sheet_name}: {e}")
                # если парсинг одного листа упал — логируем и продолжаем с другими листами
                continue

        logger.info(f"Обработка листов завершена. Успешно: {len(sheet_models)} листов, "
                    f"сгенерировано {len(flat_data)} записей flat_data")

        return sheet_models, flat_data