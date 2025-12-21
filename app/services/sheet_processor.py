# app/services/sheet_processor.py
from typing import List, Tuple, Optional
from fastapi import UploadFile
import pandas as pd
import math
from app.models.sheet_model import SheetModel
from app.core.database import mongo_connection
import logging

from app.parsers.parsers import get_sheet_parser, PARSERS

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
    для каждого листа подобрать соответствующий парсер (get_sheet_parser)
    и выполнить парсинг -> собрать SheetModel-ы и flat_data.
    Поддерживает пропуск листов по индексам (skip_sheets).
    """

    def __init__(self):
        # ничего не храним в состоянии
        pass

    async def extract_and_process_sheets(
        self,
        file: UploadFile,
        file_model,
        skip_sheets: Optional[List[int]] = None
    ) -> Tuple[List[SheetModel], List[dict]]:
        """
        file: UploadFile (fastapi)
        file_model: экземпляр FileModel (для использования year/city/file_id)
        skip_sheets: список индексов листов (0-based), которые нужно пропустить
        Возвращает: (sheet_models, flat_data)
        """
        if skip_sheets is None:
            skip_sheets = []

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
        except Exception as e:
            logger.exception("Не удалось прочитать xls/xlsx через pandas: %s", e)
            raise

        sheet_models: List[SheetModel] = []
        flat_data: List[dict] = []

        # xls is dict: {sheet_name: DataFrame}
        for idx, (sheet_name, df) in enumerate(xls.items()):
            # пропускаем по индексам, если указано
            if idx in skip_sheets:
                logger.debug("SheetProcessor: пропускаем лист по индексу %s (%s) согласно skip_sheets", idx, sheet_name)
                continue

            parser = None
            # 1) Попробуем взять парсер по точному имени
            try:
                parser = get_sheet_parser(sheet_name)
            except Exception:
                # 2) Попробуем подобрать парсер по наличию ключа PARSERS в названии листа
                for patt in PARSERS.keys():
                    if patt in str(sheet_name):
                        try:
                            parser = get_sheet_parser(patt)
                            break
                        except Exception:
                            continue
                # 3) Если не нашли - логируем и пропускаем лист
            if parser is None:
                logger.info("SheetProcessor: нет парсера для листа '%s' (индекс %d) — пропускаем", sheet_name, idx)
                continue

            try:
                # Парсеры ожидают pandas.DataFrame с индексами строк/столбцов.
                # В проекте парсеры используют NotesProcessor.process_notes и т.п.
                parsed = parser.parse(df)
                # parser.create_data и .data устанавливаются в parser.parse
                # Получим список плоских записей через generate_flat_data
                records = parser.generate_flat_data(file_model.year, file_model.city, sheet_name)
                # Создаём SheetModel-представление (совместимо с существующей моделью)
                sheet_model_doc = {
                    "file_id": file_model.file_id,
                    "sheet_name": sheet_name,
                    "sheet_fullname": sheet_name,
                    "year": file_model.year,
                    "city": file_model.city,
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
                        "value": r.get("value")
                    }
                    # file_id добавим в ingestion_service (там легче контролировать откат)
                    flat_data.append(flat)
            except Exception as e:
                logger.exception("Ошибка обработки листа %s: %s", sheet_name, e)
                # если парсинг одного листа упал — логируем и продолжаем с другими листами
                continue

        return sheet_models, flat_data
