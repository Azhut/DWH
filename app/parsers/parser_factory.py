# app/parsers/parser_factory.py
"""
Единое место выбора обработчиков форм по типу формы.
Чтобы добавить новый тип формы: FormType в form_model.py, detect_form_type,
и ветка в create_parser здесь. Логика форм сосредоточена в form_model + ParserFactory.
"""

import logging
from typing import Optional, Any

import pandas as pd

from app.domain.form.models import FormType, FormInfo, detect_form_type
from app.parsers.parsers import PARSERS as FK1_PARSERS
from app.parsers.five_fk_parser import FiveFKParser
from app.parsers.universal_parser import UniversalParser
from app.parsers.base_sheet_parser import BaseSheetParser

logger = logging.getLogger(__name__)


class ParserFactory:
    """
    Фабрика парсеров по типу формы. Единое место для добавления/изменения
    обработчиков форм: новый FormType → ветка в create_parser.
    """

    @staticmethod
    def create_parser(sheet_name: str, form_type: FormType) -> BaseSheetParser:
        """
        Создает парсер для указанного листа и типа формы.

        Args:
            sheet_name: Название листа (например, "Раздел0")
            form_type: Тип формы (1ФК, 5ФК и т.д.)

        Returns:
            Экземпляр парсера, наследованный от BaseSheetParser
        """
        logger.debug(f"Создание парсера для листа '{sheet_name}', тип формы: {form_type}")

        if form_type == FormType.FK_1:
            parser_class = FK1_PARSERS.get(sheet_name)
            if parser_class:
                parser = parser_class()
                logger.debug(f"Используется парсер 1ФК для листа '{sheet_name}'")
                return parser
            else:
                logger.warning(
                    f"Не найден специфичный парсер 1ФК для листа '{sheet_name}', "
                    "используется универсальный парсер"
                )

        elif form_type == FormType.FK_5:
            logger.info(f"Используется парсер 5ФК для листа '{sheet_name}'")
            return FiveFKParser(sheet_name)

        # Fallback: универсальный парсер для неизвестных типов или отсутствующих парсеров
        logger.info(
            f"Используется универсальный парсер для листа '{sheet_name}' (тип формы: {form_type})"
        )
        return UniversalParser(sheet_name)

    @staticmethod
    def get_available_parsers(form_type: FormType) -> dict:
        """
        Возвращает доступные парсеры для указанного типа формы.
        """
        if form_type == FormType.FK_1:
            return FK1_PARSERS.copy()
        elif form_type == FormType.FK_5:
            return {"*": FiveFKParser}
        else:
            return {"*": UniversalParser}

    # ---------------------------
    # Новый helper: парсер файла
    # ---------------------------
    @staticmethod
    def create_file_parser(form_model: Any):
        """
        Возвращает объект FileParser, у которого есть метод parse(file_path_or_buffer).
        form_model может быть:
          - экземпляром app.domain.form.models.FormInfo
          - dict с ключом 'name' и/или 'type'
          - простой строкой с названием формы
        """

        # Определяем FormType
        if form_model is None:
            form_type = FormType.UNKNOWN
        elif isinstance(form_model, FormInfo):
            form_type = form_model.type
        elif isinstance(form_model, dict):
            # пытаемся взять явно указанный тип
            typ = form_model.get("type")
            if isinstance(typ, FormType):
                form_type = typ
            elif isinstance(typ, str):
                # если строка — сопоставляем
                try:
                    form_type = FormType(typ)
                except Exception:
                    form_type = detect_form_type(form_model.get("name", ""))
            else:
                form_type = detect_form_type(form_model.get("name", ""))
        elif isinstance(form_model, str):
            form_type = detect_form_type(form_model)
        else:
            # на всякий случай
            try:
                # если объект имеет атрибут type
                form_type = getattr(form_model, "type", FormType.UNKNOWN)
                if isinstance(form_type, str):
                    form_type = FormType(form_type)
            except Exception:
                form_type = FormType.UNKNOWN

        class FileParser:
            def __init__(self, form_type: FormType):
                self.form_type = form_type

            def parse(self, file_path_or_buffer) -> dict:
                """
                Парсит весь файл и возвращает словарь {sheet_name: parsed_result}.
                parsed_result — объект, который возвращает конкретный sheet parser (обычно dict).
                file_path_or_buffer может быть путь или любой объект, который pandas.read_excel принимает.
                """
                logger.debug(f"FileParser.parse: parsing file for form_type={self.form_type}")
                try:
                    xls = pd.read_excel(file_path_or_buffer, sheet_name=None, header=None)
                except Exception as e:
                    logger.exception("Ошибка чтения Excel-файла в create_file_parser.parse")
                    raise

                results = {}
                for sheet_name, df in xls.items():
                    try:
                        sheet_parser = ParserFactory.create_parser(sheet_name, self.form_type)
                        parsed = sheet_parser.parse(df)
                        results[sheet_name] = parsed
                    except Exception:
                        # не прерываем весь файл — но логируем и кладём в results None с error
                        logger.exception("Ошибка парсинга листа '%s'", sheet_name)
                        results[sheet_name] = {"error": f"Не удалось распарсить лист {sheet_name}"}

                return results

        return FileParser(form_type)
