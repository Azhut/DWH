"""Сервис агрегата Sheet: только I/O операции (чтение Excel, округление)."""
import logging
from io import BytesIO
from typing import Dict

import pandas as pd
from fastapi import UploadFile
from python_calamine import load_workbook

from app.domain.form.models import FormInfo
from app.domain.sheet.rounding import RoundingService

logger = logging.getLogger(__name__)


class SheetService:
    """
    Сервис агрегата Sheet: отвечает только за I/O операции.
    Вся логика парсинга вынесена в parsing pipeline.
    
    Методы:
    - read_sheets: чтение Excel файла
    - round_dataframe: округление числовых данных (опционально по форме)
    """

    def round_dataframe(self, sheet_name: str, df: pd.DataFrame, form_info: FormInfo) -> pd.DataFrame:
        """
        Округление числовых данных. По умолчанию — RoundingService.
        Подкласс может отключить или изменить логику округления.
        """
        return RoundingService.round_dataframe(sheet_name, df)

    async def read_sheets(self, file: UploadFile) -> Dict[str, pd.DataFrame]:
        """
        Читает все листы через python-calamine.
        Возвращает {имя_листа: DataFrame} для совместимости с текущим парсером.
        """
        await file.seek(0)
        content = await file.read()
        await file.seek(0)

        try:
            # calamine принимает bytes напрямую — определяет формат автоматически
            wb = load_workbook(content)
        except Exception as e:
            raise RuntimeError(
                f"Ошибка чтения файла '{file.filename}' через calamine: {str(e)}"
            ) from e

        sheets = {}
        for sheet_name in wb.sheet_names:
            # calamine возвращает список списков: [[ячейка1, ячейка2, ...], ...]
            raw_data = wb.get_sheet_by_name(sheet_name).to_python()

            if not raw_data:
                logger.warning("Лист '%s' пустой, пропускаем", sheet_name)
                continue

            # Конвертируем в DataFrame БЕЗ предположений о заголовках
            # (парсер сам определит структуру через detect_table_structure)
            df = pd.DataFrame(raw_data)

            sheets[sheet_name] = df
            logger.debug(
                "Лист '%s' прочитан: %d строк × %d колонок",
                sheet_name,
                len(df),
                len(df.columns) if not df.empty else 0
            )

        logger.info("Прочитано %d листов из файла %s", len(sheets), file.filename)
        return sheets
