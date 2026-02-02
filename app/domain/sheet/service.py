"""Сервис агрегата Sheet: только I/O операции (чтение Excel, округление)."""
import logging
from io import BytesIO
from typing import Dict

import pandas as pd
from fastapi import UploadFile

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
        Читает все листы из UploadFile в словарь {sheet_name: DataFrame}.
        
        Returns:
            Словарь с именами листов как ключами и DataFrame как значениями
        """
        await file.seek(0)
        content = await file.read()
        await file.seek(0)
        buffer = BytesIO(content)
        sheets = pd.read_excel(buffer, sheet_name=None, header=None, engine=None)
        logger.debug("Прочитано %d листов из файла %s", len(sheets), file.filename)
        return sheets
