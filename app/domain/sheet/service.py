"""Сервис агрегата Sheet: только I/O операции (чтение Excel, округление)."""
import logging
from typing import Dict
import pandas as pd
from io import BytesIO

from app.domain.form.models import FormInfo
from app.domain.sheet.rounding import RoundingService

logger = logging.getLogger(__name__)


class SheetService:
    """
    Сервис агрегата Sheet

    ОТВЕТСТВЕННОСТЬ:
    - round_dataframe: округление числовых данных (опционально по форме)
    """

    def round_dataframe(self, sheet_name: str, df: pd.DataFrame, form_info: FormInfo) -> pd.DataFrame:
        """
        Округление числовых данных. По умолчанию — RoundingService.
        Подкласс может отключить или изменить логику округления.
        """
        return RoundingService.round_dataframe(sheet_name, df)

