"""
Округление числовых данных по листам. Используется в upload pipeline (ProcessSheetsStep).
"""
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP


class RoundingService:
    SHEETS_INT = {"Раздел1", "Раздел2", "Раздел5", "Раздел7"}
    SHEETS_ONE_DECIMAL = {"Раздел4", "Раздел6"}
    SHEETS_TWO_DECIMALS = {"Раздел3"}
    SPECIAL_ROW_PREFIXES = [
        "Всего спортивных сооружений с учетом объектов",
        "Всего спортивных сооружений (сумма строк",
        "Сооружения для стрелковых видов спорта – всего",
        "Объекты городской и рекреационной инфраструктуры"
    ]
    SPECIAL_COLUMN_KEYWORDS = ["Количество спортсооружений", "Всего"]
    SHEET6_ROW_PREFIX = "Всего (сумма строк"

    @staticmethod
    def _round_half_up(value: float, ndigits: int = 0) -> float:
        quant = Decimal('1') if ndigits == 0 else Decimal(f"1e-{ndigits}")
        d = Decimal(str(value))
        rounded = d.quantize(quant, rounding=ROUND_HALF_UP)
        return float(rounded)

    @staticmethod
    def round_dataframe(sheet_name: str, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Применяем правила округления в зависимости от названия листа
        def to_int_nearest(x):
            if isinstance(x, float) and not pd.isna(x):
                return int(RoundingService._round_half_up(x, 0))
            return x
        def to_one_decimal(x):
            if isinstance(x, float) and not pd.isna(x):
                return float(Decimal(str(x)).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP))
            return x
        def to_two_decimals(x):
            if isinstance(x, float) and not pd.isna(x):
                return float(Decimal(str(x)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
            return x
        for col in df.columns:
            if sheet_name in RoundingService.SHEETS_INT:
                df[col] = df[col].apply(to_int_nearest)
            elif sheet_name in RoundingService.SHEETS_ONE_DECIMAL:
                df[col] = df[col].apply(to_one_decimal)
            elif sheet_name in RoundingService.SHEETS_TWO_DECIMALS:
                df[col] = df[col].apply(to_two_decimals)
        return df
