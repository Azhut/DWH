import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Any


class RoundingService:
    """
    Сервис для округления значений в DataFrame в зависимости от названия листа и специальных правил.
    """
    SHEETS_INT = {"Раздел1", "Раздел2", "Раздел5", "Раздел7"}
    SHEETS_ONE_DECIMAL = {"Раздел4", "Раздел6"}
    SHEETS_TWO_DECIMALS = {"Раздел3"}

    SPECIAL_ROW_PREFIXES: List[str] = [
        "Всего спортивных сооружений с учетом объектов",
        "Всего спортивных сооружений (сумма строк",
        "Сооружения для стрелковых видов спорта – всего",
        "Объекты городской и рекреационной инфраструктуры, приспособленные для занятий"
    ]

    SPECIAL_COLUMN_KEYWORDS: List[str] = [
        "Количество спортсооружений",
        "Всего"
    ]

    # Для sheet6: динамический префикс всех строк, начинающихся с "Всего (сумма строк"
    SHEET6_ROW_PREFIX = "Всего (сумма строк"

    @staticmethod
    def _round_half_up(value: float, ndigits: int = 0) -> float:
        """
        Округление "половинки" вверх: 0.5 -> 1, 0.05->0.1
        """
        quant = Decimal('1') if ndigits == 0 else Decimal(f"1e-{ndigits}")
        d = Decimal(str(value))
        rounded = d.quantize(quant, rounding=ROUND_HALF_UP)
        return float(rounded)

    @staticmethod
    def round_dataframe(sheet_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Округляет числовые значения в DataFrame в зависимости от настроек для листа.
        Пропускает NaN и ненужные типы.
        """
        df_rounded = df.copy()

        # Функции преобразования
        def to_int_nearest(x: Any) -> Any:
            if isinstance(x, float) and not pd.isna(x):
                return int(RoundingService._round_half_up(x, 0))
            return x
        def to_one_decimal(x: Any) -> Any:
            if isinstance(x, float) and not pd.isna(x):
                return RoundingService._round_half_up(x, 1)
            return x
        def to_two_decimals(x: Any) -> Any:
            if isinstance(x, float) and not pd.isna(x):
                return RoundingService._round_half_up(x, 2)
            return x

        # Основная логика по листам
        if sheet_name in RoundingService.SHEETS_INT:
            df_rounded = df_rounded.map(to_int_nearest)

        elif sheet_name in RoundingService.SHEETS_ONE_DECIMAL:
            # Общий проход: один знак после запятой
            df_rounded = df_rounded.map(to_one_decimal)
            # Для Раздел6: особая строка округляется в целые
            if sheet_name == "Раздел6":
                first_col = df_rounded.columns[0]
                for idx, row in df_rounded.iterrows():
                    header = str(row[first_col])
                    if header.startswith(RoundingService.SHEET6_ROW_PREFIX):
                        for col in df_rounded.columns:
                            val = row[col]
                            if isinstance(val, float) and not pd.isna(val):
                                df_rounded.at[idx, col] = to_int_nearest(val)

        elif sheet_name in RoundingService.SHEETS_TWO_DECIMALS:
            # 1) сначала два знака после запятой
            df_rounded = df_rounded.map(to_two_decimals)
            # 2) спец-столбцы: третий столбец по индексу (Unnamed: 2)
            special_cols = []
            cols = list(df_rounded.columns)
            if len(cols) > 2:
                special_cols.append(cols[2])
            # 3) спец-строки
            first_col = cols[0] if cols else None
            for idx, row in df_rounded.iterrows():
                header = str(row[first_col]) if first_col else ''
                is_special_row = any(header.startswith(prefix) for prefix in RoundingService.SPECIAL_ROW_PREFIXES)
                for col in df_rounded.columns:
                    val = row[col]
                    if isinstance(val, float) and not pd.isna(val):
                        if is_special_row or col in special_cols:
                            df_rounded.at[idx, col] = to_int_nearest(val)

        return df_rounded
