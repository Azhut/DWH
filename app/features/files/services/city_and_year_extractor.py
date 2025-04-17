import re
from fastapi import HTTPException


class CityAndYearExtractor:
    """
    Сервис для извлечения города и года из имени файла.
    Поддерживает составные названия городов (с пробелами и дефисами)
    и форматы: .xls, .xlsx, .xlsm.
    """

    def extract(self, filename: str) -> tuple[str, int]:
        """
        Извлекает год и город из имени файла.

        Возможные ошибки:
        - Если не найден год: "Не удалось извлечь год из имени файла"
        - Если город пустой: "Название города не может быть пустым"
        - Если формат не соответствует шаблону: "Некорректный формат имени файла"
        """
        if not re.search(r"\.(xls|xlsx|xlsm)$", filename, re.IGNORECASE):
            raise HTTPException(
                status_code=400,
                detail="Некорректное расширение файла. Допустимые форматы: .xls, .xlsx, .xlsm"
            )

        cleaned_filename = re.sub(r"\b\d+[A-Za-zА-Яа-я]+\b", "", filename).strip()

        pattern = r"""
            ^               # Начало строки
            (.+?)           # Город (нежадный захват)
            \s+             # Разделитель (один или больше пробелов)
            (\d{4})         # Год (4 цифры)
            [^\d]*          # Все символы после года и до расширения (нежадный захват)
            \.              # Точка перед расширением
            (xls|xlsx|xlsm) # Расширение файла
            $               # Конец строки
        """

        match = re.match(pattern, cleaned_filename, re.VERBOSE | re.IGNORECASE)

        if not match:
            raise HTTPException(
                status_code=400,
                detail="Некорректный формат имени файла. Ожидается: 'ГОРОД ГГГГ.расширение'"
            )

        city = match.group(1).strip().upper()
        year = int(match.group(2))

        if not city:
            raise HTTPException(
                status_code=400,
                detail="Название города не может быть пустым"
            )

        return city, year
