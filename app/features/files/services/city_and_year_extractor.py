import re

from fastapi import HTTPException


class CityAndYearExtractor:
    """
    Сервис для извлечения города и года из имени файла.
    """

    def extract(self, filename: str) -> tuple[str, int]:
        """
        Извлекает год и город из имени файла.
        Пример: 'ИРБИТ 2023.xls' -> ('ИРБИТ', 2023)
        """
        match = re.match(r"([А-Яа-я]+) (\d{4})\.(xls|xlsx)", filename)
        if match:
            city = match.group(1).upper()
            year = int(match.group(2))
            return city, year
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Не удалось извлечь город и год из имени файла: {filename}"
            )
