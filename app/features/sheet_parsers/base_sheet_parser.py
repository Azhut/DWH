from abc import ABC, abstractmethod
from typing import Dict


class BaseSheetParser(ABC):
    """
    Абстрактный класс для парсинга листов.
    """

    @abstractmethod
    async def parse(self, sheet_data: Dict) -> Dict:
        headers = sheet_data["headers"]
        rows = sheet_data["rows"]

        return {
            "headers": headers,
            "rows": rows
        }
