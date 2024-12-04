from abc import ABC, abstractmethod
from typing import Dict


class BaseSheetParser(ABC):
    """
    Абстрактный класс для парсинга листов.
    """

    @abstractmethod
    async def parse(self, sheet_data: Dict) -> Dict:
        """
        Метод парсинга данных с листа.
        """
        return {'Раздел0':'data'}
