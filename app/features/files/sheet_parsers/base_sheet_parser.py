from abc import ABC, abstractmethod

class BaseParser(ABC):
    @abstractmethod
    def parse(self, sheet):
        """
        Абстрактный метод для парсинга данных с листа.
        """
        pass
