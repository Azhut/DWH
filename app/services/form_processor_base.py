from abc import ABC, abstractmethod


class FormProcessorBase(ABC):
    """
    Базовый контракт обработчика формы
    """

    form_code: str

    def __init__(self, form_model):
        """
        form_model — объект FormModel из БД
        """
        self.form_model = form_model

    @abstractmethod
    def parse_metadata(self, file_path: str) -> dict:
        pass

    @abstractmethod
    def parse_file(self, file_path: str):
        """
        Возвращает результат в ТЕКУЩЕМ формате проекта
        """
        pass

    @abstractmethod
    def validate(self, parsed_result) -> None:
        pass
