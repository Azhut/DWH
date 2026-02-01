from app.services.form_processor_base import FormProcessorBase
from app.services.form_processor_registry import FormProcessorRegistry

from app.parsers.parser_factory import ParserFactory


class OneFKProcessor(FormProcessorBase):
    form_code = "1FK"

    def parse_metadata(self, file_path: str) -> dict:
        # сейчас метаданные извлекаются внутри существующего пайплайна
        return {}

    def parse_file(self, file_path: str):
        parser = ParserFactory.get_parser(self.form_model)
        return parser.parse(file_path)

    def validate(self, parsed_result) -> None:
        # текущая логика implicit
        pass


FormProcessorRegistry.register(OneFKProcessor)
