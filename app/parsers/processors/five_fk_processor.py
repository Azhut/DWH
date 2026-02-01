from app.services.form_processor_base import FormProcessorBase
from app.services.form_processor_registry import FormProcessorRegistry

from app.parsers.parser_factory import ParserFactory


class FiveFKProcessor(FormProcessorBase):
    form_code = "5FK"

    def parse_metadata(self, file_path: str) -> dict:
        return {}

    def parse_file(self, file_path: str):
        parser = ParserFactory.get_parser(self.form_model)
        return parser.parse(file_path)

    def validate(self, parsed_result) -> None:
        pass


FormProcessorRegistry.register(FiveFKProcessor)
