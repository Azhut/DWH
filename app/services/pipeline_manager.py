from app.services.form_processor_registry import FormProcessorRegistry


class PipelineManager:
    def __init__(self, form_model):
        processor_cls = FormProcessorRegistry.get_processor(form_model.code)
        self.processor = processor_cls(form_model)

    def run(self, file_path: str):
        metadata = self.processor.parse_metadata(file_path)
        data = self.processor.parse_file(file_path)
        self.processor.validate(data)

        return {
            "metadata": metadata,
            "data": data
        }
