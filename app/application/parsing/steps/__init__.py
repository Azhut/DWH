"""Шаги parsing pipeline."""
from app.application.parsing.steps.base import ParsingPipelineStep
from app.application.parsing.steps.detect_structure import DetectTableStructureStep
from app.application.parsing.steps.process_notes import ProcessNotesStep
from app.application.parsing.steps.parse_headers import ParseHeadersStep
from app.application.parsing.steps.extract_data import ExtractDataStep
from app.application.parsing.steps.generate_flat_data import GenerateFlatDataStep

__all__ = [
    "ParsingPipelineStep",
    "DetectTableStructureStep",
    "ProcessNotesStep",
    "ParseHeadersStep",
    "ExtractDataStep",
    "GenerateFlatDataStep",
]
