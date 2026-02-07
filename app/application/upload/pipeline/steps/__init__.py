"""Шаги upload pipeline."""
from app.application.upload.pipeline.steps.base import UploadPipelineStep
from app.application.upload.pipeline.steps.extract_metadata import ExtractMetadataStep
from app.application.upload.pipeline.steps.check_uniqueness import CheckUniquenessStep
from app.application.upload.pipeline.steps.create_file_model import CreateFileModelStep
from app.application.upload.pipeline.steps.ProcessSheetsStep.process_sheets import ProcessSheetsStep
from app.application.upload.pipeline.steps.enrich_flat_data import EnrichFlatDataStep
from app.application.upload.pipeline.steps.persist import PersistStep

__all__ = [
    "UploadPipelineStep",
    "ExtractMetadataStep",
    "CheckUniquenessStep",
    "CreateFileModelStep",
    "ProcessSheetsStep",
    "EnrichFlatDataStep",
    "PersistStep",
]
