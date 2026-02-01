"""Шаги upload pipeline."""
from app.services.upload_pipeline.steps.base import UploadPipelineStep
from app.services.upload_pipeline.steps.validate_request import ValidateRequestStep
from app.services.upload_pipeline.steps.extract_metadata import ExtractMetadataStep
from app.services.upload_pipeline.steps.check_uniqueness import CheckUniquenessStep
from app.services.upload_pipeline.steps.load_form import LoadFormStep
from app.services.upload_pipeline.steps.create_file_model import CreateFileModelStep
from app.services.upload_pipeline.steps.process_sheets import ProcessSheetsStep
from app.services.upload_pipeline.steps.enrich_flat_data import EnrichFlatDataStep
from app.services.upload_pipeline.steps.persist import PersistStep

__all__ = [
    "UploadPipelineStep",
    "ValidateRequestStep",
    "ExtractMetadataStep",
    "CheckUniquenessStep",
    "LoadFormStep",
    "CreateFileModelStep",
    "ProcessSheetsStep",
    "EnrichFlatDataStep",
    "PersistStep",
]
