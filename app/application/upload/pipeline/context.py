"""Контекст upload pipeline: передаётся между шагами."""
from dataclasses import dataclass
from typing import List, Optional

from fastapi import UploadFile

from app.api.v2.schemas.files import FileResponse
from app.domain.file.models import FileInfo, FileModel
from app.domain.flat_data.models import FlatDataRecord
from app.domain.form.models import FormInfo
from app.domain.sheet.models import SheetModel


@dataclass
class UploadPipelineContext:
    """Контекст обработки одного файла в рамках upload pipeline."""

    file: UploadFile
    form_id: str

    file_info: Optional[FileInfo] = None
    form_info: Optional[FormInfo] = None
    file_model: Optional[FileModel] = None
    sheet_models: Optional[List[SheetModel]] = None
    flat_data: Optional[List[FlatDataRecord]] = None

    error: Optional[str] = None
    failed: bool = False
    file_response: Optional[FileResponse] = None
