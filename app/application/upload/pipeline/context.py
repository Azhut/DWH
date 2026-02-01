"""Контекст upload pipeline: передаётся между шагами."""
from dataclasses import dataclass, field
from typing import Any, List, Optional

from fastapi import UploadFile


@dataclass
class UploadPipelineContext:
    """Контекст обработки одного файла в рамках upload pipeline."""

    file: UploadFile
    form_id: str

    file_info: Any = None  # domain.file.FileInfo
    form_info: Any = None  # domain.form.FormInfo
    file_model: Any = None  # domain.file.FileModel
    sheet_models: Optional[List[Any]] = None
    flat_data: Optional[List[dict]] = None

    error: Optional[str] = None
    failed: bool = False
    file_response: Any = None
