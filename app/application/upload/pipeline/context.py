from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from fastapi import UploadFile

from app.domain.file.models import FileInfo, FileModel
from app.domain.flat_data.models import FlatDataRecord
from app.domain.form.models import FormInfo
from app.domain.sheet.models import SheetModel


@dataclass
class UploadPipelineContext:
    """Контекст обработки одного загруженного файла в upload pipeline."""

    form_id: str
    form_info: FormInfo
    file: UploadFile
    filename: str

    file_content: Optional[bytes] = None
    file_info: Optional[FileInfo] = None
    file_model: Optional[FileModel] = None
    workbook_sheets: Dict[str, Any] = field(default_factory=dict)
    sheets: List[SheetModel] = field(default_factory=list)

    error: Optional[str] = None
    failed: bool = False
    warnings: List[str] = field(default_factory=list)

    @property
    def flat_data(self) -> List[FlatDataRecord]:
        """Собирает все FlatDataRecord из распарсенных листов."""
        return [record for sheet in self.sheets for record in sheet.flat_data_records]
