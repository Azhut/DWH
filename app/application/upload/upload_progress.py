# app/application/upload/upload_progress.py
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from app.api.v2.schemas.files import FileResponse


@dataclass
class UploadProgress:
    """
    Состояние фоновой задачи загрузки файлов.

    Жизненный цикл:
        processing → completed | failed

    После перехода в терминальный статус поле `file_responses`
    содержит итоговые результаты по каждому файлу и может быть
    использовано для формирования финального UploadResponse.
    """

    upload_id: str
    total_files: int
    form_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)

    # --- изменяемое состояние ---
    status: str = "processing"          # processing | completed | failed
    current_file: int = 0
    processed_files: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    # Итоговые данные — заполняются только после завершения
    file_responses: List[FileResponse] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Свойства
    # ------------------------------------------------------------------

    @property
    def progress_percentage(self) -> float:
        if self.total_files == 0:
            return 0.0
        return (self.current_file / self.total_files) * 100

    # ------------------------------------------------------------------
    # Мутации (вызываются из фоновой задачи)
    # ------------------------------------------------------------------

    def add_processed_file(
        self,
        filename: str,
        success: bool = True,
        error: Optional[str] = None,
    ) -> None:
        """Фиксирует результат обработки одного файла."""
        self.processed_files.append(filename)
        self.current_file += 1
        if not success and error:
            self.errors.append(error)

    def complete(self, file_responses: List[FileResponse]) -> None:
        """
        Переводит задачу в терминальный статус «completed».
        Сохраняет итоговые результаты для отдачи клиенту через SSE.
        """
        self.file_responses = file_responses
        self.current_file = self.total_files
        self.status = "completed"

    def fail(self, file_responses: Optional[List[FileResponse]] = None) -> None:
        """
        Переводит задачу в терминальный статус «failed».
        Частичные результаты сохраняются, если они есть.
        """
        if file_responses:
            self.file_responses = file_responses
        self.status = "failed"