from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime


@dataclass
class UploadProgress:
    """Модель для хранения прогресса загрузки файлов в памяти"""
    upload_id: str
    total_files: int
    current_file: int = 0
    status: str = "processing"  # processing, completed, failed
    form_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    processed_files: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    @property
    def progress_percentage(self) -> float:
        """Возвращает процент выполнения"""
        if self.total_files == 0:
            return 0.0
        return (self.current_file / self.total_files) * 100
    
    def add_processed_file(self, filename: str, success: bool = True, error: Optional[str] = None):
        """Добавляет обработанный файл в прогресс"""
        self.processed_files.append(filename)
        self.current_file += 1
        
        if not success and error:
            self.errors.append(error)
    
    def complete(self):
        """Отмечает загрузку как завершенную"""
        self.status = "completed"
        if self.current_file >= self.total_files:
            self.current_file = self.total_files
    
    def fail(self):
        """Отмечает загрузку как неудачную"""
        self.status = "failed"
