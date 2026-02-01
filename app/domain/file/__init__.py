"""Агрегат File: модели, репозиторий, сервис."""
from app.domain.file.models import FileModel, FileStatus, FileInfo
from app.domain.file.repository import FileRepository
from app.domain.file.service import FileService

__all__ = ["FileModel", "FileStatus", "FileInfo", "FileRepository", "FileService"]
