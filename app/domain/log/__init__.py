"""Агрегат Log: модель, репозиторий, сервис."""
from app.domain.log.models import LogEntry
from app.domain.log.repository import LogRepository
from app.domain.log.service import LogService

__all__ = ["LogEntry", "LogRepository", "LogService"]
