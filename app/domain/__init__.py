"""
Domain layer: Aggregate-Centric Architecture.
Каждый агрегат (file, form, flat_data, log, sheet) — своя директория с моделями, репозиторием и сервисом.
"""
from app.domain.base import BaseRepository

__all__ = ["BaseRepository"]
