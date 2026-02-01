"""
Ядро приложения: database, exceptions, logger.
Зависимости (get_upload_manager и др.) — импортировать из app.core.dependencies.
"""
from .database import mongo_connection
from .exceptions import log_and_raise_http
from .logger import logger

__all__ = [
    "mongo_connection",
    "log_and_raise_http",
    "logger",
]