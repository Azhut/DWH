"""
Ядро приложения - только базовые компоненты
"""
from .database import mongo_connection
from .exceptions import log_and_raise_http
from .logger import logger
from .container import (
    container,
    get_data_retrieval_service,
    get_data_delete_service,
    get_data_save_service
)
from .service_factory import get_ingestion_service

__all__ = [
    "mongo_connection",
    "log_and_raise_http",
    "logger",
    "container",
    "get_data_retrieval_service",
    "get_data_delete_service",
    "get_data_save_service",
    "get_ingestion_service"
]