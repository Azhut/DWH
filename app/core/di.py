"""
Модуль зависимостей - использует единый контейнер для всех зависимостей
"""
from .container import (
    get_file_repository,
    get_flat_data_repository,
    get_logs_repository,
    get_log_service,
    get_flat_data_service,
    get_file_service,
    get_filter_service,
    get_data_retrieval_service,
    get_data_save_service,
    get_data_delete_service
)
from .service_factory import get_ingestion_service

__all__ = [
    "get_file_repository",
    "get_flat_data_repository",
    "get_logs_repository",
    "get_log_service",
    "get_flat_data_service",
    "get_file_service",
    "get_filter_service",
    "get_data_retrieval_service",
    "get_data_save_service",
    "get_data_delete_service",
    "get_ingestion_service"
]