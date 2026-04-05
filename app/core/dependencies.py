"""
Единая точка для всех зависимостей FastAPI.
Aggregate-Centric: репозитории и сервисы — из domain; сценарии — из application.
"""

from functools import lru_cache

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.database import mongo_connection
from app.domain.file import FileRepository, FileService
from app.domain.flat_data import FlatDataRepository, FlatDataService
from app.domain.form import FormRepository, FormService
from app.domain.log import LogRepository, LogService
from app.domain.sheet import SheetService
from app.application.upload import UploadManager
from app.application.data import (
    DataDeleteService,
    DataSaveService,
)
from app.application.forms import FormMaintenanceService
from app.application.parsing.registry import get_parsing_strategy_registry

@lru_cache
def get_database() -> AsyncIOMotorDatabase:
    """Возвращает закэшированный объект базы MongoDB для репозиториев и сценариев."""
    return mongo_connection.get_database()


# --- Domain: репозитории ---
@lru_cache
def get_file_repository() -> FileRepository:
    return FileRepository(get_database().get_collection("Files"))


@lru_cache
def get_flat_data_repository() -> FlatDataRepository:
    return FlatDataRepository(get_database().get_collection("FlatData"))


@lru_cache
def get_form_repository() -> FormRepository:
    return FormRepository(get_database().get_collection("Forms"))


@lru_cache
def get_logs_repository() -> LogRepository:
    return LogRepository(get_database().get_collection("Logs"))


# --- Domain: сервисы агрегатов ---
@lru_cache
def get_file_service() -> FileService:
    return FileService(get_file_repository())


@lru_cache
def get_flat_data_service() -> FlatDataService:
    return FlatDataService(get_flat_data_repository())


@lru_cache
def get_form_service() -> FormService:
    return FormService(get_form_repository())


@lru_cache
def get_log_service() -> LogService:
    return LogService(get_logs_repository())


@lru_cache
def get_sheet_service() -> SheetService:
    return SheetService()


# --- Application: сценарии ---
@lru_cache
def get_data_save_service() -> DataSaveService:
    return DataSaveService(
        file_service=get_file_service(),
        flat_data_service=get_flat_data_service(),
        log_service=get_log_service(),
    )


@lru_cache
def get_data_delete_service() -> DataDeleteService:
    return DataDeleteService(
        file_service=get_file_service(),
        flat_data_service=get_flat_data_service(),
        log_service=get_log_service(),
    )


@lru_cache
def get_form_maintenance_service() -> FormMaintenanceService:
    return FormMaintenanceService(
        form_service=get_form_service(),
        file_service=get_file_service(),
        flat_data_service=get_flat_data_service(),
        log_service=get_log_service(),
    )


@lru_cache
def get_upload_manager() -> UploadManager:
    from app.application.parsing.registry import get_parsing_strategy_registry
    return UploadManager(
        file_service=get_file_service(),
        form_service=get_form_service(),
        data_save_service=get_data_save_service(),
        parsing_registry=get_parsing_strategy_registry(),
    )


def clear_dependency_caches() -> None:
    """
    Сбрасывает кэш @lru_cache для фабрик зависимостей.

    Используется в тестах или после переконфигурации; в продакшене обычно достаточно перезапуска процесса.
    """
    get_database.cache_clear()
    get_file_repository.cache_clear()
    get_flat_data_repository.cache_clear()
    get_form_repository.cache_clear()
    get_logs_repository.cache_clear()
    get_file_service.cache_clear()
    get_flat_data_service.cache_clear()
    get_form_service.cache_clear()
    get_log_service.cache_clear()
    get_sheet_service.cache_clear()
    get_data_save_service.cache_clear()
    get_data_delete_service.cache_clear()
    get_form_maintenance_service.cache_clear()
    get_upload_manager.cache_clear()
