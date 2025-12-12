"""
Единая точка для всех зависимостей FastAPI.
Используем functools.lru_cache для синглтонов в рамках приложения.
"""
from functools import lru_cache

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.database import mongo_connection
from app.data.repositories import FileRepository
from app.data.repositories.flat_data import FlatDataRepository
from app.data.repositories.logs import LogsRepository
from app.data.services.data_delete import DataDeleteService
from app.data.services.data_retrieval import DataRetrievalService
from app.data.services.data_save import DataSaveService
from app.data.services.file_service import FileService
from app.data.services.flat_data_service import FlatDataService
from app.data.services.filter_service import FilterService
from app.data.services.log_service import LogService
from app.services.file_processor import FileProcessor
from app.services.sheet_processor import SheetProcessor
from app.services.sheet_extraction_service import SheetExtractionService
from app.services.ingestion_service import IngestionService


# Database
@lru_cache
def get_database() -> AsyncIOMotorDatabase:
    return mongo_connection.get_database()


# Repositories
@lru_cache
def get_file_repository() -> FileRepository:
    return FileRepository(get_database().get_collection("Files"))


@lru_cache
def get_flat_data_repository() -> FlatDataRepository:
    return FlatDataRepository(get_database().get_collection("FlatData"))


@lru_cache
def get_logs_repository() -> LogsRepository:
    return LogsRepository(get_database().get_collection("Logs"))


# Services
@lru_cache
def get_log_service() -> LogService:
    return LogService(get_logs_repository())


@lru_cache
def get_flat_data_service() -> FlatDataService:
    return FlatDataService(get_flat_data_repository())


@lru_cache
def get_file_service() -> FileService:
    return FileService(get_file_repository())


@lru_cache
def get_filter_service() -> FilterService:
    return FilterService(get_flat_data_repository())


@lru_cache
def get_data_retrieval_service() -> DataRetrievalService:
    return DataRetrievalService(get_filter_service())


@lru_cache
def get_data_save_service() -> DataSaveService:
    return DataSaveService(
        log_service=get_log_service(),
        flat_data_service=get_flat_data_service(),
        file_service=get_file_service()
    )


@lru_cache
def get_data_delete_service() -> DataDeleteService:
    return DataDeleteService(
        file_repo=get_file_repository(),
        flat_repo=get_flat_data_repository(),
        log_service=get_log_service()
    )


# Processing services
@lru_cache
def get_file_processor() -> FileProcessor:
    return FileProcessor()


@lru_cache
def get_sheet_processor() -> SheetProcessor:
    return SheetProcessor()


@lru_cache
def get_sheet_extraction_service() -> SheetExtractionService:
    return SheetExtractionService()


@lru_cache
def get_ingestion_service() -> IngestionService:
    return IngestionService(
        file_processor=get_file_processor(),
        sheet_processor=get_sheet_processor(),
        data_save_service=get_data_save_service()
    )