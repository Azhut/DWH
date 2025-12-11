"""
Фабрика для сервисов бизнес-логики (отдельно, чтобы избежать циклических импортов)
"""
from app.services.ingestion_service import IngestionService
from app.services.file_processor import FileProcessor
from app.services.sheet_processor import SheetProcessor
from app.services.sheet_extraction_service import SheetExtractionService
from app.core.container import container


def get_ingestion_service() -> IngestionService:
    """Фабрика для IngestionService"""
    service = container.get_service("ingestion")
    if not service:
        service = IngestionService(
            file_processor=get_file_processor(),
            sheet_processor=get_sheet_processor(),
            data_save_service=container.get_data_save_service()
        )
        container.register_service("ingestion", service)
    return service


def get_file_processor() -> FileProcessor:
    """Фабрика для FileProcessor"""
    service = container.get_service("file_processor")
    if not service:
        service = FileProcessor()
        container.register_service("file_processor", service)
    return service


def get_sheet_processor() -> SheetProcessor:
    """Фабрика для SheetProcessor"""
    service = container.get_service("sheet_processor")
    if not service:
        service = SheetProcessor()
        container.register_service("sheet_processor", service)
    return service


def get_sheet_extraction_service() -> SheetExtractionService:
    """Фабрика для SheetExtractionService"""
    service = container.get_service("sheet_extraction")
    if not service:
        service = SheetExtractionService()
        container.register_service("sheet_extraction", service)
    return service