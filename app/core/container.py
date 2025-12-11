"""
Единый DI контейнер для управления зависимостями приложения
"""
from .container_base import ContainerBase
from app.data.repositories.file import FileRepository
from app.data.repositories.flat_data import FlatDataRepository
from app.data.repositories.logs import LogsRepository
from app.data.services.data_delete import DataDeleteService
from app.data.services.data_retrieval import DataRetrievalService
from app.data.services.data_save import DataSaveService
from app.data.services.file_service import FileService
from app.data.services.flat_data_service import FlatDataService
from app.data.services.log_service import LogService
from app.data.services.filter_service import FilterService


class Container(ContainerBase):
    """Контейнер зависимостей приложения"""

    def __init__(self):
        super().__init__()

    # Репозитории
    def get_file_repository(self) -> FileRepository:
        """Фабрика для FileRepository"""
        return FileRepository(self.get_db().get_collection("Files"))

    def get_flat_data_repository(self) -> FlatDataRepository:
        """Фабрика для FlatDataRepository"""
        return FlatDataRepository(self.get_db().get_collection("FlatData"))

    def get_logs_repository(self) -> LogsRepository:
        """Фабрика для LogsRepository"""
        return LogsRepository(self.get_db().get_collection("Logs"))

    # Сервисы данных
    def get_log_service(self) -> LogService:
        """Фабрика для LogService"""
        return LogService(self.get_logs_repository())

    def get_flat_data_service(self) -> FlatDataService:
        """Фабрика для FlatDataService"""
        return FlatDataService(self.get_flat_data_repository())

    def get_file_service(self) -> FileService:
        """Фабрика для FileService"""
        return FileService(self.get_file_repository())

    def get_filter_service(self) -> FilterService:
        """Фабрика для FilterService"""
        return FilterService(self.get_flat_data_repository())

    def get_data_retrieval_service(self) -> DataRetrievalService:
        """Фабрика для DataRetrievalService"""
        service = self.get_service("data_retrieval")
        if not service:
            service = DataRetrievalService(self.get_filter_service())
            self.register_service("data_retrieval", service)
        return service
    

    def get_data_save_service(self) -> DataSaveService:
        """Фабрика для DataSaveService"""
        service = self.get_service("data_save")
        if not service:
            service = DataSaveService(
                log_service=self.get_log_service(),
                flat_data_service=self.get_flat_data_service(),
                file_service=self.get_file_service()
            )
            self.register_service("data_save", service)
        return service

    def get_data_delete_service(self) -> DataDeleteService:
        """Фабрика для DataDeleteService"""
        service = self.get_service("data_delete")
        if not service:
            service = DataDeleteService(
                file_repo=self.get_file_repository(),
                flat_repo=self.get_flat_data_repository(),
                log_service=self.get_log_service()
            )
            self.register_service("data_delete", service)
        return service


# Глобальный экземпляр контейнера
container = Container()


# Функции для зависимостей FastAPI (для обратной совместимости)
def get_file_repository() -> FileRepository:
    return container.get_file_repository()


def get_flat_data_repository() -> FlatDataRepository:
    return container.get_flat_data_repository()


def get_logs_repository() -> LogsRepository:
    return container.get_logs_repository()


def get_log_service() -> LogService:
    return container.get_log_service()


def get_flat_data_service() -> FlatDataService:
    return container.get_flat_data_service()


def get_file_service() -> FileService:
    return container.get_file_service()


def get_filter_service() -> FilterService:
    return container.get_filter_service()


def get_data_retrieval_service() -> DataRetrievalService:
    return container.get_data_retrieval_service()


def get_data_save_service() -> DataSaveService:
    return container.get_data_save_service()


def get_data_delete_service() -> DataDeleteService:
    return container.get_data_delete_service()