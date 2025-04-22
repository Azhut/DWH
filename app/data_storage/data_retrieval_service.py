import math
from typing import List, Dict, Tuple
from app.core.config import mongo_connection
from app.data_storage.repositories.flat_data_repository import FlatDataRepository
from app.data_storage.services.filter_service import FilterService



class DataRetrievalService:
    def __init__(self, filter_service: FilterService):
        db = mongo_connection.get_database()
        self.flat_data_repo = FlatDataRepository(db.get_collection("FlatData"))
        self.filter_service = filter_service

    async def get_filter_values(self, filter_name: str, applied_filters: List[Dict], pattern: str = "") -> List:
        """
        Получить значения фильтра через FilterService
        """
        return await self.filter_service.get_filter_values(filter_name, applied_filters, pattern)

    async def get_filtered_data(self, filters: List[Dict], limit: int, offset: int) -> Tuple[List, int]:
        """
        Получить данные через FilterService
        """
        return await self.filter_service.get_filtered_data(filters,limit,offset)



def create_data_retrieval_service():
    """
    Фабричный метод для создания экземпляра DataRetrievalService с инициализированным FilterService
    """
    db = mongo_connection.get_database()
    flat_data_repo = FlatDataRepository(db.get_collection("FlatData"))
    filter_service = FilterService(flat_data_repo)


    return DataRetrievalService(filter_service)

