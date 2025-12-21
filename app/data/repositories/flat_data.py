from typing import Dict, List, Tuple, Any
from app.data.repositories.base import BaseRepository


class FlatDataRepository(BaseRepository):
    """
    Репозиторий для коллекции flat_data
    """

    # порядок колонок для табличного ответа
    TABLE_FIELDS = ["year", "city", "section", "row", "column", "value"]

    def __init__(self, collection):
        super().__init__(collection)

    # ----------- базовые операции ------------

    async def distinct(self, field: str, query: Dict[str, Any]) -> List[Any]:
        """
        Получить уникальные значения поля с учётом фильтра
        """
        return await self.collection.distinct(field, query)

    async def get_filtered_data(
        self,
        query: Dict[str, Any],
        limit: int,
        offset: int
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Получить документы + общее количество

        Возвращает:
        - docs: List[Dict]
        - total: int
        """
        projection = {field: 1 for field in self.TABLE_FIELDS}
        projection["_id"] = 0

        docs = await self.find(
            query=query,
            projection=projection,
            limit=limit,
            skip=offset
        )

        total = await self.count_documents(query)

        return docs, total

    # ----------- доменные методы (опционально) ------------

    async def delete_by_form(self, form_id: str):
        """
        Удалить все данные формы
        """
        return await self.delete_many({"form": form_id})

    async def delete_by_city_and_year(self, city: str, year: int):
        """
        Удалить записи по городу и году (оставлено для совместимости)
        """
        return await self.delete_many({
            "city": city.upper(),
            "year": year
        })
