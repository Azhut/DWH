from app.data_storage.repositories.base_repository import BaseRepository

class FlatDataRepository(BaseRepository):
    def __init__(self, collection):
        super().__init__(collection)

    async def find_by_year_and_city(self, year: int, city: str):
        """
        Найти записи по году и городу
        """
        return await self.find({"year": year, "city": city})

    async def delete_by_city_and_year(self, city: str, year: int):
        """Удалить записи по городу и году."""
        return await self.delete_many({"city": city.upper(), "year": year})