from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseRepository(ABC):
    @abstractmethod
    async def get_all(self) -> List[Dict]:
        pass

    @abstractmethod
    async def get_by_filter(self, filters: Dict) -> List[Dict]:
        pass

    @abstractmethod
    async def create(self, data: Dict) -> Any:
        pass

    @abstractmethod
    async def update(self, filters: Dict, data: Dict) -> int:
        pass

    @abstractmethod
    async def delete(self, filters: Dict) -> int:
        pass