from typing import List, Optional, Dict, Any
from motor.motor_asyncio import AsyncIOMotorCollection


class FormsRepository:

    def __init__(self, collection: AsyncIOMotorCollection):
        self.collection = collection

    def _strip_id(self, doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not doc:
            return doc
        doc.pop("_id", None)
        return doc

    async def list_forms(self) -> List[Dict[str, Any]]:
        docs = await self.collection.find({}).to_list(length=None)
        return [self._strip_id(d) for d in docs]

    async def get_form(self, form_id: str) -> Optional[Dict[str, Any]]:
        doc = await self.collection.find_one({"id": form_id})
        return self._strip_id(doc)

    async def create_form(self, form_doc: Dict[str, Any]) -> Dict[str, Any]:
        await self.collection.insert_one(form_doc)
        return self._strip_id(form_doc)

    async def update_form(self, form_id: str, update_doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        result = await self.collection.update_one({"id": form_id}, {"$set": update_doc})
        if result.matched_count == 0:
            return None
        doc = await self.collection.find_one({"id": form_id})
        return self._strip_id(doc)

    async def delete_form(self, form_id: str) -> bool:
        result = await self.collection.delete_one({"id": form_id})
        return result.deleted_count > 0
