from typing import List, Optional, Dict, Any
from uuid import uuid4
from datetime import datetime

from fastapi import HTTPException

from app.data.repositories.forms import FormsRepository
from app.services.form_validation import validate_form_id


class FormsService:
    """
    Бизнес-обёртка над FormsRepository.
    Единое место для проверки существования формы (get_form_or_raise).
    """

    def __init__(self, repo: FormsRepository):
        self.repo = repo

    async def list_forms(self) -> List[Dict[str, Any]]:
        return await self.repo.list_forms()

    async def get_form(self, form_id: str) -> Optional[Dict[str, Any]]:
        return await self.repo.get_form(form_id)

    async def get_form_or_raise(self, form_id: str) -> Dict[str, Any]:
        """
        Валидирует form_id и возвращает документ формы из БД.
        При некорректном form_id — 400, при отсутствии формы — 404.
        """
        validate_form_id(form_id)
        form = await self.repo.get_form(form_id)
        if not form:
            raise HTTPException(status_code=404, detail=f"Форма '{form_id}' не найдена")
        return form

    async def create_form(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        new_id = str(uuid4())
        doc = {
            "id": new_id,
            "name": payload.get("name"),
            "spravochno_keywords": payload.get("spravochno_keywords") or [],
            "skip_sheets": payload.get("skip_sheets") or [],
            "created_at": datetime.utcnow().isoformat() + "Z"
        }
        return await self.repo.create_form(doc)

    async def update_form(self, form_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        update_doc = {}
        if "name" in payload:
            update_doc["name"] = payload.get("name")
        if "spravochno_keywords" in payload:
            update_doc["spravochno_keywords"] = payload.get("spravochno_keywords") or []
        if "skip_sheets" in payload:
            update_doc["skip_sheets"] = payload.get("skip_sheets") or []
        if not update_doc:
            return await self.get_form(form_id)
        return await self.repo.update_form(form_id, update_doc)

    async def delete_form(self, form_id: str) -> bool:
        return await self.repo.delete_form(form_id)
