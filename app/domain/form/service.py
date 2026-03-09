"""Сервис агрегата Form: валидация form_id, получение формы, CRUD."""
from typing import Any, Dict, List, Optional


from uuid import uuid4
from datetime import datetime

from app.core.exceptions import FormValidationError
from app.domain.form.models import FormInfo
from app.domain.form.repository import FormRepository


def validate_form_id(form_id: Any) -> str:
    """
    Проверяет, что form_id задан и является непустой строкой.
    Единое место для всех эндпоинтов. Валидирует только семантику форм, не ходит в БД
    """
    if form_id is None or (isinstance(form_id, str) and not form_id.strip()):
        raise FormValidationError(
            message="отсутствует обязательный параметр form_id",
            form_id=form_id
        )
    if not isinstance(form_id, str):
        raise FormValidationError(
            message="form_id должен быть строкой",
            form_id=form_id
        )
    return form_id.strip()


class FormService:
    """Вся бизнес-логика по сущности Form: валидация, получение формы, CRUD."""

    def __init__(self, repository: FormRepository):
        self._repo = repository

    async def get_form(self, form_id: str) -> Optional[FormInfo]:
        """
        Валидирует form_id, получает форму из БД и возвращает FormInfo.
        Возвращает None если форма не найдена.
        """
        form_id = validate_form_id(form_id)


        form_doc = await self._repo.get_form(form_id)
        if not form_doc:
            return None

        return FormInfo.from_mongo_doc(form_doc)

    async def get_form_or_raise(self, form_id: str) -> FormInfo:
        """
        Валидирует form_id, получает форму из БД и возвращает FormInfo.
        Выбрасывает FormValidationError если форма не найдена.
        """
        form_info = await self.get_form(form_id)
        if not form_info:
            raise FormValidationError(
            message=f"Форма '{form_id}' не найдена",
            form_id=form_id
        )
        return form_info



    async def list_forms(self) -> List[Dict[str, Any]]:
        return await self._repo.list_forms()

    async def create_form(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        new_id = str(uuid4())
        requisites = payload.get("requisites") or {}
        if not isinstance(requisites, dict):
            requisites = {}

        skip_sheets = requisites.get("skip_sheets", payload.get("skip_sheets"))
        if skip_sheets is None:
            skip_sheets = []
        if not isinstance(skip_sheets, list):
            skip_sheets = list(skip_sheets) if skip_sheets else []

        doc = {
            "id": new_id,
            "name": payload.get("name"),
            "requisites": {**requisites, "skip_sheets": skip_sheets},
            "created_at": datetime.utcnow().isoformat() + "Z",
        }
        return await self._repo.create_form(doc)

    async def ensure_form_by_name(self, *, name: str, default_requisites: Dict[str, Any]) -> Dict[str, Any]:
        """
        Гарантирует, что форма с таким именем существует.
        Если форма уже есть — дозаполняет requisites дефолтами.
        """
        existing = await self._repo.get_form_by_name_ci(name)
        if not existing:
            return await self.create_form({"name": name, "requisites": default_requisites})

        requisites = existing.get("requisites") or {}
        if not isinstance(requisites, dict):
            requisites = {}

        merged = {**(default_requisites or {}), **requisites}
        updated = await self._repo.update_form(existing["id"], {"$set": {"requisites": merged}})
        return updated or existing

    async def update_form(
        self,
        form_id: str,
        payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        update_doc: Dict[str, Any] = {}
        if "name" in payload and payload.get("name") is not None:
            update_doc["name"] = payload["name"]
        requisites = payload.get("requisites")
        if requisites is not None:
            requisites = requisites if isinstance(requisites, dict) else {}

            skip_sheets = requisites.get("skip_sheets")
            if skip_sheets is not None:
                if not isinstance(skip_sheets, list):
                    skip_sheets = list(skip_sheets) if skip_sheets else []
                requisites = {**requisites, "skip_sheets": skip_sheets}

            update_doc["requisites"] = requisites

        if not update_doc:
            return await self._repo.get_form(form_id)

        return await self._repo.update_form(form_id, {"$set": update_doc})

    async def delete_form(self, form_id: str) -> bool:
        return await self._repo.delete_form(form_id)
