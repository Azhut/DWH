from __future__ import annotations

from datetime import datetime

import pytest

from app.application.forms.maintenance import FormDeletionForbiddenError, FormMaintenanceService
from app.domain.form.service import FormService


def _banner(title: str) -> None:
    print("\n" + "=" * 88)
    print(f"TEST | {title}")
    print("=" * 88)


class InMemoryFormRepository:
    def __init__(self) -> None:
        self._docs: dict[str, dict] = {}

    async def list_forms(self):
        return list(self._docs.values())

    async def get_form(self, form_id: str):
        return self._docs.get(form_id)

    async def get_form_by_name_ci(self, name: str):
        name_l = name.lower()
        for d in self._docs.values():
            if isinstance(d.get("name"), str) and d["name"].lower() == name_l:
                return dict(d)
        return None

    async def create_form(self, form_doc: dict):
        self._docs[form_doc["id"]] = dict(form_doc)
        return dict(form_doc)

    async def update_form(self, form_id: str, update_doc: dict):
        existing = self._docs.get(form_id)
        if not existing:
            return None

        if any(k.startswith("$") for k in update_doc.keys()):
            set_doc = update_doc.get("$set") or {}
            unset_doc = update_doc.get("$unset") or {}
        else:
            set_doc = update_doc
            unset_doc = {}

        existing.update(set_doc)
        for k in unset_doc.keys():
            existing.pop(k, None)

        self._docs[form_id] = existing
        return dict(existing)

    async def delete_form(self, form_id: str) -> bool:
        return self._docs.pop(form_id, None) is not None


class StubFileService:
    def __init__(self) -> None:
        self.delete_by_form_calls: list[str] = []

    async def delete_by_form_id(self, form_id: str) -> int:
        self.delete_by_form_calls.append(form_id)
        return 10


class StubFlatDataService:
    def __init__(self) -> None:
        self.delete_by_form_calls: list[str] = []

    async def delete_by_form_id(self, form_id: str) -> int:
        self.delete_by_form_calls.append(form_id)
        return 100


@pytest.mark.asyncio
async def test_create_form_stores_skip_sheets_inside_requisites_only() -> None:
    _banner("create_form: skip_sheets stored only inside requisites")
    repo = InMemoryFormRepository()
    service = FormService(repo)

    doc = await service.create_form({"name": "Custom", "requisites": {"skip_sheets": [0]}})
    print("created.form_id :", doc["id"])
    print("created.name    :", doc["name"])
    print("created.reqs    :", doc["requisites"])
    assert doc["requisites"]["skip_sheets"] == [0]
    assert "skip_sheets" not in doc


@pytest.mark.asyncio
async def test_update_form_unsets_legacy_skip_sheets_field() -> None:
    _banner("update_form: updates requisites.skip_sheets (no root skip_sheets created)")
    repo = InMemoryFormRepository()
    service = FormService(repo)

    form_id = "f1"
    await repo.create_form(
        {
            "id": form_id,
            "name": "Any",
            "requisites": {},
            "created_at": datetime.now().isoformat() + "Z",
        }
    )

    updated = await service.update_form(form_id, {"requisites": {"skip_sheets": [2]}})
    print("updated.form_id :", form_id)
    print("updated.reqs    :", updated["requisites"] if updated else None)
    print("updated.keys    :", sorted(list(updated.keys())) if updated else None)
    assert updated is not None
    assert updated["requisites"]["skip_sheets"] == [2]
    assert "skip_sheets" not in updated


@pytest.mark.asyncio
async def test_ensure_form_by_name_merges_defaults_and_unsets_legacy() -> None:
    _banner("ensure_form_by_name: creates missing form with default requisites")
    repo = InMemoryFormRepository()
    service = FormService(repo)

    ensured = await service.ensure_form_by_name(name="1ФК", default_requisites={"skip_sheets": [0]})
    print("ensured.id      :", ensured["id"])
    print("ensured.name    :", ensured["name"])
    print("ensured.reqs    :", ensured["requisites"])
    assert ensured["name"] == "1ФК"
    assert ensured["requisites"]["skip_sheets"] == [0]
    assert "skip_sheets" not in ensured


@pytest.mark.asyncio
async def test_delete_form_with_related_forbidden_for_system_form_types() -> None:
    _banner("delete_form_with_related: forbidden for protected (system) forms")
    repo = InMemoryFormRepository()
    form_service = FormService(repo)
    file_service = StubFileService()
    flat_service = StubFlatDataService()
    maintenance = FormMaintenanceService(
        form_service=form_service,
        file_service=file_service,
        flat_data_service=flat_service,
    )

    await repo.create_form(
        {
            "id": "sys1",
            "name": "1ФК",
            "requisites": {"skip_sheets": [0]},
            "created_at": datetime.now().isoformat() + "Z",
        }
    )

    with pytest.raises(FormDeletionForbiddenError):
        await maintenance.delete_form_with_related("sys1")

    print("cascade.files.delete_calls   :", file_service.delete_by_form_calls)
    print("cascade.flat_data.delete_calls:", flat_service.delete_by_form_calls)
    assert file_service.delete_by_form_calls == []
    assert flat_service.delete_by_form_calls == []


@pytest.mark.asyncio
async def test_delete_form_with_related_deletes_related_then_form() -> None:
    _banner("delete_form_with_related: cascades FlatData + Files, then deletes Form")
    repo = InMemoryFormRepository()
    form_service = FormService(repo)
    file_service = StubFileService()
    flat_service = StubFlatDataService()
    maintenance = FormMaintenanceService(
        form_service=form_service,
        file_service=file_service,
        flat_data_service=flat_service,
    )

    await repo.create_form(
        {
            "id": "user1",
            "name": "My user form",
            "requisites": {"skip_sheets": []},
            "created_at": datetime.now().isoformat() + "Z",
        }
    )

    deleted = await maintenance.delete_form_with_related("user1")
    print("deleted         :", deleted)
    print("cascade.files   :", file_service.delete_by_form_calls)
    print("cascade.flat_data:", flat_service.delete_by_form_calls)
    assert deleted is True
    assert file_service.delete_by_form_calls == ["user1"]
    assert flat_service.delete_by_form_calls == ["user1"]
    assert await repo.get_form("user1") is None

