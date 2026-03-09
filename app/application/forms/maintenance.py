from __future__ import annotations

from typing import Set

from app.application.forms.system_forms import SYSTEM_FORMS
from app.domain.file.service import FileService
from app.domain.flat_data.service import FlatDataService
from app.domain.form.models import FormType, detect_form_type
from app.domain.form.service import FormService


class FormDeletionForbiddenError(Exception):
    def __init__(self, message: str, *, form_id: str) -> None:
        self.message = message
        self.form_id = form_id
        super().__init__(message)


_PROTECTED_TYPES: Set[FormType] = {
    detect_form_type(spec.name) for spec in SYSTEM_FORMS if detect_form_type(spec.name) != FormType.UNKNOWN
}


class FormMaintenanceService:
    """
    Сценарии жизненного цикла форм (уровень application):
    - обеспечение существования «системных» форм (по стратегиям)
    - удаление пользовательских форм с каскадной очисткой данных
    """

    def __init__(
        self,
        *,
        form_service: FormService,
        file_service: FileService,
        flat_data_service: FlatDataService,
    ) -> None:
        self._form_service = form_service
        self._file_service = file_service
        self._flat_data_service = flat_data_service

    async def ensure_system_forms_exist(self) -> None:
        for spec in SYSTEM_FORMS:
            await self._form_service.ensure_form_by_name(
                name=spec.name,
                default_requisites=spec.requisites,
            )

    async def delete_form_with_related(self, form_id: str) -> bool:
        form = await self._form_service.get_form(form_id)
        if not form:
            return False

        if form.type in _PROTECTED_TYPES:
            raise FormDeletionForbiddenError(
                message=f"Форму '{form.name}' нельзя удалить: для неё есть описанная стратегия обработки",
                form_id=form_id,
            )

        await self._flat_data_service.delete_by_form_id(form_id)
        await self._file_service.delete_by_form_id(form_id)
        return await self._form_service.delete_form(form_id)

