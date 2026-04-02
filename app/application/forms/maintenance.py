from __future__ import annotations

from typing import Set

from app.application.forms.system_forms import SYSTEM_FORMS
from app.domain.file.service import FileService
from app.domain.flat_data.service import FlatDataService
from app.domain.form.models import FormType, detect_form_type
from app.domain.form.service import FormService
from app.domain.log.service import LogService


class FormDeletionForbiddenError(Exception):
    def __init__(self, message: str, *, form_id: str) -> None:
        self.message = message
        self.form_id = form_id
        super().__init__(message)


_PROTECTED_TYPES: Set[FormType] = {
    detect_form_type(spec.name) for spec in SYSTEM_FORMS if detect_form_type(spec.name) != FormType.UNKNOWN
}


class _NullLogService:
    """No-op логирование для сценариев/тестов, где LogService не передают."""

    async def save_log(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        return None


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
        log_service: LogService | None = None,
    ) -> None:
        self._form_service = form_service
        self._file_service = file_service
        self._flat_data_service = flat_data_service
        self._log_service = log_service or _NullLogService()

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

        files_deleted = await self._file_service.delete_by_form_id(form_id)
        flat_deleted = await self._flat_data_service.delete_by_form_id(form_id)
        result = await self._form_service.delete_form(form_id)

        if result:
            await self._log_service.save_log(
                scenario="deletion",
                message=f"Удалена форма {form_id}",
                level="info",
                meta={
                    "deleted_type": "form",
                    "deleted_id": form_id,
                    "cascade": {
                        "files_deleted": files_deleted,
                        "flat_deleted": flat_deleted,
                    },
                },
            )

        return result

