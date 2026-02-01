"""
Сервис по работе с формами для upload: валидация, загрузка формы, реквизиты (в т.ч. skip_sheets).
Вся логика, связанная с формами на эндпоинте upload, делегируется сюда.
Реквизиты формы (skip_sheets и др.) — часть сущности Form, не контекста pipeline.
"""
from app.data.services.forms_service import FormsService
from app.models.form_model import FormInfo


class FormService:
    """Сервис работы с формами: валидация form_id, получение FormInfo для upload."""

    def __init__(self, forms_service: FormsService):
        self._forms_service = forms_service

    async def get_form_info_or_raise(self, form_id: str) -> FormInfo:
        """
        Валидирует form_id и возвращает FormInfo из БД.
        FormInfo содержит requisites (в т.ч. skip_sheets) — реквизиты сущности Form.
        При некорректном form_id — 400, при отсутствии формы — 404.
        """
        form_doc = await self._forms_service.get_form_or_raise(form_id)
        return FormInfo.from_mongo_doc(form_doc)
