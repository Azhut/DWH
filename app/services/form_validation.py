"""
Единое место валидации form_id для всех эндпоинтов.
Используется в upload, filters и везде, где требуется обязательный form_id.
"""
from typing import Any

from fastapi import HTTPException


def validate_form_id(form_id: Any) -> str:
    """
    Проверяет, что form_id задан и является непустой строкой.
    Возвращает form_id. При ошибке — HTTPException 400.
    """
    if form_id is None or (isinstance(form_id, str) and not form_id.strip()):
        raise HTTPException(
            status_code=400,
            detail="отсутствует обязательный параметр form_id",
        )
    if not isinstance(form_id, str):
        raise HTTPException(
            status_code=400,
            detail="form_id должен быть строкой",
        )
    return form_id.strip()
