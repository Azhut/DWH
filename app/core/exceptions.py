from typing import Any, Dict, Optional
from fastapi import HTTPException
from app.core.logger import log_app_error


class AppError(Exception):
    """
    Базовый контракт ошибки приложения.

    """

    def __init__(
        self,
        message: str,
        *,
        level: str = "error",                 # debug | info | warning | error | critical
        domain: str = "general",              # file | flat_data | form | sheet | upload | parsing | etc
        http_status: Optional[int] = None,
        meta: Optional[Dict[str, Any]] = None # произвольный технический контекст
    ):
        self.message = message
        self.level = level
        self.domain = domain
        self.http_status = http_status
        self.meta = meta or {}

        super().__init__(message)


class UploadError(AppError):
    """
    Ошибка upload-endpoint и его pipeline.

    stop_pipeline используется ТОЛЬКО upload/parsing pipeline.
    В других эндпоинтах этот класс не применяется.
    """

    def __init__(
        self,
        message: str,
        *,
        level: str = "error",
        domain: str = "upload",
        stop_pipeline: bool = True,
        http_status: Optional[int] = None,
        meta: Optional[Dict[str, Any]] = None
    ):
        self.stop_pipeline = stop_pipeline
        super().__init__(
            message,
            level=level,
            domain=domain,
            http_status=http_status,
            meta=meta,
        )


def to_http_exception(error: AppError) -> HTTPException:
    """
    Преобразование AppError в HTTPException.
    Используется на границе API.
    """

    if error.http_status is not None:
        status_code = error.http_status
    else:
        if error.level in ("error", "critical"):
            status_code = 500
        else:
            status_code = 400

    return HTTPException(
        status_code=status_code,
        detail={
            "message": error.message,
            "domain": error.domain,
            "level": error.level,
        },
    )


# app/core/exceptions.py




def log_and_raise_http(error: AppError) -> None:
    """
    Логирует ошибку и выбрасывает соответствующий HTTPException.
    """

    log_app_error(error, exc_info=True)

    status_code = error.http_status or 500
    raise HTTPException(
        status_code=status_code,
        detail=error.message,
    )