import logging
from typing import Any, Dict, Optional

from fastapi import HTTPException

from app.core.logger import logger


class FormValidationError(Exception):
    """Ошибка валидации формы (уровень домена)."""

    def __init__(self, message: str, form_id: Optional[str] = None):
        self.message = message
        self.form_id = form_id
        super().__init__(message)


class FileValidationError(Exception):
    """Ошибка валидации файла или его имени (уровень домена)."""

    def __init__(self, message: str, filename: Optional[str] = None):
        self.message = message
        self.filename = filename
        super().__init__(message)


class AppError(Exception):
    """Базовая ошибка приложения."""

    def __init__(
        self,
        message: str,
        *,
        level: str = "error",  # debug | info | warning | error | critical
        domain: str = "general",  # file | flat_data | form | sheet | upload | parsing | ...
        http_status: Optional[int] = None,
        meta: Optional[Dict[str, Any]] = None,  # произвольный технический контекст
        show_traceback: bool = False,
    ):
        self.message = message
        self.level = level
        self.domain = domain
        self.http_status = http_status
        self.meta = meta or {}
        self.show_traceback = show_traceback
        super().__init__(message)


# ============= Ошибки уровня запроса =============


class RequestValidationError(AppError):
    """
    Ошибка валидации входного запроса.

    Прерывает выполнение handler и обычно маппится в HTTP 400/404/500.
    Примеры: отсутствует form_id, пустой список файлов, несуществующая форма,
    проблемы с БД на этапе проверки.
    """

    def __init__(
        self,
        message: str,
        *,
        http_status: int = 400,
        domain: str = "upload.request",
        meta: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            level="error",
            domain=domain,
            http_status=http_status,
            meta=meta,
        )


# ============= Ошибки загрузки =============


class UploadError(AppError):
    """Базовая ошибка сценария загрузки файла."""


class CriticalUploadError(UploadError):
    """
    Критическая ошибка загрузки.

    Останавливает обработку файла и инициирует rollback.
    Примеры: ошибки валидации, конфликт уникальности, невалидный формат,
    ошибка сохранения.
    """

    def __init__(
        self,
        message: str,
        *,
        domain: str = "upload",
        http_status: Optional[int] = None,
        meta: Optional[Dict[str, Any]] = None,
        show_traceback: bool = False,
    ):
        super().__init__(
            message=message,
            level="error",
            domain=domain,
            http_status=http_status,
            meta=meta,
            show_traceback=show_traceback,
        )


class NonCriticalUploadError(UploadError):
    """
    Некритическая ошибка загрузки.

    Логируется и позволяет пайплайну продолжить обработку.
    Используется для диагностических предупреждений.
    """

    def __init__(
        self,
        message: str,
        *,
        domain: str = "upload",
        meta: Optional[Dict[str, Any]] = None,
        show_traceback: bool = False,
    ):
        super().__init__(
            message=message,
            level="warning",
            domain=domain,
            http_status=None,
            meta=meta,
            show_traceback=show_traceback,
        )


class DuplicateFileError(UploadError):
    """
    Файл уже был успешно загружен (дубликат).

    Не считается критической ошибкой: stub не создаётся,
    клиент получает информативный ответ.
    """

    def __init__(
        self,
        message: str,
        *,
        domain: str = "upload.duplicate",
        http_status: int = 409,
        meta: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            level="warning",  # это ошибка валидации/конфликта, а не авария
            domain=domain,
            http_status=http_status,
            meta=meta,
        )


# ============= Ошибки парсинга =============


class ParsingError(AppError):
    """Базовая ошибка парсинга листа."""


class CriticalParsingError(ParsingError):
    """
    Критическая ошибка парсинга.

    Останавливает обработку листа и всего файла.
    Примеры: сломанная структура таблицы, заголовки, невалидные данные.
    """

    def __init__(
        self,
        message: str,
        *,
        domain: str = "parsing",
        http_status: Optional[int] = None,
        meta: Optional[Dict[str, Any]] = None,
        show_traceback: bool = False,
    ):
        super().__init__(
            message=message,
            level="error",
            domain=domain,
            http_status=http_status,
            meta=meta,
            show_traceback=show_traceback,
        )


class NonCriticalParsingError(ParsingError):
    """
    Некритическая ошибка парсинга.

    Логируется и позволяет продолжить обработку.
    Используется для примечаний и предупреждений.
    """

    def __init__(
        self,
        message: str,
        *,
        domain: str = "parsing",
        meta: Optional[Dict[str, Any]] = None,
        show_traceback: bool = False,
    ):
        super().__init__(
            message=message,
            level="warning",
            domain=domain,
            http_status=None,
            meta=meta,
            show_traceback=show_traceback,
        )


def to_http_exception(error: AppError) -> HTTPException:
    """Преобразует AppError в HTTPException."""

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


def log_app_error(error: AppError, *, exc_info: Optional[bool] = None) -> None:
    """
    Единая точка логирования ошибок приложения.

    Передаёт все метаданные из error.meta в extra логгера.
    """

    level_name = error.level.upper()
    log_level = getattr(logging, level_name, logging.ERROR)
    extra = {"domain": error.domain, **error.meta}
    if exc_info is None:
        exc_info = error.show_traceback
    logger.log(log_level, error.message, exc_info=exc_info, extra=extra)


def log_and_raise_http(error: AppError) -> None:
    """
    Логирует ошибку и выбрасывает соответствующий HTTPException.

    Используется для ошибок уровня запроса (RequestValidationError).
    """

    log_app_error(error, exc_info=True)
    raise to_http_exception(error)
