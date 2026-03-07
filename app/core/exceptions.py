# app/core/exceptions.py
import logging
from typing import Any, Dict, Optional
from fastapi import HTTPException
from app.core.logger import logger

class FormValidationError(Exception):
    """РћС€РёР±РєР° РІР°Р»РёРґР°С†РёРё С„РѕСЂРјС‹ (СѓСЂРѕРІРµРЅСЊ РґРѕРјРµРЅР°)."""
    def __init__(self, message: str, form_id: Optional[str] = None):
        self.message = message
        self.form_id = form_id
        super().__init__(message)

class FileValidationError(Exception):
    """?????? ????????? ?????/?????????? ????? (??????? ??????)."""
    def __init__(self, message: str, filename: Optional[str] = None):
        self.message = message
        self.filename = filename
        super().__init__(message)

class AppError(Exception):
    """
    Р‘Р°Р·РѕРІР°СЏ РѕС€РёР±РєР° РїСЂРёР»РѕР¶РµРЅРёСЏ.
    """

    def __init__(
        self,
        message: str,
        *,
        level: str = "error",                 # debug | info | warning | error | critical
        domain: str = "general",              # file | flat_data | form | sheet | upload | parsing | etc
        http_status: Optional[int] = None,
        meta: Optional[Dict[str, Any]] = None, # РїСЂРѕРёР·РІРѕР»СЊРЅС‹Р№ С‚РµС…РЅРёС‡РµСЃРєРёР№ РєРѕРЅС‚РµРєСЃС‚
        show_traceback: bool = False
    ):
        self.message = message
        self.level = level
        self.domain = domain
        self.http_status = http_status
        self.meta = meta or {}
        self.show_traceback = show_traceback
        super().__init__(message)


# ============= Request Level Errors =============

class RequestValidationError(AppError):
    """
    РћС€РёР±РєР° РІР°Р»РёРґР°С†РёРё Р·Р°РїСЂРѕСЃР° вЂ” РїСЂРµСЂС‹РІР°РµС‚ РІРµСЃСЊ handler, РІРѕР·РІСЂР°С‰Р°РµС‚ HTTP 400/404/500.
    РџСЂРёРјРµСЂС‹: РѕС‚СЃСѓС‚СЃС‚РІРёРµ form_id, РїСѓСЃС‚РѕР№ СЃРїРёСЃРѕРє С„Р°Р№Р»РѕРІ, РЅРµСЃСѓС‰РµСЃС‚РІСѓСЋС‰Р°СЏ С„РѕСЂРјР°, РїСЂРѕР±Р»РµРјС‹ СЃ Р‘Р”.
    """
    def __init__(
        self,
        message: str,
        *,
        http_status: int = 400,
        domain: str = "upload.request",
        meta: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            message=message,
            level="error",
            domain=domain,
            http_status=http_status,
            meta=meta
        )


# ============= Upload Errors =============

class UploadError(AppError):
    """Р‘Р°Р·РѕРІР°СЏ РѕС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё С„Р°Р№Р»Р°."""
    pass


class CriticalUploadError(UploadError):
    """
    РљСЂРёС‚РёС‡РµСЃРєР°СЏ РѕС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё вЂ” РѕСЃС‚Р°РЅР°РІР»РёРІР°РµС‚ РїР°Р№РїР»Р°Р№РЅ С„Р°Р№Р»Р°, РґРµР»Р°РµС‚ rollback.
    РџСЂРёРјРµСЂС‹: РІР°Р»РёРґР°С†РёСЏ, СѓРЅРёРєР°Р»СЊРЅРѕСЃС‚СЊ, РѕС‚СЃСѓС‚СЃС‚РІРёРµ С„РѕСЂРјС‹, РѕС€РёР±РєР° СЃРѕС…СЂР°РЅРµРЅРёСЏ.
    """
    def __init__(
        self,
        message: str,
        *,
        domain: str = "upload",
        http_status: Optional[int] = None,
        meta: Optional[Dict[str, Any]] = None,
        show_traceback: bool = False
    ):
        super().__init__(
            message=message,
            level="error",
            domain=domain,
            http_status=http_status,
            meta=meta,
            show_traceback=show_traceback
        )


class NonCriticalUploadError(UploadError):
    """
    РќРµРєСЂРёС‚РёС‡РµСЃРєР°СЏ РѕС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё вЂ” Р»РѕРіРёСЂСѓРµС‚СЃСЏ РІ DEBUG СЂРµР¶РёРјРµ, РїР°Р№РїР»Р°Р№РЅ РїСЂРѕРґРѕР»Р¶Р°РµС‚ СЂР°Р±РѕС‚Сѓ.
    РСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РґР»СЏ РѕС‚Р»Р°РґРєРё. РџСЂРёРјРµСЂС‹: РїСЂРµРґСѓРїСЂРµР¶РґРµРЅРёСЏ Рѕ РєР°С‡РµСЃС‚РІРµ РґР°РЅРЅС‹С….
    """
    def __init__(
        self,
        message: str,
        *,
        domain: str = "upload",
        meta: Optional[Dict[str, Any]] = None,
        show_traceback: bool = False
    ):
        super().__init__(
            message=message,
            level="warning",
            domain=domain,
            http_status=None,
            meta=meta,
            show_traceback=show_traceback
        )
class DuplicateFileError(UploadError):
    """
    Р¤Р°Р№Р» СѓР¶Рµ СѓСЃРїРµС€РЅРѕ Р·Р°РіСЂСѓР¶РµРЅ (РґСѓР±Р»РёРєР°С‚).
    РќРµ СЏРІР»СЏРµС‚СЃСЏ РєСЂРёС‚РёС‡РµСЃРєРѕР№ РѕС€РёР±РєРѕР№ вЂ” stub РЅРµ СЃРѕР·РґР°С‘С‚СЃСЏ,
    РїСЂРѕСЃС‚Рѕ РІРѕР·РІСЂР°С‰Р°РµРј РѕС€РёР±РєСѓ РІ РѕС‚РІРµС‚Рµ РєР»РёРµРЅС‚Сѓ.
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
            level="warning",  # РќРµ error, С‚Р°Рє РєР°Рє СЌС‚Рѕ РІР°Р»РёРґР°С†РёСЏ, Р° РЅРµ СЃР±РѕР№
            domain=domain,
            http_status=http_status,
            meta=meta,
        )

# ============= Parsing Errors =============

class ParsingError(AppError):
    """Р‘Р°Р·РѕРІР°СЏ РѕС€РёР±РєР° РїР°СЂСЃРёРЅРіР° Р»РёСЃС‚Р°."""
    pass


class CriticalParsingError(ParsingError):
    """
    РљСЂРёС‚РёС‡РµСЃРєР°СЏ РѕС€РёР±РєР° РїР°СЂСЃРёРЅРіР° вЂ” РѕСЃС‚Р°РЅР°РІР»РёРІР°РµС‚ РѕР±СЂР°Р±РѕС‚РєСѓ Р»РёСЃС‚Р° Рё РІСЃРµРіРѕ С„Р°Р№Р»Р°.
    РџСЂРёРјРµСЂС‹: СЃС‚СЂСѓРєС‚СѓСЂР° С‚Р°Р±Р»РёС†С‹, Р·Р°РіРѕР»РѕРІРєРё, РЅРµРІР°Р»РёРґРЅС‹Рµ РґР°РЅРЅС‹Рµ.
    """
    def __init__(
        self,
        message: str,
        *,
        domain: str = "parsing",
        http_status: Optional[int] = None,
        meta: Optional[Dict[str, Any]] = None,
        show_traceback: bool = False
    ):
        super().__init__(
            message=message,
            level="error",
            domain=domain,
            http_status=http_status,
            meta=meta,
            show_traceback = show_traceback
        )


class NonCriticalParsingError(ParsingError):
    """
    РќРµРєСЂРёС‚РёС‡РµСЃРєР°СЏ РѕС€РёР±РєР° РїР°СЂСЃРёРЅРіР° вЂ” Р»РѕРіРёСЂСѓРµС‚СЃСЏ РІ DEBUG СЂРµР¶РёРјРµ, РїСЂРѕРґРѕР»Р¶Р°РµРј РѕР±СЂР°Р±РѕС‚РєСѓ.
    РСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РґР»СЏ РѕС‚Р»Р°РґРєРё. РџСЂРёРјРµСЂС‹: РїСЂРёРјРµС‡Р°РЅРёСЏ, РѕРєСЂСѓРіР»РµРЅРёРµ, РїСЂРµРґСѓРїСЂРµР¶РґРµРЅРёСЏ.
    """
    def __init__(
        self,
        message: str,
        *,
        domain: str = "parsing",
        meta: Optional[Dict[str, Any]] = None,
        show_traceback: bool = False
    ):
        super().__init__(
            message=message,
            level="warning",
            domain=domain,
            http_status=None,
            meta=meta,
            show_traceback=show_traceback
        )


def to_http_exception(error: AppError) -> HTTPException:
    """
    РџСЂРµРѕР±СЂР°Р·РѕРІР°РЅРёРµ AppError РІ HTTPException.
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


def log_app_error(error: AppError, *, exc_info: Optional[bool] = None) -> None:
    """
    Р•РґРёРЅР°СЏ С‚РѕС‡РєР° Р»РѕРіРёСЂРѕРІР°РЅРёСЏ РѕС€РёР±РѕРє РїСЂРёР»РѕР¶РµРЅРёСЏ.
    РџРµСЂРµРґР°С‘С‚ РІСЃРµ РјРµС‚Р°РґР°РЅРЅС‹Рµ РёР· error.meta РІ extra Р»РѕРіРіРµСЂР°.
    """
    level_name = error.level.upper()
    log_level = getattr(logging, level_name, logging.ERROR)
    extra = {"domain": error.domain, **error.meta}
    if exc_info is None:
        exc_info = error.show_traceback
    logger.log(log_level, error.message, exc_info=exc_info, extra=extra)


def log_and_raise_http(error: AppError) -> None:
    """
    Р›РѕРіРёСЂСѓРµС‚ РѕС€РёР±РєСѓ Рё РІС‹Р±СЂР°СЃС‹РІР°РµС‚ СЃРѕРѕС‚РІРµС‚СЃС‚РІСѓСЋС‰РёР№ HTTPException.
    РСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РґР»СЏ РѕС€РёР±РѕРє СѓСЂРѕРІРЅСЏ Р·Р°РїСЂРѕСЃР° (RequestValidationError).
    """
    log_app_error(error, exc_info=True)
    raise to_http_exception(error)