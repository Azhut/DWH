from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)

def log_and_raise_http(status_code: int, detail: str, exc: Exception = None):
    if exc:
        logger.error(f"{detail}: {str(exc)}", exc_info=True)
    else:
        logger.error(detail)
    raise HTTPException(status_code=status_code, detail=detail)