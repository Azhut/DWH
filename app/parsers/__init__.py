"""
Парсеры: оставлены только компоненты, используемые domain/parsing.
- header_fixer: нормализация заголовков (морфология, ручной маппинг).
- notes_processor: обработка примечаний «Справочно» (только для 1ФК).
"""
from app.parsers.header_fixer import fix_header, finalize_header_fixing
from app.parsers.notes_processor import NotesProcessor, _SERVICE_EMPTY

__all__ = ["fix_header", "finalize_header_fixing", "NotesProcessor", "_SERVICE_EMPTY"]
