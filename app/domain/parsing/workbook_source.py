"""Снимок входных данных рабочей книги для доменного парсинга (без зависимости от upload-контекста)."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ParsingWorkbookSource:
    """
    Неизменяемый снимок файла на границе upload → parsing.

    Заполняется один раз в ProcessSheetsStep из UploadPipelineContext
    (file_content + file_info.extension), чтобы не дублировать по отдельности
    поля в ParsingPipelineContext и не рисковать рассинхроном.
    """

    content: bytes
    extension: str
