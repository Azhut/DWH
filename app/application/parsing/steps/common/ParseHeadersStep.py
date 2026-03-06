"""Шаг: парсинг заголовков через domain/parsing."""
import logging
from collections import Counter

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep
from app.core.exceptions import CriticalParsingError
from app.domain.parsing import parse_headers

logger = logging.getLogger(__name__)


def _check_duplicate_headers(headers: list[str], sheet_name: str) -> None:
    """
    Проверяет горизонтальные заголовки на дубликаты.
    При обнаружении — бросает CriticalParsingError с подробной информацией.

    Args:
        headers: Список горизонтальных заголовков
        sheet_name: Имя листа для логирования

    Raises:
        CriticalParsingError: Если найдены дубликаты
    """
    if not headers:
        return

    # Находим все дубликаты
    header_counts = Counter(headers)
    duplicates = {
        header: count
        for header, count in header_counts.items()
        if count > 1
    }

    if duplicates:
        # Собираем подробную информацию о дубликатах
        duplicate_details = []
        for header, count in duplicates.items():
            positions = [i for i, h in enumerate(headers) if h == header]
            duplicate_details.append(
                f"'{header}' (повторяется {count} раз, позиции: {positions})"
            )

        error_msg = (
            f"Обнаружены дубликаты горизонтальных заголовков на листе '{sheet_name}'. "
            f"Это указывает на артефакты форматирования Excel. "
            f"Дубликаты: {'; '.join(duplicate_details)}"
        )

        raise CriticalParsingError(
            message=error_msg,
            domain="parsing.steps.parse_headers",
            meta={
                "sheet_name": sheet_name,
                "duplicate_headers": list(duplicates.keys()),
                "duplicate_details": duplicate_details,
                "total_headers": len(headers),
                "unique_headers": len(set(headers)),
            },
            show_traceback=False,
        )


class ParseHeadersStep(BaseParsingStep):
    """
    Парсит горизонтальные и вертикальные заголовки по структуре из контекста.

    Требует: ctx.table_structure — должен быть заполнен DetectTableStructureStep.
    Записывает: ctx.sheet_model.horizontal_headers, ctx.sheet_model.vertical_headers.
    """

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        if ctx.table_structure is None:
            raise CriticalParsingError(
                f"ParseHeadersStep: структура таблицы не определена для листа '{ctx.sheet_name}'. "
                "DetectTableStructureStep должен выполняться перед ParseHeadersStep.",
                domain="parsing.steps.parse_headers",
                meta={"sheet_name": ctx.sheet_name},
            )

        df = ctx.processed_dataframe if ctx.processed_dataframe is not None else ctx.raw_dataframe

        try:
            result = parse_headers(df, ctx.table_structure)
        except Exception as e:
            raise CriticalParsingError(
                f"Ошибка парсинга заголовков листа '{ctx.sheet_name}': {e}",
                domain="parsing.steps.parse_headers",
                meta={"sheet_name": ctx.sheet_name, "error": str(e)},
                show_traceback=True,
            ) from e

        _check_duplicate_headers(result.horizontal, ctx.sheet_name)

        # Финальные результаты — в sheet_model (единственный источник правды)
        ctx.sheet_model.horizontal_headers = result.horizontal
        ctx.sheet_model.vertical_headers = result.vertical

        logger.debug(
            "Заголовки для листа '%s': горизонтальных=%d, вертикальных=%d",
            ctx.sheet_name,
            len(result.horizontal),
            len(result.vertical),
        )

