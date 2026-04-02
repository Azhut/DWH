"""Шаг: парсинг заголовков через domain/parsing."""
import logging
from collections import Counter

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep
from app.core.exceptions import CriticalParsingError
from app.domain.parsing import parse_headers
from app.domain.parsing.header_parsing import (
    drop_leading_horizontal_path_segments,
    strip_horizontal_leading_okei_banner,
    strip_horizontal_leading_section_banner,
)

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

    def __init__(
        self,
        horizontal_header_leading_levels_to_drop: int = 0,
        *,
        horizontal_header_strip_fk1_banner: bool = False,
    ) -> None:
        self._horizontal_header_leading_levels_to_drop = max(
            0,
            int(horizontal_header_leading_levels_to_drop),
        )
        # Флаг исторически назван «fk1_banner»; сейчас управляет только снятием сегмента с «ОКЕИ».
        self._horizontal_header_strip_okei_banner = horizontal_header_strip_fk1_banner

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        if ctx.table_structure is None:
            raise CriticalParsingError(
                f"ParseHeadersStep: структура таблицы не определена для листа '{ctx.sheet_name}'. "
                "DetectTableStructureStep должен выполняться перед ParseHeadersStep.",
                domain="parsing.steps.parse_headers",
                meta={"sheet_name": ctx.sheet_name},
            )

        if ctx.processed_dataframe is None:
            raise CriticalParsingError(
                f"ParseHeadersStep: отсутствует подготовленный DataFrame для листа '{ctx.sheet_name}'. "
                "NormalizeDataFrameStep должен выполняться до DetectTableStructureStep/ParseHeadersStep.",
                domain="parsing.steps.parse_headers",
                meta={"sheet_name": ctx.sheet_name},
            )

        df = ctx.processed_dataframe

        # Возможность принудительно переключить режим построения вертикальной иерархии
        # (эвристики vs indent). Передаётся как форма-специфичный параметр.
        vertical_hierarchy_mode = (
            (ctx.form_info.requisites or {}).get("vertical_hierarchy_mode")  # type: ignore[union-attr]
            or "auto"
        )

        try:
            result = parse_headers(
                df,
                ctx.table_structure,
                sheet_name=ctx.sheet_model.sheet_fullname,
                workbook_source=ctx.workbook_source,
                vertical_hierarchy_mode=vertical_hierarchy_mode,
                form_requisites=ctx.form_info.requisites,
            )
        except Exception as e:
            raise CriticalParsingError(
                f"Ошибка парсинга заголовков листа '{ctx.sheet_name}': {e}",
                domain="parsing.steps.parse_headers",
                meta={"sheet_name": ctx.sheet_name, "error": str(e)},
                show_traceback=True,
            ) from e

        horizontal = result.horizontal
        horizontal = [strip_horizontal_leading_section_banner(h) for h in horizontal]
        if self._horizontal_header_strip_okei_banner:
            horizontal = [strip_horizontal_leading_okei_banner(h) for h in horizontal]
        if self._horizontal_header_leading_levels_to_drop:
            horizontal = [
                drop_leading_horizontal_path_segments(
                    h,
                    self._horizontal_header_leading_levels_to_drop,
                )
                for h in horizontal
            ]

        _check_duplicate_headers(horizontal, ctx.sheet_name)

        ctx.sheet_model.horizontal_headers = horizontal
        ctx.sheet_model.vertical_headers = result.vertical

        logger.debug(
            "Заголовки для листа '%s': горизонтальных=%d, вертикальных=%d",
            ctx.sheet_name,
            len(horizontal),
            len(result.vertical),
        )

