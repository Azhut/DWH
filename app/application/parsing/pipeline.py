"""Оркестратор parsing pipeline: запускает шаги для парсинга одного листа."""
import logging
from typing import List, Optional

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps import ParsingPipelineStep

logger = logging.getLogger(__name__)


class ParsingPipelineRunner:
    """Запускает последовательность шагов для парсинга одного листа."""

    def __init__(self, steps: List[ParsingPipelineStep]):
        self.steps = steps

    async def run_for_sheet(self, ctx: ParsingPipelineContext) -> None:
        """
        Выполняет все шаги pipeline для парсинга листа.
        Ошибки собираются в ctx.errors, но не прерывают выполнение (если шаг не критичный).
        """
        logger.debug(
            "Начало parsing pipeline для листа '%s' (форма: %s)",
            ctx.sheet_name,
            ctx.form_info.type.value,
        )

        for step in self.steps:
            if ctx.failed:
                logger.warning(
                    "Parsing pipeline прерван для листа '%s' из-за критической ошибки",
                    ctx.sheet_name,
                )
                break

            try:
                await step.execute(ctx)
            except Exception as e:
                error_msg = f"Ошибка в шаге {step.__class__.__name__}: {str(e)}"
                logger.exception("Ошибка выполнения шага %s для листа '%s'", step.__class__.__name__, ctx.sheet_name)
                ctx.add_error(error_msg)
                # Решаем, продолжать ли выполнение после ошибки
                # Для критических шагов можно прервать выполнение
                if self._is_critical_step(step):
                    break

        if ctx.errors:
            logger.warning(
                "Parsing pipeline завершён с ошибками для листа '%s': %s",
                ctx.sheet_name,
                "; ".join(ctx.errors),
            )
        else:
            logger.debug("Parsing pipeline успешно завершён для листа '%s'", ctx.sheet_name)

    def _is_critical_step(self, step: ParsingPipelineStep) -> bool:
        """Определяет, является ли шаг критическим (прерывает выполнение при ошибке)."""
        # По умолчанию все шаги некритичные, но можно переопределить
        return False


def build_parsing_pipeline(
    form_type: str,
    sheet_name: str,
    auto_detect_structure: bool = False,
    header_row_range: Optional[tuple] = None,
    vertical_header_col: int = 0,
    start_data_row: Optional[int] = None,
) -> ParsingPipelineRunner:
    """
    Собирает pipeline для парсинга листа с заданными параметрами.

    Args:
        form_type: Тип формы (1ФК, 5ФК и т.д.)
        sheet_name: Название листа
        auto_detect_structure: Автоматически определять структуру таблицы (для 5ФК)
        header_row_range: Диапазон строк заголовков (start, end) для 1ФК
        vertical_header_col: Колонка с вертикальными заголовками
        start_data_row: Строка начала данных (для 1ФК)

    Returns:
        ParsingPipelineRunner с настроенными шагами
    """
    from app.application.parsing.steps import (
        DetectTableStructureStep,
        ProcessNotesStep,
        ParseHeadersStep,
        ExtractDataStep,
        GenerateFlatDataStep,
    )

    steps: List[ParsingPipelineStep] = []

    # Шаг 1: Определение структуры таблицы (для 5ФК) или использование фиксированных параметров (для 1ФК)
    if auto_detect_structure:
        steps.append(DetectTableStructureStep())
    else:
        # Для 1ФК используем фиксированные параметры из конфигурации
        if header_row_range and start_data_row is not None:
            steps.append(
                DetectTableStructureStep(
                    fixed_header_range=header_row_range,
                    fixed_vertical_col=vertical_header_col,
                    fixed_data_start_row=start_data_row,
                )
            )
        else:
            # Fallback: используем значения по умолчанию
            steps.append(
                DetectTableStructureStep(
                    fixed_header_range=(1, 4),
                    fixed_vertical_col=0,
                    fixed_data_start_row=5,
                )
            )

    # Шаг 2: Обработка примечаний
    steps.append(ProcessNotesStep())

    # Шаг 3: Парсинг заголовков
    steps.append(ParseHeadersStep())

    # Шаг 4: Извлечение данных
    steps.append(ExtractDataStep())

    # Шаг 5: Генерация flat_data
    steps.append(GenerateFlatDataStep())

    return ParsingPipelineRunner(steps=steps)
