"""Оркестратор parsing pipeline: запускает шаги для парсинга одного листа."""
import logging
from typing import List

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import ParsingPipelineStep
from app.core.exceptions import (
    CriticalParsingError,
    NonCriticalParsingError,
    log_app_error,
)

logger = logging.getLogger(__name__)


class ParsingPipelineRunner:
    """
    Запускает последовательность шагов для парсинга одного листа.

    Обработка ошибок:
    - CriticalParsingError: логируется, записывается в ctx.errors, пробрасывается
      наверх в ProcessSheetsStep — тот решает судьбу всего файла.
    - NonCriticalParsingError: логируется, записывается в ctx.warnings,
      выполнение продолжается со следующего шага.
    - Exception (непредвиденная): оборачивается в CriticalParsingError и пробрасывается.

    Никогда не поглощает CriticalParsingError — это ответственность вызывающего кода.
    """

    def __init__(self, steps: List[ParsingPipelineStep]) -> None:
        self.steps = steps

    async def run_for_sheet(self, ctx: ParsingPipelineContext) -> None:
        """
        Выполняет все шаги pipeline для парсинга листа.

        Args:
            ctx: Контекст парсинга листа.

        Raises:
            CriticalParsingError: если любой шаг завершился критической ошибкой.
                                  Вызывающий код обязан обработать это исключение.
        """
        logger.debug(
            "Начало parsing pipeline: лист='%s', форма='%s', шагов=%d",
            ctx.sheet_name,
            ctx.form_info.type.value,
            len(self.steps),
        )

        for step in self.steps:
            step_name = step.__class__.__name__

            try:
                await step.execute(ctx)

            except CriticalParsingError as e:
                # Логируем и пробрасываем — лист (и весь файл) должен упасть
                log_app_error(e)
                ctx.errors.append(e.message)
                logger.error(
                    "Критическая ошибка на шаге '%s' для листа '%s': %s",
                    step_name,
                    ctx.sheet_name,
                    e.message,
                )
                raise

            except NonCriticalParsingError as e:
                # Логируем и продолжаем — шаг не критичный
                log_app_error(e)
                ctx.add_warning(e.message)
                logger.warning(
                    "Предупреждение на шаге '%s' для листа '%s': %s",
                    step_name,
                    ctx.sheet_name,
                    e.message,
                )
                continue

            except Exception as e:
                # Непредвиденное исключение — оборачиваем и пробрасываем как критическое
                critical = CriticalParsingError(
                    message=f"Непредвиденная ошибка на шаге '{step_name}': {e}",
                    domain="parsing.pipeline",
                    meta={
                        "step": step_name,
                        "sheet_name": ctx.sheet_name,
                        "form_type": ctx.form_info.type.value,
                        "error": str(e),
                    },
                    show_traceback=True,
                )
                log_app_error(critical, exc_info=True)
                ctx.errors.append(critical.message)
                raise critical from e

        if ctx.warnings:
            logger.info(
                "Parsing pipeline завершён с предупреждениями для листа '%s': %s",
                ctx.sheet_name,
                "; ".join(ctx.warnings),
            )
        else:
            logger.debug(
                "Parsing pipeline успешно завершён для листа '%s'",
                ctx.sheet_name,
            )