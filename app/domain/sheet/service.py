"""Сервис агрегата Sheet: только I/O операции (чтение Excel, округление)."""
import logging
from typing import Dict
import pandas as pd
from io import BytesIO

from app.domain.form.models import FormInfo
from app.domain.sheet.rounding import RoundingService

logger = logging.getLogger(__name__)


class SheetService:
    """
    Сервис агрегата Sheet: отвечает только за I/O операции.
    Вся логика парсинга вынесена в parsing pipeline.

    ОТВЕТСТВЕННОСТЬ:
    - read_sheets: чтение Excel файла из байтов в памяти
    - round_dataframe: округление числовых данных (опционально по форме)

    НЕ ОТВЕТСТВЕНЕН ЗА:
    - Чтение UploadFile (делает ReadFileContentStep)
    - Парсинг структуры листов (делает parsing pipeline)
    """

    def round_dataframe(self, sheet_name: str, df: pd.DataFrame, form_info: FormInfo) -> pd.DataFrame:
        """
        Округление числовых данных. По умолчанию — RoundingService.
        Подкласс может отключить или изменить логику округления.
        """
        return RoundingService.round_dataframe(sheet_name, df)

    def read_sheets(self, file_content: bytes, filename: str) -> Dict[str, pd.DataFrame]:
        """
        Читает все листы Excel из байтов в памяти через pandas + calamine.

        Args:
            file_content: содержимое Excel файла в байтах (из ctx.file_content)
            filename: имя файла для логирования (из ctx.filename)

        Returns:
            Dict[str, pd.DataFrame]: {имя_листа: DataFrame без заголовков}

        Raises:
            ValueError: если file_content пустой
            RuntimeError: если не удалось прочитать файл через calamine

        Notes:
            - Использует engine='calamine' (поддерживает .xls, .xlsx, .xlsm)
            - header=None - заголовки парсит parsing pipeline
            - dtype=object - отключена автотипизация pandas
        """
        if not file_content:
            raise ValueError(f"Пустое содержимое файла '{filename}'")

        logger.debug(
            "Чтение Excel файла '%s': %d байт",
            filename,
            len(file_content)
        )

        # Проверка сигнатуры файла
        self._validate_excel_format(file_content, filename)

        try:
            # Читаем все листы из BytesIO
            sheets = pd.read_excel(
                BytesIO(file_content),
                sheet_name=None,  # все листы
                header=None,  # заголовки парсим сами
                engine="calamine",
                dtype=object  # отключаем автотипизацию
            )
        except Exception as e:
            # Дополнительная диагностика
            logger.error(
                "Ошибка чтения '%s' через calamine. Размер: %d байт, сигнатура: %s",
                filename,
                len(file_content),
                file_content[:8].hex() if len(file_content) >= 8 else "N/A"
            )
            raise RuntimeError(
                f"Ошибка чтения файла '{filename}' через pandas+calamine: {str(e)}"
            ) from e

        result: Dict[str, pd.DataFrame] = {}

        for sheet_name, df in sheets.items():
            if df.empty:
                logger.warning("Лист '%s' пустой, пропускаем", sheet_name)
                continue

            result[sheet_name] = df

            logger.debug(
                "Лист '%s' прочитан: %d строк × %d колонок",
                sheet_name,
                len(df),
                len(df.columns)
            )

        logger.info(
            "Прочитано %d листов из файла %s",
            len(result),
            filename
        )

        return result

    def _validate_excel_format(self, content: bytes, filename: str) -> None:
        """
        Проверяет, что файл действительно в формате Excel по magic bytes.

        Поддерживаемые форматы:
        - .xls (BIFF5/8): D0 CF 11 E0 (OLE2/CFB)
        - .xlsx/.xlsm: 50 4B 03 04 (ZIP)

        Args:
            content: содержимое файла в байтах
            filename: имя файла для сообщений об ошибках

        Raises:
            ValueError: если формат файла некорректен

        Notes:
            Не блокирует чтение при неизвестной сигнатуре (только warning),
            так как calamine может поддерживать дополнительные форматы.
        """
        if len(content) < 4:
            raise ValueError(f"Файл '{filename}' слишком мал: {len(content)} байт")

        magic = content[:4]

        # .xls (старый формат OLE2)
        if magic == b'\xD0\xCF\x11\xE0':
            logger.debug("Обнаружен .xls (OLE2) формат для файла %s", filename)
            return

        # .xlsx/.xlsm (ZIP формат)
        if magic == b'PK\x03\x04':
            logger.debug("Обнаружен .xlsx/.xlsm (ZIP) формат для файла %s", filename)
            return

        # HTML замаскированный под Excel (частая проблема)
        if content[:5].lower() == b'<html' or content[:4].lower() == b'<!do':
            raise ValueError(
                f"Файл '{filename}' является HTML, а не Excel файлом. "
                f"Возможно, это экспорт из веб-приложения."
            )

        # Неизвестный формат - предупреждение, но не блокируем
        logger.warning(
            "Файл '%s' имеет неожиданную сигнатуру: %s. Пробуем прочитать через calamine...",
            filename,
            magic.hex()
        )