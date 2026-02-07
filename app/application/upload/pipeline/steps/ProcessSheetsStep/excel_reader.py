import logging
from typing import Dict
from io import BytesIO

import pandas as pd

logger = logging.getLogger(__name__)


class ExcelReader:
    """
    Application-level reader Excel файлов.
    """

    def read(self, content: bytes, filename: str) -> Dict[str, pd.DataFrame]:
        """
        Читает все листы Excel из байтов в памяти через pandas + calamine.

        Args:
            content: содержимое Excel файла в байтах (из ctx.file_content)
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
        if not content:
            raise ValueError(f"Пустое содержимое файла '{filename}'")

        logger.debug(
            "ExcelReader: чтение файла '%s' (%d байт)",
            filename,
            len(content),
        )

        self._validate_excel_format(content, filename)
        try:
            sheets = pd.read_excel(
                BytesIO(content),
                sheet_name=None,
                header=None,
                dtype=object,
                engine="calamine",
            )
        except Exception as e:
            logger.error(
                "ExcelReader: ошибка чтения '%s': %s",
                filename,
                e,
            )
            raise RuntimeError(
                f"Ошибка чтения Excel файла '{filename}': {str(e)}"
            ) from e

        result: Dict[str, pd.DataFrame] = {}

        for sheet_name, df in sheets.items():
            if df is None or df.empty:
                logger.debug(
                    "ExcelReader: лист '%s' пустой, пропуск",
                    sheet_name,
                )
                continue

            result[sheet_name] = df

            logger.debug(
                "ExcelReader: лист '%s' прочитан (%d × %d)",
                sheet_name,
                len(df),
                len(df.columns),
            )

        logger.info(
            "ExcelReader: прочитано %d листов из файла %s",
            len(result),
            filename,
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