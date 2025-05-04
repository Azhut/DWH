from typing import List, Tuple
from fastapi import UploadFile, HTTPException

from app.core.exception_handler import log_and_raise_http
from app.core.logger import logger
from app.data_storage.data_save_service import create_data_save_service
from app.features.files.services.sheet_extraction_service import SheetExtractionService
from app.features.sheet_parsers.parsers import get_sheet_parser
from app.models.file_status import FileStatus
from app.models.sheet_model import SheetModel
from app.data_storage.services.file_service import FileService
from app.data_storage.repositories.file_repository import FileRepository
from app.core.config import mongo_connection


class SheetProcessor:
    def __init__(self):
        self.sheet_extractor = SheetExtractionService()
        self.data_service = create_data_save_service()

    async def extract_and_process_sheets(self, file: UploadFile, city: str, year: int) -> Tuple[List[SheetModel], List[dict]]:
        logger.info(f"Начало обработки листов файла {file.filename}")
        try:
            if not await is_file_unique(file.filename):
                msg = f"Файл '{file.filename}' уже был загружен."
                log_and_raise_http(400, msg)

            sheets = await self.sheet_extractor.extract(file)
            result = await self._process_sheets(file.filename, sheets, city, year)
            logger.info(f"Успешно обработано {len(sheets)} листов файла {file.filename}")
            return result
        except HTTPException:
            raise
        except Exception as e:
            log_and_raise_http(500, "Ошибка при обработке листов", e)

    async def _process_sheets(self, file_id: str, sheets: List[dict], city: str, year: int) -> Tuple[List[SheetModel], List[dict]]:
        sheet_models = []
        all_flat_data = []

        for sheet in sheets:
            if sheet["sheet_name"] == 'Раздел0':
                continue
            try:
                parser = get_sheet_parser(sheet["sheet_name"])
                parsed_data = parser.parse(sheet["data"])
                flat_data = parser.generate_flat_data(year, city, sheet["sheet_name"])

                logger.debug(f"Generated {len(flat_data)} flat records")
                logger.debug("Parsed data structure:")
                self._log_parsed_data(parsed_data)

                all_flat_data.extend(flat_data)
                sheet_model = self._build_sheet_model(file_id, sheet, parsed_data, city, year)
                sheet_models.append(sheet_model)

            except Exception as e:
                await self._handle_processing_error(sheet, e)

        return sheet_models, all_flat_data

    def _build_sheet_model(self, file_id: str, sheet: dict, parsed_data: dict, city: str, year: int) -> SheetModel:
        return SheetModel(
            file_id=file_id,
            sheet_name=sheet["sheet_name"],
            sheet_fullname=sheet['data'].columns[0],
            year=year,
            city=city,
            headers=parsed_data.get("headers", {}),
            data=parsed_data.get("data", [])
        )

    def _log_parsed_data(self, data, indent=0):
        """Рекурсивное логирование структуры данных"""
        if isinstance(data, dict):
            for key, value in data.items():
                logger.debug(f"{' ' * indent}Key: {key} ({type(value).__name__})")
                self._log_parsed_data(value, indent + 4)
        elif isinstance(data, list):
            logger.debug(f"{' ' * indent}List[{len(data)} items]")
            if data:
                self._log_parsed_data(data[0], indent + 4)
        else:
            logger.debug(f"{' ' * indent}Value type: {type(data).__name__}")

    async def _handle_processing_error(self, sheet, error):
        msg = (f"Ошибка обработки раздела {sheet['sheet_name']}: {error}"
               f"Убедитесь, что разделы имеют название 'Раздел0' 'Раздел1' и т.д."
               f"Также убедитесь, что файл имеет структуру таблиц и заголовков, схожими с остальными файлами 1ФК")
        logger.error(msg, exc_info=True)
        await self.data_service.save_logs(msg, level="error")
        log_and_raise_http(400, msg)



async def is_file_unique(file_id: str) -> bool:
    """
    Проверяет, существует ли успешно обработанный файл с данным идентификатором.
    Возвращает True, если файл:
    - Не существует в базе
    - Существует, но имеет статус != "success"
    """
    file_service = FileService(FileRepository(mongo_connection.get_database().get_collection("Files")))
    existing_file = await file_service.get_file_by_id(file_id)

    return not existing_file or existing_file.status != FileStatus.SUCCESS