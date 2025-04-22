from typing import List, Tuple

from fastapi import UploadFile, HTTPException

from app.core.logger import logger
from app.data_storage.data_save_service import create_data_save_service
from app.features.files.services.sheet_extraction_service import SheetExtractionService
from app.features.sheet_parsers.parsers import get_sheet_parser
from app.models.sheet_model import SheetModel
from app.data_storage.services.file_service import FileService
from app.data_storage.repositories.file_repository import FileRepository
from app.core.config import mongo_connection


class SheetProcessor:
    def __init__(self):
        self.sheet_extractor = SheetExtractionService()
        self.data_service = create_data_save_service()

    async def is_file_unique(self, file_id: str) -> bool:
        """
        Проверяет, существует ли успешно обработанный файл с данным идентификатором.
        Возвращает True, если файл:
        - Не существует в базе
        - Существует, но имеет статус != "success"
        """
        file_service = FileService(FileRepository(mongo_connection.get_database().get_collection("Files")))
        existing_file = await file_service.get_file_by_id(file_id)

        return not existing_file or existing_file.status != "success"

    async def extract_and_process_sheets(self, file: UploadFile, city: str, year: int) -> Tuple[List[SheetModel], List[dict]]:

        if not await self.is_file_unique(file.filename):
            raise HTTPException(status_code=400, detail=f"Файл '{file.filename}' уже был загружен.")

        sheets = await self.sheet_extractor.extract(file)
        return await self._process_sheets(file.filename, sheets, city, year)

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
        error_msg = f"Error processing sheet {sheet['sheet_name']}: {str(error)}"
        logger.error(error_msg, exc_info=True)
        await self.data_service.save_logs(error_msg, level="error")
        raise HTTPException(
            status_code=400,
            detail=error_msg
        )