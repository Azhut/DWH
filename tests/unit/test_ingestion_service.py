import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi import UploadFile, HTTPException

from app.services.ingestion_service import IngestionService
from app.models.file_status import FileStatus



class TestIngestionService:

    @pytest.fixture
    def service(self):
        return IngestionService()

    @pytest.fixture
    def mock_upload_file(self):
        mock_file = Mock(spec=UploadFile)
        mock_file.filename = "TEST_CITY 2023.xlsx"
        mock_file.read = AsyncMock(return_value=b"test_content")
        mock_file.seek = AsyncMock()
        return mock_file

    @pytest.mark.asyncio
    async def test_process_files_success(self, service, mock_upload_file):
        """Тест успешной обработки файлов"""
        with patch.object(service.file_processor, 'validate_and_extract_metadata') as mock_validate, \
                patch.object(service.sheet_processor, 'extract_and_process_sheets') as mock_extract, \
                patch.object(service.data_service, 'process_and_save_all') as mock_save:
            # Настройка моков
            mock_validate.return_value = Mock(
                city="TEST_CITY",
                year=2023,
                filename="TEST_CITY 2023.xlsx",
                extension="xlsx"
            )
            mock_extract.return_value = ([], [])
            mock_save.process_and_save_all = AsyncMock()

            result = await service.process_files([mock_upload_file])

            # Проверки
            assert "successfully" in result.message
            assert len(result.details) == 1
            assert result.details[0].status == FileStatus.SUCCESS.value
            assert result.details[0].filename == "TEST_CITY 2023.xlsx"
            assert result.details[0].error == ""

    @pytest.mark.asyncio
    async def test_process_files_validation_error(self, service, mock_upload_file):
        """Тест обработки ошибки валидации"""
        with patch.object(service.file_processor, 'validate_and_extract_metadata') as mock_validate:
            mock_validate.side_effect = HTTPException(400, "Validation error")

            result = await service.process_files([mock_upload_file])

            assert result.details[0].status == FileStatus.FAILED.value
            assert "Validation error" in result.details[0].error

    @pytest.mark.asyncio
    async def test_process_files_extraction_error(self, service, mock_upload_file):
        """Тест обработки ошибки извлечения данных"""
        with patch.object(service.file_processor, 'validate_and_extract_metadata') as mock_validate, \
                patch.object(service.sheet_processor, 'extract_and_process_sheets') as mock_extract:
            mock_validate.return_value = Mock(
                city="TEST_CITY",
                year=2023,
                filename="TEST_CITY 2023.xlsx",
                extension="xlsx"
            )
            mock_extract.side_effect = Exception("Extraction error")

            result = await service.process_files([mock_upload_file])

            assert result.details[0].status == FileStatus.FAILED.value
            assert "Extraction error" in result.details[0].error

    @pytest.mark.asyncio
    async def test_process_files_multiple_files(self, service):
        """Тест обработки нескольких файлов"""
        mock_files = [
            Mock(spec=UploadFile, filename="CITY1 2023.xlsx"),
            Mock(spec=UploadFile, filename="CITY2 2022.xlsx"),
        ]

        for mock_file in mock_files:
            mock_file.read = AsyncMock(return_value=b"content")
            mock_file.seek = AsyncMock()

        with patch.object(service.file_processor, 'validate_and_extract_metadata') as mock_validate, \
                patch.object(service.sheet_processor, 'extract_and_process_sheets') as mock_extract, \
                patch.object(service.data_service, 'process_and_save_all') as mock_save:
            mock_validate.side_effect = [
                Mock(city="CITY1", year=2023, filename="CITY1 2023.xlsx", extension="xlsx"),
                Mock(city="CITY2", year=2022, filename="CITY2 2022.xlsx", extension="xlsx"),
            ]
            mock_extract.return_value = ([], [])
            mock_save.process_and_save_all = AsyncMock()

            result = await service.process_files(mock_files)

            assert len(result.details) == 2
            assert "2 files processed" in result.message