import pytest
from unittest.mock import Mock, patch
from fastapi import HTTPException

from app.services.file_processor import FileProcessor, FileMetadata


class TestFileProcessor:

    @pytest.fixture
    def processor(self):
        return FileProcessor()

    def test_validate_file_extension_success(self, processor):
        """Тест успешной валидации расширений файлов"""
        valid_filenames = [
            "test.xlsx",
            "test.XLSX",
            "test.xls",
            "test.XLS",
            "test.xlsm",
            "test.XLSM"
        ]

        for filename in valid_filenames:
            # Не должно быть исключения
            processor._validate_file_extension(filename)

    def test_validate_file_extension_invalid(self, processor):
        """Тест валидации неверных расширений"""
        invalid_filenames = [
            "test.txt",
            "test.pdf",
            "test.doc",
            "test",
            "test.xlsx.exe"
        ]

        for filename in invalid_filenames:
            with pytest.raises(HTTPException) as exc_info:
                processor._validate_file_extension(filename)

            assert exc_info.value.status_code == 400
            assert "Некорректное расширение файла" in exc_info.value.detail

    @pytest.mark.parametrize("filename,expected_city,expected_year", [
        ("MOSCOW 2023.xlsx", "MOSCOW", 2023),
        ("SAINT-PETERSBURG 2022.xls", "SAINT-PETERSBURG", 2022),
        ("EKATERINBURG 2021.xlsm", "EKATERINBURG", 2021),
        ("НИЖНИЙ НОВГОРОД 2020.xlsx", "НИЖНИЙ НОВГОРОД", 2020),
    ])
    def test_extract_city_year_success(self, processor, filename, expected_city, expected_year):
        """Параметризованный тест извлечения города и года"""
        city, year = processor._extract_city_year(filename)

        assert city == expected_city
        assert year == expected_year

    # def test_extract_city_year_invalid_format(self, processor):
    #     """Тест извлечения из неверного формата"""
    #     invalid_filenames = [
    #         "2023.xlsx",  # нет города
    #         "MOSCOW.xlsx",  # нет года
    #         "MOSCOW 202.xlsx",  # неполный год
    #         "MOSCOW 20235.xlsx",  # длинный год
    #         "MOSCOW-2023.xlsx",  # другой разделитель
    #     ]
    #
    #     for filename in invalid_filenames:
    #         with pytest.raises(HTTPException) as exc_info:
    #             processor._extract_city_year(filename)
    #
    #         assert exc_info.value.status_code == 400

    def test_extract_city_year_invalid_year(self, processor):
        """Тест извлечения с неверным годом"""
        invalid_years = [
            "MOSCOW 1899.xlsx",  # слишком ранний год
            "MOSCOW 2101.xlsx",  # слишком поздний год
        ]

        for filename in invalid_years:
            with pytest.raises(HTTPException) as exc_info:
                processor._extract_city_year(filename)

            assert exc_info.value.status_code == 400

    def test_validate_and_extract_metadata_success(self, processor):
        """Тест полной валидации и извлечения метаданных"""
        mock_file = Mock()
        mock_file.filename = "MOSCOW 2023.xlsx"

        with patch.object(processor, '_validate_file_extension'), \
                patch.object(processor, '_extract_city_year', return_value=("MOSCOW", 2023)):
            metadata = processor.validate_and_extract_metadata(mock_file)

            assert isinstance(metadata, FileMetadata)
            assert metadata.city == "MOSCOW"
            assert metadata.year == 2023
            assert metadata.filename == "MOSCOW 2023.xlsx"
            assert metadata.extension == "xlsx"

    def test_validate_and_extract_metadata_validation_error(self, processor):
        """Тест обработки ошибки валидации"""
        mock_file = Mock()
        mock_file.filename = "invalid.txt"

        with patch.object(processor, '_validate_file_extension') as mock_validate:
            mock_validate.side_effect = HTTPException(400, "Invalid extension")

            with pytest.raises(HTTPException) as exc_info:
                processor.validate_and_extract_metadata(mock_file)

            assert exc_info.value.status_code == 400