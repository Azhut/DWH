# Тесты для обработки файлов

import pytest
from app.features.files.services.file_metadata_service import FileMetadataService

file_metadata_service = FileMetadataService()

def test_validate_and_extract_metadata():
    filename = "Москва 2023.xlsx"
    city, year = file_metadata_service.validate_and_extract_metadata(filename)
    assert city == "МОСКВА"
    assert year == 2023