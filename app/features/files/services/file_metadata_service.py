# Centralized service for file validation and metadata extraction
from fastapi import UploadFile, HTTPException
from app.features.files.services.file_validation_service import FileValidationService
from app.features.files.services.city_and_year_extractor import CityAndYearExtractor

class FileMetadataService:
    def __init__(self):
        self.file_validator = FileValidationService()
        self.city_year_extractor = CityAndYearExtractor()

    def validate_and_extract_metadata(self, file: UploadFile) -> tuple[str, int]:
        """
        Validates the file and extracts metadata (city and year).

        :param file: Uploaded file
        :return: Tuple containing city and year
        """
        self.file_validator.validate(file.filename)
        return self.city_year_extractor.extract(file.filename)