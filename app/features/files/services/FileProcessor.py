from fastapi import UploadFile

from app.features.files.services.city_and_year_extractor import CityAndYearExtractor
from app.features.files.services.file_validation_service import FileValidationService


class FileProcessor:
    def __init__(self):
        self.file_validator = FileValidationService()
        self.city_year_extractor = CityAndYearExtractor()

    def validate_and_extract_metadata(self, file: UploadFile) -> tuple[str, int]:
        self.file_validator.validate(file.filename)
        return self.city_year_extractor.extract(file.filename)
