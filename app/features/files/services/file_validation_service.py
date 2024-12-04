from fastapi import HTTPException

class FileValidationService:
    valid_extensions = (".xlsx", ".xls")

    def validate(self, filename: str):
        """
        Проверка валидности расширения файла.
        """
        if not filename.endswith(self.valid_extensions):
            raise HTTPException(
                status_code=400,
                detail=f"Неподдерживаемый формат файла: {filename}. Допустимые форматы: {', '.join(self.valid_extensions)}"
            )
