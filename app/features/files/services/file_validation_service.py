from fastapi import HTTPException

class FileValidationService:
    VALID_EXTENSIONS = [".xlsx", ".xls", ".xlsm"]

    def validate(self, filename: str):
        if not any(filename.endswith(ext) for ext in self.VALID_EXTENSIONS):
            raise HTTPException(status_code=400,
                                detail=f"Invalid file type: {filename}. Allowed types: {self.VALID_EXTENSIONS}")
