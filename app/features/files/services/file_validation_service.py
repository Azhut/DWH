from fastapi import HTTPException

class FileValidationService:
    """
    Сервис для валидации файлов.
    Проверяет расширение файлов на допустимые форматы.
    """
    @staticmethod
    def validate_file_extension(filename: str):
        """
        Проверяет расширение файла на допустимые форматы.
        :param filename: Имя файла.
        :raises HTTPException: В случае неподдерживаемого формата.
        """
        valid_extensions = (".xlsx", ".xls", ".xlsm")
        if not filename.endswith(valid_extensions):
            raise HTTPException(
                status_code=400,
                detail=f"Неподдерживаемый формат файла: {filename}. Допустимые форматы: {', '.join(valid_extensions)}"
            )
