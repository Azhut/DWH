from app.data.repositories.file import FileRepository
from app.data.repositories.flat_data import FlatDataRepository
from app.data.services.log_service import LogService
from fastapi import HTTPException

class DataDeleteService:
    def __init__(
        self,
        file_repo: FileRepository,
        flat_repo: FlatDataRepository,
        log_service: LogService
    ):
        self.file_repo = file_repo
        self.flat_repo = flat_repo
        self.log_service = log_service

    async def delete_file(self, file_id: str):
        # Проверяем наличие файла
        existing = await self.file_repo.find_by_file_id(file_id)
        if not existing:
            raise HTTPException(404, f"Файл '{file_id}' не найден")

        try:
            # Сначала удаляем плоские данные, связанные с file_id
            await self.flat_repo.delete_many({"file_id": file_id})
        except Exception as e:
            # Если не удалось удалить flat - логируем и бросаем исключение
            await self.log_service.save_log(f"Ошибка при удалении FlatData для {file_id}: {e}", level="error")
            raise HTTPException(500, f"Ошибка при удалении связанных данных: {str(e)}")

        try:
            result = await self.file_repo.delete_one({"file_id": file_id})
            # delete_one возвращает DeleteResult; у motor атрибут deleted_count доступен
            if getattr(result, "deleted_count", None) == 0:
                raise HTTPException(404, f"Файл '{file_id}' не найден при финальном удалении")
        except HTTPException:
            raise
        except Exception as e:
            await self.log_service.save_log(f"Ошибка при удалении записи Files для {file_id}: {e}", level="error")
            raise HTTPException(500, f"Ошибка при удалении записи файла: {str(e)}")

        await self.log_service.save_log(f"Удалён файл {file_id}", level="info")
