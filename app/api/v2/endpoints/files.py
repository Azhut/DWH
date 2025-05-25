from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from app.core.config import mongo_connection
from app.data_storage.repositories.logs_repository import LogsRepository
from app.data_storage.services.log_service import LogService

router = APIRouter()
log_service = LogService(LogsRepository(mongo_connection.get_database().get_collection("Logs")))

@router.get("/files")
async def list_files(
    limit: int = Query(100, ge=0),
    offset: int = Query(0, ge=0),
    year: Optional[int] = None  # Добавить параметр
):
    try:
        db = mongo_connection.get_database()
        query = {} if year is None else {"year": year}  # Фильтрация по году
        cursor = db.get_collection("Files").find(query).skip(offset).limit(limit)
        docs = await cursor.to_list(length=None)
        return [
            {
                "filename": doc["filename"],
                "status": doc["status"],
                "error": doc.get("error", ""),
                "upload_timestamp": doc["upload_timestamp"],
                "updated_at": doc.get("updated_at", doc["upload_timestamp"]),
                "year": doc.get("year")  # Добавить поле year в ответ
            }
            for doc in docs
        ]
    except Exception as e:
        raise HTTPException(500, str(e) + "... Обратитесь к разработчикам")


@router.delete("/files/{file_id}")
async def delete_file_record(file_id: str):
    db = mongo_connection.get_database()
    files_col = db.get_collection("Files")
    try:
        result = await files_col.delete_one({"file_id": file_id})
        if result.deleted_count == 0:
            raise HTTPException(404, f"Файл '{file_id}' не найден")

        await log_service.save_log(f"Удален файл {file_id}", level="info")
        return {"detail": f"Запись файла '{file_id}' успешно удалена"}
    except HTTPException:
        raise
    except Exception as e:

        await log_service.save_log(f"Ошибка удаления файла: {file_id}: {e}", level="error")
        raise HTTPException(500, str(e) + "... Обратитесь к разработчикам")
