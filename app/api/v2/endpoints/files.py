from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from app.core.config import mongo_connection

router = APIRouter()

# app/api/v2/endpoints/files.py
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