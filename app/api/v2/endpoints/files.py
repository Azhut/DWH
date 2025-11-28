from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Depends
from app.data.services.data_delete import DataDeleteService
from app.api.v2.schemas.files import FileListResponse, DeleteFileResponse
from app.core.database import mongo_connection

router = APIRouter()


@router.get("/files", response_model=List[FileListResponse])
async def list_files(
    limit: int = Query(100, ge=0),
    offset: int = Query(0, ge=0),
    year: Optional[int] = None
):
    try:
        db = mongo_connection.get_database()
        files_col = db.get_collection("Files")
        query = {} if year is None else {"year": year}
        cursor = files_col.find(query).skip(offset).limit(limit)
        docs = await cursor.to_list(length=None)
        result = []
        for doc in docs:
            result.append(FileListResponse(
                file_id=doc.get("file_id"),
                filename=doc.get("filename"),
                status=doc.get("status"),
                error=doc.get("error"),
                upload_timestamp=doc.get("upload_timestamp"),
                updated_at=doc.get("updated_at", doc.get("upload_timestamp")),
                year=doc.get("year")
            ))
        return result
    except Exception as e:
        raise HTTPException(500, f"{str(e)}... Обратитесь к разработчикам")


@router.delete("/files/{file_id}", response_model=DeleteFileResponse)
async def delete_file_record(
    file_id: str,
    delete_service: DataDeleteService = Depends(DataDeleteService)
):
    try:
        await delete_service.delete_file(file_id)
        return DeleteFileResponse(detail=f"Запись файла '{file_id}' успешно удалена")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"{str(e)}... Обратитесь к разработчикам")
