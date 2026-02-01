from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Depends

from app.api.v2.schemas.files import FileListResponse, DeleteFileResponse
from app.data.file_delete import DataDeleteService
from app.core.dependencies import get_data_delete_service, get_file_service
from app.domain.file.service import FileService

router = APIRouter()


@router.get("/files", response_model=List[FileListResponse])
async def list_files(
    limit: int = Query(100, ge=0),
    offset: int = Query(0, ge=0),
    year: Optional[int] = None,
    file_service: FileService = Depends(get_file_service),
):
    try:
        docs = await file_service.list_files(limit=limit, offset=offset, year=year)
        result = []
        for doc in docs:
            result.append(FileListResponse(
                file_id=doc.get("file_id"),
                filename=doc.get("filename"),
                status=doc.get("status"),
                error=doc.get("error"),
                upload_timestamp=doc.get("upload_timestamp"),
                updated_at=doc.get("updated_at", doc.get("upload_timestamp")),
                year=doc.get("year"),
                form_id=doc.get("form_id"),
            ))
        return result
    except Exception as e:
        raise HTTPException(500, f"{str(e)}... Обратитесь к разработчикам")


@router.delete("/files/{file_id}", response_model=DeleteFileResponse)
async def delete_file_record(
    file_id: str,
    svc: DataDeleteService = Depends(get_data_delete_service)
):
    try:
        await svc.delete_file(file_id)
        return DeleteFileResponse(detail=f"Запись файла '{file_id}' успешно удалена")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"{str(e)}... Обратитесь к разработчикам")
