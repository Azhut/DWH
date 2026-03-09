from typing import Optional

from fastapi import APIRouter, Depends, Query, Response

from app.core.dependencies import get_log_service, get_file_service
from app.domain.file.models import FileStatus
from app.domain.file.service import FileService
from app.domain.log.repository import LogRepository
from app.domain.log.service import LogService

import csv
import io
import json
from datetime import datetime


router = APIRouter()


@router.get("/logs/download")
async def download_logs(
    limit: int = Query(1000, ge=1),
    scenario: Optional[str] = None,
    level: Optional[str] = None,
    from_id: Optional[str] = None,
    log_service: LogService = Depends(get_log_service),
    file_service: FileService = Depends(get_file_service),
) -> Response:
    """
    Выгрузка логов и ошибок загрузки в CSV.
    Колонки: timestamp,scenario,level,message,meta_json.
    """
    # Логи из коллекции Logs
    repo: LogRepository = log_service._repo  # используем репозиторий, инкапсулированный в сервисе
    logs = await repo.find_logs(limit=limit, scenario=scenario, level=level, from_id=from_id)

    def _fmt_ts(value: datetime) -> str:
        return value.isoformat()

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["timestamp", "scenario", "level", "message", "meta_json"])

    # Логи из Logs
    for log in logs:
        ts = _fmt_ts(log.get("timestamp"))
        scen = log.get("scenario") or ""
        lvl = log.get("level") or ""
        msg = str(log.get("message") or "")
        meta = log.get("meta") or {}
        meta_json = json.dumps(meta, ensure_ascii=False)
        writer.writerow([ts, scen, lvl, msg, meta_json])

    # Ошибки upload из Files
    failed_files = await file_service.list_files_by_status(FileStatus.FAILED, limit=limit)
    for doc in failed_files:
        ts = _fmt_ts(doc.get("updated_at") or doc.get("upload_timestamp"))
        scen = "upload_error"
        lvl = "error"
        file_id = doc.get("file_id")
        msg = f"Upload failed for file {file_id}"
        meta = {
            "file_id": file_id,
            "filename": doc.get("filename"),
            "form_id": doc.get("form_id"),
            "error": doc.get("error"),
            "status": doc.get("status"),
        }
        meta_json = json.dumps(meta, ensure_ascii=False)
        writer.writerow([ts, scen, lvl, msg, meta_json])

    csv_body = buffer.getvalue()

    return Response(
        content=csv_body,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename=\"logs.csv\"'},
    )

