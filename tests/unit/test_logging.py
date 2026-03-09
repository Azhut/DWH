from __future__ import annotations

from datetime import datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v2.endpoints.logs import router as logs_router
from app.core.dependencies import get_file_service, get_log_service
from app.domain.log.service import LogService, MAX_LOGS


class FakeLogRepository:
    def __init__(self) -> None:
        self.inserted: list[dict] = []
        self.cleanup_calls: list[int] = []

    async def insert_one(self, document: dict) -> None:
        self.inserted.append(document)

    async def cleanup_old_logs(self, max_logs: int) -> None:
        self.cleanup_calls.append(max_logs)


@pytest.mark.asyncio
async def test_log_service_save_log_persists_entry_and_triggers_cleanup() -> None:
    repo = FakeLogRepository()
    service = LogService(repo)

    await service.save_log(
        scenario="deletion",
        message="Test deletion log",
        level="error",
        meta={"deleted_type": "file", "deleted_id": "f1"},
        logger="test_logger",
        pathname="/path/to/file.py",
        lineno=42,
    )

    # Проверяем, что запись была сохранена
    assert len(repo.inserted) == 1
    doc = repo.inserted[0]

    assert doc["scenario"] == "deletion"
    assert doc["meta"] == {"deleted_type": "file", "deleted_id": "f1"}
    assert doc["level"] == "error"
    assert doc["message"] == "Test deletion log"
    assert doc["logger"] == "test_logger"
    assert doc["pathname"] == "/path/to/file.py"
    assert doc["lineno"] == 42
    assert "_id" in doc
    assert isinstance(doc["timestamp"], datetime)

    # Проверяем, что была запущена очистка старых логов по количеству
    assert repo.cleanup_calls == [MAX_LOGS]


class FakeLogService:
    def __init__(self, logs: list[dict]) -> None:
        class _Repo:
            def __init__(self, items: list[dict]) -> None:
                self._items = items

            async def find_logs(self, **_: object) -> list[dict]:
                return list(self._items)

        self._repo = _Repo(logs)


class FakeFileService:
    def __init__(self, docs: list[dict]) -> None:
        self._docs = docs

    async def list_files_by_status(self, *args, **kwargs) -> list[dict]:
        return list(self._docs)


def _build_test_app(fake_log_service: FakeLogService, fake_file_service: FakeFileService) -> TestClient:
    app = FastAPI()
    app.include_router(logs_router, prefix="/api/v2")

    app.dependency_overrides[get_log_service] = lambda: fake_log_service
    app.dependency_overrides[get_file_service] = lambda: fake_file_service

    return TestClient(app)


def test_logs_download_aggregates_logs_and_failed_files() -> None:
    log_ts = datetime(2025, 1, 1, 12, 0, 0)
    logs = [
        {
            "_id": "1",
            "timestamp": log_ts,
            "scenario": "deletion",
            "level": "error",
            "message": "Failed to delete file",
            "meta": {"deleted_type": "file", "deleted_id": "f1"},
        }
    ]

    files = [
        {
            "file_id": "f2",
            "filename": "test.xlsx",
            "form_id": "form-1",
            "error": "parse error",
            "status": "failed",
            "upload_timestamp": datetime(2025, 1, 1, 12, 5, 0),
        }
    ]

    client = _build_test_app(FakeLogService(logs), FakeFileService(files))

    response = client.get("/api/v2/logs/download?limit=10")
    assert response.status_code == 200
    body = response.text.splitlines()

    # Заголовок + минимум две строки (из Logs и из Files)
    assert body[0] == "timestamp,scenario,level,message,meta_json"
    assert len(body) >= 3

    # Проверяем, что в CSV присутствуют оба сценария
    csv_text = "\n".join(body)
    assert "deletion" in csv_text
    assert "upload_error" in csv_text

