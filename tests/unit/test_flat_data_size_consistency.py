import pytest
from unittest.mock import AsyncMock

from app.domain.flat_data.models import FlatDataRecord
from app.domain.flat_data.service import FlatDataService


class _BulkWriteResult:
    __slots__ = ("inserted_count", "bulk_api_result")

    def __init__(self, inserted_count: int) -> None:
        self.inserted_count = inserted_count
        self.bulk_api_result: dict = {"writeErrors": []}


class MockCollection:
    def __init__(self):
        self.storage = []
        self.bulk_write_call_count = 0
        self.insert_one_call_count = 0

    async def bulk_write(self, operations, ordered=False, session=None, **kwargs):
        self.bulk_write_call_count += 1
        docs = [op._doc for op in operations]
        self.storage.extend(docs)
        return _BulkWriteResult(len(docs))

    async def insert_one(self, doc, session=None, **kwargs):
        self.insert_one_call_count += 1
        self.storage.append(doc)
        return AsyncMock(inserted_id="mock_id")

    async def count_documents(self, query, session=None, **kwargs):
        return len([d for d in self.storage if d.get("file_id") == query.get("file_id")])


@pytest.mark.asyncio
async def test_save_flat_data_returns_actual_inserted_count_on_bulk_partial_success():
    """Проверяет, что save_flat_data возвращает реальное число вставленных документов,
    а не длину входного списка (Hypothesis H2)."""
    mock_repo = AsyncMock()
    mock_collection = MockCollection()
    mock_repo.collection = mock_collection
    mock_repo.insert_one = mock_collection.insert_one
    mock_repo.count_documents = mock_collection.count_documents
    service = FlatDataService(mock_repo)

    # Создаем 3842 записи, но 32 из них будут "дублями" (симулируем через mock)
    records = [FlatDataRecord(year=2023, reporter="TEST", section="Раздел1", row=f"R{i}", column=f"C{i}", value=i,
                              file_id="f1") for i in range(3842)]

    # Патчим bulk_write так, чтобы он "пропускал" каждый 120-й документ (имитация 32 пропущенных)
    original_bulk = mock_collection.bulk_write

    async def failing_bulk(*args, **kwargs):
        ops = args[0]
        success_docs = [op._doc for i, op in enumerate(ops) if i % 120 != 0]
        mock_collection.storage.extend(success_docs)
        return _BulkWriteResult(len(success_docs))

    mock_collection.bulk_write = failing_bulk
    mock_repo.bulk_write_ops = failing_bulk

    inserted_total = await service.save_flat_data(records)
    actual_in_db = await mock_repo.count_documents({"file_id": "f1"})

    print(f"📊 Returned by service: {inserted_total}")
    print(f"💾 Actual in mock DB:   {actual_in_db}")

    # Если рассинхрон подтвердится, этот assert упадет. Это и будет подтверждением бага.
    assert inserted_total == actual_in_db, f"Рассинхрон: сервис вернул {inserted_total}, в БД {actual_in_db}"


@pytest.mark.asyncio
async def test_pipeline_updates_flat_data_size_after_persist():
    """Интеграционный тест: проверяет, что flat_data_size обновляется на реальное число вставленных записей."""
    from app.application.upload.pipeline.steps.FinalizeFileModelStep import FinalizeFileModelStep
    from app.application.data import DataSaveService
    from app.application.upload.pipeline.context import UploadPipelineContext
    from app.domain.file.service import FileService
    from app.domain.form.models import FormInfo
    from io import BytesIO
    from fastapi import UploadFile

    # Mock сервисов
    mock_file_service = AsyncMock()
    mock_file_service.update_or_create = AsyncMock()
    mock_flat_data_service = AsyncMock()
    # Симулируем, что из 3842 записей реально вставилось 3810
    mock_flat_data_service.save_flat_data = AsyncMock(return_value=3810)
    mock_log_service = AsyncMock()

    save_svc = DataSaveService(
        file_service=mock_file_service,
        flat_data_service=mock_flat_data_service,
        log_service=mock_log_service
    )

    # Создаем контекст
    file_model = AsyncMock()
    file_model.flat_data_size = 0
    file_model.file_id = "test-uuid"
    ctx = AsyncMock()
    ctx.file_model = file_model
    ctx.flat_data = [AsyncMock() for _ in range(3842)]

    await save_svc.process_and_save_all(file_model, ctx.flat_data)

    # Проверяем, что flat_data_size был обновлен на returned value (3810), а не остался 3842
    assert file_model.flat_data_size == 3810
    mock_file_service.update_or_create.assert_awaited()