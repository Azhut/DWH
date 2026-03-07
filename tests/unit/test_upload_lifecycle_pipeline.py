from io import BytesIO
from unittest.mock import AsyncMock

import pytest
from fastapi import UploadFile

from app.application.upload.pipeline.context import UploadPipelineContext
from app.application.upload.pipeline.pipeline import UploadPipelineRunner
from app.application.upload.pipeline.steps.AcquireFileRecordStep import AcquireFileRecordStep
from app.core.exceptions import CriticalUploadError
from app.domain.file.models import FileModel, FileStatus


class FailingStep:
    async def execute(self, ctx: UploadPipelineContext) -> None:
        raise CriticalUploadError(
            message="forced failure",
            domain="tests.upload_lifecycle",
            http_status=500,
        )


@pytest.mark.asyncio
async def test_critical_error_uses_single_acquired_record_for_rollback() -> None:
    file_service = AsyncMock()
    file_service.get_by_filename_and_status.return_value = None
    file_service.get_by_filename.return_value = None
    file_service.update_or_create = AsyncMock()

    data_save_service = AsyncMock()
    data_save_service.rollback = AsyncMock()
    data_save_service.save_file = AsyncMock()

    pipeline = UploadPipelineRunner(
        steps=[AcquireFileRecordStep(file_service), FailingStep()],
        data_save_service=data_save_service,
    )

    upload_file = UploadFile(filename="TEST 2025.xlsx", file=BytesIO(b"123"), size=3)
    ctx = UploadPipelineContext(file=upload_file, form_id="form-1", form_info=None)

    await pipeline.run_for_file(ctx)

    assert ctx.failed is True
    file_service.update_or_create.assert_awaited_once()
    acquired_model = file_service.update_or_create.await_args.args[0]

    data_save_service.rollback.assert_awaited_once()
    rollback_model = data_save_service.rollback.await_args.args[0]
    assert rollback_model.file_id == acquired_model.file_id

    data_save_service.save_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_duplicate_upload_does_not_change_db_state() -> None:
    existing_success = FileModel.create_processing(filename="TEST 2025.xlsx", form_id="form-1")
    existing_success.status = FileStatus.SUCCESS

    file_service = AsyncMock()
    file_service.get_by_filename_and_status.return_value = existing_success
    file_service.get_by_filename.return_value = None
    file_service.update_or_create = AsyncMock()

    data_save_service = AsyncMock()
    data_save_service.rollback = AsyncMock()

    pipeline = UploadPipelineRunner(
        steps=[AcquireFileRecordStep(file_service)],
        data_save_service=data_save_service,
    )

    upload_file = UploadFile(filename="TEST 2025.xlsx", file=BytesIO(b"123"), size=3)
    ctx = UploadPipelineContext(file=upload_file, form_id="form-1", form_info=None)

    await pipeline.run_for_file(ctx)

    assert ctx.failed is True
    assert "already been uploaded" in (ctx.error or "")
    file_service.update_or_create.assert_not_awaited()
    data_save_service.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_retries_reuse_existing_failed_file_id() -> None:
    existing_failed = FileModel.create_processing(filename="TEST 2025.xlsx", form_id="form-1")
    existing_failed.status = FileStatus.FAILED

    file_service = AsyncMock()
    file_service.get_by_filename_and_status.return_value = None
    file_service.get_by_filename.return_value = existing_failed
    file_service.update_or_create = AsyncMock()

    data_save_service = AsyncMock()
    data_save_service.rollback = AsyncMock()

    pipeline = UploadPipelineRunner(
        steps=[AcquireFileRecordStep(file_service)],
        data_save_service=data_save_service,
    )

    upload_file = UploadFile(filename="TEST 2025.xlsx", file=BytesIO(b"123"), size=3)
    ctx = UploadPipelineContext(file=upload_file, form_id="form-1", form_info=None)

    await pipeline.run_for_file(ctx)

    assert ctx.failed is False
    assert ctx.file_model is not None
    assert ctx.file_model.file_id == existing_failed.file_id
    assert ctx.file_model.status == FileStatus.PROCESSING
    assert ctx.file_model.error is None

    file_service.update_or_create.assert_awaited_once()


@pytest.mark.asyncio
async def test_same_filename_other_form_creates_new_record() -> None:
    file_service = AsyncMock()
    file_service.get_by_filename_and_status.return_value = None
    file_service.get_by_filename.return_value = None
    file_service.update_or_create = AsyncMock()

    data_save_service = AsyncMock()
    data_save_service.rollback = AsyncMock()

    pipeline = UploadPipelineRunner(
        steps=[AcquireFileRecordStep(file_service)],
        data_save_service=data_save_service,
    )

    upload_file = UploadFile(filename="TEST 2025.xlsx", file=BytesIO(b"123"), size=3)
    ctx = UploadPipelineContext(file=upload_file, form_id="form-2", form_info=None)

    await pipeline.run_for_file(ctx)

    assert ctx.failed is False
    assert ctx.file_model is not None
    assert ctx.file_model.form_id == "form-2"
    file_service.update_or_create.assert_awaited_once()
