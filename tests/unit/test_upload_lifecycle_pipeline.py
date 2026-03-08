from io import BytesIO

import pytest
from fastapi import UploadFile

from app.application.upload.pipeline.context import UploadPipelineContext
from app.application.upload.pipeline.pipeline import UploadPipelineRunner
from app.application.upload.pipeline.steps.AcquireFileRecordStep import AcquireFileRecordStep
from app.core.exceptions import CriticalUploadError
from app.domain.file.models import FileModel, FileStatus
from app.domain.form.models import FormInfo


FILENAME = "TEST 2025.xlsx"
FORM_ID = "form-1"


class FailingStep:
    def __init__(self, message: str) -> None:
        self._message = message

    async def execute(self, ctx: UploadPipelineContext) -> None:
        raise CriticalUploadError(
            message=self._message,
            domain="tests.upload_lifecycle",
            http_status=500,
        )


class PersistSuccessStep:
    def __init__(self, data_save_service) -> None:
        self._data_save_service = data_save_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        await self._data_save_service.process_and_save_all(ctx.file_model, [])


class InMemoryFileService():
    def __init__(self) -> None:
        self._records: dict[tuple[str, str], FileModel] = {}
        self.update_calls = 0

    @staticmethod
    def _key(filename: str, form_id: str | None) -> tuple[str, str]:
        return filename, form_id or ""

    async def get_by_filename_and_status(
        self,
        filename: str,
        status: FileStatus,
        form_id: str | None,
    ) -> FileModel | None:
        model = self._records.get(self._key(filename, form_id))
        if model and model.status == status:
            return model.model_copy(deep=True)
        return None

    async def get_by_filename(self, filename: str, form_id: str | None) -> FileModel | None:
        model = self._records.get(self._key(filename, form_id))
        return model.model_copy(deep=True) if model else None

    async def update_or_create(self, file_model: FileModel) -> None:
        self.update_calls += 1
        self._records[self._key(file_model.filename, file_model.form_id)] = file_model.model_copy(deep=True)

    def snapshot(self, filename: str, form_id: str) -> FileModel | None:
        model = self._records.get(self._key(filename, form_id))
        return model.model_copy(deep=True) if model else None


class InMemoryDataSaveService:
    def __init__(self, file_service: InMemoryFileService) -> None:
        self._file_service = file_service
        self.rollback_calls = 0
        self.process_calls = 0

    async def rollback(self, file_model: FileModel, error: str) -> None:
        self.rollback_calls += 1
        file_model.status = FileStatus.FAILED
        file_model.error = error
        await self._file_service.update_or_create(file_model)

    async def process_and_save_all(self, file_model: FileModel, flat_data) -> None:
        self.process_calls += 1
        file_model.status = FileStatus.SUCCESS
        file_model.error = None
        await self._file_service.update_or_create(file_model)


def _build_form_info(form_id: str) -> FormInfo:
    return FormInfo(id=form_id, name=f"Test form {form_id}")


def _build_context(filename: str, form_id: str) -> UploadPipelineContext:
    upload_file = UploadFile(filename=filename, file=BytesIO(b"123"), size=3)
    return UploadPipelineContext(
        file=upload_file,
        filename=upload_file.filename or "",
        form_id=form_id,
        form_info=_build_form_info(form_id),
    )


def _print_step_result(
    step_no: int,
    title: str,
    ctx: UploadPipelineContext,
    model: FileModel | None,
    updates_before: int,
    updates_after: int,
) -> None:
    print("\n" + "=" * 88)
    print(f"STEP {step_no}/4 | {title}")
    print("-" * 88)
    print(f"ctx.failed: {ctx.failed}")
    print(f"ctx.error : {ctx.error!r}")
    if model:
        print(f"db.file_id: {model.file_id}")
        print(f"db.status : {model.status.value}")
        print(f"db.error  : {model.error!r}")
    else:
        print("db.record : None")
    print(f"files.update_or_create delta: {updates_after - updates_before} (total={updates_after})")
    print("=" * 88)


@pytest.mark.asyncio
async def test_upload_lifecycle_pipeline_full_flow_with_pretty_console_output() -> None:
    file_service = InMemoryFileService()
    data_save_service = InMemoryDataSaveService(file_service)

    # Step 1: upload fails, file record must be created and marked FAILED.
    before = file_service.update_calls
    pipeline = UploadPipelineRunner(
        steps=[AcquireFileRecordStep(file_service), FailingStep("parse error #1")],
        data_save_service=data_save_service,
    )
    ctx1 = _build_context(FILENAME, FORM_ID)
    await pipeline.run_for_file(ctx1)
    record1 = file_service.snapshot(FILENAME, FORM_ID)
    after = file_service.update_calls
    _print_step_result(1, "First upload fails and creates Files record", ctx1, record1, before, after)

    assert ctx1.failed is True
    assert record1 is not None
    assert record1.status == FileStatus.FAILED
    assert record1.error == "parse error #1"

    first_file_id = record1.file_id

    # Step 2: same file fails again, same file_id must be reused, error must be updated.
    before = file_service.update_calls
    pipeline = UploadPipelineRunner(
        steps=[AcquireFileRecordStep(file_service), FailingStep("parse error #2")],
        data_save_service=data_save_service,
    )
    ctx2 = _build_context(FILENAME, FORM_ID)
    await pipeline.run_for_file(ctx2)
    record2 = file_service.snapshot(FILENAME, FORM_ID)
    after = file_service.update_calls
    _print_step_result(2, "Second failed upload reuses record and updates error", ctx2, record2, before, after)

    assert ctx2.failed is True
    assert record2 is not None
    assert record2.file_id == first_file_id
    assert record2.status == FileStatus.FAILED
    assert record2.error == "parse error #2"

    # Step 3: successful upload must reuse same record, set SUCCESS and clear error.
    before = file_service.update_calls
    pipeline = UploadPipelineRunner(
        steps=[AcquireFileRecordStep(file_service), PersistSuccessStep(data_save_service)],
        data_save_service=data_save_service,
    )
    ctx3 = _build_context(FILENAME, FORM_ID)
    await pipeline.run_for_file(ctx3)
    record3 = file_service.snapshot(FILENAME, FORM_ID)
    after = file_service.update_calls
    _print_step_result(3, "Successful upload reuses record and clears error", ctx3, record3, before, after)

    assert ctx3.failed is False
    assert record3 is not None
    assert record3.file_id == first_file_id
    assert record3.status == FileStatus.SUCCESS
    assert record3.error is None

    # Step 4: duplicate upload after SUCCESS must not change DB state.
    before = file_service.update_calls
    pipeline = UploadPipelineRunner(
        steps=[AcquireFileRecordStep(file_service)],
        data_save_service=data_save_service,
    )
    ctx4 = _build_context(FILENAME, FORM_ID)
    await pipeline.run_for_file(ctx4)
    record4 = file_service.snapshot(FILENAME, FORM_ID)
    after = file_service.update_calls
    _print_step_result(4, "Duplicate upload is rejected with no DB updates", ctx4, record4, before, after)

    assert ctx4.failed is True
    assert "already been uploaded" in (ctx4.error or "")
    assert after == before
    assert record4 is not None
    assert record4.file_id == first_file_id
    assert record4.status == FileStatus.SUCCESS
    assert record4.error is None
