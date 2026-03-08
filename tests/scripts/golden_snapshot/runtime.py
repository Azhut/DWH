from __future__ import annotations

import copy
import json
import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from fastapi import UploadFile

from app.application.parsing.registry import (
    ParsingStrategyRegistry,
    get_parsing_strategy_registry,
)
from app.application.upload.pipeline import UploadPipelineContext, build_default_pipeline
from app.core.dependencies import get_form_service
from app.domain.file.models import FileStatus
from app.domain.file.service import validate_and_extract_metadata_from_filename
from app.domain.flat_data.models import FlatDataRecord
from app.domain.form.models import FormInfo, FormType, detect_form_type


PROJECT_ROOT = Path(__file__).resolve().parents[3]
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"


def _with_runtime_requisites_defaults(form_info: FormInfo) -> FormInfo:
    requisites = dict(form_info.requisites or {})
    if form_info.type == FormType.FK_1 and "skip_sheets" not in requisites:
        requisites["skip_sheets"] = [0]
    form_info.requisites = requisites
    return form_info


@dataclass(slots=True)
class CaptureDataSaveService:
    """
    In-memory replacement for persistence in PersistStep.
    Keeps the exact payload that would be sent to DataSaveService.
    """

    saved_file_model: Any | None = None
    saved_flat_data: list[FlatDataRecord] = field(default_factory=list)
    rollback_calls: list[dict[str, Any]] = field(default_factory=list)
    save_file_calls: list[Any] = field(default_factory=list)

    async def process_and_save_all(self, file_model, flat_data=None):
        self.saved_file_model = copy.deepcopy(file_model)
        self.saved_flat_data = []
        for record in flat_data or []:
            if isinstance(record, FlatDataRecord):
                self.saved_flat_data.append(record.model_copy(deep=True))
            else:
                self.saved_flat_data.append(FlatDataRecord(**record))
        return None

    async def rollback(self, file_model, error: str):
        self.rollback_calls.append(
            {
                "file_model": copy.deepcopy(file_model),
                "error": error,
            }
        )
        return None

    async def save_file(self, file_model):
        self.save_file_calls.append(copy.deepcopy(file_model))
        return None


@dataclass(slots=True)
class InMemoryFileService:
    """
    Minimal in-memory replacement for FileService methods used in upload pipeline.
    Useful for deterministic fixture runs without DB side effects.
    """

    _by_filename_form: dict[tuple[str, str | None], Any] = field(default_factory=dict)
    _by_file_id: dict[str, Any] = field(default_factory=dict)

    async def get_by_filename_and_status(
        self,
        filename: str,
        status: FileStatus,
        form_id: str | None = None,
    ):
        model = self._by_filename_form.get((filename, form_id))
        if model and getattr(model, "status", None) == status:
            return copy.deepcopy(model)
        return None

    async def get_by_filename(self, filename: str, form_id: str | None = None):
        model = self._by_filename_form.get((filename, form_id))
        return copy.deepcopy(model) if model else None

    async def update_or_create(self, file_model) -> None:
        snapshot = copy.deepcopy(file_model)
        self._by_filename_form[(snapshot.filename, snapshot.form_id)] = snapshot
        self._by_file_id[snapshot.file_id] = snapshot

    def validate_and_extract_metadata_from_filename(self, filename: str):
        return validate_and_extract_metadata_from_filename(filename)


async def resolve_form_info(
    form_id: str,
    *,
    form_name: str | None = None,
    requisites: Mapping[str, Any] | None = None,
    prefer_database: bool = True,
) -> FormInfo:
    """
    Resolve FormInfo with API-compatible priority:
    1) DB form by form_id (when prefer_database=True)
    2) deterministic fallback from form_name (if provided)
    """
    if prefer_database:
        db_error: Exception | None = None
        try:
            form_service = get_form_service()
            db_form = await form_service.get_form(form_id)
            if db_form is not None:
                if requisites:
                    merged = dict(db_form.requisites or {})
                    merged.update(dict(requisites))
                    db_form.requisites = merged
                return _with_runtime_requisites_defaults(db_form)
        except Exception as exc:
            db_error = exc

        if form_name is None:
            if db_error is not None:
                raise db_error
            raise ValueError(f"Form '{form_id}' not found in DB and no form_name fallback provided")

    if form_name is None:
        raise ValueError("form_name must be provided when prefer_database=False")

    return _with_runtime_requisites_defaults(
        FormInfo(
            id=form_id,
            name=form_name,
            type=detect_form_type(form_name),
            requisites=dict(requisites or {}),
        )
    )


async def run_upload_pipeline(
    *,
    file_path: Path,
    form_id: str,
    form_name: str | None = None,
    requisites: Mapping[str, Any] | None = None,
    parsing_registry: ParsingStrategyRegistry | None = None,
    data_save_service: Any | None = None,
    file_service: Any | None = None,
    prefer_database_form: bool = True,
) -> tuple[UploadPipelineContext, CaptureDataSaveService]:
    """
    Runs exactly the same upload pipeline chain as API, with replaceable persistence.
    Returns pipeline context and capture service (for API-equivalence checks).
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Fixture not found: {file_path}")

    with open(file_path, "rb") as handle:
        content = handle.read()

    upload_file = UploadFile(
        filename=file_path.name,
        file=BytesIO(content),
        size=len(content),
    )

    form_info = await resolve_form_info(
        form_id,
        form_name=form_name,
        requisites=requisites,
        prefer_database=prefer_database_form,
    )

    in_memory_file_service = file_service or InMemoryFileService()
    capture = data_save_service if data_save_service is not None else CaptureDataSaveService()
    registry = parsing_registry or get_parsing_strategy_registry()

    pipeline = build_default_pipeline(
        file_service=in_memory_file_service,
        data_save_service=capture,
        parsing_registry=registry,
    )

    ctx = UploadPipelineContext(
        file=upload_file,
        form_id=form_id,
        form_info=form_info,
        filename=file_path.name,
    )
    await pipeline.run_for_file(ctx)
    return ctx, capture


def load_snapshot(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def save_snapshot(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return int(value)
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8")
        except Exception:
            return str(value)
    return value


def normalize_year(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return int(value)
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except Exception:
        return None


def record_to_dict(record: FlatDataRecord | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(record, FlatDataRecord):
        doc = record.model_dump(exclude_none=False)
    else:
        doc = dict(record)
    return {
        "year": normalize_year(doc.get("year")),
        "reporter": doc.get("reporter"),
        "section": doc.get("section"),
        "row": doc.get("row"),
        "column": doc.get("column"),
        "value": normalize_value(doc.get("value")),
        "file_id": doc.get("file_id"),
        "form": doc.get("form"),
    }


def record_signature(
    record: FlatDataRecord | Mapping[str, Any],
    *,
    include_file_meta: bool = False,
) -> tuple[Any, ...]:
    doc = record_to_dict(record)
    base = (
        doc["year"],
        doc["reporter"],
        doc["section"],
        doc["row"],
        doc["column"],
        doc["value"],
    )
    if include_file_meta:
        return base + (doc["file_id"], doc["form"])
    return base


def count_by_section(records: Iterable[FlatDataRecord | Mapping[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}
    for record in records:
        section = record_to_dict(record).get("section") or "unknown"
        result[section] = result.get(section, 0) + 1
    return result


def find_checkpoint_value(
    records: Sequence[FlatDataRecord | Mapping[str, Any]],
    *,
    section: str | None,
    row: str,
    column: str,
) -> Any:
    for record in records:
        item = record_to_dict(record)
        if section is not None and item.get("section") != section:
            continue
        if item.get("row") == row and item.get("column") == column:
            return item.get("value")
    return None


def compare_record_sets(
    left: Sequence[FlatDataRecord | Mapping[str, Any]],
    right: Sequence[FlatDataRecord | Mapping[str, Any]],
    *,
    include_file_meta: bool = False,
    max_mismatches: int = 10,
) -> dict[str, Any]:
    left_counter = Counter(
        record_signature(item, include_file_meta=include_file_meta) for item in left
    )
    right_counter = Counter(
        record_signature(item, include_file_meta=include_file_meta) for item in right
    )

    only_left = left_counter - right_counter
    only_right = right_counter - left_counter

    mismatches: list[dict[str, Any]] = []
    for signature, count in only_left.items():
        mismatches.append({"side": "left_only", "count": count, "signature": signature})
        if len(mismatches) >= max_mismatches:
            break
    if len(mismatches) < max_mismatches:
        for signature, count in only_right.items():
            mismatches.append({"side": "right_only", "count": count, "signature": signature})
            if len(mismatches) >= max_mismatches:
                break

    return {
        "equal": not only_left and not only_right,
        "left_count": sum(left_counter.values()),
        "right_count": sum(right_counter.values()),
        "left_unique": len(left_counter),
        "right_unique": len(right_counter),
        "left_only_total": sum(only_left.values()),
        "right_only_total": sum(only_right.values()),
        "mismatches": mismatches,
    }


def checkpoint_specs_from_snapshot(snapshot: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not snapshot:
        return []
    seen: set[tuple[Any, Any, Any]] = set()
    specs: list[dict[str, Any]] = []
    for checkpoint in snapshot.get("checkpoints", []):
        section = checkpoint.get("section")
        row = checkpoint.get("row")
        column = checkpoint.get("column")
        if not row or not column:
            continue
        key = (section, row, column)
        if key in seen:
            continue
        seen.add(key)
        specs.append({"section": section, "row": row, "column": column})
    return specs


def build_snapshot_payload(
    ctx: UploadPipelineContext,
    *,
    checkpoints: Sequence[Mapping[str, Any]] | None = None,
    description: str = "Golden snapshot generated from upload pipeline output.",
) -> dict[str, Any]:
    records = list(ctx.flat_data)
    normalized = [record_to_dict(record) for record in records]

    year = next((item["year"] for item in normalized if item.get("year") is not None), None)
    reporter = next(
        (item["reporter"] for item in normalized if item.get("reporter") is not None),
        None,
    )

    sections = count_by_section(records)
    payload = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "file_name": ctx.file.filename,
            "form_id": ctx.form_id,
            "description": description,
        },
        "stats": {
            "year": year,
            "reporter": reporter,
            "total_flat_records": len(records),
            "total_sheets": len(sections),
            "by_section": sections,
        },
        "sheets": [],
        "checkpoints": [],
    }

    for sheet in ctx.sheets:
        sheet_name = sheet.sheet_name or sheet.sheet_fullname
        payload["sheets"].append(
            {
                "name": sheet_name,
                "flat_records_count": sections.get(sheet_name, 0),
                "headers_count": {
                    "horizontal": len(sheet.horizontal_headers),
                    "vertical": len(sheet.vertical_headers),
                },
                "first_horizontal": sheet.horizontal_headers[0] if sheet.horizontal_headers else "",
                "first_vertical": sheet.vertical_headers[0] if sheet.vertical_headers else "",
            }
        )

    for checkpoint in checkpoints or []:
        section = checkpoint.get("section")
        row = checkpoint.get("row")
        column = checkpoint.get("column")
        if not row or not column:
            continue
        payload["checkpoints"].append(
            {
                "year": year,
                "reporter": reporter,
                "section": section,
                "row": row,
                "column": column,
                "value": find_checkpoint_value(
                    records,
                    section=section,
                    row=row,
                    column=column,
                ),
            }
        )

    return payload






