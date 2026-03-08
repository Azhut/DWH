"""
Compare parsed fixture flat_data with data already stored in MongoDB.

Usage example:
python tests/scripts/golden_snapshot/compare_with_db.py ^
  --fixture tests/fixtures/1fk/АЛАПАЕВСК 2020.xls ^
  --form-id eab639f7-78c4-4e08-bd27-756bac5cf571 ^
  --form-name 1ФК ^
  --file-id <FILE_ID_FROM_DB> ^
  --report-output tests/scripts/golden_snapshot/db_compare_report.xlsx
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

import pandas as pd
from motor.motor_asyncio import AsyncIOMotorClient

from config.config import config
from tests.scripts.golden_snapshot.runtime import (
    PROJECT_ROOT,
    compare_record_sets,
    count_by_section,
    record_to_dict,
    run_upload_pipeline,
)


DEFAULT_FORM_ID = "eab639f7-78c4-4e08-bd27-756bac5cf571"
DEFAULT_FORM_NAME = "1ФК"
DEFAULT_FIXTURE = Path("tests/fixtures/1fk/АЛАПАЕВСК 2020.xls")
DEFAULT_REPORT_OUTPUT = Path("tests/scripts/golden_snapshot/db_compare_report.xlsx")


def _resolve_path(path_value: str | Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


async def _fetch_db_payload(file_id: str) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    client = AsyncIOMotorClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        await client.admin.command("ping")
        database = client[config.DATABASE_NAME]
        flat_data = await database.FlatData.find({"file_id": file_id}, {"_id": 0}).to_list(length=None)
        file_doc = await database.Files.find_one({"file_id": file_id}, {"_id": 0})
        return flat_data, file_doc
    finally:
        client.close()


def _build_report(
    *,
    ctx_records: list[dict[str, Any]],
    db_records: list[dict[str, Any]],
    comparison: dict[str, Any],
    file_doc: dict[str, Any] | None,
    report_path: Path,
    max_preview: int,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)

    by_section_ctx = count_by_section(ctx_records)
    by_section_db = count_by_section(db_records)

    section_rows = []
    for section in sorted(set(by_section_ctx) | set(by_section_db)):
        left = by_section_ctx.get(section, 0)
        right = by_section_db.get(section, 0)
        section_rows.append(
            {
                "Section": section,
                "Context": left,
                "MongoDB": right,
                "Diff": right - left,
                "Match": left == right,
            }
        )

    overview_rows = [
        {"Metric": "Mongo URI", "Value": config.MONGO_URI},
        {"Metric": "Database", "Value": config.DATABASE_NAME},
        {"Metric": "Context records", "Value": len(ctx_records)},
        {"Metric": "MongoDB records", "Value": len(db_records)},
        {"Metric": "Equivalent", "Value": comparison["equal"]},
        {"Metric": "Only in context", "Value": comparison["left_only_total"]},
        {"Metric": "Only in DB", "Value": comparison["right_only_total"]},
        {
            "Metric": "File in DB",
            "Value": file_doc.get("filename") if file_doc else "not found",
        },
        {
            "Metric": "Sheets in DB",
            "Value": ", ".join(file_doc.get("sheets", [])) if file_doc else "",
        },
    ]

    mismatch_df = pd.DataFrame(comparison.get("mismatches", []))
    if mismatch_df.empty:
        mismatch_df = pd.DataFrame([{"side": "", "count": 0, "signature": "No mismatches"}])

    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        pd.DataFrame(overview_rows).to_excel(writer, sheet_name="Overview", index=False)
        pd.DataFrame(section_rows).to_excel(writer, sheet_name="BySection", index=False)
        mismatch_df.to_excel(writer, sheet_name="Mismatches", index=False)
        pd.DataFrame(ctx_records[:max_preview]).to_excel(writer, sheet_name="CtxPreview", index=False)
        pd.DataFrame(db_records[:max_preview]).to_excel(writer, sheet_name="DbPreview", index=False)


async def main_async() -> int:
    parser = argparse.ArgumentParser(description="Compare parsed fixture with MongoDB records")
    parser.add_argument("--fixture", default=str(DEFAULT_FIXTURE))
    parser.add_argument("--form-id", default=DEFAULT_FORM_ID)
    parser.add_argument("--form-name", default=DEFAULT_FORM_NAME)
    parser.add_argument("--file-id", required=True)
    parser.add_argument("--report-output", default=str(DEFAULT_REPORT_OUTPUT))
    parser.add_argument("--max-preview", type=int, default=1000)
    parser.add_argument(
        "--use-db-form",
        action="store_true",
        help="Load FormInfo from DB instead of deterministic --form-name.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return non-zero exit code when data differs.",
    )
    args = parser.parse_args()

    fixture_path = _resolve_path(args.fixture)
    report_path = _resolve_path(args.report_output)

    ctx, _ = await run_upload_pipeline(
        file_path=fixture_path,
        form_id=args.form_id,
        form_name=args.form_name,
        prefer_database_form=args.use_db_form,
    )
    if ctx.failed:
        print(f"ERROR: pipeline failed for fixture {fixture_path.name}: {ctx.error}")
        return 2

    db_records_raw, file_doc = await _fetch_db_payload(args.file_id)

    ctx_records = [record_to_dict(record) for record in ctx.flat_data]
    db_records = [record_to_dict(record) for record in db_records_raw]

    comparison = compare_record_sets(ctx_records, db_records, include_file_meta=False)

    _build_report(
        ctx_records=ctx_records,
        db_records=db_records,
        comparison=comparison,
        file_doc=file_doc,
        report_path=report_path,
        max_preview=max(1, args.max_preview),
    )

    print("INFO: comparison summary")
    print(json.dumps(comparison, ensure_ascii=False, indent=2, default=str))
    print(f"OK: report generated: {report_path}")

    if args.strict and not comparison["equal"]:
        return 3
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()

