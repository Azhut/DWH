"""
Golden snapshot workflow for manual validation and deterministic fixture snapshots.

Phases:
1) visual   -> build an Excel report for manual review.
2) snapshot -> generate/update *.expected.json after table review.
3) both     -> run visual then snapshot.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from tests.scripts.golden_snapshot.runtime import (
    PROJECT_ROOT,
    build_snapshot_payload,
    checkpoint_specs_from_snapshot,
    compare_record_sets,
    count_by_section,
    load_snapshot,
    record_to_dict,
    run_upload_pipeline,
    save_snapshot,
)
from tests.scripts.golden_snapshot.table_builder import (
    build_flat_data_preview,
    build_multiindex_dataframe,
    build_summary_table,
)


DEFAULT_FORM_ID = "eab639f7-78c4-4e08-bd27-756bac5cf571"
DEFAULT_FORM_NAME = "1ФК"
DEFAULT_FIXTURE = Path("tests/fixtures/1fk/АЛАПАЕВСК 2020.xls")
DEFAULT_REPORT_OUTPUT = Path("tests/scripts/golden_snapshot/visual_report.xlsx")
DEFAULT_SNAPSHOT_OUTPUT = Path("tests/fixtures/1fk_snapshots/АЛАПАЕВСК 2020.expected.json")


def _resolve_path(path_value: str | Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def _sheet_name(base: str, suffix: str) -> str:
    safe = base.replace(":", "_").replace("/", "_").replace("\\", "_")
    limit = 31 - len(suffix)
    return f"{safe[:limit]}{suffix}"


def _load_checkpoint_specs(snapshot_path: Path, checkpoint_file: Path | None) -> list[dict[str, Any]]:
    if checkpoint_file is not None:
        payload = load_snapshot(checkpoint_file)
        if isinstance(payload, dict):
            if "checkpoints" in payload:
                return checkpoint_specs_from_snapshot(payload)
            if "items" in payload:
                return checkpoint_specs_from_snapshot({"checkpoints": payload["items"]})
        if isinstance(payload, list):
            return checkpoint_specs_from_snapshot({"checkpoints": payload})
        raise ValueError("checkpoint file must contain list or object with 'checkpoints'")

    if snapshot_path.exists():
        existing = load_snapshot(snapshot_path)
        specs = checkpoint_specs_from_snapshot(existing)
        if specs:
            return specs

    return []


def _sheet_payload(sheet) -> dict[str, Any]:
    return {
        "headers": {
            "horizontal": list(sheet.horizontal_headers),
            "vertical": list(sheet.vertical_headers),
        },
        "flat_data": [record_to_dict(record) for record in sheet.flat_data_records],
    }


def _render_visual_report(ctx, persisted_records, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    ctx_records = [record_to_dict(record) for record in ctx.flat_data]
    persisted = [record_to_dict(record) for record in persisted_records]

    api_match = compare_record_sets(ctx_records, persisted, include_file_meta=True)

    by_section_ctx = count_by_section(ctx_records)
    by_section_db = count_by_section(persisted)

    overall_rows = [
        {"Metric": "File", "Value": ctx.filename},
        {"Metric": "Form ID", "Value": ctx.form_id},
        {"Metric": "Context records", "Value": len(ctx_records)},
        {"Metric": "Persist payload records", "Value": len(persisted)},
        {"Metric": "API-equivalent match", "Value": api_match["equal"]},
        {"Metric": "Only in context", "Value": api_match["left_only_total"]},
        {"Metric": "Only in persist payload", "Value": api_match["right_only_total"]},
    ]

    by_section_rows = []
    all_sections = sorted(set(by_section_ctx) | set(by_section_db))
    for section in all_sections:
        left = by_section_ctx.get(section, 0)
        right = by_section_db.get(section, 0)
        by_section_rows.append(
            {
                "Section": section,
                "Context": left,
                "Persist payload": right,
                "Diff": right - left,
                "Match": left == right,
            }
        )

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        pd.DataFrame(overall_rows).to_excel(writer, sheet_name="Overview", index=False)
        pd.DataFrame(by_section_rows).to_excel(writer, sheet_name="BySection", index=False)

        mismatch_df = pd.DataFrame(api_match["mismatches"])
        if mismatch_df.empty:
            mismatch_df = pd.DataFrame([{"side": "", "count": 0, "signature": "No mismatches"}])
        mismatch_df.to_excel(writer, sheet_name="ApiMismatches", index=False)

        for sheet in ctx.sheets:
            sheet_payload = _sheet_payload(sheet)
            table_df, levels_count, diagnostics = build_multiindex_dataframe(
                sheet_payload,
                max_rows=10000,
                max_cols=10000,
            )
            summary_df = build_summary_table(sheet_payload, diagnostics=diagnostics)
            preview_df = build_flat_data_preview(sheet_payload, max_rows=500)

            base_name = sheet.sheet_name or sheet.sheet_fullname or "sheet"

            table_name = _sheet_name(base_name, "_tbl")
            summary_name = _sheet_name(base_name, "_stats")
            preview_name = _sheet_name(base_name, "_flat")

            table_df.to_excel(writer, sheet_name=table_name)
            summary_df.to_excel(writer, sheet_name=summary_name, index=False)
            preview_df.to_excel(writer, sheet_name=preview_name, index=False)

            ws_table = writer.sheets[table_name]
            ws_table.freeze_panes = ws_table.cell(row=levels_count + 2, column=2)

            ws_summary = writer.sheets[summary_name]
            ws_summary.freeze_panes = "A2"

            ws_preview = writer.sheets[preview_name]
            ws_preview.freeze_panes = "A2"


async def run_visual_phase(args: argparse.Namespace) -> int:
    fixture_path = _resolve_path(args.fixture)
    report_path = _resolve_path(args.report_output)

    ctx, capture = await run_upload_pipeline(
        file_path=fixture_path,
        form_id=args.form_id,
        form_name=args.form_name,
        prefer_database_form=args.use_db_form,
    )

    if ctx.failed:
        print(f"ERROR: pipeline failed for {fixture_path.name}: {ctx.error}")
        return 2

    _render_visual_report(ctx, capture.saved_flat_data, report_path)
    print(f"OK: visual report generated: {report_path}")
    return 0


async def run_snapshot_phase(args: argparse.Namespace) -> int:
    fixture_path = _resolve_path(args.fixture)
    snapshot_path = _resolve_path(args.snapshot_output)
    checkpoint_path = _resolve_path(args.checkpoint_file) if args.checkpoint_file else None

    ctx, capture = await run_upload_pipeline(
        file_path=fixture_path,
        form_id=args.form_id,
        form_name=args.form_name,
        prefer_database_form=args.use_db_form,
    )

    if ctx.failed:
        print(f"ERROR: pipeline failed for {fixture_path.name}: {ctx.error}")
        return 2

    api_match = compare_record_sets(ctx.flat_data, capture.saved_flat_data, include_file_meta=True)
    if args.strict_api and not api_match["equal"]:
        print("ERROR: context data differs from API-equivalent persist payload.")
        print(json.dumps(api_match, ensure_ascii=False, indent=2, default=str))
        return 3

    checkpoint_specs = _load_checkpoint_specs(snapshot_path, checkpoint_path)
    payload = build_snapshot_payload(
        ctx,
        checkpoints=checkpoint_specs,
        description=(
            "Golden snapshot generated from upload pipeline output. "
            "Verified against API-equivalent persist payload."
        ),
    )

    save_snapshot(snapshot_path, payload)
    print(f"OK: snapshot generated: {snapshot_path}")
    print(f"INFO: checkpoints used: {len(checkpoint_specs)}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Golden snapshot utility")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common_arguments(target: argparse.ArgumentParser) -> None:
        target.add_argument("--fixture", default=str(DEFAULT_FIXTURE))
        target.add_argument("--form-id", default=DEFAULT_FORM_ID)
        target.add_argument("--form-name", default=DEFAULT_FORM_NAME)
        target.add_argument(
            "--use-db-form",
            action="store_true",
            default=True,
            help="Load FormInfo from DB (API-compatible mode).",
        )
        target.add_argument(
            "--no-db-form",
            action="store_false",
            dest="use_db_form",
            help="Use deterministic form_name/requisites without DB.",
        )

    visual = subparsers.add_parser("visual", help="Generate manual visual report")
    add_common_arguments(visual)
    visual.add_argument("--report-output", default=str(DEFAULT_REPORT_OUTPUT))

    snapshot = subparsers.add_parser("snapshot", help="Generate expected snapshot json")
    add_common_arguments(snapshot)
    snapshot.add_argument("--snapshot-output", default=str(DEFAULT_SNAPSHOT_OUTPUT))
    snapshot.add_argument(
        "--checkpoint-file",
        default=None,
        help="Optional JSON with checkpoints (list or {'checkpoints': [...]})",
    )
    snapshot.add_argument(
        "--strict-api",
        action="store_true",
        default=True,
        help="Fail snapshot generation when pipeline and API-equivalent payload differ.",
    )
    snapshot.add_argument(
        "--no-strict-api",
        action="store_false",
        dest="strict_api",
        help="Allow snapshot generation even if API-equivalence check fails.",
    )

    both = subparsers.add_parser("both", help="Run visual + snapshot phases")
    add_common_arguments(both)
    both.add_argument("--report-output", default=str(DEFAULT_REPORT_OUTPUT))
    both.add_argument("--snapshot-output", default=str(DEFAULT_SNAPSHOT_OUTPUT))
    both.add_argument("--checkpoint-file", default=None)
    both.add_argument("--strict-api", action="store_true", default=True)
    both.add_argument("--no-strict-api", action="store_false", dest="strict_api")

    return parser


async def main_async() -> int:
    parser = build_parser()

    # IDE-friendly mode: run defaults when script is started without CLI args.
    if len(sys.argv) == 1:
        args = parser.parse_args(["both", "--use-db-form"])
        print("INFO: no CLI args detected, running default IDE mode: both --use-db-form")
    else:
        args = parser.parse_args()

    if args.command == "visual":
        return await run_visual_phase(args)

    if args.command == "snapshot":
        return await run_snapshot_phase(args)

    if args.command == "both":
        visual_code = await run_visual_phase(args)
        if visual_code != 0:
            return visual_code
        return await run_snapshot_phase(args)

    parser.error(f"Unknown command: {args.command}")
    return 1


def main() -> None:
    raise SystemExit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()





