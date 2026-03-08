import json
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from tests.scripts.golden_snapshot.runtime import (
    build_snapshot_payload,
    checkpoint_specs_from_snapshot,
    compare_record_sets,
    load_snapshot,
    run_upload_pipeline,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class SnapshotCase:
    fixture_path: str
    snapshot_path: str
    form_id: str
    form_name: str
    requisites: dict[str, object] = field(default_factory=dict)


SNAPSHOT_CASES = [
    SnapshotCase(
        fixture_path="tests/fixtures/1fk/АЛАПАЕВСК 2020.xls",
        snapshot_path="tests/fixtures/1fk_snapshots/АЛАПАЕВСК 2020.expected.json",
        form_id="eab639f7-78c4-4e08-bd27-756bac5cf571",
        form_name="1ФК",
        requisites={"skip_sheets": [0]},
    ),
]


def _case_id(case: SnapshotCase) -> str:
    return Path(case.fixture_path).name


async def _run_case(case: SnapshotCase):
    fixture_path = PROJECT_ROOT / case.fixture_path
    ctx, capture = await run_upload_pipeline(
        file_path=fixture_path,
        form_id=case.form_id,
        form_name=case.form_name,
        requisites=case.requisites,
        prefer_database_form=True,
    )
    assert not ctx.failed, f"Pipeline failed: {ctx.error}"
    return ctx, capture


@pytest.mark.asyncio
@pytest.mark.parametrize("case", SNAPSHOT_CASES, ids=_case_id)
async def test_snapshot_matches_pipeline_output(case: SnapshotCase):
    snapshot_path = PROJECT_ROOT / case.snapshot_path
    if not snapshot_path.exists():
        pytest.skip(
            f"Snapshot not found: {snapshot_path}. "
            "Generate it with tests/scripts/golden_snapshot/generate_golden_snapshot.py"
        )

    expected = load_snapshot(snapshot_path)
    checkpoint_specs = checkpoint_specs_from_snapshot(expected)

    ctx, _ = await _run_case(case)
    actual = build_snapshot_payload(
        ctx,
        checkpoints=checkpoint_specs,
        description=expected.get("meta", {}).get("description", ""),
    )

    assert actual["stats"] == expected["stats"]
    assert actual["sheets"] == expected["sheets"]
    assert actual["checkpoints"] == expected["checkpoints"]
    assert actual["meta"]["file_name"] == expected["meta"]["file_name"]
    assert actual["meta"]["form_id"] == expected["meta"]["form_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", SNAPSHOT_CASES, ids=_case_id)
async def test_pipeline_payload_is_api_equivalent(case: SnapshotCase):
    ctx, capture = await _run_case(case)

    comparison = compare_record_sets(
        ctx.flat_data,
        capture.saved_flat_data,
        include_file_meta=True,
    )

    assert comparison["equal"], json.dumps(
        comparison,
        ensure_ascii=False,
        indent=2,
        default=str,
    )




