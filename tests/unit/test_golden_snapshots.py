import json
from pathlib import Path
from io import BytesIO
from typing import List, Dict, Any, Optional
import pytest
from fastapi import UploadFile
from colorama import Fore, Style, init as colorama_init
colorama_init(autoreset=True)
from app.application.upload.pipeline import build_default_pipeline, UploadPipelineContext
from app.core.dependencies import (
    get_file_service,
    get_form_service,
    get_sheet_service,
)
from app.application.parsing.registry import get_parsing_strategy_registry
from app.domain.flat_data.models import FlatDataRecord

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"
SNAPSHOTS_DIR = FIXTURES_DIR / "1fk_snapshots"


class NoOpDataSaveService:
    async def process_and_save_all(self, file_model, flat_data=None):
        return None

    async def rollback(self, file_model, error: str):
        return None

    async def save_file(self, file_model):
        return None

FK1_FORM_ID = "eab639f7-78c4-4e08-bd27-756bac5cf571"

TEST_FILES: List[tuple[str, str, str]] = [
    (
        "1fk/АЛАПАЕВСК 2020.xls",
        "АЛАПАЕВСК 2020.expected.json",
        FK1_FORM_ID,
    ),
]


def load_snapshot(snapshot_name: str) -> dict:
    snapshot_path = SNAPSHOTS_DIR / snapshot_name
    if not snapshot_path.exists():
        pytest.skip(
            f"Snapshot {snapshot_path} отсутствует, сгенерируй его "
            "скриптом scripts/golden_snapshot/generate_golden_snapshot.py"
        )
    with open(snapshot_path, "r", encoding="utf-8") as f:
        return json.load(f)


async def run_parsing_pipeline(file_path: Path, form_id: str) -> UploadPipelineContext:
    with open(file_path, "rb") as f:
        content = f.read()

    upload_file = UploadFile(
        filename=file_path.name,
        file=BytesIO(content),
        size=len(content),
    )

    form_service = get_form_service()
    form_info = await form_service.get_form_or_raise(form_id)

    file_service = get_file_service()
    data_save_service = NoOpDataSaveService()
    sheet_service = get_sheet_service()

    get_parsing_strategy_registry(sheet_service=sheet_service)

    pipeline = build_default_pipeline(
        file_service=file_service,
        data_save_service=data_save_service,
    )

    ctx = UploadPipelineContext(
        file=upload_file,
        form_id=form_id,
        form_info=form_info,
    )

    await pipeline.run_for_file(ctx)
    return ctx


def count_by_section(flat_data: List[FlatDataRecord]) -> Dict[str, int]:
    by_section: Dict[str, int] = {}
    for rec in flat_data:
        section = rec.section or "unknown"
        by_section[section] = by_section.get(section, 0) + 1
    return by_section


def find_checkpoint_value(
    flat_data: List[FlatDataRecord],
    section: str,
    row: str,
    column: str,
) -> Optional[Any]:
    for rec in flat_data:
        if rec.section == section and rec.row == row and rec.column == column:
            return rec.value
    return None


def check(name: str, expected: Any, actual: Any):
    exp_str = Fore.GREEN + repr(expected) + Style.RESET_ALL
    act_color = Fore.CYAN if expected == actual else Fore.RED
    act_str = act_color + repr(actual) + Style.RESET_ALL
    print(f"   {name:25} expected={exp_str:<15} actual={act_str}")
    assert actual == expected, f"{name}: ожидалось {expected!r}, получено {actual!r}"


def check_sheet(name: str, exp_h: int, exp_v: int, act_h: int, act_v: int):
    h_exp = Fore.GREEN + str(exp_h) + Style.RESET_ALL
    v_exp = Fore.GREEN + str(exp_v) + Style.RESET_ALL
    h_act = (Fore.CYAN + str(act_h) + Style.RESET_ALL if exp_h == act_h else Fore.RED + str(act_h) + Style.RESET_ALL)
    v_act = (Fore.CYAN + str(act_v) + Style.RESET_ALL if exp_v == act_v else Fore.RED + str(act_v) + Style.RESET_ALL)
    print(
        f"   {name:20} h exp={h_exp:<5} act={h_act:<5}  v exp={v_exp:<5} act={v_act}"
    )
    assert act_h == exp_h, f"{name}.h: ожидалось {exp_h}, получено {act_h}"
    assert act_v == exp_v, f"{name}.v: ожидалось {exp_v}, получено {act_v}"


def check_checkpoint(section: str, row: str, column: str, expected: Any, actual: Any):
    exp_col = Fore.GREEN + repr(expected) + Style.RESET_ALL
    act_color = Fore.CYAN if expected == actual else Fore.RED
    act_col = act_color + repr(actual) + Style.RESET_ALL
    name = f"cp[{section}/{row}/{column}]"
    print(f"   {name:30} expected={exp_col:<15} actual={act_col}")
    if expected is None:
        assert actual is not None, f"{name}: значение не найдено"
    else:
        assert actual == expected, f"{name}: ожидалось {expected!r}, получено {actual!r}"


class TestGoldenSnapshots:

    @pytest.mark.asyncio
    @pytest.mark.parametrize("file_name,snapshot_name,form_id", TEST_FILES)
    async def test_file_parsing_matches_snapshot(
            self,
            file_name: str,
            snapshot_name: str,
            form_id: str,
    ):
        file_path = FIXTURES_DIR / file_name
        expected = load_snapshot(snapshot_name)

        print(f"\n🔍 Тест: {file_name} ({snapshot_name})")
        assert file_path.exists(), f"Файл не найден: {file_path}"

        ctx = await run_parsing_pipeline(file_path, form_id)
        assert not ctx.failed, f"Pipeline упал: {ctx.error}"

        flat_data = ctx.flat_data  # property — агрегация из ctx.sheets

        # 1. year
        actual_year = flat_data[0].year if flat_data else None
        check("year", expected["stats"]["year"], actual_year)

        # 2. reporter
        actual_reporter = flat_data[0].reporter if flat_data else None
        check("reporter", expected["stats"]["reporter"], actual_reporter)

        # 3. total_flat_records
        check("total_flat_records", expected["stats"]["total_flat_records"], len(flat_data))

        # 4. total_sheets
        by_section = count_by_section(flat_data)
        check("total_sheets", expected["stats"]["total_sheets"], len(by_section))

        # 5. by_section
        for section, count in expected["stats"]["by_section"].items():
            check(f"by_section[{section}]", count, by_section.get(section, 0))

        # 6. sheets (headers_count)
        for expected_sheet in expected["sheets"]:
            actual_sheet = next(
                (s for s in ctx.sheets if s.sheet_name == expected_sheet["name"]),
                None,
            )
            assert actual_sheet is not None, f"Лист {expected_sheet['name']} не найден"

            check_sheet(
                expected_sheet["name"],
                expected_sheet["headers_count"]["horizontal"],
                expected_sheet["headers_count"]["vertical"],
                len(actual_sheet.horizontal_headers),
                len(actual_sheet.vertical_headers),
            )

        # 7. checkpoints
        for cp in expected["checkpoints"]:
            actual_value = find_checkpoint_value(
                flat_data,
                cp["section"],
                cp["row"],
                cp["column"],
            )
            check_checkpoint(
                cp["section"], cp["row"], cp["column"], cp.get("value"), actual_value
            )

        print("✅ Тест завершён\n")