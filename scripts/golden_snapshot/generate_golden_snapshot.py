"""
Генератор Golden Snapshots для тестов парсинга.
Минималистичный вариант: пути правятся вручную в этом файле.

Использование:
    1. Отредактируй TEST_FILE_PATH, FORM_ID ниже
    2. Проверь visual_report.xlsx и .expected.json
"""
from pathlib import Path
from tests import FORM_ID
import asyncio
import json
from io import BytesIO
from typing import List, Dict, Any
from app.application.parsing.registry import get_parsing_strategy_registry
import pandas as pd
from fastapi import UploadFile
from app.application.upload.pipeline import build_default_pipeline, UploadPipelineContext
from app.core.dependencies import (
    get_file_service,
    get_form_service,
    get_sheet_service,
)


TEST_FILE_PATH = Path("../../tests/fixtures/1fk/АЛАПАЕВСК 2020.xls")
FORM_ID = FORM_ID
OUTPUT_EXCEL_PATH = Path(__file__).parent / "visual_report.xlsx"
OUTPUT_SNAPSHOT_PATH = Path("../../tests/fixtures/1fk_snapshots/АЛАПАЕВСК 2020.expected.json")



CHECKPOINTS: List[Dict[str, Any]] = [
    {
        "year": 2020,
        "reporter": "АЛАПАЕВСК",
        "section": "Раздел3",
        "row": "Площадь спортивных залов (м2)",
        "column": "Единовременная пропускная способность (человек) | из них в сельской местности",
        "value": "X",
    },
    {
        "year": 2020,
        "reporter": "АЛАПАЕВСК",
        "section": "Раздел2",
        "row": "Количество физкультурных и спортивных мероприятий, проведенных организацией самостоятельно в отчетный период",
        "column": "Справочно",
        "value": "255.0",
    },
    {
        "year": 2020,
        "reporter": "АЛАПАЕВСК",
        "section": "Раздел2",
        "row": "общеобразовательные организации",
        "column": "Численность занимающихся физической культурой и спортом (человек) | из общей численности занимающихся (гр. 4): | в возрасте | 3 - 15лет",
        "value": 3739,
    },
    {
        "year": 2020,
        "reporter": "АЛАПАЕВСК",
        "section": "Раздел7",
        "row": "Присвоено спортивных разрядов",
        "column": "Всего",
        "value": 49,
    }
]



class NoOpDataSaveService:
    async def process_and_save_all(self, file_model, flat_data=None):
        return None

    async def rollback(self, file_model, error: str):
        return None

    async def save_file(self, file_model):
        return None



async def run_pipeline_simulation(file_path: Path, form_id: str) -> UploadPipelineContext:
    print(f"🚀 Запуск pipeline для файла: {file_path.name}")

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

    try:
        await pipeline.run_for_file(ctx)
    except Exception as e:
        print(f"⚠️ Pipeline завершил с ошибкой (возможно ожидаемой): {e}")

    return ctx


def generate_visual_report(ctx: UploadPipelineContext, output_path: Path) -> None:
    print(f"📊 Генерация визуального отчета: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    from scripts.golden_snapshot.table_builder import (
        build_multiindex_dataframe,
        build_summary_table,
        build_flat_data_preview,
        write_multiindex_excel,
    )

    flat_data = ctx.flat_data  # property — агрегация из ctx.sheets
    total_records = len(flat_data)

    by_section: Dict[str, int] = {}
    for rec in flat_data:
        section = rec.section or "unknown"
        by_section[section] = by_section.get(section, 0) + 1

    numeric_values = [rec.value for rec in flat_data if isinstance(rec.value, (int, float))]
    numeric_sum = sum(numeric_values) if numeric_values else 0
    numeric_count = len(numeric_values)
    empty_count = sum(1 for rec in flat_data if rec.value is None or rec.value == "")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:  # type: ignore
        # Вкладка 0: общая статистика
        overall_stats = {
            "Метрика": [
                "Всего записей", "Заполнено", "Пустых", "% заполненности",
                "Сумма числовых", "Среднее числовое", "Количество листов",
            ],
            "Значение": [
                total_records,
                total_records - empty_count,
                empty_count,
                f"{((total_records - empty_count) / total_records * 100) if total_records > 0 else 0:.1f}%",
                round(numeric_sum, 2) if numeric_count > 0 else "N/A",
                round(numeric_sum / numeric_count, 2) if numeric_count > 0 else "N/A",
                len(by_section),
            ],
        }
        pd.DataFrame(overall_stats).to_excel(writer, sheet_name="Общая_статистика", index=False)

        # Вкладка 1: статистика по листам
        section_stats = [
            {"Лист": section, "Записей": count}
            for section, count in sorted(by_section.items())
        ]
        pd.DataFrame(section_stats).to_excel(writer, sheet_name="По_листам", index=False)

        # Вкладки по каждому листу — используем ctx.sheets (List[SheetModel])
        for sheet in ctx.sheets:
            sheet_name_clean = (sheet.sheet_name or sheet.sheet_fullname)[:30].replace(":", "_").replace("/", "_")

            # Строим sheet_data в формате, который ожидает table_builder
            sheet_data = {
                "headers": {
                    "horizontal": sheet.horizontal_headers,
                    "vertical": sheet.vertical_headers,
                },
                "data": [
                    {
                        "column_header": rec.column,
                        "values": [{"row_header": rec.row, "value": rec.value}],
                    }
                    for rec in sheet.flat_data_records
                ],
            }

            try:
                pivot_df, n_levels = build_multiindex_dataframe(sheet_data, max_rows=10000, max_cols=10000)
                temp_path = output_path.parent / f"_temp_{sheet_name_clean}.xlsx"
                write_multiindex_excel(pivot_df, str(temp_path), sheet_name=f"{sheet_name_clean}_Таблица")
                temp_df = pd.read_excel(temp_path, sheet_name=0)
                temp_df.to_excel(writer, sheet_name=f"{sheet_name_clean}_Таблица", index=False)
                temp_path.unlink()
                print(f"  ✅ {sheet_name_clean}_Таблица: {pivot_df.shape[0]} × {pivot_df.shape[1]} ({n_levels} уровней)")
            except Exception as e:
                print(f"  ⚠️ Не удалось создать таблицу для {sheet_name_clean}: {e}")

            try:
                summary_df = build_summary_table(sheet_data)
                summary_df.to_excel(writer, sheet_name=f"{sheet_name_clean}_Статистика", index=False)
                print(f"  ✅ {sheet_name_clean}_Статистика: сохранено")
            except Exception as e:
                print(f"  ⚠️ Не удалось создать статистику для {sheet_name_clean}: {e}")

            try:
                flat_df = build_flat_data_preview(sheet_data, max_rows=200)
                if not flat_df.empty:
                    flat_df.to_excel(writer, sheet_name=f"{sheet_name_clean}_Данные", index=False)
                    print(f"  ✅ {sheet_name_clean}_Данные: {len(flat_df)} записей")
                else:
                    print(f"  ⚠️ {sheet_name_clean}_Данные: пустой лист")
            except Exception as e:
                print(f"  ⚠️ Не удалось создать данные для {sheet_name_clean}: {e}")

    print(f"✅ Отчёт сохранён: {output_path}")
    print(f"📊 Всего записей (flat_data): {total_records}")


def generate_snapshot(
        ctx: UploadPipelineContext,
        output_path: Path,
        checkpoints: List[Dict[str, Any]],
) -> None:
    print(f"📸 Генерация JSON-снапшота: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    from datetime import datetime

    flat_data = ctx.flat_data  # property — агрегация из ctx.sheets
    total_records = len(flat_data)

    year = flat_data[0].year if flat_data and flat_data[0].year else None
    reporter = flat_data[0].reporter if flat_data and flat_data[0].reporter else None

    by_section: Dict[str, int] = {}
    for rec in flat_data:
        section = rec.section or "unknown"
        by_section[section] = by_section.get(section, 0) + 1

    snapshot = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "file_name": ctx.file.filename,
            "form_id": ctx.form_id,
            "description": "Golden Master. Статистика из ctx.flat_data (как в БД).",
        },
        "stats": {
            "year": year,
            "reporter": reporter,
            "total_flat_records": total_records,
            "total_sheets": len(by_section),
            "by_section": by_section,
        },
        "sheets": [],
        "checkpoints": [],
    }

    # Детали по листам — из ctx.sheets (List[SheetModel])
    for sheet in ctx.sheets:
        sheet_name = sheet.sheet_name or sheet.sheet_fullname
        sheet_info = {
            "name": sheet_name,
            "flat_records_count": by_section.get(sheet_name, 0),
            "headers_count": {
                "horizontal": len(sheet.horizontal_headers),
                "vertical": len(sheet.vertical_headers),
            },
            "first_horizontal": sheet.horizontal_headers[0] if sheet.horizontal_headers else "",
            "first_vertical": sheet.vertical_headers[0] if sheet.vertical_headers else "",
        }
        snapshot["sheets"].append(sheet_info)

    # Контрольные точки
    for cp in checkpoints:
        found = None
        for rec in flat_data:
            if (rec.section == cp.get("section") and
                    rec.row == cp["row"] and
                    rec.column == cp["column"]):
                found = rec.value
                break

        snapshot["checkpoints"].append({
            "year": year,
            "reporter": reporter,
            "section": cp.get("section"),
            "row": cp["row"],
            "column": cp["column"],
            "value": found,
        })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    print(f"✅ Снапшот сохранён: {output_path}")


async def main():
    if not TEST_FILE_PATH.exists():
        print(f"❌ Файл не найден: {TEST_FILE_PATH.absolute()}")
        print("💡 Проверь путь и запусти скрипт из корня проекта")
        return

    ctx = await run_pipeline_simulation(TEST_FILE_PATH, FORM_ID)

    if ctx.failed:
        print(f"❌ Pipeline упал: {ctx.error}")
        print("Исправь ошибки парсинга перед генерацией снапшота!")
        return

    generate_visual_report(ctx, OUTPUT_EXCEL_PATH)
    generate_snapshot(ctx, OUTPUT_SNAPSHOT_PATH, CHECKPOINTS)

    print("\n" + "=" * 70)
    print("✅ ГОТОВО!")
    print("=" * 70)
    print(f"1. Открой: {OUTPUT_EXCEL_PATH.absolute()}")
    print(f"2. Проверь вкладку 'Общая_статистика' и 'По_листам'")
    print(f"3. Если всё ок — закоммить: {OUTPUT_SNAPSHOT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())