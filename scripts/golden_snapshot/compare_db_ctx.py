"""
Скрипт для сравнения flat_data из контекста и из MongoDB.
"""
import asyncio
import sys
from pathlib import Path
from typing import Dict, List, Any, Tuple
from io import BytesIO
from fastapi import UploadFile
from app.application.upload.pipeline import build_default_pipeline, UploadPipelineContext
from app.core.dependencies import (
    get_file_service,
    get_form_service,
    get_sheet_service,
    get_data_save_service,
)
from motor.motor_asyncio import AsyncIOMotorClient
from config.config import config
from app.application.parsing.registry import get_parsing_strategy_registry

# === КОНФИГУРАЦИЯ ===
TEST_FILE_PATH = Path("../../tests/fixtures/1fk/АЛАПАЕВСК 2020.xls")
FORM_ID = "eab639f7-78c4-4e08-bd27-756bac5cf571"
FILE_ID = None


async def get_flat_data_from_ctx(file_path: Path, form_id: str) -> Tuple[UploadPipelineContext, int, Dict[str, int]]:
    """
    Запускает pipeline и возвращает контекст.
    Считает записи из ctx.flat_data (только непустые значения, как в БД).
    Группирует по section (листам).
    """
    print(f"🚀 Запуск pipeline для получения ctx.flat_data...")

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

    class NoOpDataSaveService:
        async def process_and_save_all(self, file_model, flat_data=None):
            return None

        async def rollback(self, file_model, error: str):
            return None

        async def save_file(self, file_model):
            return None

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
        print(f"⚠️ Pipeline завершил с ошибкой: {e}")

    # === ИСПРАВЛЕНО: считаем из ctx.flat_data, группируем по section ===
    count_by_section = {}
    total_count = 0

    if ctx.flat_data:
        print(f"\n📑 Обработка ctx.flat_data ({len(ctx.flat_data)} записей):")
        for rec in ctx.flat_data:
            section = rec.section or "unknown"
            count_by_section[section] = count_by_section.get(section, 0) + 1
            total_count += 1

        # Вывод по листам
        for section, count in sorted(count_by_section.items()):
            print(f"   {section}: {count} записей")
    else:
        print(f"\n⚠️ ctx.flat_data пуст!")

    # Для информации: покажем сколько листов в sheet_models
    if ctx.sheet_models:
        print(f"\n💾 ctx.sheet_models: {len(ctx.sheet_models)} листов")
        for sheet in ctx.sheet_models:
            sheet_data = sheet.data or []
            # Считаем непустые значения в sheet_model
            sheet_count = sum(
                1 for col in sheet_data
                for val_obj in col.get("values", [])
                if val_obj.get("value") is not None
                and val_obj.get("value") != "__EMPTY__"
            )
            print(f"   {sheet.sheet_name}: {sheet_count} непустых ячеек")

    return ctx, total_count, count_by_section


async def get_flat_data_from_db(file_id: str) -> Tuple[int, Dict[str, int]]:
    """Получает flat_data из MongoDB."""
    print(f"\n🔍 Подключение к MongoDB...")

    client = AsyncIOMotorClient(
        config.MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    try:
        await client.admin.command('ping')
        db = client[config.DATABASE_NAME]

        print(f"📊 Загрузка flat_data для file_id={file_id}...")

        total = await db.FlatData.count_documents({"file_id": file_id})

        by_section = await db.FlatData.aggregate([
            {"$match": {"file_id": file_id}},
            {"$group": {"_id": "$section", "count": {"$sum": 1}}},
        ]).to_list(length=None)

        section_counts = {doc["_id"]: doc["count"] for doc in by_section}

        file_doc = await db.Files.find_one({"file_id": file_id})
        if file_doc:
            print(f"📁 Файл: {file_doc.get('filename', 'N/A')}")
            print(f"📑 Листы: {file_doc.get('sheets', [])}")
            print(f"📊 size в файле: {file_doc.get('size', 'N/A')}")

        return total, section_counts

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        raise
    finally:
        client.close()


def compare_by_section(
        ctx_sheet_counts: Dict[str, int],
        db_section_counts: Dict[str, int]
) -> List[Dict[str, Any]]:
    """Сравнивает количество записей по листам."""
    results = []
    all_sheets = set(ctx_sheet_counts.keys()) | set(db_section_counts.keys())

    for sheet_name in sorted(all_sheets):
        ctx_count = ctx_sheet_counts.get(sheet_name, 0)
        db_count = db_section_counts.get(sheet_name, 0)
        diff = db_count - ctx_count

        results.append({
            "sheet": sheet_name,
            "ctx_count": ctx_count,
            "db_count": db_count,
            "diff": diff,
            "match": ctx_count == db_count,
        })

    return results


def print_comparison_report(
        ctx_total: int,
        db_total: int,
        by_section: List[Dict[str, Any]],
) -> None:
    """Выводит отчёт о сравнении."""
    print("\n" + "=" * 80)
    print("📊 ОТЧЁТ СРАВНЕНИЯ: ctx.flat_data (непустые) vs MongoDB")
    print("=" * 80)

    print(f"\n📈 ОБЩЕЕ КОЛИЧЕСТВО ЗАПИСЕЙ:")
    print(f"   ctx.flat_data (непустые): {ctx_total}")
    print(f"   MongoDB:                  {db_total}")
    print(f"   Разница:                  {db_total - ctx_total:+d}")

    if ctx_total == db_total:
        print(f"   ✅ СОВПАДЕНИЕ!")
    else:
        print(f"   ❌ РАСХОЖДЕНИЕ!")

    print(f"\n📑 ПО ЛИСТАМ:")
    print(f"   {'Лист':<20} {'ctx':>10} {'БД':>10} {'Разница':>10} {'Статус':>10}")
    print(f"   {'-' * 20} {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 10}")

    all_match = True
    for row in by_section:
        status = "✅" if row["match"] else "❌"
        if not row["match"]:
            all_match = False
        print(f"   {row['sheet']:<20} {row['ctx_count']:>10} {row['db_count']:>10} {row['diff']:>+10} {status:>10}")

    print("\n" + "=" * 80)
    if all_match:
        print("✅ ВСЕ ЛИСТЫ СОВПАДАЮТ!")
        print("\n💡 Вывод: Данные в БД сохранены корректно!")
    else:
        print("❌ ЕСТЬ РАСХОЖДЕНИЯ!")
        print("\n💡 Возможные причины:")
        print("   - Дубликаты в БД (проверь уникальность по file_id+section+row+column)")
        print("   - Данные от предыдущих загрузок того же файла")
        print("   - Ошибка в EnrichFlatDataStep или PersistStep")
        print("   - Rollback не сработал корректно")
    print("=" * 80)


async def main():
    if not TEST_FILE_PATH.exists():
        print(f"❌ Файл не найден: {TEST_FILE_PATH.absolute()}")
        return

    # 1. Получаем данные из контекста
    ctx, ctx_total, ctx_by_sheet = await get_flat_data_from_ctx(TEST_FILE_PATH, FORM_ID)

    if ctx.failed:
        print(f"❌ Pipeline упал: {ctx.error}")
        return

    print(f"\n✅ ctx.sheet_models (непустые): {ctx_total} записей\n")

    # 2. Получаем file_id
    file_id = FILE_ID
    if not file_id and ctx.file_model:
        file_id = ctx.file_model.file_id
        print(f"📁 file_id из контекста: {file_id}\n")

    if not file_id:
        print("❌ Не удалось определить file_id. Укажи FILE_ID в конфигурации скрипта.")
        return

    # 3. Получаем данные из БД
    db_total, db_by_sheet = await get_flat_data_from_db(file_id)
    print(f"✅ MongoDB: {db_total} записей\n")

    # 4. Сравниваем по листам
    by_section = compare_by_section(ctx_by_sheet, db_by_sheet)

    # 5. Выводим отчёт
    print_comparison_report(ctx_total, db_total, by_section)


if __name__ == "__main__":
    asyncio.run(main())