#!/usr/bin/env python3
"""
Скрипт для создания отчёта из MongoDB (аналог visual_report).
Помогает найти расхождения между ctx.flat_data и данными в БД.
"""
import asyncio
import sys
from pathlib import Path
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from config.config import config
import pandas as pd

# === КОНФИГУРАЦИЯ (правь вручную) ===
# ID файла, который нужно проверить (возьми из визуального отчёта или БД)
FILE_ID = "fdc83293-d67a-41f0-a2d8-7388e6f41e37"

# Путь для отчёта
OUTPUT_EXCEL_PATH = Path(__file__).parent / "db_report.xlsx"


async def get_flat_data_from_db(file_id: str) -> list[dict]:
    """
    Получает все flat_data записи для конкретного файла из MongoDB.
    """
    print(f"🔍 Подключение к MongoDB...")

    client = AsyncIOMotorClient(
        config.MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    try:
        # Проверка подключения
        await client.admin.command('ping')
        print(f"✅ Подключение успешно: {config.MONGO_URI}")

        db = client[config.DATABASE_NAME]
        collection = db.FlatData

        # Получаем все записи для файла
        print(f"📊 Загрузка flat_data для file_id={file_id}...")
        cursor = collection.find({"file_id": file_id})
        records = await cursor.to_list(length=None)

        print(f"✅ Найдено записей в БД: {len(records)}")

        # Получаем информацию о файле
        file_doc = await db.Files.find_one({"file_id": file_id})
        if file_doc:
            print(f"📁 Файл: {file_doc.get('filename', 'N/A')}")
            print(f"📑 Листы: {file_doc.get('sheets', [])}")
            print(f"📊 size в файле: {file_doc.get('size', 'N/A')}")

        return records

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        raise
    finally:
        client.close()


def build_db_dataframe(records: list[dict]) -> pd.DataFrame:
    """
    Преобразует записи из БД в DataFrame для отчёта.
    """
    if not records:
        return pd.DataFrame()

    # Преобразуем в плоский формат
    flat_rows = []
    for rec in records:
        flat_rows.append({
            "year": rec.get("year"),
            "reporter": rec.get("reporter"),
            "section": rec.get("section"),
            "row": rec.get("row"),
            "column": rec.get("column"),
            "value": rec.get("value"),
        })

    df = pd.DataFrame(flat_rows)
    return df


def build_pivot_from_db(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Строит pivot-таблицы по листам (section).
    """
    sheets = {}

    if "section" not in df.columns:
        return {"all": df}

    for section in df["section"].unique():
        section_df = df[df["section"] == section].copy()

        # Строим pivot: index=row, columns=column, values=value
        try:
            pivot = section_df.pivot_table(
                index="row",
                columns="column",
                values="value",
                aggfunc="first",  # Если дубликаты — берём первое
                fill_value=""
            )
            sheets[section] = pivot
        except Exception as e:
            print(f"⚠️ Не удалось построить pivot для {section}: {e}")
            sheets[section] = section_df

    return sheets


def build_summary_from_db(df: pd.DataFrame) -> pd.DataFrame:
    """
    Создаёт сводную статистику по данным из БД.
    """
    if df.empty:
        return pd.DataFrame()

    total_cells = len(df)
    filled_cells = df["value"].notna().sum()
    empty_cells = total_cells - filled_cells

    numeric_values = pd.to_numeric(df["value"], errors="coerce")
    numeric_sum = numeric_values.sum()
    numeric_count = numeric_values.notna().sum()

    # Группировка по листам
    section_stats = []
    if "section" in df.columns:
        for section in df["section"].unique():
            section_df = df[df["section"] == section]
            section_stats.append({
                "Лист": section,
                "Записей": len(section_df),
                "Заполнено": section_df["value"].notna().sum(),
            })

    summary = {
        "Метрика": [
            "Всего записей",
            "Заполнено",
            "Пустых",
            "% заполненности",
            "Сумма числовых",
            "Среднее числовое",
        ],
        "Значение": [
            total_cells,
            filled_cells,
            empty_cells,
            f"{(filled_cells / total_cells * 100) if total_cells > 0 else 0:.1f}%",
            round(numeric_sum, 2) if numeric_count > 0 else "N/A",
            round(numeric_sum / numeric_count, 2) if numeric_count > 0 else "N/A",
        ],
    }

    summary_df = pd.DataFrame(summary)

    return summary_df, pd.DataFrame(section_stats) if section_stats else pd.DataFrame()


def generate_db_report(records: list[dict], output_path: Path) -> None:
    """
    Генерирует Excel-отчёт на основе данных из БД.
    """
    print(f"📊 Генерация отчёта: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = build_db_dataframe(records)

    if df.empty:
        print("⚠️ Нет данных для отчёта")
        return

    # Строим pivot по листам
    pivots = build_pivot_from_db(df)

    # Строим сводку
    summary_df, section_stats_df = build_summary_from_db(df)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Вкладка 1: Сводная статистика
        summary_df.to_excel(writer, sheet_name="Статистика", index=False)
        if not section_stats_df.empty:
            section_stats_df.to_excel(writer, sheet_name="По_листам", index=False)

        # Вкладка 2+: Pivot по каждому листу
        for section_name, pivot_df in pivots.items():
            sheet_name = section_name[:30].replace(":", "_").replace("/", "_")
            try:
                pivot_df.to_excel(writer, sheet_name=f"{sheet_name}_pivot")
            except Exception as e:
                print(f"⚠️ Не удалось сохранить pivot для {section_name}: {e}")
                # Fallback: сохраняем как есть
                pivot_df.reset_index().to_excel(writer, sheet_name=f"{sheet_name}_flat", index=False)

        # Вкладка N: Все данные плоским списком
        df.head(5000).to_excel(writer, sheet_name="Все_данные", index=False)

    print(f"✅ Отчёт сохранён: {output_path}")
    print(f"📊 Всего записей в БД: {len(records)}")


async def main():
    if not FILE_ID or FILE_ID == "ВСТАВЬ_FILE_ID_ИЗ_БД":
        print("❌ Укажи FILE_ID в конфигурации скрипта!")
        print("💡 Возьми file_id из visual_report.xlsx или из БД")
        return

    # 1. Получаем данные из БД
    records = await get_flat_data_from_db(FILE_ID)

    if not records:
        print("❌ Нет записей в БД для этого файла")
        return

    # 2. Генерируем отчёт
    generate_db_report(records, OUTPUT_EXCEL_PATH)

    # 3. Инструкция для сравнения
    print("\n" + "=" * 70)
    print("🔍 СРАВНЕНИЕ ДАННЫХ")
    print("=" * 70)
    print(f"1. Открой: {OUTPUT_EXCEL_PATH.absolute()}")
    print(f"2. Открой visual_report.xlsx (из ctx.flat_data)")
    print(f"3. Сравни количество записей:")
    print(f"   - В БД: {len(records)}")
    print(f"   - В ctx: 1069 (из логов)")
    print(f"4. Проверь вкладку 'Статистика' — там общее количество")
    print(f"5. Проверь вкладку 'По_листам' — расхождения по листам")
    print("\n💡 Возможные причины расхождений:")
    print("   - Дубликаты в БД (проверь уникальность)")
    print("   - Данные от предыдущих загрузок того же файла")
    print("   - Ошибка в EnrichFlatDataStep или PersistStep")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())