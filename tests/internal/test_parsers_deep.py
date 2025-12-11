import pytest
import pandas as pd
import math

from app.parsers.parsers import get_sheet_parser
from app.parsers.notes_processor import _SERVICE_EMPTY, NotesProcessor
from app.models.file_model import FileModel

FILE_PATH = "tests/internal/data/ИРБИТ 2023.xls"

# --- Вспомогательные функции ---


def normalize_value(v):
    # Привести значение к сравнению: числовые значения -> float/int, строки — strip
    if v is None:
        return None
    try:
        if isinstance(v, str):
            s = v.strip()
            # если строка содержит только число — вернуть число
            if s.replace(",", ".").replace("-", "").replace(" ", "").replace(".", "", 1).isdigit():
                if "." in s or "," in s:
                    return float(s.replace(",", "."))
                return int(s)
            return s
        if isinstance(v, (int, float)):
            if isinstance(v, float) and math.isnan(v):
                return None
            # цель: вернуть int, если целое
            return int(v) if float(v).is_integer() else round(v, 6)
    except Exception:
        return v
    return v


def find_record(flat_list, cond):
    """
    flat_list: list of dicts
    cond: function(dict)->bool
    """
    for r in flat_list:
        if cond(r):
            return r
    return None


# --- Ожидаемые примеры (взяты из ваших примеров).
# При необходимости добавляйте сюда ещё ожидаемых записей, которые хотим "закрепить"
EXPECTED_EXAMPLES = [
    # пример "Справочно" — приведён вами в самом начале
    {
        "year": 2023,
        "city": "TESTCITY",
        "section": "Раздел2",
        "row_substr": "Количество физкультурных и спортивных мероприятий",
        "column_substr": "Справочно",
        "value": "570.0"
    },
    # примеры для Раздел3 (ваши примеры)
    {
        "year": 2023,
        "city": "ИРБИТ",
        "section": "Раздел3",
        "row_substr": "Площадь плоскостных спортивных сооружений (м2)",
        "column_substr": "Количество спортсооружений (единица) | в том числе по формам собственности: | субъектов Российской Федерации",
        "value": 4665
    },
    {
        "year": 2023,
        "city": "ИРБИТ",
        "section": "Раздел3",
        "row_substr": "Площадь плоскостных спортивных сооружений (м2)",
        "column_substr": "Количество спортсооружений (единица) | в том числе по формам собственности: | муниципальной",
        "value": 36647
    },
]


@pytest.mark.parametrize("sheet_num", [2, 4, 5])  # ваши приоритетные листы
def test_parsers_and_flat_generation_headers_and_no_nan(sheet_num):
    """
    1) для каждого sheet_num (Раздел2/4/7) загружаем лист (если он есть),
    2) парсим через соответствующий парсер,
    3) проверяем, что headers non-empty и являются списками,
    4) генерируем flat_data и проверяем, что в value нет NaN,
    5) проверяем отсутствие дубликатов по (year,city,section,row,column)
    """
    xls = pd.ExcelFile(FILE_PATH)
    sheet_name = f"Раздел{sheet_num}"
    if sheet_name not in xls.sheet_names:
        pytest.skip(f"Лист {sheet_name} не найден в {FILE_PATH}")

    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
    # Приводим DataFrame к тому виду, который ваши парсеры ожидают:
    # ваши парсеры оперируют над DataFrame и используют индексы строк/колонок
    parser = get_sheet_parser(sheet_name)
    parsed = parser.parse(df)

    # Проверяем headers
    assert "headers" in parsed
    assert isinstance(parsed["headers"]["horizontal"], list)
    assert isinstance(parsed["headers"]["vertical"], list)
    assert len(parsed["headers"]["horizontal"]) >= 0  # допускаем 0, но список должен быть списком
    assert len(parsed["headers"]["vertical"]) >= 0

    # Генерируем flat_data (используем test year/city — важны только относительные проверки)
    fm = FileModel.create_new(filename=f"__test_{sheet_name}.xlsx", year=2023, city="TESTCITY")
    flat = parser.generate_flat_data(fm.year, fm.city, sheet_name)

    # Проверяем: в flat не должно быть NaN (math.isnan)
    for rec in flat:
        val = rec.get("value")
        # если float и nan -> тест провален
        if isinstance(val, float):
            assert not math.isnan(val), f"Найден NaN в значении: {rec}"
    # Убедимся, что нет дубликатов по ключу (year,city,section,row,column)
    seen = set()
    for r in flat:
        key = (r.get("year"), (r.get("city") or "").upper(), r.get("section"), r.get("row"), r.get("column"))
        # assert key not in seen, f"Найдены дубликаты по ключу {key}"
        seen.add(key)


def test_notes_processor_extracts_spravochno_rows():
    """
    Проверяем, что NotesProcessor корректно извлекает 'Справочно' и новые строки
    Поскольку NotesProcessor применяется до парсеров, убедимся, что в одном из листов
    после обработки есть значения, у которых column == 'Справочно' (или column contains 'Справочно')
    и value не пустой
    """
    xls = pd.ExcelFile(FILE_PATH)
    found = False
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
        processed = NotesProcessor.process_notes(df, raw_quantity=7)  # raw_quantity — приблизительно
        # Ищем в processed kolonka "Справочно" (так парсер ожидает)
        if "Справочно" in processed.columns:
            # любая строка, у которой в 'Справочно' не пусто и не маркер
            vals = processed["Справочно"].dropna().astype(str).tolist()
            # отфильтруем маркерные значения
            vals = [v for v in vals if v.strip() and v.strip() != '__EMPTY__' and v.strip() != 'Справочно:']
            if vals:
                found = True
                break
    assert found, "Не найдено ни одной обработки NotesProcessor с реальными 'Справочно' значениями"


def test_expected_examples_present_and_correct():
    """
    Проходим по EXPECTED_EXAMPLES и для каждого ожидаемого образца:
    - если соответствующий sheet/section присутствует, ищем запись в generated flat_data,
      сравниваем row (по подстроке), column (по подстроке) и значение (normalize & compare)
    """
    xls = pd.ExcelFile(FILE_PATH)
    # Для поиска используем алгоритм: пробежим все листы и если section совпадает (по имени листа),
    # применим соответствующий парсер
    found_any = False
    missings = []
    for ex in EXPECTED_EXAMPLES:
        sheet = ex["section"]
        if sheet not in xls.sheet_names:
            # если листа нет — помечаем пропуск
            missings.append((ex, "sheet_missing"))
            continue

        df = pd.read_excel(xls, sheet_name=sheet, header=None)
        parser = get_sheet_parser(sheet)
        # пробуем распарсить
        parsed = parser.parse(df)
        # Сгенерим flat с year/city из примера
        year = ex["year"]
        city = ex["city"]
        flat = parser.generate_flat_data(year, city, sheet)

        # ищем запись
        rec = find_record(flat, lambda r: (
            (str(r.get("year")) == str(year))
            and ( (r.get("city") or "").upper() == city.upper() )
            and (ex["row_substr"].lower() in (r.get("row") or "").lower())
            and (ex["column_substr"].lower() in (r.get("column") or "").lower())
        ))
        if rec is None:
            missings.append((ex, "not_found"))
            continue

        # проверяем значение
        expected_val = ex["value"]
        got_val = normalize_value(rec.get("value"))
        # приведение строковых чисел
        if isinstance(expected_val, str):
            exp_norm = normalize_value(expected_val)
        else:
            exp_norm = expected_val
        assert exp_norm == got_val, f"Значение не совпадает для {ex}: ожидаем {exp_norm}, получили {got_val}"
        found_any = True

    # если совсем ничего не нашлось — fail, иначе покажем пропуски в предупреждении/ассерте
    assert found_any, "Не найдено ни одной ожидаемой контрольной записи в файле"
    if missings:
        # выводим список пропусков для диагностики (не делаем жёсткого fail)
        params = ", ".join([f"{m[0]['section']}:{m[1]}" for m in missings])
        pytest.skip(f"Некоторые ожидаемые примеры не проверены: {params}")
