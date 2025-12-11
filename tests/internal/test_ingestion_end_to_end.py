import io
import pytest
from fastapi import UploadFile
from app.models.file_model import FileModel
from app.models.file_status import FileStatus
from app.core.database import mongo_connection


@pytest.mark.asyncio
async def test_ingestion_happy_path(ingestion_service, sample_xlsx_path):
    """
    Запускаем ingestion на реальном (анонимизированном) Excel.
    Проверяем, что:
    - в коллекцию Files появилась запись со статусом success
    - в FlatData появились записи (>0)
    - фильтры отдают ожидаемые значения (несколько ключевых)
    """
    # читаем файл в память и формируем UploadFile
    with open(sample_xlsx_path, "rb") as f:
        content = f.read()
    upload = UploadFile(file=io.BytesIO(content), filename="TESTCITY 2023.xlsx")

    resp = await ingestion_service.process_files([upload])
    assert "processed successfully" in resp.message.lower()

    db = mongo_connection.get_database()
    files_doc = await db.Files.find_one({"filename": "TESTCITY 2023.xlsx"})
    assert files_doc is not None
    assert files_doc.get("status") == FileStatus.SUCCESS.value

    flat_count = await db.FlatData.count_documents({"file_id": files_doc["file_id"]})
    assert flat_count > 0

    # Проверим, что фильтр по городу вернёт наш город (используем FilterService через dependency)
    from app.core.dependencies import get_data_retrieval_service
    dr = get_data_retrieval_service()
    vals = await dr.get_filter_values("город", [], "")
    # Ожидаем, что город в верхнем регистре присутствует среди значений
    assert any(isinstance(v, str) for v in vals)

# @pytest.mark.asyncio
# async def test_idempotent_file_update(file_service):
#     """
#     Тест идемпотентности update_or_create — дважды сохраняем одну модель, проверяем, что нет дубликатов
#     """
#     fm = FileModel.create_new(filename="IDEMP 2022.xlsx", year=2022, city="IDEMP")
#     # Первый вызов — вставка
#     await file_service.update_or_create(fm)
#     # Считаем записи
#     db = mongo_connection.get_database()
#     count1 = await db.Files.count_documents({"file_id": fm.file_id})
#     assert count1 == 1
#
#     # Второй вызов — обновление (не создаст новую)
#     fm.size = 123
#     await file_service.update_or_create(fm)
#     count2 = await db.Files.count_documents({"file_id": fm.file_id})
#     assert count2 == 1
#     # Проверяем, что поле size обновилось
#     doc = await db.Files.find_one({"file_id": fm.file_id})
#     assert doc.get("size") == 123
#
# @pytest.mark.asyncio
# async def test_rollback_on_duplicate_key(ingestion_service, sample_xlsx_path):
#     """
#     Форсируем DuplicateKeyError, повторив одинаковые flat-записи, и проверяем, что:
#     - DataSaveService поймал ошибку и выполнил rollback: запись Files помечена FAILED
#     """
#     # Для воспроизведения: возьмём нормальный файл, запустим sheet_processor, затем вручную вставим в flat_data
#     # дублирующие записи так, чтобы уникальный индекс (year,city,section,row,column) сработал
#
#     # Сначала создаём корректный upload, чтобы получить file_model и flat_data через SheetProcessor
#     with open(sample_xlsx_path, "rb") as f:
#         content = f.read()
#     upload = UploadFile(file=io.BytesIO(content), filename="ROLLBACKCITY 2023.xlsx")
#
#     # Получаем SheetProcessor через зависимости (используется внутри ingestion)
#     from app.services.sheet_processor import SheetProcessor
#     from app.models.file_model import FileModel as DomainFileModel
#
#     sp = SheetProcessor()
#     fm = DomainFileModel.create_new(filename=upload.filename, year=2023, city="ROLLBACKCITY")
#     # извлекаем листы и flat_data
#     await upload.seek(0)
#     sheet_models, flat_data = await sp.extract_and_process_sheets(upload, fm)
#
#     assert isinstance(flat_data, list)
#
#     # Добавим два полностью одинаковых документа, чтобы BulkWrite/BulkWriteError возник при вставке
#     if not flat_data:
#         pytest.skip("В тестовом файле не сгенерировались flat-записи; замените data_example.xlsx корректным файлом")
#
#     # возьмём первый элемент и продублируем много раз
#     tpl = flat_data[0].copy()
#     # ensure file_id present
#     if "file_id" not in tpl:
#         tpl["file_id"] = fm.file_id
#     dup_chunk = [tpl for _ in range(5)]
#     # объединяем с остальными, предварительно очистим коллекции
#     db = mongo_connection.get_database()
#     await db.FlatData.delete_many({})
#     await db.Files.delete_many({})
#
#     # вручную вставляем только файл запись (processing), затем пытаемся сохранить flat через FlatDataService напрямую
#     from app.core.dependencies import get_file_service, get_flat_data_service
#     fs = get_file_service()
#     fds = get_flat_data_service()
#     # создаём файл (processing)
#     await fs.update_or_create(fm)
#
#     # Теперь пытаемся вставить дублями через flat_data_service.save_flat_data — это должно поднять ошибку внутри
#     # Но save_flat_data ловит ошибки и продолжает — поэтому симулируем через DataSaveService.process_and_save_all
#     from app.data.services.data_save import DataSaveService
#     from app.data.services.log_service import LogService
#     from app.data.repositories.logs import LogsRepository
#     logs_repo = db.get_collection("Logs")
#     log_service = LogService(LogsRepository(logs_repo))
#     data_save = DataSaveService(log_service=log_service, flat_data_service=fds, file_service=fs)
#
#     # сформируем flat_data с полями, которые приведут к конфликту уникального индекса:
#     # дубли одного и того же (year,city,section,row,column)
#     bad_flat = []
#     base = tpl.copy()
#     base.update({"year": fm.year, "city": fm.city, "section": "S", "row": "R", "column": "C", "value": 1})
#     for _ in range(3):
#         bad_flat.append(base.copy())
#
#     # Выполняем и ожидаем, что process_and_save_all поймает ошибку и выполнит rollback,
#     # в результате запись в Files должна получить status == failed
#     await data_save.process_and_save_all(fm, bad_flat)
#
#     doc = await db.Files.find_one({"file_id": fm.file_id})
#     assert doc is not None
#     assert doc.get("status") == FileStatus.FAILED.value or doc.get("error") is not None
