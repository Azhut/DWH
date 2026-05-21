# Вопросы по проекту — ответы (формат «вопрос — ответ»)

## Контекст итерации

1. **Как официально называется именно эта итерация (например, «4-я итерация: формализация пайплайнов»)?** — Предлагаемое официальное название: **«Итерация: формализация Upload/Parsing pipeline и асинхронная загрузка с прогрессом (SSE)»**. В коде это выражено появлением `app/application/upload/pipeline/*`, `app/application/parsing/*`, `UploadManager` с `upload_id` и SSE-эндпоинтом `/upload-progress/{upload_id}`.

2. **Какие изменения точно входят в эту итерацию, а какие нужно считать «сделано раньше» и не описывать подробно?** — В эту итерацию входят:
   - формализация обработки файла как фиксированного `UploadPipelineRunner` со списком шагов (`AcquireFileRecordStep ... PersistStep`);
   - формализация обработки листа как `ParsingPipelineRunner` + канонический набор шагов, собираемых стратегией (`DefaultFormParsingStrategy.build_steps_for_sheet`);
   - введение `ParsingStrategyRegistry` (manual vs default) как единой точки выбора стратегии;
   - переход `POST /upload` на **фон** (`202 + upload_id`) и добавление SSE `/upload-progress/{upload_id}`;
   - унификация ошибок через `app/core/exceptions.py` и централизованное логирование `log_app_error`;
   - транзакционное/компенсирующее сохранение и rollback в `DataSaveService`;
   - индексы Mongo на старте (`app/application/data/indexes.py`);
   - обслуживание системных форм (`FormMaintenanceService.ensure_system_forms_exist`).

   «Сделано раньше» (как базовая платформа, без углубления): многослойность API/Application/Domain/Infrastructure, сама сущность `Form` как конфигурация и общий принцип «обрабатывать по форме», наличие FlatData/Files как сущностей.

3. **Какая была ключевая боль до начала этой итерации в 1–2 фразах?** — Логика загрузки/парсинга была сложнее контролируема: ошибки и частичные состояния (Files vs FlatData) приводили к рассинхрону, дубликатам и непредсказуемым падениям; также UX загрузки страдал из‑за отсутствия прогресса и слабой наблюдаемости.

4. **Какие 3–5 результата итерации вы считаете главными и обязательными для текста?** —
   - фиксированные пайплайны `UploadPipelineRunner` и `ParsingPipelineRunner` (воспроизводимый порядок шагов);
   - строгая модель ошибок (critical/non-critical/duplicate) и управляемый rollback;
   - разделение стратегий форм (manual `FK1FormParsingStrategy` vs default `AutoFormParsingStrategy`) через `ParsingStrategyRegistry`;
   - асинхронная загрузка `POST /upload` + SSE прогресс `/upload-progress/{upload_id}`;
   - индексы и целостность в Mongo (уникальный индекс FlatData + индекс уникальности Files).

5. **Есть ли изменения, которые были начаты, но не завершены (чтобы корректно описать ограничения)?** — Да:
   - прогресс загрузки хранится **в памяти процесса** (`UploadManager._upload_progress`), что ограничивает multi-worker режим (нужен Redis/внешнее хранилище);
   - `RoundingStep` в common — это контракт без реализации; округление реализовано только для 1ФК (`FK1RoundingStep`);
   - часть “websocket”‑идеи упоминается в задачах, но текущий рабочий путь прогресса в API — SSE.

## Состояние системы до изменений

6. **Как выглядела система прямо до текущей итерации (модули, потоки, ответственность компонентов)?** — До итерации основная концепция была: `Form` → выбор обработчика/логики формы → обработка файла как «единый конвейер» на уровне сервисов/handler’ов. В текущей итерации этот конвейер материализован в два строгих пайплайна: `upload` (файл) и `parsing` (лист).

7. **Какие компоненты уже были стабильными до итерации?** — Базовые сущности и сценарии: `Files`, `FlatData`, формы (`Forms`), эндпоинты выборки (`/filtered-data`, `/filter-values`, `/filters-names`) и удаление файла (`DELETE /files/{file_id}`) как бизнес‑контракты.

8. **Где именно логика была «размазана» (какие файлы/сервисы)?** — По исторической линии это проявлялось в смешении оркестрации (порядок шагов) и бизнес‑логики внутри сервисов загрузки/парсинга. Текущая итерация вынесла оркестрацию в `UploadPipelineRunner` (`app/application/upload/pipeline/pipeline.py`) и `ParsingPipelineRunner` (`app/application/parsing/pipeline.py`), а бизнес‑правила — в шаги/стратегии.

9. **Какие проблемы проявлялись чаще всего в проде/на тестах до изменений?** — По фактическим фиксам/тестам в репозитории: дубликаты заголовков (ломали ключи/плоские записи), рассинхрон `flat_data_size` vs реально вставленных документов, дубли/конфликты по уникальным индексам FlatData, нестабильность snapshot‑тестов.

10. **Какие типовые ошибки пользователей или данных вы наблюдали до доработок?** —
   - “грязные” Excel: скрытые/служебные колонки, смещённые таблицы;
   - листы с неожиданными именами (не `РазделN`);
   - неоднозначные/дублирующиеся заголовки;
   - пустые листы/разрывы в таблицах.

## Архитектура и слои

11. **Подтвердите целевую структуру слоев (API/Application/Domain/Infrastructure) и что в каждом слое сейчас находится.** —
   - API: `app/api/v2/endpoints/*`, `app/api/v2/schemas/*`.
   - Application (use-cases/orchestration): `app/application/upload/*`, `app/application/parsing/*`, `app/application/data/*`, `app/application/forms/*`.
   - Domain (агрегаты, модели, сервисы): `app/domain/file/*`, `app/domain/form/*`, `app/domain/flat_data/*`, `app/domain/sheet/*`, `app/domain/parsing/*`, `app/domain/log/*`.
   - Infrastructure: конкретика Mongo находится в репозиториях (`get_collection("Files"|"FlatData"|...)`) и `app/core/database.py` / `motor`.

12. **Какие зависимости между слоями являются допустимыми, а какие запрещены?** — По фактической структуре:
   - допустимо: API → Application/Domain; Application → Domain; Domain → (только свои модели/контракты).
   - нежелательно/запрещено: Domain → API; шаги/стратегии не должны зависеть от FastAPI/эндпоинтов.

13. **Есть ли отдельный слой/модуль для orchestration use-cases?** — Да: `app/application/*`. Конкретно orchestration для загрузки: `UploadManager`, `UploadPipelineRunner`. Для парсинга: `ParsingPipelineRunner` + стратегии.

14. **Какие доменные сущности считаются ключевыми сейчас (Form, File, SheetModel, FlatData и др.)?** — `FormInfo` (`FormType`, `requisites`), `FileModel` (`Files`), `SheetModel` (результаты парсинга листа), `FlatDataRecord` (`FlatData`), `LogEntry` (`Logs`).

15. **Какие сущности были изменены в текущей итерации по полям или смыслу?** — Подтверждённое по коду изменение: реквизиты формы (`skip_sheets` и др.) должны жить **внутри** `FormInfo.requisites` (тест `test_forms_maintenance.py`). Также `SheetModel` закреплён как «source of truth» результата парсинга.

## Upload pipeline (нужен полный технический список шагов)

16. **Дайте точный список шагов upload_pipeline в фактическом порядке.** — Сборка в `app/application/upload/pipeline/pipeline.py`:
   1) `AcquireFileRecordStep`
   2) `ReadFileContentStep`
   3) `ExtractMetadataStep`
   4) `ReadWorkbookStep`
   5) `ProcessSheetsStep`
   6) `FinalizeFileModelStep`
   7) `EnrichFlatDataStep`
   8) `PersistStep`

17. **Для каждого шага upload pipeline: входные данные шага?** —
   - `AcquireFileRecordStep`: `ctx.filename`, `ctx.form_id`.
   - `ReadFileContentStep`: `ctx.file`.
   - `ExtractMetadataStep`: `ctx.filename`, (пишет в `ctx.file_model` если он есть).
   - `ReadWorkbookStep`: `ctx.file_content`, `ctx.filename`.
   - `ProcessSheetsStep`: `ctx.file_model`, `ctx.workbook_sheets`, `ctx.form_info`, `ctx.file_content`/`ctx.file_info` (для `ParsingWorkbookSource`).
   - `FinalizeFileModelStep`: `ctx.file_model`, `ctx.sheets`.
   - `EnrichFlatDataStep`: `ctx.file_model`, `ctx.sheets`.
   - `PersistStep`: `ctx.file_model`, `ctx.flat_data`.

18. **Для каждого шага upload pipeline: что он пишет в контекст?** —
   - `AcquireFileRecordStep`: `ctx.file_model`.
   - `ReadFileContentStep`: `ctx.file_content`.
   - `ExtractMetadataStep`: `ctx.file_info`, а также `ctx.file_model.year/reportер`.
   - `ReadWorkbookStep`: `ctx.workbook_sheets`.
   - `ProcessSheetsStep`: `ctx.sheets` (список `SheetModel`).
   - `FinalizeFileModelStep`: `ctx.file_model.sheets`, `ctx.file_model.flat_data_size`.
   - `EnrichFlatDataStep`: обогащает каждую `FlatDataRecord` из `ctx.flat_data` полями `file_id/form/year/reporter`.
   - `PersistStep`: не пишет в контекст напрямую; инициирует сохранение через `DataSaveService`.

19. **Для каждого шага upload pipeline: какие исключения может выбросить?** —
   - `AcquireFileRecordStep`: `DuplicateFileError` (409), `CriticalUploadError`.
   - `ReadFileContentStep`: `CriticalUploadError`.
   - `ExtractMetadataStep`: `CriticalUploadError` (в т.ч. обёртка `FileValidationError`).
   - `ReadWorkbookStep`: `CriticalUploadError`.
   - `ProcessSheetsStep`: `CriticalUploadError` (в т.ч. обёртка `CriticalParsingError`).
   - `FinalizeFileModelStep`: `CriticalUploadError`.
   - `EnrichFlatDataStep`: `CriticalUploadError`.
   - `PersistStep`: `CriticalUploadError`.

20. **Для каждого шага upload pipeline: какие побочные эффекты (БД, файловая система, логи)?** —
   - `AcquireFileRecordStep`: запись/обновление документа в `Files` через `FileService.update_or_create`.
   - `ReadFileContentStep`: нет БД; чтение в память.
   - `ExtractMetadataStep`: нет прямой БД (но пишет в `file_model`).
   - `ReadWorkbookStep`: нет БД; парсинг байтов в DataFrame’ы.
   - `ProcessSheetsStep`: нет БД; строит `SheetModel` и `FlatDataRecord` (пока в памяти).
   - `FinalizeFileModelStep`: нет БД; меняет поля `file_model`.
   - `EnrichFlatDataStep`: нет БД; меняет плоские записи.
   - `PersistStep`: запись `Files`, вставка `FlatData` (bulk_write/insert_one), запись `Logs` через `DataSaveService`.

21. **На каком шаге создается запись файла?** — `AcquireFileRecordStep`.

22. **На каком шаге загружается и валидируется workbook?** — Загрузка bytes: `ReadFileContentStep`, извлечение листов из workbook: `ReadWorkbookStep`.

23. **На каком шаге происходит запуск parsing pipeline по листам?** — `ProcessSheetsStep`.

24. **На каком шаге формируется flat_data?** — На уровне листа — в parsing pipeline шагом `GenerateFlatDataStep`; на уровне файла доступно через `UploadPipelineContext.flat_data` после `ProcessSheetsStep`.

25. **На каком шаге выполняется сохранение и финализация статуса файла?** — `PersistStep` вызывает `DataSaveService.process_and_save_all(...)`, который ставит `FileStatus.SUCCESS` и пишет `flat_data_size`.

## Parsing pipeline (тоже полный технический список шагов)

26. **Дайте точный список шагов parsing_pipeline в фактическом порядке.** — Канонический порядок собирается в `DefaultFormParsingStrategy.build_steps_for_sheet(...)`:
   1) `NormalizeSheetNameStep(normalize_fn=...)`
   2) `NormalizeDataFrameStep()`
   3) `DetectTableStructureStep(strategy=...)`
   4) *(опционально)* дополнительные шаги стратегии перед заголовками
   5) `ParseHeadersStep(...)`
   6) `ExtractDataStep(deduplicate_columns=...)`
   7) `GenerateFlatDataStep()`

   Для 1ФК (manual) в середину добавляются `FK1RoundingStep` (условно) и `ProcessNotesStep`.

27. **Для каждого шага parsing pipeline: входные данные шага?** —
   - `NormalizeSheetNameStep`: `ctx.sheet_model.sheet_fullname`, `normalize_fn`.
   - `NormalizeDataFrameStep`: `ctx.raw_dataframe`.
   - `DetectTableStructureStep`: `ctx.processed_dataframe`.
   - `FK1RoundingStep` (если применён): `ctx.processed_dataframe`, `ctx.sheet_name`.
   - `ProcessNotesStep`: `ctx.processed_dataframe`.
   - `ParseHeadersStep`: `ctx.table_structure`, `ctx.processed_dataframe`, `ctx.form_info.requisites`, `ctx.workbook_source`.
   - `ExtractDataStep`: `ctx.table_structure`, `ctx.sheet_model.horizontal_headers`, `ctx.sheet_model.vertical_headers`, `ctx.processed_dataframe`.
   - `GenerateFlatDataStep`: `ctx.extracted_data`.

28. **Для каждого шага parsing pipeline: что он пишет в контекст/SheetModel?** —
   - `NormalizeSheetNameStep`: `ctx.sheet_model.sheet_name`.
   - `NormalizeDataFrameStep`: `ctx.processed_dataframe`.
   - `DetectTableStructureStep`: `ctx.table_structure`.
   - `FK1RoundingStep`: перезаписывает `ctx.processed_dataframe`.
   - `ProcessNotesStep`: перезаписывает `ctx.processed_dataframe` (или откатывает на `raw_dataframe` при ошибке).
   - `ParseHeadersStep`: `ctx.sheet_model.horizontal_headers`, `ctx.sheet_model.vertical_headers`.
   - `ExtractDataStep`: `ctx.extracted_data`.
   - `GenerateFlatDataStep`: `ctx.sheet_model.flat_data_records`.

29. **Для каждого шага parsing pipeline: какие исключения может выбросить?** —
   - `NormalizeSheetNameStep`: не бросает.
   - `NormalizeDataFrameStep`: `CriticalParsingError`.
   - `DetectTableStructureStep`: `CriticalParsingError`.
   - `FK1RoundingStep`: `NonCriticalParsingError`.
   - `ProcessNotesStep`: `NonCriticalParsingError`.
   - `ParseHeadersStep`: `CriticalParsingError` (в т.ч. при дублях заголовков).
   - `ExtractDataStep`: `CriticalParsingError`.
   - `GenerateFlatDataStep`: `NonCriticalParsingError` (если `extracted_data` нет), либо `CriticalParsingError` при сбое генерации.

30. **Какие шаги обязательны для всех форм?** — В каноническом пайплайне обязательны: `NormalizeSheetNameStep`, `NormalizeDataFrameStep`, `DetectTableStructureStep`, `ParseHeadersStep`, `ExtractDataStep`, `GenerateFlatDataStep`.

31. **Какие шаги добавляются стратегией формы (например, для FK1)?** — Для `FK1FormParsingStrategy` в `get_additional_steps_before_headers(...)` добавляются:
   - `FK1RoundingStep` (для листов с `apply_rounding=True` в `_SHEET_CONFIGS`);
   - `ProcessNotesStep` (всегда).

32. **Где происходит нормализация имени листа?** — `NormalizeSheetNameStep`.

33. **Где определяется структура таблицы?** — `DetectTableStructureStep` (результат в `ctx.table_structure`).

34. **Где парсятся горизонтальные/вертикальные заголовки?** — `ParseHeadersStep` (пишет в `sheet_model.horizontal_headers/vertical_headers`).

35. **Где извлекаются данные и где строится flat_data по листу?** — Данные: `ExtractDataStep` (`ctx.extracted_data`), flat_data: `GenerateFlatDataStep` (`sheet_model.flat_data_records`).

## Связь upload и parsing pipeline

36. **Как именно upload_pipeline вызывает parsing_pipeline (класс/метод)?** — `ProcessSheetsStep.execute()` вызывает `ParsingStrategyRegistry.build_pipeline_for_sheet(...)`, затем запускает `await pipeline.run_for_sheet(parsing_ctx)`.

37. **Что передается в parsing pipeline как контекст из upload pipeline?** — Формируется `ParsingPipelineContext`:
   - `sheet_model` (новый `SheetModel(sheet_fullname=sheet_name)`),
   - `raw_dataframe` (DataFrame листа),
   - `form_info` (из upload контекста),
   - `workbook_source` (если есть `ctx.file_content` и `ctx.file_info` → `ParsingWorkbookSource(content, extension)`).

38. **Как ошибка листа конвертируется в ошибку файла?** — В `ProcessSheetsStep._parse_sheet`: `CriticalParsingError` перехватывается и оборачивается в `CriticalUploadError` с `domain="upload.process_sheets"`.

39. **Какие ошибки parsing pipeline считаются критичными на уровне upload pipeline?** — Все `CriticalParsingError` (они превращаются в `CriticalUploadError` и останавливают обработку файла).

40. **Почему технически было выбрано 2 пайплайна, а не 1 (конкретные причины, не общие слова)?** —
   - разные единицы ответственности: файл vs лист;
   - разные контексты данных: `UploadPipelineContext` (байты, FileModel, workbook) vs `ParsingPipelineContext` (DataFrame, структура, заголовки);
   - возможность пропускать листы на уровне стратегии (`build_pipeline_for_sheet` возвращает `None`), не усложняя file-level пайплайн;
   - проще типизировать и локализовать ошибки (листовые ошибки — в parsing, файл — в upload) и корректно делать rollback по file_id.

41. **Какие плюсы это дало в сопровождении и расширении (желательно с примерами из кода)?** —
   - фиксированный список шагов upload в `build_default_pipeline(...)` упрощает аудит и тестирование;
   - добавление формы = добавить стратегию + шаги, не трогая upload (`ParsingStrategyRegistry.register(...)`);
   - leaf-level правки (например, дубль заголовков) делаются в одном шаге `ParseHeadersStep` и становятся общей защитой.

## Стратегии парсинга и формы

42. **Как устроен ParsingStrategyRegistry (правило выбора стратегии)?** — `ParsingStrategyRegistry.get_strategy(form_type)`:
   - если `form_type` зарегистрирован (например, `FK_1`) → manual стратегия;
   - иначе → `_default` (`AutoFormParsingStrategy`).

43. **Какие manual-стратегии есть сейчас?** — `FK1FormParsingStrategy` (для `FormType.FK_1`).

44. **Что делает Auto-стратегия и по каким правилам она решает, обрабатывать лист или нет?** — `AutoFormParsingStrategy.should_process_sheet(...)`:
   - обрабатывает только листы, имя которых матчится на `^\s*раздел\s*\d+\s*$`;
   - пропускает листы по индексу, если `sheet_index` входит в `form_info.requisites["skip_sheets"]`.

45. **Как работает should_process_sheet (условия для auto и FK1)?** —
   - Auto: regex `РазделN` + `skip_sheets`.
   - FK1: `normalize_sheet_name(sheet_name)` должен быть в `_SHEET_CONFIGS` + `skip_sheets`.

46. **Какие requisites формы влияют на поведение pipeline (например, skip_sheets, deduplicate и т.п.)?** — Подтверждённые в коде:
   - `skip_sheets`: влияет на пропуск листов (auto и fk1);
   - `deduplicate_columns`: влияет на `ExtractDataStep(deduplicate_columns=...)` (через `DefaultFormParsingStrategy.get_deduplicate_columns`);
   - `vertical_hierarchy_mode`: влияет на `ParseHeadersStep` (передаётся в `parse_headers`);
   - `horizontal_header_leading_levels_to_drop`: влияет на `ParseHeadersStep` (drop ведущих сегментов);
   - `horizontal_header_strip_fk1_banner`: включает/выключает снятие сегмента «ОКЕИ».

47. **Что нужно сделать, чтобы добавить новую форму (шаги по коду)?** —
   1) определить правила формы (обычно через `requisites` в `Forms`);
   2) если нужна ручная логика: создать стратегию `app/application/parsing/strategies/<new>.py` (наследник `DefaultFormParsingStrategy`);
   3) зарегистрировать её в `ParsingStrategyRegistry._register_manual_forms()`;
   4) при необходимости добавить шаги в `app/application/parsing/steps/forms/<new>/` и подключить через `get_additional_steps_before_headers`.

## Ошибки, rollback, целостность

48. **Дайте точный перечень классов исключений, которые используются в пайплайнах.** — Из `app/core/exceptions.py` и шагов:
   - `RequestValidationError`
   - `CriticalUploadError`, `NonCriticalUploadError`, `DuplicateFileError`
   - `CriticalParsingError`, `NonCriticalParsingError`
   - доменные: `FormValidationError`, `FileValidationError`

49. **Где проходит граница между critical и non-critical ошибками?** — Граница формализована типами:
   - `Critical*` → остановка обработки и (для upload) rollback;
   - `NonCritical*` → warning и продолжение пайплайна.

50. **Когда и как запускается rollback?** — В `UploadPipelineRunner._handle_critical_error`: если `ctx.file_model.file_id` существует → вызывается `data_save_service.rollback(ctx.file_model, ctx.error)`.

51. **Что именно откатывается (какие коллекции/сущности)?** — `DataSaveService.rollback` удаляет `FlatData` по `file_id` и помечает `Files` как `FAILED` (по возможности в транзакции), затем пишет лог в `Logs`.

52. **Как обрабатывается duplicate file (технический сценарий)?** — В `AcquireFileRecordStep`: если найден existing `SUCCESS` по `(filename, form_id)` → кидается `DuplicateFileError(409)`. В `UploadPipelineRunner` это ловится отдельно и завершает обработку файла без вызова rollback, помечая `ctx.failed=True` и `ctx.error`.

53. **Какие проверки целостности добавлены в этой итерации?** — Подтверждённые:
   - уникальный индекс FlatData `main_unique_idx` на `(file_id, year, reporter, section, row, column)`;
   - проверка дубликатов горизонтальных заголовков в `ParseHeadersStep` → `CriticalParsingError`;
   - контроль рассинхрона inserted_count vs expected_count в `DataSaveService` и логирование discrepancy;
   - post-insert verification в `FlatDataService.save_flat_data`: проверка `count_documents(file_id)`.

54. **Какие кейсы дублей/конфликтов были исправлены фактически?** —
   - дубли горизонтальных заголовков (явная диагностика + падение);
   - дубли данных FlatData (ошибка 11000 оборачивается в `CriticalUploadError`);
   - рассинхрон flat_data_size (тест `test_flat_data_size_consistency.py`).

## API и асинхронный сценарий загрузки

55. **Какие endpoint’ы обязательны для описания в курсовой (список)?** — Минимально по текущей системе:
   - `POST /api/v2/upload`
   - `GET /api/v2/upload-progress/{upload_id}`
   - `GET /api/v2/files`
   - `DELETE /api/v2/files/{file_id}`
   - `GET /api/v2/filters-names`
   - `POST /api/v2/filter-values`
   - `POST /api/v2/filtered-data`
   - `GET /api/v2/forms`, `GET /api/v2/forms/{id}`, `POST /api/v2/forms`, `PUT /api/v2/forms/{id}`, `DELETE /api/v2/forms/{id}`
   - `GET /api/v2/logs/download`
   - `GET /api/v2/health`

56. **Что принимает POST /upload (поля, ограничения)?** — `multipart/form-data`:
   - `files`: список файлов (обязателен);
   - `form_id`: query-параметр (обязателен).
   Ограничения/валидация: пустой список файлов/пустые имена → `RequestValidationError` (400); отсутствие `form_id` на уровне FastAPI → 422.

57. **Что возвращает POST /upload (формат ответа)?** — `UploadResponse`:
   - `message: str`
   - `details: []` (пусто при 202)
   - `upload_id: str`.

58. **Какой формат событий у upload-progress (SSE поля)?** — По `UploadProgressResponse` и реализации SSE:
   - `upload_id`, `status`, `current`, `total`, `progress_percentage`, `processed_files`, `errors`;
   - в финальном событии дополнительно `result` (dict с полным `UploadResponse`).

59. **Какие финальные статусы загрузки существуют?** — Для SSE: `completed` или `failed`. Для файловых результатов внутри `UploadResponse.details`: статусы `success`/`failed`.

60. **Где и как хранится прогресс обработки (в памяти/БД)?** — В памяти процесса: `UploadManager._upload_progress: Dict[str, UploadProgress]`.

61. **Когда прогресс удаляется и почему?** — В SSE генераторе: при терминальном статусе вызывается `upload_manager.cleanup_upload_progress(upload_id)`, чтобы освободить память и не держать завершённые задачи.

## База данных

62. **Какие коллекции MongoDB используются в текущей версии?** — `Files`, `FlatData`, `Forms`, `Logs`.

63. **Какие ключевые поля есть в Files, Forms, FlatData, Logs?** —
   - `Files`: `file_id`, `form_id`, `filename`, `year`, `reporter`, `status`, `error`, `upload_timestamp`, `updated_at`, `sheets`, `flat_data_size`.
   - `Forms`: `id`, `name`, `requisites`, `created_at`.
   - `FlatData`: `year`, `reporter`, `section`, `row`, `column`, `value`, `file_id`, `form`.
   - `Logs`: `_id`, `timestamp`, `scenario`, `level`, `message`, `meta`, а также `logger/pathname/lineno`.

64. **Какие индексы создаются на старте приложения?** — `create_indexes()` → `MongoIndexManager`:
   - `FlatData.main_unique_idx` (unique): `(file_id, year, reporter, section, row, column)`;
   - `FlatData.form_idx`: `(form)`;
   - `FlatData.reporter_year_idx`: `(reporter, year)`;
   - `FlatData.text_search_idx`: text на `(column, row)`;
   - `Files.uniq_file_id` (unique): `(file_id)`;
   - `Files.uniq_filename_form_id` (unique): `(filename, form_id)`.

65. **Какие массовые операции записи используются (bulk_write и т.д.)?** — Вставка FlatData: `bulk_write` с `InsertOne` чанками (`FlatDataService.save_flat_data`). При сбое — fallback на `insert_one`.

66. **Какие оптимизации хранения/сжатия введены в этой итерации?** — В коде зафиксированы чанки для bulk_insert, нормализация значений к BSON‑совместимым типам (`_to_builtin`). Конкретные настройки compression на уровне Mongo-конфига в коде не обнаружены (может быть на уровне окружения/админки).

## Тестирование

67. **Какие виды тестов есть сейчас (unit/integration/snapshot)?** —
   - unit: `tests/unit/*`;
   - «golden snapshot» тесты пайплайна: `tests/unit/test_golden_snapshots_pipeline.py` + скрипты `tests/scripts/golden_snapshot/*`;
   - client/API smoke tests: `tests/client/*` (через `requests`).

68. **Какие сценарии тестов покрывают именно новые пайплайны?** —
   - жизненный цикл записи файла и дубликаты: `test_upload_lifecycle_pipeline.py`;
   - корректность snapshot-пайплайна и эквивалентность API/сохранённых данных: `test_golden_snapshots_pipeline.py`;
   - консистентность `flat_data_size` и фактических вставок: `test_flat_data_size_consistency.py`;
   - поведение стратегий/заголовков: `test_horizontal_header_leading_drop.py`.

69. **Какие регрессии были пойманы и исправлены в этой итерации?** — Подтверждённые тестами: дубли заголовков, рассинхрон inserted_count, правила обрезки горизонтальных заголовков (section/okei/banner), запрет удаления системных форм.

70. **Что именно стабилизировали в snapshot-тестах?** — Зафиксирован запуск пайплайна «с нуля» и сравнение результата в унифицированном JSON payload (см. `run_upload_pipeline`, `build_snapshot_payload`).

71. **Есть ли конкретные метрики/цифры по тестам, которые можно безопасно вставить в отчет?** — Без запуска CI безопасно утверждать только то, что есть в коде/константах. Например: `MAX_LOGS = 1000`, chunk‑механика bulk_insert, обязательные поля SSE и payload’ов. Про процент покрытия/кол-во тестов — только после прогона.

## Наблюдаемость и эксплуатация

72. **Какие улучшения по логированию сделаны в этой итерации?** —
   - единая точка логирования `log_app_error` (пишет domain+meta в extra);
   - доменная коллекция `Logs` + сервис `LogService` с автоочисткой;
   - endpoint выгрузки `/logs/download` (CSV).

73. **Какие данные логируются при critical/non-critical ошибках?** — Через `AppError`: `message`, `domain`, `level`, `meta`; для critical часто включается `show_traceback=True`.

74. **Есть ли отдельный API для логов и как он используется фронтендом?** — Есть `GET /api/v2/logs/download` (CSV). Его можно использовать для выгрузки и диагностики, в т.ч. включающей “upload_error” записи из `Files`.

75. **Что изменилось в docker/healthcheck?** —
   - `Dockerfile` запускает `uvicorn main:app` на 2700;
   - `docker-compose.yml` содержит healthcheck для Mongo (`db.adminCommand('ping')`);
   - API health endpoint: `GET /api/v2/health` делает `db.command('ping')` и возвращает `healthy|degraded`.

76. **Есть ли автоочистка логов/временных данных?** — Есть автоочистка логов по количеству: `LogService.save_log` вызывает `cleanup_old_logs(MAX_LOGS)`.

## Реальные кейсы и примеры для текста

77. **Приведите 2–3 реальных технических кейса «было/стало» (суть проблемы и как решена).** —
   - Было: дубли горизонтальных заголовков ломали построение данных молча/неявно. Стало: `ParseHeadersStep` детектит дубли и падает `CriticalParsingError` с позициями.
   - Было: рассинхрон между ожидаемым количеством flat_data и реально вставленным (bulk_write частично успешен). Стало: `FlatDataService.save_flat_data` возвращает фактический `inserted_count`, а `DataSaveService` логирует discrepancy.
   - Было: пользователь ждёт окончания upload синхронно. Стало: `POST /upload` → 202 и SSE прогресс с финальным результатом.

78. **Приведите 1 пример файла/листа, который раньше ломался, а теперь проходит корректно.** — Подтверждаемый по фикстурам: `tests/fixtures/1fk/АЛАПАЕВСК 2020.xls` проходит в golden snapshot тестах.

79. **Есть ли фрагменты кода, которые вы хотите обязательно вставить как листинги (кроме сборки upload pipeline)?** — Рекомендуемые листинги:
   - `DefaultFormParsingStrategy.build_steps_for_sheet(...)` (канонический parsing pipeline);
   - `ProcessSheetsStep` (граница upload→parsing и обёртка ошибок);
   - SSE генератор в `upload_progress.py` (контракт прогресса).

80. **Нужен ли в тексте псевдокод parsing pipeline (или только описание шагов)?** — Лучше дать **псевдокод + список шагов**, потому что parsing pipeline строится стратегией и имеет hook‑вставки для форм.

81. **Нужно ли добавлять таблицу сравнения «до/после» по симптомам и эффектам?** — Да, это полезно: симптом → причина → изменение в архитектуре → эффект. Таблица хорошо “приземляет” рассказ.

## Границы и честные ограничения

82. **Какие ограничения системы остаются нерешенными после итерации?** —
   - прогресс upload в памяти процесса (нет распределённого прогресса);
   - parsing стратегии manual есть только для 1ФК; остальные формы — auto, что может быть недостаточно для нестандартных форм;
   - транзакции в Mongo зависят от настроек `config.MONGO_USE_TRANSACTIONS` и лимитов.

83. **Какие риски вы считаете самыми важными на следующую итерацию?** —
   - multi-worker развёртывание (потеря прогресса/состояния в памяти);
   - рост объёма FlatData и стоимость индексов/транзакций;
   - усложнение авто‑детекта структуры для новых форматов.

84. **Что точно нельзя утверждать в тексте (чтобы не завысить результат)?** — Нельзя утверждать:
   - что система полностью готова к горизонтальному масштабированию (пока прогресс в памяти);
   - что все форматы отчётности поддерживаются без ручных доработок;
   - что транзакционность гарантирована всегда (она условная, зависит от конфигурации и лимитов).

85. **Какие планы следующей итерации нужно указать обязательно?** —
   - вынести прогресс загрузки в внешнее хранилище (Redis) и/или job queue;
   - расширить набор manual‑стратегий или улучшить auto‑детект;
   - добавить мониторинг/метрики по времени шагов (профилирование уже заложено).

## Оформление и подача

86. **Подтвердите, что в основной части делаем строгую техническую структуру с симметрией разделов для upload/parsing.** — Да: одинаковая структура для обоих пайплайнов (цель → контекст → шаги → ошибки → эффекты/точки расширения).

87. **Хотите ли добавить отдельный подпункт «Архитектурные решения текущей итерации» перед пайплайнами?** — Да, это логично: кратко перечислить решения (2 пайплайна, стратегия+реестр, SSE прогресс, типизированные ошибки, rollback, индексы).

88. **Оставляем ли текущие рисунки-TODO или заменяем на текстовые схемы/таблицы без рисунков?** — Рекомендуется заменить на текстовые схемы/таблицы: они проще поддерживаются и не требуют актуализации изображений.

89. **В таблице 1: какие именно колонки хотите видеть, чтобы точно избежать наложения и сохранить смысл?** — Рекомендуемые колонки:
   - «Область» (upload/parsing/db/api)
   - «Было (симптом)»
   - «Причина»
   - «Изменение (код/модуль)»
   - «Стало (эффект)»
   - «Ограничения/риски»

90. **Нужен ли отдельный раздел с терминологией (краткий глоссарий) или это лишнее?** — Желательно: короткий глоссарий (Form/requisites, SheetModel, FlatDataRecord, critical/non-critical, pipeline step, strategy, SSE).
