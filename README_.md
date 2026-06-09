# DWH Sport API

## 1. Что это за проект
DWH Sport API — это backend-сервис для приема, валидации, парсинга и хранения табличной спортивной отчетности из Excel-файлов (`.xls`, `.xlsx`, `.xlsm`).

Сервис превращает разнородные листы отчетов в унифицированную витрину `FlatData`, которую можно:
- фильтровать по бизнес-полям,
- постранично выгружать в таблицу,
- использовать как источник данных для внешнего frontend/BI.

Ключевая идея проекта: стандартизировать сбор отчетности и дать единый API вместо ручной обработки Excel.

## 1.1 Упрощенное дерево файлов проекта
```
DWH/
├── app/
│   ├── api/              # HTTP-контракты (эндпоинты, схемы)
│   │   └── v2/           # API v2 endpoints
│   ├── application/      # Бизнес-сценарии и оркестрация
│   │   ├── data/         # Сервисы сохранения/удаления данных
│   │   ├── forms/        # Сервисы управления формами
│   │   ├── parsing/      # Parsing pipeline и стратегии
│   │   └── upload/       # Upload pipeline и шаги
│   ├── domain/           # Доменные модели, сервисы, репозитории
│   │   ├── file/         # Агрегат File
│   │   ├── flat_data/    # Агрегат FlatData
│   │   ├── form/         # Агрегат Form
│   │   ├── log/          # Агрегат Log
│   │   ├── parsing/      # Доменная логика парсинга
│   │   └── sheet/        # Агрегат Sheet
│   ├── core/             # Инфраструктурные компоненты
│   │   ├── database.py           # Подключение к MongoDB
│   │   ├── dependencies.py       # DI контейнер
│   │   ├── duckdb_database.py    # Подключение к DuckDB
│   │   ├── exceptions.py         # Иерархия исключений
│   │   ├── logger.py             # Логирование
│   │   ├── mongo_transactions.py # Транзакции MongoDB
│   │   └── profiling.py          # Профилирование пайплайнов
│   └── utils/            # Утилиты
├── config/               # Конфигурация
├── tests/                # Тесты
│   ├── fixtures/         # Тестовые файлы и snapshots
│   ├── scripts/          # Скрипты для golden snapshot
│   └── unit/             # Unit-тесты
├── docker/               # Docker конфигурации
├── documentation/        # Дополнительная документация
├── main.py               # Точка входа приложения
├── Dockerfile            # Сборка Docker образа
└── docker-compose.yml    # Оркестрация сервисов
```

## 2. Бизнес-цель и ценность
Проект закрывает три задачи:
- Автоматизация обработки отчетов: снижение ручного труда при консолидации таблиц.
- Единая модель данных: независимо от вариаций исходных форм, на выходе единый плоский формат.
- Управляемая эксплуатация: контейнерное развертывание, предсказуемый запуск, встроенные проверки состояния и логи.

## 3. Основные возможности
### 3.1 Загрузка и обработка отчетов
- Принимает один или несколько Excel-файлов через `POST /api/v2/upload`.
- Валидация имени файла: выделение `reporter` и `year`, проверка расширения.
- Асинхронная фоновая обработка (ответ `202 Accepted` сразу после приема).
- Защита от повторной успешной загрузки того же файла в рамках формы.

### 3.2 Прогресс обработки в реальном времени
- `GET /api/v2/upload-progress/{upload_id}` отдает SSE-поток.
- Клиент видит текущее состояние (`processing/completed/failed`), процент, список обработанных файлов, ошибки.
- Финальное SSE-событие содержит итоговый `UploadResponse` по всем файлам.

### 3.3 Работа с формами
- CRUD форм: `GET/POST/PUT/DELETE /api/v2/forms...`.
- На старте автоматически обеспечивается наличие системных форм (`1ФК`, `5ФК`).
- Для форм можно задавать реквизиты (например, `skip_sheets`) для управления парсингом.

### 3.4 Доступ к данным витрины
- `GET /api/v2/filters-names` — список доступных фильтров.
- `POST /api/v2/filter-values` — значения для конкретного фильтра с учетом уже выбранных условий.
- `POST /api/v2/filtered-data` — выдача таблицы с пагинацией.

### 3.5 Работа с файлами и данными
- `GET /api/v2/files` — список загруженных файлов со статусами.
- `DELETE /api/v2/files/{file_id}` — каскадное удаление записи файла и связанного `FlatData`.

### 3.6 Эксплуатация и диагностика
- `GET /api/v2/health` — проверка API + ping MongoDB.
- `GET /api/v2/logs/download` — CSV выгрузка логов и upload-ошибок.
- Автосоздание индексов MongoDB на старте.

## 4. Доменная модель данных
Проект использует MongoDB с основными коллекциями:
- `Files` — метаинформация о загруженных файлах и статус обработки.
- `FlatData` — нормализованные строки витрины (`year`, `reporter`, `section`, `row`, `column`, `value`, `file_id`, `form`). Может храниться в MongoDB или DuckDB (настраивается через `FLATDATA_STORAGE`).
- `Forms` — справочник форм и их реквизитов.
- `Logs` — системные/бизнес-логи обработки и операций.

Ключевые индексы:
- `Files`: уникальный `file_id`, уникальная пара (`filename`, `form_id`).
- `FlatData`: составной индекс для фильтрации и уникальный индекс для защиты от дублей строк (при хранении в MongoDB).

## 5. Архитектурные концепты
Проект структурирован по слоям:
- `app/api` — HTTP-контракты (эндпоинты, схемы).
- `app/application` — бизнес-сценарии и оркестрация пайплайнов.
- `app/domain` — доменные модели, сервисы и репозитории.
- `app/core` — инфраструктурные компоненты (БД, исключения, зависимости, транзакции).

Поддержка двух хранилищ данных:
- MongoDB — основное хранилище для метаданных (Files, Forms, Logs) и опционально для FlatData.
- DuckDB — опциональное высокопроизводительное хранилище для FlatData (аналитические запросы).

### 5.1 Upload pipeline (обработка файла)
Upload pipeline — это цепочка шагов для обработки каждого загруженного файла. Пайплайн реализован через паттерн Chain of Responsibility с контекстом `UploadPipelineContext`.

**Шаги пайплайна (по порядку):**
1. **AcquireFileRecordStep** — создание или актуализация записи файла в БД со статусом `PROCESSING`. Проверка на дубликаты по уникальному индексу `(filename, form_id)`.
2. **ReadFileContentStep** — чтение байтов файла в память (FastAPI закрывает UploadFile после возврата ответа).
3. **ExtractMetadataStep** — извлечение метаданных из имени файла: `reporter` (субъект), `year` (год), проверка расширения.
4. **ReadWorkbookStep** — чтение Excel workbook через pandas с backend calamine (высокопроизводительное чтение .xls/.xlsx/.xlsm).
5. **ProcessSheetsStep** — итерация по листам workbook, для каждого листа запускается Parsing Pipeline через стратегию формы.
6. **FinalizeFileModelStep** — финализация модели файла: подсчет статистики, установка статуса `SUCCESS` или `FAILED`.
7. **EnrichFlatDataStep** — обогащение всех записей FlatData метаданными файла (`file_id`, `year`, `reporter`, `form`).
8. **PersistStep** — сохранение данных в БД через DataSaveService с поддержкой транзакций.

**Обработка ошибок в Upload Pipeline:**
- `CriticalUploadError` — останавливает обработку файла, инициирует rollback записи файла в БД, пробрасывается наверх.
- `DuplicateFileError` — файл уже был успешно загружен, не считается критической ошибкой, клиент получает 409 Conflict.
- `NonCriticalUploadError` — логируется как warning, позволяет пайплайну продолжить выполнение.
- Непредвиденные исключения — оборачиваются в `CriticalUploadError` с traceback, инициируют rollback.

**Rollback механизм:**
- При критической ошибке вызывается `DataSaveService.rollback()`, который:
  - Обновляет статус файла на `FAILED` с сообщением об ошибке.
  - Удаляет все FlatData записи, связанные с `file_id`.
  - Выполняется в транзакции (если поддерживается БД) или с компенсационными операциями.

### 5.2 Parsing Pipeline (парсинг листов)
Parsing Pipeline — это цепочка шагов для парсинга отдельного листа Excel. Запускается из ProcessSheetsStep для каждого листа, который должна обработать стратегия формы.

**Архитектура Parsing Pipeline:**
- **ParsingStrategyRegistry** — реестр стратегий парсинга, связывает `FormType` с конкретной реализацией `BaseFormParsingStrategy`.
- **BaseFormParsingStrategy** — абстрактный базовый класс стратегии. Определяет:
  - `should_process_sheet()` — нужно ли обрабатывать данный лист.
  - `build_steps_for_sheet()` — возвращает список шагов для листа.
- **DefaultFormParsingStrategy** — базовая реализация с типовым pipeline фаз, который можно расширять через хуки.

**Реализованные стратегии:**
- `FK1FormParsingStrategy` — ручная стратегия для формы `1ФК` с фиксированной структурой и специфическими шагами (округление, обработка notes).
- `AutoFormParsingStrategy` — универсальная стратегия для `5ФК` и пользовательских форм с авто-детекцией структуры.

**Типовой pipeline (DefaultFormParsingStrategy):**
1. **NormalizeSheetNameStep** — нормализация имени листа к каноническому виду (например, "Раздел 4" → "Раздел4").
2. **NormalizeDataFrameStep** — нормализация DataFrame по строке нумерации столбцов 1..n, обрезка невалидных колонок.
3. **DetectTableStructureStep** — определение структуры таблицы:
   - Для автоформ: поиск строки нумерации 1..n, определение границ заголовков и данных.
   - Для ручных форм: использование фиксированной структуры из конфигурации.
4. **Дополнительные шаги** (форма-специфичные, через хук `get_additional_steps_before_headers()`):
   - Для 1ФК: `FK1RoundingStep`, `ProcessNotesStep`.
5. **ParseHeadersStep** — парсинг горизонтальных и вертикальных заголовков с обработкой многоуровневой иерархии.
6. **ExtractDataStep** — извлечение данных из ячеек с учетом структуры и заголовков. Поддерживает дедупликацию колонок (для 5ФК).
7. **GenerateFlatDataStep** — построение плоских записей FlatData из извлеченных данных.

**Обработка ошибок в Parsing Pipeline:**
- `CriticalParsingError` — останавливает обработку листа, пробрасывается наверх в ProcessSheetsStep, который решает судьбу всего файла.
- `NonCriticalParsingError` — логируется как warning, добавляется в `ctx.warnings`, выполнение продолжается.
- Непредвиденные исключения — оборачиваются в `CriticalParsingError` с traceback.

### 5.3 Обработка переносов строк (\n) в заголовках
Обработка переносов строк реализована в `HeaderFixer` (`app/domain/parsing/header_fixer.py`). Класс использует многоуровневый подход для принятия решения: склеивать слова вокруг `\n` или заменять на пробел.

**Приоритеты принятия решения:**
1. **Manual map** — явные ручные override-ы из JSON-файла (высший приоритет).
2. **Эвристика по символам** — быстрый O(1) анализ символов вокруг `\n`:
   - Пунктуация перед `\n` (`,.;:!?)»–—"`) → пробел.
   - Дефис перед `\n` → join (мягкий перенос).
   - Заглавная буква после `\n` → пробел.
   - Цифра с любой стороны → пробел.
   - Пробел до или после `\n` → пробел.
3. **Словарь-сет** — проверка склеенного слова в наборе известных словоформ pymorphy3 (O(1) lookup).
4. **pymorphy3 + LRU cache** — морфологический анализ для неоднозначных случаев с кешированием результатов.

**Сохранение новых случаев:**
- Все решения, дошедшие до pymorphy3 (реальные edge cases), сохраняются в manual map JSON-файл.
- При следующем запуске эти случаи будут разрешены на уровне 1 (manual map), что ускоряет обработку.
- Механизм позволяет системе "обучаться" на новых кейсах без изменения кода.

**Сложность:**
- Эвристика: O(1) на разрыв.
- Словарь-сет: O(1) lookup.
- pymorphy3: O(1) с LRU cache (размер кеша 4096).
- Общая сложность линейная относительно количества разрывов в заголовке.

### 5.4 Стратегии определения структуры таблицы
Определение структуры таблицы (границы заголовков, начало данных, валидные колонки) реализовано через паттерн Strategy.

**FixedStructureStrategy** (для 1ФК):
- Использует фиксированные значения из конфигурации: `header_start_row`, `header_end_row`, `data_start_row`, `vertical_header_column`.
- Не выполняет авто-детекцию, полагается на заранее известную структуру формы.

**AutoDetectStructureStrategy** (для 5ФК и автоформ):
- Автоматически находит строку нумерации столбцов 1..n в первых 80 строках листа.
- Определяет валидные колонки: от столбца с "1" до столбца с "n" включительно.
- Все, что вне прогона 1..n, считается невалидными колонками и отрезается.
- Определяет границы заголовков: всё выше строки нумерации — зона заголовков.
- Определяет начало данных: `data_start_row = numbering_row + 1`.
- Если строка нумерации не найдена — выбрасывает ошибку (для автоформ это критично).

**Алгоритм поиска прогона 1..n:**
- Ищет первую "1" в строке.
- Проверяет, что далее идут строго 2, 3, 4, ... без пропусков.
- Между числами последовательности мусор быть не может.
- Выбирает строку с самым длинным прогоном (или с самым левым началом при равной длине).

### 5.5 Работа с базой данных
Проект использует MongoDB как основное хранилище и DuckDB как опциональное аналитическое хранилище для FlatData.

**MongoDB (motor - async driver):**
- **Подключение:** `DatabaseConnection` в `app/core/database.py` держит singleton `AsyncIOMotorClient`.
- **Пул соединений:** настроен с `maxPoolSize=100`, `minPoolSize=10`, таймауты для работы с параллельными запросами.
- **Коллекции:**
  - `Files` — метаинформация о загруженных файлах и статус обработки.
  - `FlatData` — нормализованные строки витрины (опционально, если `FLATDATA_STORAGE=mongo`).
  - `Forms` — справочник форм и их реквизитов.
  - `Logs` — системные/бизнес-логи обработки и операций.
- **Индексы:**
  - `Files`: уникальный `file_id`, уникальная пара `(filename, form_id)` для защиты от дублей.
  - `FlatData`: составной индекс для фильтрации, уникальный индекс для защиты от дублей строк (при хранении в MongoDB).

**DuckDB (опционально для FlatData):**
- **Подключение:** `DuckDBConnection` в `app/core/duckdb_database.py`.
- **Использование:** высокопроизводительное аналитическое хранилище для FlatData, если `FLATDATA_STORAGE=duckdb`.
- **Преимущества:** быстрые аналитические запросы, columnar storage, поддержка SQL.
- **Staging механизм:** при сохранении используется staging таблица для проверки дубликатов перед promote в основную таблицу.

### 5.6 Механизм транзакций
Транзакции реализованы в `app/core/mongo_transactions.py` через функцию `run_in_transaction()`.

**Поддержка транзакций:**
- MongoDB поддерживает multi-document транзакции только в режиме replica set.
- В текущем docker-compose используется одиночный MongoDB без replica set.
- Приложение автоматически определяет поддержку транзакций и использует fallback при необходимости.

**Логика работы:**
1. Если `MONGO_USE_TRANSACTIONS=false` — выполняется без сессии транзакции (`session=None`).
2. Если включено — пытается открыть сессию и стартовать транзакцию.
3. При ошибке "Transaction not supported" (коды 20, 303, 116 или сообщение про replica set):
   - Логируется warning.
   - Выполняется одна попытка без транзакции с `session=None`.
4. При других ошибках транзакции — пробрасывается исключение наверх.

**Сценарии сохранения:**
- **Малый объем** (до `MONGO_TRANSACTION_MAX_FLAT_RECORDS`): транзакционный сценарий — все операции в одной транзакции.
- **Большой объем:** пакетная запись чанками (`FLATDATA_BULK_CHUNK_SIZE`) с компенсационной очисткой при ошибках.
- **DuckDB:** staging → проверка дубликатов → promote в основную таблицу с rollback при ошибках.

**Rollback:**
- Для MongoDB: при критической ошибке пайплайна вызывается `DataSaveService.rollback()`, который обновляет статус файла и удаляет FlatData записи.
- Для DuckDB: при ошибке staging удаляется, выполняется cleanup основной таблицы по `file_id`.

### 5.7 Dependency Injection (DI)
DI реализован через фабричные функции с `@lru_cache` в `app/core/dependencies.py`.

**Принципы:**
- **Aggregate-Centric:** репозитории и сервисы — из domain; сценарии — из application.
- **Singleton через LRU cache:** каждый фабричный метод кешируется, возвращая один экземпляр.
- **Явные зависимости:** каждый сервис/репозиторий получает зависимости через конструктор.

**Иерархия зависимостей:**
```
get_database() (singleton)
  ↓
get_file_repository() → get_file_service()
get_flat_data_repository() → get_flat_data_service()
get_form_repository() → get_form_service()
get_logs_repository() → get_log_service()
get_sheet_service() (без репозитория)
  ↓
get_data_save_service(file_service, flat_data_service, log_service)
get_data_delete_service(file_service, flat_data_service, log_service)
get_form_maintenance_service(form_service, file_service, flat_data_service, log_service)
get_upload_manager(file_service, form_service, data_save_service, parsing_registry)
```

**Очистка кеша:**
- Функция `clear_dependency_caches()` сбрасывает все LRU cache.
- Используется в тестах или после переконфигурации.
- В продакшене обычно достаточно перезапуска процесса.

### 5.8 Обработка исключений и ошибок
Иерархия исключений реализована в `app/core/exceptions.py`.

**Базовые классы:**
- `AppError` — базовая ошибка приложения с полями:
  - `message` — сообщение об ошибке.
  - `level` — уровень логирования (debug/info/warning/error/critical).
  - `domain` — доменная область (file/flat_data/form/upload/parsing/...).
  - `http_status` — HTTP статус для маппинга (опционально).
  - `meta` — произвольный технический контекст.
  - `show_traceback` — нужно ли логировать traceback.

**Ошибки валидации (уровень домена):**
- `FormValidationError` — ошибка валидации формы.
- `FileValidationError` — ошибка валидации файла или его имени.

**Ошибки уровня запроса:**
- `RequestValidationError` — прерывает выполнение handler, маппится в HTTP 400/404/500.

**Ошибки загрузки:**
- `CriticalUploadError` — останавливает обработку файла, инициирует rollback.
- `NonCriticalUploadError` — логируется как warning, позволяет пайплайну продолжить.
- `DuplicateFileError` — файл уже был успешно загружен, HTTP 409.

**Ошибки парсинга:**
- `CriticalParsingError` — останавливает обработку листа и всего файла.
- `NonCriticalParsingError` — логируется как warning, позволяет продолжить.

**Функции обработки:**
- `to_http_exception(error)` — преобразует `AppError` в `HTTPException` для FastAPI.
- `log_app_error(error)` — единая точка логирования с передачей `meta` в `extra` логгера.
- `log_and_raise_http(error)` — логирует и выбрасывает `HTTPException` (для RequestValidationError).

**Стратегия обработки:**
- Критические ошибки — логируются с traceback, инициируют rollback, пробрасываются наверх.
- Некритические ошибки — логируются как warning, добавляются в контекст, выполнение продолжается.
- Непредвиденные исключения — оборачиваются в критические с traceback.

### 5.9 Golden Snapshot механизм
Golden Snapshot — это механизм детерминистического тестирования пайплайна парсинга через эталонные snapshots.

**Назначение:**
- Валидация изменений в parsing pipeline.
- Защита от регрессий при рефакторинге.
- Ручная верификация результатов парсинга через визуальные отчеты.

**Структура snapshot файла (.expected.json):**
```json
{
  "description": "Описание snapshot",
  "meta": {
    "file_name": "Имя файла",
    "form_id": "ID формы",
    "generated_at": "Время генерации"
  },
  "stats": {
    "total_sheets": число,
    "total_flat_records": число
  },
  "sheets": [
    {
      "sheet_name": "Имя листа",
      "sheet_fullname": "Полное имя листа",
      "headers": {
        "horizontal": ["Заголовки колонок"],
        "vertical": ["Заголовки строк"]
      },
      "flat_data_records": [
        {
          "year": 2020,
          "reporter": "СУБЪЕКТ",
          "section": "Раздел1",
          "row": "Строка1",
          "column": "Колонка1",
          "value": 100
        }
      ]
    }
  ],
  "checkpoints": [] // Опциональные контрольные точки
}
```

**Генерация snapshot:**
Скрипт `tests/scripts/golden_snapshot/generate_golden_snapshot.py` поддерживает три режима:

1. **visual** — генерирует Excel-отчет `visual_report.xlsx` для ручной проверки:
   - Overview — общая статистика.
   - BySection — разбивка по разделам.
   - ApiMismatches — расхождения между pipeline и API-equivalent payload.
   - Для каждого листа: таблица, статистика, preview FlatData.

2. **snapshot** — генерирует `.expected.json` после проверки visual отчета:
   - Запускает upload pipeline на фикстуре.
   - Проверяет API-эквивалентность (сравнивает `ctx.flat_data` и persist payload).
   - Сохраняет snapshot с checkpoint-спецификациями (если указаны).

3. **both** — последовательно запускает visual и snapshot.

**Сохранение файла:**
- Snapshot сохраняется в `tests/fixtures/{form}_snapshots/{filename}.expected.json`.
- Файл содержит полную структуру распарсенных данных для всех листов.
- Включает метаинформацию для отслеживания контекста генерации.

**Сложность:**
- Генерация: O(N) где N — количество записей FlatData (линейная).
- Сравнение: O(N) с использованием hash-based сравнения для оптимизации.
- Проверка API-эквивалентности: O(N) с детальной диагностикой расхождений.

**Фильтрация и checkpoints:**
- Checkpoints позволяют задавать ожидаемые значения для конкретных ячеек.
- Используются для валидации критически важных участков данных.
- При тестировании проверяется, что фактические значения соответствуют checkpoint-спецификациям.

**Запуск тестов:**
```bash
pytest tests/unit/test_golden_snapshots_pipeline.py
```
Тест проверяет:
- Соответствие snapshot output pipeline'у.
- API-эквивалентность результата pipeline.

### 5.10 Фильтрация данных FlatData
Фильтрация данных реализована в `FlatDataService` (`app/domain/flat_data/service.py`).

**Модель FlatDataRecord:**
```python
{
  "year": int,           # Год отчетности
  "reporter": str,       # Субъект (в верхнем регистре)
  "section": str,        # Раздел
  "row": str,            # Строка
  "column": str,         # Колонка
  "value": int/float/str, # Значение
  "file_id": str,        # ID файла
  "form": str            # ID формы
}
```

**Маппинг фильтров API → поля БД:**
```python
FILTER_MAP = {
  "год": "year",
  "субъект": "reporter",
  "раздел": "section",
  "строка": "row",
  "колонка": "column"
}
```

**Построение запроса:**
- Фильтры комбинируются через `$and`.
- Поддерживается pattern search (regex) для значений фильтров.
- `reporter` автоматически приводится к верхнему регистру.

**Кеширование значений фильтров:**
- Результаты `get_filter_values()` кешируются в памяти процесса.
- Ключ кеша: hash от (filter_name, applied_filters, pattern, form_id).
- Размер кеша: 128 записей с LRU eviction.
- Ускоряет повторные запросы фильтров с теми же условиями.

**Сложность фильтрации:**
- Построение запроса: O(F) где F — количество фильтров.
- Выполнение запроса: зависит от индексов в БД (обычно O(log N) для поиска).
- Кеширование: O(1) lookup при попадании в кеш.

**Выдача данных:**
- `get_filtered_data()` возвращает таблицу строк с пагинацией (limit/offset).
- Общее количество строк считается отдельным запросом для корректной пагинации.
- Значения нормализуются: NaN → None, float → int если целое, round(2) для дробных.

## 5.11 Алгоритмы обработки файлов (детально)

### 5.11.1 Извлечение метаданных из имени файла
Алгоритм валидации и извлечения метаданных реализован в `FileService.validate_and_extract_metadata_from_filename()`.

**Псевдокод:**
```
function validate_and_extract_metadata(filename):
    if filename is empty:
        raise FileValidationError("File name cannot be empty")

    if "." not in filename:
        raise FileValidationError("File name must include an extension")

    stem, ext = filename.rsplit(".", 1)
    if ext is empty:
        raise FileValidationError("File name has an empty extension")

    if ext.lower() not in [".xlsx", ".xls", ".xlsm"]:
        raise FileValidationError("Invalid file extension")

    year_matches = findall(r"[0-9]{4}", stem)
    if len(year_matches) != 1:
        raise FileValidationError("File name must contain exactly one 4-digit year")

    year = int(year_matches[0])
    subject_raw = stem[:year_match.start()] + stem[year_match.end():]
    reporter = normalize_spaces(subject_raw).upper()

    if reporter is empty:
        raise FileValidationError("Reporter cannot be empty")
    if year < 1900 or year > 2100:
        raise FileValidationError("Invalid year")

    return FileInfo(reporter, year, ext.lower())
```

**Примеры:**
- `"МОСКВА 2020.xlsx"` → reporter="МОСКВА", year=2020, extension="xlsx"
- `"САНКТ-ПЕТЕРБУРГ_2021.xls"` → reporter="САНКТ-ПЕТЕРБУРГ", year=2021, extension="xls"
- `"2020 МОСКВА.xlsx"` → reporter="МОСКВА", year=2020, extension="xlsx"

### 5.11.2 Чтение Excel файлов
Чтение Excel реализовано в `ExcelReader.read()` через pandas с backend calamine.

**Псевдокод:**
```
function read_excel(content_bytes, filename):
    if content_bytes is empty:
        raise ValueError("Empty file content")

    validate_excel_format(content_bytes, filename)

    try:
        sheets_dict = pandas.read_excel(
            BytesIO(content_bytes),
            sheet_name=None,           # Все листы
            header=None,                # Без автопарсинга заголовков
            dtype=object,               # Все как строки/объекты
            engine="calamine"           # Высокопроизводительный движок
        )
    except Exception as e:
        raise RuntimeError(f"Failed to read Excel: {e}")

    result = {}
    for sheet_name, df in sheets_dict.items():
        if df is not empty:
            result[sheet_name] = df

    return result
```

**Валидация формата по magic bytes:**
- `.xls` (OLE2): `\xD0\xCF\x11\xE0`
- `.xlsx/.xlsm` (ZIP): `PK\x03\x04`
- HTML маскируется под Excel: проверяется на `<html` или `<!do`

**Движок calamine:**
- Поддерживает .xls, .xlsx, .xlsm
- Быстрее openpyxl для чтения
- Не выполняет автотипизацию (dtype=object)
- Не парсит заголовки (header=None) — это делает parsing pipeline

### 5.11.3 Определение структуры таблицы (детально)
Алгоритм авто-детекции структуры реализован в `auto_detect_table_layout()`.

**Псевдокод для AutoDetectStructureStrategy:**
```
function auto_detect_table_layout(dataframe, sheet_name):
    # Шаг 1: Поиск строки нумерации 1..n
    numbering_row, first_col, last_col, seq_len = find_numbering_row(dataframe)

    if numbering_row is None:
        raise ValueError("Numbering row not found")

    # Шаг 2: Определение границ заголовков
    header_end_row = numbering_row - 1
    header_start_row = find_first_non_empty_row(
        dataframe,
        max_row=header_end_row,
        col_range=[first_col, last_col]
    )

    # Шаг 3: Начало данных
    data_start_row = numbering_row + 1

    # Шаг 4: Вертикальный заголовок (всегда колонка 0 после обрезки)
    vertical_header_column = 0

    return TableStructure(
        header_start_row,
        header_end_row,
        data_start_row,
        vertical_header_column
    )
```

**Алгоритм поиска прогона 1..n:**
```
function find_numbering_row(dataframe, max_rows=80, min_seq_len=3):
    best_result = None

    for row_idx in range(min(max_rows, len(dataframe))):
        run = find_1_to_n_run(dataframe.iloc[row_idx], min_seq_len)
        if run is None:
            continue

        start_col, end_col, seq_len = run
        if seq_len < min_seq_len:
            continue

        if best_result is None or seq_len > best_result.seq_len:
            best_result = (row_idx, start_col, end_col, seq_len)

    return best_result
```

**Алгоритм поиска прогона 1..n в строке:**
```
function find_1_to_n_run(row_values, min_seq_len):
    expected = 1
    in_sequence = False
    finished = False
    start_col = None
    end_col = None

    for col_idx, value in enumerate(row_values):
        parsed = to_positive_int(value)

        if not in_sequence:
            if parsed == 1:
                in_sequence = True
                start_col = col_idx
                end_col = col_idx
                expected = 2
            continue

        if not finished:
            if parsed == expected:
                end_col = col_idx
                expected += 1
                continue
            if parsed is None:
                finished = True
                continue
            return None  # Нарушение последовательности

        if finished:
            break  # После NaN игнорируем всё

    if start_col is None:
        return None

    seq_len = expected - 1
    if seq_len < min_seq_len:
        return None

    return (start_col, end_col, seq_len)
```

### 5.11.4 Парсинг заголовков (детально)
Алгоритм парсинга заголовков реализован в `parse_headers()`.

**Парсинг горизонтальных заголовков:**
```
function parse_horizontal_headers(header_rows_dataframe):
    # Заполнение пустых ячеек (проброс сверху и слева)
    fill_empty_cells(header_rows_dataframe)

    horizontal = []
    n_rows = len(header_rows_dataframe)

    for col_idx in range(1, header_rows_dataframe.shape[1]):
        path = []
        current = header_rows_dataframe.iloc[n_rows - 1, col_idx]
        path.append(current)

        # Поднимаемся вверх, собирая иерархию
        for row_idx in range(n_rows - 2, -1, -1):
            val = header_rows_dataframe.iloc[row_idx, col_idx]
            if val != current:
                path.insert(0, val)
                current = val

        horizontal.append(PATH_SEPARATOR.join(path))

    return horizontal
```

**Парсинг вертикальных заголовков с иерархией:**
```
function parse_vertical_headers(sheet, structure, workbook_source):
    # Извлечение значений из колонки вертикальных заголовков
    vertical_values = sheet.iloc[structure.data_start_row:, structure.vertical_header_column]
    vertical_values = vertical_values[vertical_values.notna()].tolist()

    # Определение режима иерархии
    mode = get_vertical_hierarchy_mode()  # "auto", "indent", "heuristics"

    if mode in ["auto", "indent"] and is_xlsx_like(workbook_source):
        # Пытаемся получить indent levels из openpyxl
        indent_levels = try_get_indent_levels_from_openpyxl(
            workbook_source.content,
            sheet_name,
            structure,
            vertical_df_row_indices
        )

        if indent_levels and any(level > 0 for level in indent_levels):
            # Конвертируем indent в глубины (0, 1, 2, ...)
            depths = convert_indents_to_depths(indent_levels)
        else:
            # Fallback на эвристику
            depths = estimate_depths_heuristic(vertical_values)
    else:
        # Эвристика по текстовым маркерам
        depths = estimate_depths_heuristic(vertical_values)

    # Строим иерархические пути по глубинам
    vertical_paths = build_hierarchy_paths_from_depths(
        vertical_values,
        depths,
        max_path_segments
    )

    # Нормализация (удаление \n, артефактов)
    vertical_paths = normalize_headers(vertical_paths)

    return vertical_paths
```

**Эвристика определения глубин (для вертикальных заголовков):**
```
function estimate_depths_heuristic(values, config):
    depths = []
    current_context_depth = 0
    in_subblock = False

    for raw_value in values:
        s = str(raw_value).strip()
        leading_spaces = len(s) - len(s.lstrip(" "))
        sl = s.lower()

        base_level = leading_spaces // config.leading_spaces_per_level

        # Триггеры вложенности
        opens_phrase = sl.startswith("из них") or sl.startswith("в том числе")
        dash = s.startswith("-")
        space_only_child = leading_spaces >= config.min_leading_spaces_for_child_hint

        # Логика определения глубины
        if opens_phrase:
            depth = current_context_depth + 1
            current_context_depth = depth
            in_subblock = True
        elif dash:
            depth = current_context_depth + 1
            current_context_depth = depth
            in_subblock = True
        elif space_only_child:
            depth = current_context_depth
            in_subblock = True
        elif in_subblock and not should_exit_subblock(s, config):
            depth = current_context_depth
        else:
            depth = base_level
            current_context_depth = base_level
            in_subblock = False

        depths.append(depth)

    # Нормализация к 0-based
    baseline = depths[0]
    if baseline > 0:
        depths = [d - baseline for d in depths]

    return depths
```

### 5.11.5 Общий алгоритм обработки файла (Upload Pipeline)
```
# Шаг 1: Валидация запроса
validate_request(files, form_id)

# Шаг 2: Чтение файлов в память
for file in files:
    content = await file.read()
    buffered.append((filename, content_type, content))

# Шаг 3: Создание upload_id и прогресса
upload_id = generate_uuid()
progress = UploadProgress(upload_id, total_files=len(buffered))

# Шаг 4: Фоновая обработка каждого файла
for filename, content_type, content in buffered:
    # 4.1 AcquireFileRecordStep
    file_model = create_or_update_file(filename, form_id, status="PROCESSING")

    # 4.2 ReadFileContentStep
    file_content = content

    # 4.3 ExtractMetadataStep
    file_info = validate_and_extract_metadata_from_filename(filename)
    # reporter, year, extension

    # 4.4 ReadWorkbookStep
    workbook_sheets = excel_reader.read(file_content, filename)
    # {sheet_name: DataFrame}

    # 4.5 ProcessSheetsStep (для каждого листа)
    for sheet_name, sheet_df in workbook_sheets.items():
        # 4.5.1 Получение стратегии формы
        strategy = parsing_registry.get_strategy(form_info.type)

        # 4.5.2 Проверка: нужно ли обрабатывать лист
        if not strategy.should_process_sheet(sheet_name, sheet_index, form_info):
            continue

        # 4.5.3 Построение parsing pipeline
        parsing_steps = strategy.build_steps_for_sheet(sheet_name, form_info)

        # 4.5.4 Выполнение parsing pipeline
        # NormalizeSheetNameStep
        # NormalizeDataFrameStep
        # DetectTableStructureStep
        # [Дополнительные шаги формы]
        # ParseHeadersStep
        # ExtractDataStep
        # GenerateFlatDataStep
        parsed_sheet = run_parsing_pipeline(sheet_df, parsing_steps)

        add_to_context(parsed_sheet)

    # 4.6 FinalizeFileModelStep
    file_model.status = "SUCCESS"
    file_model.stats = calculate_statistics()

    # 4.7 EnrichFlatDataStep
    for flat_record in context.flat_data:
        flat_record.file_id = file_model.file_id
        flat_record.year = file_info.year
        flat_record.reporter = file_info.reporter
        flat_record.form = form_id

    # 4.8 PersistStep
    if use_transactions and record_count < threshold:
        save_in_transaction(file_model, flat_records)
    else:
        save_in_chunks(file_model, flat_records, chunk_size)

    # 4.9 Обновление прогресса
    progress.add_processed_file(filename, success=True)
```

## 5.12 Примеры сценариев обработки файлов

### 5.12.1 Сценарий: 2 файла 1ФК (1 успешный, 1 неуспешный)

**Входные данные:**
- Файл 1: `"МОСКВА 2020 1ФК.xlsx"` (корректный)
- Файл 2: `"INVALID.xls"` (невалидное имя)
- Form ID: `eab639f7-78c4-4e08-bd27-756bac5cf571` (1ФК)

**Обработка файла 1 (МОСКВА 2020 1ФК.xlsx):**

```
1. AcquireFileRecordStep
   - Создается запись Files со статусом PROCESSING
   - file_id = "uuid-1"
   - Проверка на дубликаты: (filename, form_id) — уникально

2. ReadFileContentStep
   - Читаются байты файла в память
   - file_content = <bytes>

3. ExtractMetadataStep
   - Парсинг имени: "МОСКВА 2020 1ФК.xlsx"
   - stem = "МОСКВА 2020 1ФК"
   - ext = "xlsx"
   - year_matches = ["2020"]
   - year = 2020
   - subject_raw = "МОСКВА  1ФК"
   - reporter = "МОСКВА 1ФК"
   - file_info = {reporter: "МОСКВА 1ФК", year: 2020, extension: "xlsx"}

4. ReadWorkbookStep
   - Валидация magic bytes: PK\x03\x04 (ZIP/.xlsx)
   - pandas.read_excel(engine="calamine", header=None, dtype=object)
   - workbook_sheets = {"Раздел1": DataFrame(50x20), "Раздел2": DataFrame(30x15)}

5. ProcessSheetsStep для листа "Раздел1"
   - Стратегия: FK1FormParsingStrategy
   - should_process_sheet("Раздел1", 0, form_info) → True

   Parsing Pipeline:
   a) NormalizeSheetNameStep
      - "Раздел1" → "Раздел1" (уже канонический)

   b) NormalizeDataFrameStep
      - Поиск строки нумерации 1..n в первых 80 строках
      - Найдена строка 5: [NaN, 1, 2, 3, ..., 15]
      - first_col = 1, last_col = 15
      - Обрезка DataFrame к колонкам [1:15]
      - data_start_row = 6

   c) DetectTableStructureStep
      - FixedStructureStrategy для 1ФК
      - header_start_row = 0
      - header_end_row = 4
      - data_start_row = 6
      - vertical_header_column = 0

   d) FK1RoundingStep (форма-специфичный)
      - Округление значений в ячейках до 2 знаков

   e) ProcessNotesStep (форма-специфичный)
      - Обработка примечаний в 1ФК

   f) ParseHeadersStep
      - Горизонтальные заголовки:
        * Заполнение пустых ячеек сверху/слева
        * Сбор иерархии: ["Раздел1|Всего", "Раздел1|Из них|мужчины", ...]
        * Удаление баннера "Раздел"
        * Удаление баннера "ОКЕИ" (для 1ФК)
      - Вертикальные заголовки:
        * Извлечение значений из колонки 0
        * Эвристика глубин по маркерам "из них", тире, отступам
        * Построение путей: ["Спорт", "Спорт|Футбол", "Спорт|Футбол|Мужчины", ...]

   g) ExtractDataStep
      - Извлечение значений из ячеек по структуре
      - Дедупликация колонок: OFF (для 1ФК)

   h) GenerateFlatDataStep
      - Построение плоских записей:
        {year: 2020, reporter: "МОСКВА 1ФК", section: "Раздел1",
         row: "Спорт|Футбол", column: "Всего", value: 1000, ...}

6. ProcessSheetsStep для листа "Раздел2"
   - Аналогичная обработка
   - Добавляются записи FlatData для Раздел2

7. FinalizeFileModelStep
   - status = "SUCCESS"
   - stats = {total_sheets: 2, total_flat_records: 150}

8. EnrichFlatDataStep
   - Все 150 записей обогащаются:
     file_id = "uuid-1", year = 2020, reporter = "МОСКВА 1ФК", form = "1ФК"

9. PersistStep
   - Транзакционное сохранение (150 < threshold)
   - Files.update({status: "SUCCESS", ...})
   - FlatData.insert_many(150 records)
   - Коммит транзакции

10. Обновление прогресса
    - progress.add_processed_file("МОСКВА 2020 1ФК.xlsx", success=True)
```

**Обработка файла 2 (INVALID.xls):**

```
1. AcquireFileRecordStep
   - Создается запись Files со статусом PROCESSING
   - file_id = "uuid-2"

2. ReadFileContentStep
   - Читаются байты файла

3. ExtractMetadataStep
   - Парсинг имени: "INVALID.xls"
   - stem = "INVALID"
   - ext = "xls"
   - year_matches = [] (нет 4-значного года)
   - ОШИБКА: FileValidationError("File name must contain exactly one 4-digit year")

4. Обработка ошибки
   - CriticalUploadError с сообщением об ошибке
   - Вызов rollback: Files.update({status: "FAILED", error: "..."})
   - FlatData.delete_by_file_id("uuid-2") (пусто, но для надежности)

5. Обновление прогресса
    - progress.add_processed_file("INVALID.xls", success=False, error="...")
```

**Итоговый ответ клиенту:**
```json
{
  "upload_id": "upload-uuid",
  "files": [
    {
      "filename": "МОСКВА 2020 1ФК.xlsx",
      "status": "success",
      "file_id": "uuid-1"
    },
    {
      "filename": "INVALID.xls",
      "status": "failed",
      "error": "File name must contain exactly one 4-digit year"
    }
  ]
}
```

### 5.12.2 Сценарий: успешная загрузка файла 5ФК

**Входные данные:**
- Файл: `"АРТИНСКАЯ СШ 2024 5ФК.xlsm"`
- Form ID: `3b5ca99e-cdc7-4590-b4d7-b9d6d95ebc69` (5ФК)

**Отличия от 1ФК:**

```
1. ExtractMetadataStep
   - reporter = "АРТИНСКАЯ СШ", year = 2024, extension = "xlsm"

2. ProcessSheetsStep для листа "Раздел 4"
   - Стратегия: AutoFormParsingStrategy

   Parsing Pipeline:
   a) NormalizeSheetNameStep
      - "Раздел 4" → "Раздел4" (нормализация)

   b) NormalizeDataFrameStep
      - Авто-поиск строки нумерации 1..n
      - Найдена строка 3: [NaN, 1, 2, 3, ..., 20]
      - Обрезка к колонкам [1:20]

   c) DetectTableStructureStep
      - AutoDetectStructureStrategy
      - Авто-определение границ по строке нумерации
      - header_start_row = 0 (первая непустая до нумерации)
      - header_end_row = 2
      - data_start_row = 4

   d) [Дополнительные шаги отсутствуют] (для 5ФК нет специфичных шагов)

   e) ParseHeadersStep
      - Горизонтальные заголовки:
        * Сбор иерархии
        * Удаление баннера "Раздел" (для всех форм)
        * НЕ удаление баннера "ОКЕИ" (только для 1ФК)
      - Вертикальные заголовки:
        * Попытка получить indent levels из openpyxl (.xlsm поддерживается)
        * Если indent > 0: использование indent-based иерархии
        * Иначе: эвристика по маркерам

   f) ExtractDataStep
      - Дедупликация колонок: ON (для 5ФК)
      - Если колонка "Всего" встречается 2 раза → оставляется только первое вхождение

   g) GenerateFlatDataStep
      - Построение плоских записей аналогично 1ФК
```

## 5.13 Сценарий получения данных с фильтрами

### 5.13.1 Получение списка доступных фильтров
```
GET /api/v2/filters-names?form_id=...

Response:
{
  "filters": ["год", "субъект", "раздел", "строка", "колонка"]
}
```

### 5.13.2 Получение значений для фильтра
```
POST /api/v2/filter-values?form_id=...
Request:
{
  "filter_name": "субъект",
  "filters": [
    {"filter-name": "год", "values": [2020, 2021]}
  ],
  "pattern": "МОСК"
}

Алгоритм:
1. Валидация filter_name в FILTER_MAP
2. Маппинг: "субъект" → "reporter"
3. Построение запроса:
   {
     "$and": [
       {"year": {"$in": [2020, 2021]}},
       {"reporter": {"$regex": "МОСК", "$options": "i"}}
     ]
   }
4. Проверка кеша по ключу hash(("субъект", filters, "МОСК", form_id))
5. Если в кеше → вернуть из кеша
6. Иначе:
   - repo.distinct("reporter", query)
   - Сортировка результатов
   - Сохранение в кеш (LRU, 128 записей)
   - Возврат значений

Response:
{
  "filter_name": "субъект",
  "values": ["МОСКВА", "МОСКОВСКАЯ ОБЛАСТЬ"]
}
```

### 5.13.3 Получение отфильтрованных данных
```
POST /api/v2/filtered-data?form_id=...
Request:
{
  "filters": [
    {"filter-name": "год", "values": [2020]},
    {"filter-name": "субъект", "values": ["МОСКВА"]},
    {"filter-name": "раздел", "values": ["Раздел1"]}
  ],
  "limit": 50,
  "offset": 0
}

Алгоритм:
1. Валидация всех filter_names
2. Маппинг фильтров:
   - "год" → "year"
   - "субъект" → "reporter"
   - "раздел" → "section"
3. Построение запроса:
   {
     "$and": [
       {"year": {"$in": [2020]}},
       {"reporter": {"$in": ["МОСКВА"]}},
       {"section": {"$in": ["Раздел1"]}}
     ]
   }
4. repo.get_filtered_data(query, limit=50, offset=0)
   - Выполняет два запроса:
     a) count_documents(query) → total
     b) find(query).limit(50).skip(0) → docs
5. Пост-обработка документов:
   - Для каждого doc:
     * year = doc["year"]
     * reporter = doc["reporter"]
     * section = doc["section"]
     * row = doc["row"]
     * column = doc["column"]
     * value = normalize_value(doc["value"])
       - NaN → None
       - float.is_integer() → int
       - иначе → round(value, 2)
   - Формирование строки: [year, reporter, section, row, column, value]

Response:
{
  "headers": ["год", "субъект", "раздел", "строка", "колонка", "значение"],
  "data": [
    [2020, "МОСКВА", "Раздел1", "Спорт", "Всего", 1000],
    [2020, "МОСКВА", "Раздел1", "Спорт", "Мужчины", 600],
    ...
  ],
  "size": 50,
  "max_size": 150
}
```

## 6. API (краткий каталог)
- `POST /api/v2/upload`
- `GET /api/v2/upload-progress/{upload_id}`
- `GET /api/v2/forms`
- `GET /api/v2/forms/{form_id}`
- `POST /api/v2/forms`
- `PUT /api/v2/forms/{form_id}`
- `DELETE /api/v2/forms/{form_id}`
- `GET /api/v2/files`
- `DELETE /api/v2/files/{file_id}`
- `GET /api/v2/filters-names`
- `POST /api/v2/filter-values`
- `POST /api/v2/filtered-data`
- `GET /api/v2/logs/download`
- `GET /api/v2/health`

## 7. Развертывание у заказчика (Docker) — подробно

### 7.1 Что используется
- Docker image приложения: собирается из `Dockerfile`.
- Docker Compose: оркестрация `app` + `mongo` через `docker-compose.yml`.
- Постоянное хранилище Mongo: именованный том `mongodb_data`.

### 7.2 Состав docker-compose
Поднимаются 2 сервиса:
- `app`:
  - образ `dwh-app`, контейнер `sport_api`.
  - порт `2700:2700` (API доступен с хоста на 2700).
  - зависит от `mongo` по healthcheck.
  - `restart: unless-stopped`.
  - том `duckdb_data:/data/duckdb` для хранения FlatData в DuckDB (если включено).
- `mongo`:
  - образ `mongo:5.0`, контейнер `mongodb`.
  - порт `2701:27017` (внутри контейнера Mongo всегда 27017).
  - том `mongodb_data:/data/db`.
  - healthcheck: `mongo --eval "db.adminCommand('ping')"`.

### 7.3 Как работает сеть Docker в этом проекте
- Compose создает отдельную внутреннюю сеть проекта.
- Внутри сети контейнеры общаются по имени сервиса.
- Поэтому `app` подключается к БД по `MONGO_URI=mongodb://mongo:27017`:
  - `mongo` — DNS-имя контейнера Mongo внутри docker-сети.
  - `27017` — внутренний порт Mongo.

Важно:
- Внешний порт `2701` нужен только для доступа к Mongo с хоста (админские задачи).
- Для связи `app -> mongo` внешний порт не используется.

### 7.4 Как собирается образ приложения (Dockerfile)
`Dockerfile` делает следующие шаги:
1. База: `python:3.10-slim`.
2. `WORKDIR /app`.
3. Копирование `requirements-prod.txt` в контейнер как `requirements.txt`.
4. `pip install --no-cache-dir -r requirements.txt`.
5. `COPY . .` — копирование исходников проекта.
6. `ENV PORT=2700`.
7. Запуск: `uvicorn main:app --host 0.0.0.0 --port 2700`.

Что важно про `.dockerignore`:
- Исключены технические артефакты (git, кеши, IDE и т.д.).
- Исключены `*.md` и `profiling_data/`.
- Это уменьшает build context и ускоряет сборку.

### 7.5 Порядок запуска контейнеров
При `docker compose up -d`:
1. Запускается `mongo`.
2. Docker ждет, пока healthcheck `mongo` станет `healthy`.
3. После этого запускается `app`.

На старте `app` выполняет lifecycle-инициализацию:
- создание/актуализация индексов в Mongo,
- инициализация реестра стратегий парсинга,
- обеспечение системных форм (`1ФК`, `5ФК`).

### 7.6 Переменные окружения и их роль
Ключевые переменные для сервиса:
- `APP_ENV` — режим (`production`/`development`).
- `MONGO_URI` — строка подключения к Mongo.
- `DATABASE_NAME` — имя БД.
- `API_HOST`, `API_PORT` — bind-параметры API.
- `DEBUG` — уровень debug-поведения.
- `ENABLE_PROFILING` — включение профилирования.
- `FLATDATA_STORAGE` — хранилище для FlatData (`mongo` или `duckdb`).
- `DUCKDB_PATH` — путь к файлу DuckDB (при использовании DuckDB).
- `MONGO_USE_TRANSACTIONS` — использовать ли транзакции.
- `MONGO_TRANSACTION_MAX_FLAT_RECORDS` — порог, до которого применяется транзакционный сценарий.
- `FLATDATA_BULK_CHUNK_SIZE` — размер чанка пакетной вставки `FlatData`.

### 7.7 Транзакции MongoDB и важный нюанс
В коде поддержаны транзакции, но для полноценных multi-document транзакций MongoDB должен работать как replica set.

В текущем `docker-compose.yml` поднимается одиночный `mongo:5.0` без инициализации replica set. Что это значит:
- приложение пытается работать транзакционно,
- если сервер транзакции не поддерживает, автоматически включает fallback без транзакции.

Итог:
- сервис остается работоспособным,
- но строгая атомарность multi-document операций зависит от режима Mongo.

В репозитории есть скрипт `docker/mongodb/replica-set-init.sh`, который можно использовать для перехода к replica set-конфигурации в прод-контуре заказчика.

### 7.8 Пошаговый сценарий развертывания на сервере заказчика
Пример для Linux-сервера:

1. Установить Docker и Docker Compose plugin.
2. Склонировать репозиторий в рабочую директорию.
3. Перейти в каталог проекта:
```bash
cd /path/to/DWH
```
4. Проверить/настроить переменные в `docker-compose.yml` под среду заказчика (особенно `MONGO_URI`, `DATABASE_NAME`, порты).
5. Собрать и поднять сервисы:
```bash
docker compose up -d --build
```
6. Проверить статус:
```bash
docker compose ps
```
7. Проверить health API:
```bash
curl http://<server-host>:2700/api/v2/health
```

Ожидаемый результат:
- контейнер `mongodb` — `healthy`,
- контейнер `sport_api` — `running`,
- endpoint `/api/v2/health` возвращает `status: healthy` или `degraded` (если проблема с БД).

### 7.9 Обновление версии (релиз)
Базовый цикл обновления:
```bash
cd /path/to/DWH
git pull
docker compose up -d --build
```

Что происходит:
- подтягивается новый код,
- пересобирается образ `dwh-app`,
- контейнер `app` перезапускается,
- данные Mongo сохраняются, так как лежат в named volume `mongodb_data`.

### 7.10 Резервное копирование и восстановление
Так как данные находятся в named volumes, контейнеры можно пересоздавать без потери данных.

Для Mongo рекомендуется использовать `mongodump`/`mongorestore`:
```bash
docker exec mongodb mongodump --db sport_data --out /data/db/dump
docker exec mongodb mongorestore --db sport_data /data/db/dump/sport_data
```

Для DuckDB (если используется) файл базы данных находится в volume `duckdb_data` и может быть копирован напрямую:
```bash
docker cp sport_api:/data/duckdb/flat_data.duckdb ./backup/
```

### 7.11 Проверка и диагностика в эксплуатации
Основные команды:
```bash
docker compose ps
docker compose logs -f app
docker compose logs -f mongo
```

Проверки API:
- `GET /api/v2/health` — общее состояние.
- `GET /api/v2/logs/download` — выгрузка диагностических записей.

Типовые симптомы и причины:
- `app` не стартует: Mongo не прошел healthcheck или ошибка в env.
- Ошибки транзакций: Mongo без replica set (включится fallback).
- Медленная загрузка больших отчетов: стоит проверить `FLATDATA_BULK_CHUNK_SIZE`, ресурсы CPU/RAM и размеры файлов.

## 8. Локальный запуск без Docker (для разработки)
```bash
pip install -r requirements-dev.txt
python main.py
```

Для локальной разработки Mongo обычно запускается отдельно, а в `.env` указывается:
- `MONGO_URI=mongodb://localhost:27017`
- `DATABASE_NAME=sport_data`
- `FLATDATA_STORAGE=mongo` (или `duckdb` для использования DuckDB)
- `DUCKDB_PATH=./data/duckdb/flat_data.duckdb` (при использовании DuckDB)

## 9. Тестирование
Запуск unit-тестов:
```bash
pytest
```

Основной фокус тестов — пайплайн загрузки, парсинг, логирование и консистентность `FlatData`.

## 10. Ограничения и важные замечания
- SSE-прогресс хранится в памяти процесса (`UploadManager`), без внешнего broker/кеша.
- При горизонтальном масштабировании потребуется вынести состояние прогресса в отдельное хранилище (например, Redis).
- Для строгой транзакционности Mongo в production рекомендуется replica set-конфигурация.

## 11. Краткое резюме
DWH Sport API — это специализированный сервис обработки спортивной Excel-отчетности с понятным API, формализованным пайплайном парсинга и готовым контейнерным контуром развертывания.

Для заказчика это дает:
- предсказуемое внедрение через Docker,
- прозрачную эксплуатацию (health/logs),
- масштабируемую основу для дальнейшего развития витрины данных.
