# Golden Snapshot: генерация и тестирование

Этот документ описывает текущий процесс работы со snapshot-тестами после переноса скриптов в `tests/scripts`.

## Где лежат основные файлы

- Скрипт генерации отчёта и snapshot:
  - `tests/scripts/golden_snapshot/generate_golden_snapshot.py`
- Runtime-утилиты (запуск pipeline, сравнение данных):
  - `tests/scripts/golden_snapshot/runtime.py`
- Построение таблиц для визуального отчёта:
  - `tests/scripts/golden_snapshot/table_builder.py`
- Snapshot-тесты:
  - `tests/unit/test_golden_snapshots_pipeline.py`
- Snapshot-фикстуры:
  - `tests/fixtures/1fk_snapshots/*.expected.json`

## Что нужно перед запуском

1. Активировать окружение проекта.
2. Убедиться, что установлены зависимости (минимум: `pytest`, `pandas`, `openpyxl`, и зависимости приложения).
3. Для API-совместимого режима (`--use-db-form`) должна быть доступна MongoDB с формой `form_id`.

## Как сгенерировать новый snapshot

### Вариант 1: из IDE

Откройте `tests/scripts/golden_snapshot/generate_golden_snapshot.py` и запустите без аргументов.

Поведение по умолчанию:
- скрипт сам запускает режим `both --use-db-form`
- генерирует:
  - `visual_report.xlsx`
  - `.expected.json`

### Вариант 2: из консоли

Запуск из корня проекта:

```bash
py -3 tests/scripts/golden_snapshot/generate_golden_snapshot.py visual --use-db-form
```

```bash
py -3 tests/scripts/golden_snapshot/generate_golden_snapshot.py snapshot --use-db-form
```

```bash
py -3 tests/scripts/golden_snapshot/generate_golden_snapshot.py both --use-db-form
```

Основные полезные параметры:
- `--fixture` — путь к Excel-файлу фикстуры
- `--form-id` — ID формы
- `--form-name` — fallback-имя формы
- `--snapshot-output` — куда сохранить `.expected.json`
- `--report-output` — куда сохранить `visual_report.xlsx`
- `--checkpoint-file` — внешний JSON с checkpoints
- `--no-db-form` — запуск без чтения формы из БД

## Как запустить тесты

### Только snapshot-тесты

```bash
py -3 -m pytest -q tests/unit/test_golden_snapshots_pipeline.py
```

### Весь набор unit-тестов

```bash
py -3 -m pytest -q tests/unit
```

## Что проверяют тесты

Файл `tests/unit/test_golden_snapshots_pipeline.py` содержит две ключевые проверки:

1. `test_snapshot_matches_pipeline_output`
- Запускает upload pipeline на фикстуре.
- Строит фактический payload snapshot.
- Сравнивает с `.expected.json`:
  - `stats`
  - `sheets`
  - `checkpoints`
  - ключевые `meta` поля (`file_name`, `form_id`).

2. `test_pipeline_payload_is_api_equivalent`
- Сравнивает `ctx.flat_data` и payload, который ушёл бы в `PersistStep`.
- Проверяет API-эквивалентность результата pipeline.
- Выводит диагностический JSON при несовпадениях.

## Рекомендуемый рабочий процесс

1. Обновить/добавить fixture (`tests/fixtures/...`).
2. Сгенерировать `visual` и вручную проверить таблицы в `visual_report.xlsx`.
3. Сгенерировать `snapshot`.
4. Запустить `pytest` для `test_golden_snapshots_pipeline.py`.
5. Закоммитить обновлённый `.expected.json` вместе с изменениями кода (если были).

## Частые проблемы

- Ошибка по `Раздел0` в 1ФК:
  - В runtime предусмотрен fallback `skip_sheets=[0]` для `1ФК`, если `skip_sheets` не задан.
- Ошибки путей после переноса `scripts -> tests/scripts`:
  - Проверяйте, что команды и импорты используют `tests/scripts/...`.
- Нет `pandas` / `pytest`:
  - Установите зависимости в используемом интерпретаторе IDE/терминала.
