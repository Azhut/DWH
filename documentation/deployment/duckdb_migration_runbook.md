# Runbook: миграция FlatData на DuckDB

## Ветки Git

| Ветка | Назначение |
|-------|------------|
| `backup/pre-duckdb-migration` | Снимок кода до миграции (откат реализации) |
| `DuckDB_migration` | Вся работа по DuckDB |

Откат кода:

```bash
git checkout backup/pre-duckdb-migration
```

## Переменные окружения

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `FLATDATA_STORAGE` | `duckdb` | `duckdb` или `mongo` (legacy) |
| `DUCKDB_PATH` | `./data/flat_data.duckdb` | Путь к файлу БД |
| `DUCKDB_MIGRATION_BATCH_SIZE` | `50000` | Батч для скрипта миграции |

В Docker: `DUCKDB_PATH=/data/duckdb/flat_data.duckdb`, volume `duckdb_data`.

## Проверка перед запуском

1. Убедиться, что текущая ветка — `DuckDB_migration`.
2. Проверить, что в рабочем каталоге нет лишних незакоммиченных изменений, кроме `scripts/migrate_flatdata_mongo_to_duckdb.py`.
3. Подключиться к файлу `DUCKDB_PATH` и убедиться, что:
   - таблица `flat_data_staging` пуста;
   - таблица `migration_state` пуста или содержит корректное состояние последней миграции;
   - `flat_data` содержит либо старое рабочее состояние, либо это чистый файл, который вы хотите перезаписать.
4. Если вы хотите начать с нуля, используйте `--reset`.

## Важные изменения в скрипте

Скрипт теперь поддерживает безопасный прогон без записи:

- `--dry-run` — проверка чтения MongoDB, нормализации и логики скрипта без записи в DuckDB.
- `--dry-run-limit <n>` — остановка после обработки `n` документов в dry-run режиме.
- В dry-run режиме `migration_state` не обновляется, а состояние в DuckDB не меняется.
- `--reset` очищает и `flat_data`, и `migration_state` при реальном запуске.
- `--dry-run --reset` ничего не удаляет: это только проверка поведения скрипта.

## Порядок выката на VM

1. Остановить приём загрузок (сообщить пользователям) или остановить `sport_api`.
2. `git pull` ветки `DuckDB_migration`, `docker compose build`, поднять **только mongo** при необходимости.
3. Запустить миграцию данных (может идти часами):

```bash
cd /home/user/dashboards/DWH
docker compose up -d mongo
nohup docker compose run --rm app python scripts/migrate_flatdata_mongo_to_duckdb.py \
  >> /home/user/dashboards/migration_flatdata.log 2>&1 &
tail -f /home/user/dashboards/migration_flatdata.log
```

4. Дождаться строки `Миграция завершена` и сверки count в логе.
5. Поднять полный стек: `docker compose up -d`.
6. Проверить `GET /api/v2/health` — `duckdb: true`, фильтры по 5ФК.

## Безопасные прогоны

- Быстрая проверка скрипта без записи:
  ```bash
  docker compose run --rm app python scripts/migrate_flatdata_mongo_to_duckdb.py --dry-run --dry-run-limit 1000
  ```
- Проверка работы нормализации и чтения Mongo без изменения DuckDB:
  ```bash
  docker compose run --rm app python scripts/migrate_flatdata_mongo_to_duckdb.py --dry-run
  ```
- Полный перезапуск миграции с очисткой:
  ```bash
  docker compose run --rm app python scripts/migrate_flatdata_mongo_to_duckdb.py --reset
  ```

## Что может пойти не так

- `duckdb_migration_runbook.md` файл может быть неактуален, если ветка не та — убедитесь, что вы на `DuckDB_migration`.
- В `DUCKDB_PATH` может лежать старый файл с мусором: проверьте содержимое таблиц перед запуском.
- Если в `flat_data_staging` остались строки, они могут конфликтовать с новым промоутом.
- Если `migration_state` содержит некорректный `last_id`, скрипт продолжит не с начала, а с этого `_id`.
- Если MongoDB активно пишет в `FlatData` во время миграции, прогон может быть непоследовательным.
- `ON CONFLICT DO NOTHING` может скрыть дубли и не дать точного числа «вставлено», поэтому нужно сверять absolute counts.
- Если на сервере недостаточно памяти, DuckDB может замедлиться или завершиться с ошибкой.
- Если `--reset` указан по ошибке, данные в DuckDB будут удалены и восстановлению не подлежат.

## Проверка после миграции

1. Убедиться, что лог содержит `Миграция завершена`.
2. Проверить, что `flat_data_staging` остаётся пустой.
3. Сверить `mongo_total` и `duckdb_total` в логах; небольшое расхождение допускается только за счёт `skipped_invalid`.
4. Проверить `GET /api/v2/health`.

Повторный запуск скрипта без `--reset` продолжит с последнего `_id`.

Полный перезапуск миграции:

```bash
docker compose run --rm app python scripts/migrate_flatdata_mongo_to_duckdb.py --reset
```

## Откат на Mongo (без потери кода)

1. `git checkout backup/pre-duckdb-migration` (или `main` до merge).
2. В `.env` / compose: `FLATDATA_STORAGE=mongo`.
3. `docker compose up -d` — FlatData снова читается из MongoDB (данные в Mongo не трогались).

## Новые загрузки после cutover

Upload пишет в DuckDB через staging (`flat_data_staging` → проверки → `flat_data`).
Mongo: только `Files`, `Forms`, `Logs`.
