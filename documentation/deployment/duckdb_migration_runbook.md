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
