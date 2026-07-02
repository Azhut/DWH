# Документация DWH Sport API

## Обзор

- [overview.md](overview.md) — архитектура, пайплайны upload/parsing, доменная модель, API-каталог, деплой, тестирование

## API

- [api/upload_api_docs.md](api/upload_api_docs.md) — `POST /upload`, асинхронная загрузка
- [api/upload_progress_api.md](api/upload_progress_api.md) — SSE `/upload-progress/{upload_id}`

## Архитектура и решения

- [architecture/flatdata_storage_decision.md](architecture/flatdata_storage_decision.md) — выбор DuckDB vs Mongo для FlatData
- [architecture/hierarchy_fix_summary.md](architecture/hierarchy_fix_summary.md) — исправление иерархии в header_parsing
- [architecture/uniqueness_check.md](architecture/uniqueness_check.md) — проверка уникальности записей

## Развёртывание и эксплуатация

- [deployment/vm_architecture.md](deployment/vm_architecture.md) — архитектура на VM заказчика
- [deployment/duckdb_migration_runbook.md](deployment/duckdb_migration_runbook.md) — миграция FlatData Mongo → DuckDB

## Тестирование

- [testing/snapshots_guide.md](testing/snapshots_guide.md) — инструкция по golden snapshots
- [testing/snapshots_tests.md](testing/snapshots_tests.md) — снепшоты и тесты

## Отчёты и Q&A

- [reports/iteration_report.md](reports/iteration_report.md) — отчёт о текущей итерации
- [reports/iteration_qa.md](reports/iteration_qa.md) — вопросы и ответы по итерации

## Внутреннее

- [internal/TODO.md](internal/TODO.md) — задачи команды
