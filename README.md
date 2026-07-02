# DWH Sport API

Backend-сервис для приёма, парсинга и хранения спортивной отчётности из Excel (`.xls`, `.xlsx`, `.xlsm`). Преобразует листы отчётов в витрину `FlatData` с фильтрацией и выдачей через REST API.

## Быстрый старт

**Docker (рекомендуется):**

```bash
docker compose up -d --build
curl http://localhost:2700/api/v2/health
```

**Локально:**

```bash
pip install -r requirements-dev.txt
cp .env.example .env   # отредактировать под среду
python main.py
```

## Документация

| Раздел | Описание |
|--------|----------|
| [documentation/INDEX.md](documentation/INDEX.md) | Оглавление всей документации |
| [documentation/overview.md](documentation/overview.md) | Архитектура, пайплайны, API, деплой |
| [documentation/api/](documentation/api/) | Upload API, SSE progress |
| [documentation/deployment/](documentation/deployment/) | VM, Docker, миграция DuckDB |
| [documentation/testing/](documentation/testing/) | Golden snapshots |

## Стек

- Python 3.10, FastAPI, pandas
- MongoDB — метаданные (`Files`, `Forms`, `Logs`)
- DuckDB — витрина `FlatData` (по умолчанию, см. `FLATDATA_STORAGE`)

## Тесты

```bash
pytest
```
