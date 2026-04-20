





















Проверил все ключевые зависимости. Вот итоговый анализ:

## Проверка зависимостей

**✅ Все используемые библиотеки указаны корректно:**

| Библиотека | Где используется | Статус |
|------------|-----------------|--------|
| `fastapi` | [main.py](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/main.py:0:0-0:0), endpoints | ✅ |
| `motor` | [database.py](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/app/core/database.py:0:0-0:0), [dependencies.py](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/app/core/dependencies.py:0:0-0:0) | ✅ |
| `pandas` | [ExcelReader.py](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/app/application/upload/pipeline/readers/ExcelReader.py:0:0-0:0), parsing steps | ✅ |
| `pydantic`/`pydantic-settings` | [config/config.py](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/config/config.py:0:0-0:0), schemas | ✅ |
| `pymongo` | Через motor | ✅ |
| `prometheus_client` | [upload.py](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/app/api/v2/schemas/upload.py:0:0-0:0) метрики | ✅ |
| `pymorphy3` | [header_fixer.py](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/app/domain/parsing/header_fixer.py:0:0-0:0) | ✅ |
| `colorama` | [logger.py](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/app/core/logger.py:0:0-0:0) цветные логи | ✅ |
| `uvicorn` | [main.py](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/main.py:0:0-0:0) запуск | ✅ |
| `numpy` | Зависимость pandas | ✅ |
| `starlette` | [main.py](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/main.py:0:0-0:0) CORS | ✅ |
| `psutil` | [profiling.py](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/app/core/profiling.py:0:0-0:0) мониторинг памяти | ✅ |
| `python-dotenv` | [main.py](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/main.py:0:0-0:0) загрузка env | ✅ |
| `openpyxl` | Резервный движок pandas | ✅ |
| `python-calamine` | [ExcelReader.py](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/app/application/upload/pipeline/readers/ExcelReader.py:0:0-0:0) engine | ✅ |
| `python-multipart` | FastAPI UploadFile | ✅ |

**⚠️ Потенциально лишняя зависимость:**
- `requests==2.32.4` — не найдено использования в коде приложения ([app/](cci:9://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/app:0:0-0:0)). Возможно, используется в scripts/ или tests/, либо осталась от legacy кода.

**Вывод:** [requirements-prod.txt](cci:7://file:///c:/Users/Egor/Desktop/Projects/Min_sport/DWH/requirements-prod.txt:0:0-0:0) содержит все необходимые зависимости для production. Единственное замечание — `requests` возможно не используется, но его наличие не критично.