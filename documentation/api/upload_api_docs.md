# Upload API — Техническая документация

## Обзор

Загрузка файлов реализована как двухфазный процесс:

1. **`POST /upload`** — принимает файлы, немедленно возвращает `202 Accepted` с `upload_id`
2. **`GET /upload-progress/{upload_id}`** — SSE-поток с прогрессом; последнее событие содержит полный результат

Такой подход позволяет клиенту отображать прогресс в реальном времени без блокирующего ожидания и без дополнительных polling-запросов.

---

## Эндпоинты

### `POST /api/v2/upload`

Принимает файлы, валидирует запрос и запускает фоновую обработку.

**Query-параметры**

| Параметр  | Тип    | Обязательный | Описание         |
|-----------|--------|:------------:|------------------|
| `form_id` | string | ✅            | Идентификатор формы |

**Тело запроса** — `multipart/form-data`

| Поле    | Тип           | Описание                      |
|---------|---------------|-------------------------------|
| `files` | `UploadFile[]` | Один или несколько файлов     |

**Ответы**

| Статус | Условие                                              |
|--------|------------------------------------------------------|
| `202`  | Запрос валиден, обработка запущена в фоне            |
| `400`  | Нет файлов или невалидный `form_id`                  |
| `404`  | Форма с указанным `form_id` не найдена               |
| `422`  | Отсутствует обязательный query-параметр `form_id`    |
| `500`  | Внутренняя ошибка на уровне запроса                  |

**Тело ответа `202`** — схема `UploadResponse`

```json
{
  "message": "Upload accepted. Track progress at GET /api/v2/upload-progress/a1b2c3d4-...",
  "details": [],
  "upload_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

> `details` всегда пустой при `202`. Финальный список результатов по файлам
> приходит в последнем событии SSE-потока.

---

### `GET /api/v2/upload-progress/{upload_id}`

SSE-поток прогресса фоновой задачи. Клиент подключается сразу после
получения `upload_id` и держит соединение открытым до терминального события.

**Path-параметры**

| Параметр    | Тип    | Описание                            |
|-------------|--------|-------------------------------------|
| `upload_id` | string | UUID, полученный из `POST /upload` |

**Ответы**

| Статус | Условие                          |
|--------|----------------------------------|
| `200`  | Соединение открыто, поток идёт   |
| `404`  | `upload_id` не найден            |

**Headers ответа**

```
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
X-Accel-Buffering: no
```

---

## Формат SSE-событий

### Промежуточное событие (`status: "processing"`)

```
data: {"upload_id": "...", "status": "processing", "current": 2, "total": 5,
       "progress_percentage": 40.0, "processed_files": ["file1.xls", "file2.xlsx"],
       "errors": []}
```

### Финальное событие (`status: "completed"` или `"failed"`)

Финальное событие дополнительно содержит поле **`result`** — полный `UploadResponse`.
После его отправки сервер закрывает соединение и освобождает память задачи.

```
data: {"upload_id": "...", "status": "completed", "current": 5, "total": 5,
       "progress_percentage": 100.0, "processed_files": [...], "errors": [],
       "result": {
         "message": "5 files processed successfully, 0 failed.",
         "upload_id": "...",
         "details": [
           {"filename": "file1.xls", "status": "success", "error": null},
           {"filename": "file2.xlsx", "status": "success", "error": null}
         ]
       }}
```

### Схема полей события

| Поле                 | Тип            | Всегда | Описание                                              |
|----------------------|----------------|:------:|-------------------------------------------------------|
| `upload_id`          | string         | ✅      | UUID задачи                                           |
| `status`             | string         | ✅      | `processing` / `completed` / `failed`                 |
| `current`            | int            | ✅      | Количество обработанных файлов                        |
| `total`              | int            | ✅      | Общее количество файлов                               |
| `progress_percentage`| float          | ✅      | Процент выполнения (0.0–100.0)                        |
| `processed_files`    | string[]       | ✅      | Имена обработанных файлов                             |
| `errors`             | string[]       | ✅      | Сообщения об ошибках по отдельным файлам              |
| `result`             | UploadResponse | ❌      | Только в финальном событии. Полный результат обработки |

### Схема `result` (UploadResponse)

| Поле        | Тип            | Описание                                  |
|-------------|----------------|-------------------------------------------|
| `message`   | string         | Сводка: `"N processed successfully, M failed."` |
| `upload_id` | string         | UUID задачи                               |
| `details`   | FileResponse[] | Результаты по каждому файлу               |

### Схема `FileResponse`

| Поле       | Тип             | Описание                               |
|------------|-----------------|----------------------------------------|
| `filename` | string          | Имя файла                              |
| `status`   | `success/failed`| Статус обработки файла                 |
| `error`    | string / null   | Описание ошибки, если `status: failed` |

---

## Жизненный цикл задачи

```
POST /upload
    │
    ├─ валидация запроса ──────────────────────────── 400/404/422/500
    │
    ├─ читаем файлы в память
    ├─ регистрируем UploadProgress (status: "processing")
    ├─ запускаем asyncio.create_task(...)
    └─ → 202 { upload_id, message, details: [] }

asyncio background task:
    ├─ load_form(form_id)
    ├─ для каждого файла:
    │   ├─ process_file(pipeline)
    │   └─ progress.add_processed_file(...)   ← SSE читает это
    │
    ├─ все success → progress.complete(file_responses)
    └─ есть failed  → progress.fail(file_responses)

GET /upload-progress/{upload_id} (SSE):
    ├─ poll каждые 100 мс
    ├─ отправляем событие если данные изменились
    ├─ при status ∈ {completed, failed}:
    │   ├─ добавляем result = UploadResponseBuilder.build_response(...)
    │   ├─ отправляем финальное событие
    │   ├─ cleanup_upload_progress(upload_id)
    │   └─ закрываем соединение
```

---

## Пример клиентского кода

### Python

```python
import json
import requests

BASE = "http://localhost:2700/api/v2"

# 1. Загрузить файлы
with open("report.xlsx", "rb") as f:
    response = requests.post(
        f"{BASE}/upload",
        files=[("files", f)],
        params={"form_id": "my-form"},
    )

assert response.status_code == 202
upload_id = response.json()["upload_id"]
print(f"Задача принята: {upload_id}")

# 2. Следить за прогрессом и получить результат
with requests.get(f"{BASE}/upload-progress/{upload_id}", stream=True) as sse:
    for line in sse.iter_lines(decode_unicode=True):
        if not line.startswith("data: "):
            continue
        event = json.loads(line[6:])
        print(f"{event['progress_percentage']:.0f}% — {event['status']}")

        if event["status"] in ("completed", "failed"):
            result = event["result"]
            print(result["message"])
            for detail in result["details"]:
                print(f"  {detail['filename']}: {detail['status']}")
            break
```

### JavaScript (браузер)

```js
const BASE = "http://localhost:2700/api/v2";

// 1. Загрузить файлы
const form = new FormData();
form.append("files", fileInput.files[0]);

const res = await fetch(`${BASE}/upload?form_id=my-form`, {
  method: "POST",
  body: form,
});
const { upload_id } = await res.json(); // 202

// 2. Следить за прогрессом
const source = new EventSource(`${BASE}/upload-progress/${upload_id}`);

source.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(`${event.progress_percentage.toFixed(0)}% — ${event.status}`);

  if (event.status === "completed" || event.status === "failed") {
    console.log(event.result.message);
    event.result.details.forEach(d =>
      console.log(`  ${d.filename}: ${d.status}`)
    );
    source.close();
  }
};
```

---

## Компоненты

| Класс / модуль          | Путь                                                     | Ответственность                                               |
|-------------------------|----------------------------------------------------------|---------------------------------------------------------------|
| `UploadManager`         | `app/application/upload/upload_manager.py`               | Оркестрация: валидация, запуск фона, хранение прогресса       |
| `UploadProgress`        | `app/application/upload/upload_progress.py`              | Модель состояния задачи; хранит промежуточный и финальный результат |
| `UploadResponseBuilder` | `app/application/upload/response_builder.py`             | Единственная точка формирования `UploadResponse`              |
| `upload` endpoint       | `app/api/v2/endpoints/upload.py`                         | `POST /upload` → 202                                          |
| `upload_progress` endpoint | `app/api/v2/endpoints/upload_progress.py`             | `GET /upload-progress/{id}` → SSE                             |
| `UploadResponse`        | `app/api/v2/schemas/upload.py`                           | Pydantic-схема финального ответа                              |
| `UploadProgressResponse`| `app/api/v2/schemas/upload.py`                           | Pydantic-схема SSE-события                                    |

---

## Важные ограничения

- **Хранение в памяти.** `UploadProgress` хранится в словаре внутри `UploadManager`. При перезапуске сервиса или при работе с несколькими воркерами (gunicorn/uvicorn multi-worker) прогресс теряется. Для production-окружения с несколькими воркерами следует вынести хранилище в Redis или аналог.
- **Очистка памяти.** Запись в `_upload_progress` удаляется сразу после отправки финального SSE-события. Если клиент не подключился к SSE — запись остаётся до перезапуска. При необходимости добавить TTL-очистку фоновой задачей.
- **Интервал поллинга.** SSE-генератор опрашивает состояние каждые 100 мс (`_POLL_INTERVAL`). Для задач с очень быстрой обработкой можно уменьшить, для медленных — увеличить.
