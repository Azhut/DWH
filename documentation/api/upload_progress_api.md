# API: Отслеживание прогресса загрузки

## Получение upload_id

`upload_id` выдаётся при инициации загрузки через endpoint:

```
POST /api/v2/upload?form_id={form_id}
```

**Тело запроса:** `multipart/form-data` с файлами

**Пример ответа:**
```json
{
  "message": "Upload accepted. Track progress via /api/v2/upload-progress/{upload_id}",
  "details": [],
  "upload_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

Обработка файлов запускается в фоне, а клиент получает `upload_id` для отслеживания прогресса.

---

## Endpoint прогресса

```
GET /api/v2/upload-progress/{upload_id}
```

**Content-Type:** `text/event-stream` (Server-Sent Events)

---

## Параметры запроса

| Параметр | Тип | Обязательный | Описание |
|----------|-----|--------------|----------|
| `upload_id` | string | да | ID загрузки, полученный при старте операции |

---

## Формат ответа (SSE)

Соединение устанавливается как SSE-поток. Сервер отправляет события в формате:

```
data: {"current": 2, "total": 5, ...}\n\n
```

### Структура данных события

| Поле | Тип | Описание |
|------|-----|----------|
| `current` | integer | Номер текущего обрабатываемого файла |
| `total` | integer | Общее количество файлов |
| `status` | string | Текущий статус загрузки |
| `processed_files` | array[string] | Список уже обработанных файлов |
| `progress_percentage` | float | Процент выполнения (0.0 - 100.0) |
| `errors` | array[string] | Ошибки по отдельным файлам |

---

## Статусы загрузки

| Статус | Описание |
|--------|----------|
| `processing` | Идёт обработка файлов |
| `completed` | Все файлы успешно обработаны |
| `failed` | Произошла критическая ошибка |

---

## Пример запроса

```bash
curl -N "http://localhost:8000/api/v2/upload-progress/abc123"
```

---

## Пример ответа (SSE-поток)

```
data: {"current": 1, "total": 3, "status": "processing", "processed_files": ["file1.xlsx"], "progress_percentage": 33.3, "errors": []}

data: {"current": 2, "total": 3, "status": "processing", "processed_files": ["file1.xlsx", "file2.xlsx"], "progress_percentage": 66.7, "errors": []}

data: {"current": 3, "total": 3, "status": "completed", "processed_files": ["file1.xlsx", "file2.xlsx", "file3.xlsx"], "progress_percentage": 100.0, "errors": []}
```

---

## Коды ошибок

| Код | Сценарий |
|-----|----------|
| `404` | `upload_id` не найден |

При получении 404 соединение не устанавливается.

---

## Особенности поведения

- Соединение автоматически закрывается при статусах `completed` или `failed`
- Данные отправляются только при изменении состояния
- Период проверки обновлений: 100мс
- После завершения прогресс очищается из памяти
