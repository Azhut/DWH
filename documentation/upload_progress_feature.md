# Upload Progress Feature

## Реализована функциональность отслеживания прогресса загрузки файлов с полной обратной совместимостью.

### Что добавлено:

#### 1. **UploadResponse схема** (`app/api/v2/schemas/upload.py`)
```python
class UploadResponse(BaseModel):
    message: str
    details: List[FileResponse]
    upload_id: Optional[str] = None  # Optional для обратной совместимости
```

#### 2. **UploadProgress модель** (`app/application/upload/upload_progress.py`)
- Хранит состояние прогресса в памяти
- Отслеживает количество обработанных файлов
- Рассчитывает процент выполнения
- Собирает ошибки

#### 3. **UploadManager изменения** (`app/application/upload/upload_manager.py`)
- Генерирует `upload_id` для каждой загрузки
- Обновляет прогресс при обработке каждого файла
- Хранит прогресс в памяти (`self._upload_progress`)
- Добавлены методы `get_upload_progress()` и `cleanup_upload_progress()`

#### 4. **SSE Endpoint** (`app/api/v2/endpoints/upload_progress.py`)
- Новый endpoint `/api/v2/upload-progress/{upload_id}`
- Server-Sent Events для real-time обновлений
- Автоматическая очистка памяти после завершения

#### 5. **Зависимости**
- Добавлен `ssefastapi~=0.1.2` в `requirements-prod.txt`
- Новый router подключен в `main.py`

### Как это работает:

#### **Для существующих клиентов (обратная совместимость):**
```javascript
const response = await fetch('/api/v2/upload', {method: 'POST', body: formData});
const result = await response.json();
// result.upload_id будет undefined/null - все работает как раньше
console.log(result.details); // Массив обработанных файлов
```

#### **Для новых клиентов с прогрессом:**
```javascript
// 1. Загружаем файлы
const response = await fetch('/api/v2/upload', {method: 'POST', body: formData});
const {upload_id} = await response.json();

// 2. Подписываемся на прогресс
const eventSource = new EventSource(`/api/v2/upload-progress/${upload_id}`);
eventSource.onmessage = (event) => {
    const progress = JSON.parse(event.data);
    console.log(`Обработано ${progress.current} из ${progress.total} файлов`);
    updateProgressBar(progress.progress_percentage);
};
```

### Формат SSE сообщений:
```json
{
    "current": 2,
    "total": 5,
    "status": "processing",
    "processed_files": ["file1.xlsx", "file2.xlsx"],
    "progress_percentage": 40.0,
    "errors": []
}
```

### Тестирование:
✅ Обратная совместимость подтверждена
✅ Старый формат ответа работает с `upload_id: null`
✅ Новый формат работает с `upload_id: "uuid"`
✅ Все импорты успешны

### Установка:
```bash
pip install ssefastapi~=0.1.2
```

### Преимущества:
- **Полная обратная совместимость** - существующие клиенты не затрагиваются
- **Real-time прогресс** - мгновенные обновления статуса
- **Минимальные изменения** - не затрагивает существующую логику обработки
- **Автоматическая очистка** - память освобождается после завершения
- **Простая интеграция** -只需要 SSE клиент на фронтенде
