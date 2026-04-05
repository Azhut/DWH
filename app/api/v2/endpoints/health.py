from datetime import datetime
from fastapi import APIRouter, Depends
from app.core.database import mongo_connection
from app.core.dependencies import get_log_service

router = APIRouter()


@router.get("/health")
async def health_check():
    """
    Простой healthcheck для Docker.
    Проверяет доступность MongoDB и базовую работоспособность API.
    """
    try:
        # Проверяем подключение к MongoDB
        db = mongo_connection.get_database()
        await db.command('ping')
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "database": "connected"
        }
    except Exception as e:
        # В случае ошибки подключения к БД все равно возвращаем 200,
        # но с информацией о проблеме для детального мониторинга
        return {
            "status": "degraded",
            "timestamp": datetime.utcnow().isoformat(),
            "database": "disconnected",
            "error": str(e)
        }
