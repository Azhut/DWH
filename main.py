import os

import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from app.api.v2.endpoints.upload import router as upload_router
from app.api.v2.endpoints.filters import router as filters_router
from app.api.v2.endpoints.files import router as files_router
from config import config
from launcher import get_launcher


def create_app() -> FastAPI:
    app = FastAPI(
        title="Document Processing API",
        description="API для загрузки и обработки Excel-файлов",
        version="2.0.0",
        debug=config.DEBUG
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(upload_router, prefix="/api/v2", tags=["upload"])
    app.include_router(filters_router, prefix="/api/v2", tags=["filters"])
    app.include_router(files_router, prefix="/api/v2", tags=["files"])

    return app


# Создаем приложение
app = create_app()

if __name__ == "__main__":
    # Убедимся что мы в правильной директории
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # Получаем соответствующий лаунчер
    launcher = get_launcher(config.APP_ENV)

    # Запускаем проверки
    launcher.run_checks()

    # Выводим информацию о запуске
    launcher.print_startup_info()

    # Запускаем сервер
    uvicorn.run(
        "main:app",
        host=config.API_HOST,
        port=config.API_PORT,
        reload=config.DEBUG,
        log_level="debug" if config.DEBUG else "info"
    )