import uvicorn
from fastapi import FastAPI
from api.v2.endpoints.upload import router as upload_router
from api.v2.endpoints.filters import router as filters_router  # Исправлен импорт

def create_app() -> FastAPI:
    app = FastAPI(
        title="Document Processing API",
        description="API для загрузки и обработки Excel-файлов",
        version="2.0.0"  # Обновили версию API
    )

    # Исправленные префиксы
    app.include_router(upload_router, prefix="/api/v2/files", tags=["files"])
    app.include_router(filters_router, prefix="/api/v2", tags=["filters"])  # Измененный префикс

    return app

app = create_app()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=2700, reload=True)