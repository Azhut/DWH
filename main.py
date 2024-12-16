import os

import httpx
import uvicorn
from fastapi import FastAPI
from app.api.v1.endpoints.upload import router as upload_router
from app.api.v1.endpoints.document import router as document_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="Document Processing API",
        description="API для загрузки и обработки Excel-файлов",
        version="1.0.0"
    )
    app.include_router(upload_router, prefix="/api/v1/files", tags=["files"])
    app.include_router(document_router, prefix="/api/v1/documents", tags=["documents"])

    return app


app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))  # Читаем порт из переменной окружения
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload= True)
