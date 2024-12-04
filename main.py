import uvicorn
from fastapi import FastAPI
from app.api.v1.endpoints.upload import router as upload_router

def create_app() -> FastAPI:
    """
    Создает и настраивает экземпляр FastAPI-приложения.
    """

    app = FastAPI()

    app.include_router(upload_router, prefix="/api/v1/files", tags=["files"])

    return app

app = create_app()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
