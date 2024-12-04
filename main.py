import uvicorn
from fastapi import FastAPI
from app.api.v1.routers.upload_router import router as upload_router

def create_app() -> FastAPI:
    """
    Создает и настраивает экземпляр FastAPI-приложения.
    """
    app = FastAPI()
    app.include_router(upload_router)
    return app

app = create_app()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
