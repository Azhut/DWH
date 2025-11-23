import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from app.api.v2.endpoints.upload import router as upload_router
from app.api.v2.endpoints.filters import router as filters_router
from app.api.v2.endpoints.files import router as files_router

def create_app() -> FastAPI:
    app = FastAPI(
        title="Document Processing API",
        description="API для загрузки и обработки Excel-файлов",
        version="2.0.0"
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

app = create_app()

if __name__ == "__main__":

    uvicorn.run("main:app", host="0.0.0.0", port=2700)