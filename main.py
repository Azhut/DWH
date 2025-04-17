import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from app.api.v2.endpoints.upload import router as upload_router
from app.api.v2.endpoints.filters import router as filters_router
# main.py
from fastapi import Depends
from app.core.dependencies import get_sheet_repository, get_flat_data_repository
from app.data_storage.data_retrieval_service import DataRetrievalService
from app.data_storage.repositories.base import BaseRepository


def get_data_retrieval_service(
    sheet_repo: BaseRepository = Depends(get_sheet_repository),
    flat_repo: BaseRepository = Depends(get_flat_data_repository)
) -> DataRetrievalService:
    return DataRetrievalService(sheet_repo, flat_repo)

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

    return app

app = create_app()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=2700)