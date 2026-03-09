import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from app.api.v2.endpoints.files import router as files_router
from app.api.v2.endpoints.filters import router as filters_router
from app.api.v2.endpoints.forms import router as forms_router
from app.api.v2.endpoints.upload import router as upload_router
from app.application.data.indexes import create_indexes
from config.config import config
from launcher import get_launcher


def create_app() -> FastAPI:
    app = FastAPI(
        title="Document Processing API",
        version="2.0.0",
        debug=(config.APP_ENV == "development"),
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.on_event("startup")
    async def _create_db_indexes() -> None:
        await create_indexes()
        from app.application.parsing.registry import get_parsing_strategy_registry
        get_parsing_strategy_registry()
        from app.core.dependencies import get_form_maintenance_service
        await get_form_maintenance_service().ensure_system_forms_exist()

    app.include_router(upload_router, prefix="/api/v2", tags=["upload"])
    app.include_router(filters_router, prefix="/api/v2", tags=["filters"])
    app.include_router(files_router, prefix="/api/v2", tags=["files"])
    app.include_router(forms_router, prefix="/api/v2", tags=["forms"])

    return app


app = create_app()


if __name__ == "__main__":
    launcher = get_launcher(config.APP_ENV)
    launcher.run_checks()
    launcher.print_startup_info()

    uvicorn.run(
        "main:app",
        host=config.API_HOST,
        port=config.API_PORT,
        reload=(config.APP_ENV == "development"),
        log_level="debug" if config.APP_ENV == "development" else "info",
    )
