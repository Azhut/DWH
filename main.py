import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.exception_handlers import http_exception_handler as fastapi_http_exception_handler
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from app.api.v2.endpoints.files import router as files_router
from app.api.v2.endpoints.logs import router as logs_router
from app.api.v2.endpoints.filters import router as filters_router
from app.api.v2.endpoints.forms import router as forms_router
from app.api.v2.endpoints.upload import router as upload_router
from app.application.data.indexes import create_indexes
from app.core.dependencies import get_log_service
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

    @app.exception_handler(HTTPException)
    async def http_exception_logging_handler(request: Request, exc: HTTPException):
        path = request.url.path
        if path.startswith("/api/v2/filters") or path.startswith("/api/v2/files"):
            log_service = get_log_service()
            await log_service.save_log(
                scenario="retrieval",
                message=f"Retrieval error {exc.status_code} on {path}",
                level="error",
                meta={
                    "endpoint": path,
                    "params": dict(request.query_params),
                    "error": exc.detail,
                },
            )
        return await fastapi_http_exception_handler(request, exc)

    @app.exception_handler(Exception)
    async def unhandled_exception_logging_handler(request: Request, exc: Exception):
        log_service = get_log_service()
        await log_service.save_log(
            scenario="connection",
            message=f"Unhandled error on {request.url.path}",
            level="error",
            meta={
                "path": request.url.path,
                "method": request.method,
                "error": repr(exc),
            },
        )
        return JSONResponse(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"},
        )

    app.include_router(upload_router, prefix="/api/v2", tags=["upload"])
    app.include_router(filters_router, prefix="/api/v2", tags=["filters"])
    app.include_router(files_router, prefix="/api/v2", tags=["files"])
    app.include_router(forms_router, prefix="/api/v2", tags=["forms"])
    app.include_router(logs_router, prefix="/api/v2", tags=["logs"])

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
