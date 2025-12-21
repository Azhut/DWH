from fastapi import FastAPI
from config.config import config
from launcher import get_launcher

from app.api.v2.endpoints.upload import router as upload_router
from app.api.v2.endpoints.filters import router as filters_router
from app.api.v2.endpoints.files import router as files_router
from app.api.v2.endpoints.forms import router as forms_router

from starlette.middleware.cors import CORSMiddleware

def create_app() -> FastAPI:
    app = FastAPI(
        title="Document Processing API",
        version="2.0.0",
        debug=(config.APP_ENV == "development")
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
    app.include_router(forms_router, prefix="/api/v2", tags=["forms"])

    return app

app = create_app()

if __name__ == "__main__":
    launcher = get_launcher(config.APP_ENV)
    launcher.run_checks()
    launcher.print_startup_info()


    import uvicorn
    uvicorn.run(
        "main:app",
        host=config.API_HOST,
        port=config.API_PORT,
        reload=(config.APP_ENV == "development"),
        log_level="debug" if config.APP_ENV == "development" else "info"
    )
