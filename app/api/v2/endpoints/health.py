from datetime import datetime

from fastapi import APIRouter

from app.core.database import mongo_connection
from config.config import config

router = APIRouter()


@router.get("/health")
async def health_check():
    """
    Простой healthcheck для Docker.
    Проверяет MongoDB и (при FLATDATA_STORAGE=duckdb) файл DuckDB.
    """
    mongo_ok = False
    duckdb_ok = None
    errors: list[str] = []

    try:
        db = mongo_connection.get_database()
        await db.command("ping")
        mongo_ok = True
    except Exception as e:
        errors.append(f"mongo: {e}")

    if (config.FLATDATA_STORAGE or "mongo").lower() == "duckdb":
        from app.core.duckdb_database import duckdb_connection

        duckdb_ok = await duckdb_connection.ping()
        if not duckdb_ok:
            errors.append("duckdb: ping failed")

    if mongo_ok and (duckdb_ok is None or duckdb_ok):
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "database": "connected",
            "flatdata_storage": config.FLATDATA_STORAGE,
            "duckdb": duckdb_ok,
        }

    return {
        "status": "degraded",
        "timestamp": datetime.utcnow().isoformat(),
        "database": "connected" if mongo_ok else "disconnected",
        "flatdata_storage": config.FLATDATA_STORAGE,
        "duckdb": duckdb_ok,
        "error": "; ".join(errors) if errors else None,
    }
