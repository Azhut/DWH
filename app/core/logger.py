import logging
from datetime import datetime, timezone


from colorama import Fore, Style, init as colorama_init
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from config import config
from app.domain.log.models import LogEntry
from app.core.exceptions import AppError


# =========================
# Base logger setup
# =========================

colorama_init(autoreset=True)

LEVEL_COLOR_MAP = {
    "DEBUG": Fore.CYAN,
    "INFO": Fore.GREEN,
    "WARNING": Fore.YELLOW,
    "ERROR": Fore.RED,
    "CRITICAL": Fore.MAGENTA,
}


class ColorFormatter(logging.Formatter):
    """
    Цветной formatter для stdout.
    Поддерживает extra:
    - domain
    - meta
    """

    def format(self, record: logging.LogRecord) -> str:
        color = LEVEL_COLOR_MAP.get(record.levelname, "")
        reset = Style.RESET_ALL

        domain = getattr(record, "domain", None)
        file_id = getattr(record, "file_id", None)

        prefix_parts = []
        if domain:
            prefix_parts.append(f"[{domain}]")
        if file_id:
            prefix_parts.append(f"[file_id={file_id}]")

        prefix = " ".join(prefix_parts)
        if prefix:
            record.msg = f"{prefix} {record.msg}"

        formatted = super().format(record)
        return f"{color}{formatted}{reset}"


level = logging.DEBUG if config.DEBUG else logging.INFO

logger = logging.getLogger("sport_api")
logger.setLevel(level)


if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    console_formatter = ColorFormatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_formatter)

    logger.addHandler(console_handler)


# подавляем шум от pymongo
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("pymongo.topology").setLevel(logging.WARNING)
logging.getLogger("pymongo.serverSelection").setLevel(logging.WARNING)
logging.getLogger("pymongo.connection").setLevel(logging.WARNING)


# =========================
# Mongo logging (production)
# =========================

if config.APP_ENV == "production":
    try:
        mongo_client = MongoClient(
            config.MONGO_URI,
            serverSelectionTimeoutMS=5000,
        )
        mongo_client.admin.command("ping")

        logs_collection = mongo_client[config.DATABASE_NAME]["Logs"]

        class MongoHandler(logging.Handler):
            """
            Пишет логи уровня ERROR | CRITICAL в MongoDB
            по схеме LogEntry
            """

            def emit(self, record: logging.LogRecord) -> None:
                if record.levelno < logging.ERROR:
                    return

                try:
                    entry = LogEntry(
                        timestamp=datetime.now(timezone.utc),
                        level=record.levelname,
                        message=record.getMessage(),
                        logger=record.name,
                        pathname=record.pathname,
                        lineno=record.lineno,
                        file_id=getattr(record, "file_id", None),
                        domain=getattr(record, "domain", None),
                    )
                    logs_collection.insert_one(entry.to_mongo_doc())
                except PyMongoError:
                    logging.getLogger("sport_api").exception(
                        "Не удалось записать лог в Mongo"
                    )

        mongo_handler = MongoHandler()
        mongo_handler.setLevel(logging.ERROR)
        logger.addHandler(mongo_handler)

        logger.info("Логирование в Mongo настроено (production).")

    except ExceptionGroup as exc:
        logger.warning(
            "Не удалось подключиться к Mongo для логирования. Логи будут выводиться только в stdout."
        )

def log_app_error(error: AppError, *, exc_info: bool = False) -> None:
    """
    Единая точка логирования AppError / UploadError.
    Передаёт все метаданные из error.meta в extra логгера.
    """
    level_name = error.level.upper()
    log_level = getattr(logging, level_name, logging.ERROR)
    print(log_level)
    extra = {"domain": error.domain, **error.meta}

    logger.log(
        log_level,
        error.message,
        exc_info=exc_info,
        extra=extra,
    )