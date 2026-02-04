import logging
from datetime import datetime, timezone


from colorama import Fore, Style, init as colorama_init
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from config import config
from app.domain.log.models import LogEntry


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

        # Извлекаем все поля из extra, кроме служебных
        extra_fields = {
            k: v for k, v in record.__dict__.items()
            if k not in ['args', 'msg', 'levelname', 'levelno', 'pathname', 'filename',
                         'module', 'lineno', 'funcName', 'created', 'msecs', 'relativeCreated',
                         'thread', 'threadName', 'process', 'processName', 'exc_info',
                         'exc_text', 'stack_info', 'message', 'name']
               and not k.startswith('_')
        }


        prefix_parts = []
        if 'domain' in extra_fields:
            prefix_parts.append(f"[{extra_fields.pop('domain')}]")


        if extra_fields:
            meta_str = " ".join(f"{k}={v}" for k, v in extra_fields.items() if v is not None)
            if meta_str:
                prefix_parts.append(f"[{meta_str}]")

        prefix = " ".join(prefix_parts)
        if prefix:
            record.msg = f"{prefix} {record.msg}"

        formatted = super().format(record)
        return f"{color}{formatted}{reset}"


level = logging.DEBUG if config.DEBUG else logging.INFO

logger = logging.getLogger("sport_api")
logger.setLevel(level)



if not logger.handlers and config.DEBUG:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

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
            Пишет все логи в MongoDB по схеме LogEntry.
            Автоматически удаляет старые логи при превышении 1000 записей.
            """

            MAX_LOGS = 1000

            def emit(self, record: logging.LogRecord) -> None:
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
                    self._cleanup_old_logs()
                except PyMongoError:
                    # Ошибку при записи логов в MongoDB выводим только в консоль в DEBUG режиме
                    if config.DEBUG:
                        logging.getLogger("sport_api").exception(
                            "Не удалось записать лог в Mongo"
                        )

            def _cleanup_old_logs(self) -> None:
                """Удаляет старые логи, оставляя только MAX_LOGS последних."""
                try:
                    count = logs_collection.count_documents({})
                    if count > self.MAX_LOGS:
                        # Находим timestamp самого старого лога, который нужно сохранить
                        skip_count = self.MAX_LOGS
                        oldest_to_keep = list(
                            logs_collection.find({}, {"timestamp": 1})
                            .sort("timestamp", -1)
                            .skip(skip_count)
                            .limit(1)
                        )
                        if oldest_to_keep:
                            oldest_timestamp = oldest_to_keep[0]["timestamp"]
                            logs_collection.delete_many({"timestamp": {"$lt": oldest_timestamp}})
                except PyMongoError:
                    if config.DEBUG:
                        logging.getLogger("sport_api").exception(
                            "Ошибка при удалении старых логов из Mongo"
                        )

        mongo_handler = MongoHandler()
        mongo_handler.setLevel(logging.DEBUG)
        logger.addHandler(mongo_handler)

        if config.DEBUG:
            logger.info("Логирование в Mongo настроено (production mode).")

    except Exception as exc:
        if config.DEBUG:
            logger.warning(
                "Не удалось подключиться к Mongo для логирования. "
                "В production режиме логи будут потеряны."
            )

