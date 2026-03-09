import logging

from colorama import Fore, Style, init as colorama_init

from config import config


# =========================
# Base logger setup (stdout only)
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
            k: v
            for k, v in record.__dict__.items()
            if k
            not in [
                "args",
                "msg",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "process",
                "processName",
                "exc_info",
                "exc_text",
                "stack_info",
                "message",
                "name",
            ]
            and not k.startswith("_")
        }

        prefix_parts = []
        if "domain" in extra_fields:
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

