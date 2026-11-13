import contextvars
import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, Dict

from dotenv import load_dotenv

load_dotenv()

_LOG_CONTEXT: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar(
    "taskflow_log_context",
    default={},
)
_CONTEXT_FIELDS = (
    "worker_id",
    "job_id",
    "attempt_number",
    "event_type",
    "event_id",
    "message_id",
    "client_id",
)
_STANDARD_RECORD_FIELDS = set(logging.makeLogRecord({}).__dict__)

LOG_FILE = os.getenv("LOG_FILE", "taskflow.log")
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_HANDLERS = {
    item.strip().lower()
    for item in os.getenv("LOG_HANDLERS", "console,file").split(",")
    if item.strip()
}
LOG_FORMAT_JSON = os.getenv("LOG_FORMAT_JSON", "false").lower() == "true"
MAX_LOG_SIZE = int(os.getenv("MAX_LOG_SIZE", str(10 * 1024 * 1024)))
BACKUP_COUNT = int(os.getenv("BACKUP_COUNT", "3"))


class Colors:
    RESET = "\033[0m"
    DIM = "\033[2m"
    CYAN = "\033[36m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    MAGENTA = "\033[35m"
    BLUE = "\033[94m"


def _component_name(logger_name: str) -> str:
    if logger_name.startswith("src.workers"):
        return "worker"
    if logger_name.startswith("src.publisher"):
        return "publisher"
    if logger_name.startswith("uvicorn"):
        return "api"
    if logger_name.startswith("src.routers"):
        return "api"
    return logger_name.removeprefix("src.") or "taskflow"


class TaskFlowContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        context = _LOG_CONTEXT.get()
        for field in _CONTEXT_FIELDS:
            if not hasattr(record, field):
                setattr(record, field, context.get(field))
        record.component = _component_name(record.name)
        return True


class TaskFlowFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, timezone.utc).astimezone()
        timestamp_text = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        context = " ".join(
            f"{field}={getattr(record, field)}"
            for field in _CONTEXT_FIELDS
            if getattr(record, field, None) is not None
        )
        details = f" {context}" if context else ""
        line = (
            f"{timestamp_text} {record.levelname:<8} "
            f"[{record.component}] {record.getMessage()}{details}"
        )
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


class ColoredTaskFlowFormatter(TaskFlowFormatter):
    _LEVEL_COLORS = {
        logging.DEBUG: Colors.CYAN,
        logging.INFO: Colors.GREEN,
        logging.WARNING: Colors.YELLOW,
        logging.ERROR: Colors.RED,
        logging.CRITICAL: Colors.MAGENTA,
    }

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, timezone.utc).astimezone()
        timestamp_text = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        level_color = self._LEVEL_COLORS.get(record.levelno, Colors.RESET)
        context = " ".join(
            f"{field}={getattr(record, field)}"
            for field in _CONTEXT_FIELDS
            if getattr(record, field, None) is not None
        )
        details = f" {context}" if context else ""
        line = (
            f"{Colors.DIM}{timestamp_text}{Colors.RESET} "
            f"{level_color}{record.levelname:<8}{Colors.RESET} "
            f"{Colors.BLUE}[{record.component}]{Colors.RESET} "
            f"{record.getMessage()}{details}"
        )
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


class TaskFlowJSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, timezone.utc)
        payload: Dict[str, Any] = {
            "timestamp": timestamp.isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "component": _component_name(record.name),
            "logger": record.name,
            "message": record.getMessage(),
        }

        for field in _CONTEXT_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value

        extra_fields = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _STANDARD_RECORD_FIELDS
            and key not in _CONTEXT_FIELDS
            and key != "component"
            and not key.startswith("_")
        }
        payload.update(extra_fields)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def set_log_context(**fields: Any) -> None:
    context = _LOG_CONTEXT.get().copy()
    context.update(
        {field: value for field, value in fields.items() if field in _CONTEXT_FIELDS}
    )
    _LOG_CONTEXT.set(context)


def clear_log_context() -> None:
    _LOG_CONTEXT.set({})


def _get_log_level() -> int:
    level = logging.getLevelName(LOG_LEVEL)
    if not isinstance(level, int):
        raise ValueError(f"Invalid LOG_LEVEL: {LOG_LEVEL}")
    return level


def _get_formatter() -> logging.Formatter:
    if LOG_FORMAT_JSON:
        return TaskFlowJSONFormatter()
    return TaskFlowFormatter()


def _create_file_handler(formatter: logging.Formatter) -> logging.Handler:
    os.makedirs(LOG_DIR, exist_ok=True)
    handler = RotatingFileHandler(
        os.path.join(LOG_DIR, LOG_FILE),
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setFormatter(formatter)
    handler.addFilter(TaskFlowContextFilter())
    return handler


def configure_root_logger() -> logging.Logger:
    invalid_handlers = LOG_HANDLERS - {"console", "file"}
    if invalid_handlers:
        raise ValueError(
            f"Unsupported LOG_HANDLERS: {', '.join(sorted(invalid_handlers))}"
        )
    if MAX_LOG_SIZE < 1:
        raise ValueError("MAX_LOG_SIZE must be greater than zero")
    if BACKUP_COUNT < 0:
        raise ValueError("BACKUP_COUNT cannot be negative")

    root_logger = logging.getLogger()
    root_logger.setLevel(_get_log_level())

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()
    root_logger.filters.clear()

    formatter = _get_formatter()
    if "file" in LOG_HANDLERS:
        root_logger.addHandler(_create_file_handler(formatter))

    if "console" in LOG_HANDLERS:
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = formatter
        if sys.stdout.isatty() and not LOG_FORMAT_JSON:
            console_formatter = ColoredTaskFlowFormatter()
        console_handler.setFormatter(console_formatter)
        console_handler.addFilter(TaskFlowContextFilter())
        root_logger.addHandler(console_handler)

    return root_logger


def configure_uvicorn_logging() -> bool:
    configure_root_logger()
    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(logger_name)
        logger.propagate = True
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()
    return True
