import json
import logging

import structlog

LoggerType = structlog.stdlib.BoundLogger


class UnstructuredLoggingFormatter(logging.Formatter):
    def format(self, record):
        try:
            data = json.loads(record.getMessage())
        except json.JSONDecodeError:
            data = {"message": record.getMessage()}

        message_parts = [data.pop("level", record.levelname).upper()]
        for k, v in data.items():
            if v is None:
                continue
            if k in ("func_name", "event", "timestamp"):
                message_parts.append(v)
            elif k in ("exception", "error"):
                if isinstance(v, Exception):
                    message_parts.append(f"{v.__class__.__name__}: {v!s}")
                else:
                    message_parts.append(v)
            else:
                message_parts.append(f"{k}={v!r}")

        if record.exc_info:
            message_parts.append(self.formatException(record.exc_info))

        return " | ".join(message_parts)


def setup_logging(
    filename: str | None = None,
    level: int | None = None,
    log_to_file: bool = True,
    log_to_stream: bool = True,
) -> None:
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    handlers = []

    if log_to_stream:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(UnstructuredLoggingFormatter())
        handlers.append(console_handler)

    if log_to_file and filename is not None:
        file_handler = logging.FileHandler(filename)
        file_handler.setFormatter(logging.Formatter("%(message)s"))
        handlers.append(file_handler)

    if not handlers:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(UnstructuredLoggingFormatter())
        handlers.append(console_handler)

    logging.basicConfig(
        level=level or logging.INFO,
        format="%(message)s",
        handlers=handlers,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.format_exc_info,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.filter_by_level,
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
